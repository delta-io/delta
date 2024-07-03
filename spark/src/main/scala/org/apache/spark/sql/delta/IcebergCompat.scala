/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.internal.MDC
import org.apache.spark.sql.types._

/**
 * Utils to validate the IcebergCompatV1 table feature, which is responsible for keeping Delta
 * tables in valid states (see the Delta spec for full invariants, dependencies, and requirements)
 * so that they are capable of having Delta to Iceberg metadata conversion applied to them. The
 * IcebergCompatV1 table feature does not implement, specify, or control the actual metadata
 * conversion; that is handled by the Delta UniForm feature.
 *
 * Note that UniForm (Iceberg) depends on IcebergCompatV1, but IcebergCompatV1 does not depend on or
 * require UniForm (Iceberg). It is perfectly valid for a Delta table to have IcebergCompatV1
 * enabled but UniForm (Iceberg) not enabled.
 */

object IcebergCompatV1 extends IcebergCompat(
  version = 1,
  config = DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED,
  requiredTableFeatures = Seq(ColumnMappingTableFeature),
  requiredTableProperties = Seq(RequireColumnMapping),
  checks = Seq(
    CheckOnlySingleVersionEnabled,
    CheckAddFileHasStats,
    CheckNoPartitionEvolution,
    CheckNoListMapNullType,
    CheckNoDeletionVector,
    CheckVersionChangeNeedsRewrite)
)

object IcebergCompatV2 extends IcebergCompat(
  version = 2,
  config = DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED,
  requiredTableFeatures = Seq(ColumnMappingTableFeature),
  requiredTableProperties = Seq(RequireColumnMapping),
  checks = Seq(
    CheckOnlySingleVersionEnabled,
    CheckAddFileHasStats,
    CheckTypeInV2AllowList,
    CheckNoPartitionEvolution,
    CheckNoDeletionVector,
    CheckVersionChangeNeedsRewrite)
)

/**
 * All IcebergCompatVx should extend from this base class
 *
 * @param version the compat version number
 * @param config  the DeltaConfig for this IcebergCompat version
 * @param requiredTableFeatures a list of table features it relies on
 * @param requiredTableProperties a list of table properties it relies on.
 *                                See [[RequiredDeltaTableProperty]]
 * @param checks  a list of checks this IcebergCompatVx will perform.
 *                @see [[RequiredDeltaTableProperty]]
 */
case class IcebergCompat(
    version: Integer,
    config: DeltaConfig[Option[Boolean]],
    requiredTableFeatures: Seq[TableFeature],
    requiredTableProperties: Seq[RequiredDeltaTableProperty[_<:Any]],
    checks: Seq[IcebergCompatCheck]) extends DeltaLogging {
  def isEnabled(metadata: Metadata): Boolean = config.fromMetaData(metadata).getOrElse(false)

  /**
   * Expected to be called after the newest metadata and protocol have been ~ finalized.
   *
   * Furthermore, this should be called *after*
   * [[UniversalFormat.enforceIcebergInvariantsAndDependencies]].
   *
   * If you are enabling IcebergCompatV1 and are creating a new table, this method will
   * automatically upgrade the table protocol to support ColumnMapping and set it to 'name' mode,
   * too.
   *
   * If you are disabling IcebergCompatV1, this method will also disable Universal Format (Iceberg),
   * if it is enabled.
   *
   * @param actions The actions to be committed in the txn. We will only look at the [[AddFile]]s.
   *
   * @return tuple of options of (updatedProtocol, updatedMetadata). For either action, if no
   *         updates need to be applied, will return None.
   */
  def enforceInvariantsAndDependencies(
      prevSnapshot: Snapshot,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      isCreatingOrReorgTable: Boolean,
      actions: Seq[Action]): (Option[Protocol], Option[Metadata]) = {
    val prevProtocol = prevSnapshot.protocol
    val prevMetadata = prevSnapshot.metadata
    val wasEnabled = this.isEnabled(prevMetadata)
    val isEnabled = this.isEnabled(newestMetadata)
    val tableId = newestMetadata.id

    (wasEnabled, isEnabled) match {
      case (_, false) => (None, None) // not enable or disabling, Ignore
      case (_, true) => // Enabling now or already-enabled
        val tblFeatureUpdates = scala.collection.mutable.Set.empty[TableFeature]
        val tblPropertyUpdates = scala.collection.mutable.Map.empty[String, String]

        // Check we have all required table features
        requiredTableFeatures.foreach { f =>
          (prevProtocol.isFeatureSupported(f), newestProtocol.isFeatureSupported(f)) match {
            case (_, true) => // all good
            case (false, false) => // txn has not supported it!
              // Note: this code path should be impossible, since the IcebergCompatVxTableFeature
              //       specifies ColumnMappingTableFeature as a required table feature. Thus,
              //       it should already have been added during
              //       OptimisticTransaction::updateMetadataInternal
              if (isCreatingOrReorgTable) {
                tblFeatureUpdates += f
              } else {
                throw DeltaErrors.icebergCompatMissingRequiredTableFeatureException(version, f)
              }
            case (true, false) => // txn is removing/un-supporting it!
              throw DeltaErrors.icebergCompatDisablingRequiredTableFeatureException(version, f)
          }
        }

        // Check we have all required delta table properties
        requiredTableProperties.foreach {
          case RequiredDeltaTableProperty(deltaConfig, validator, autoSetValue) =>
            val newestValue = deltaConfig.fromMetaData(newestMetadata)
            val newestValueOkay = validator(newestValue)
            val newestValueExplicitlySet = newestMetadata.configuration.contains(deltaConfig.key)

            val err = DeltaErrors.icebergCompatWrongRequiredTablePropertyException(
              version, deltaConfig.key, newestValue.toString, autoSetValue)

            if (!newestValueOkay) {
              if (!newestValueExplicitlySet && isCreatingOrReorgTable) {
                // This case covers both CREATE and REPLACE TABLE commands that
                // did not explicitly specify the required deltaConfig. In these
                // cases, we set the property automatically.
                tblPropertyUpdates += deltaConfig.key -> autoSetValue
              } else {
                // In all other cases, if the property value is not compatible
                // with the IcebergV1 requirements, we fail
                throw err
              }
            }
        }

        // Update Protocol and Metadata if necessary
        val protocolResult = if (tblFeatureUpdates.nonEmpty) {
          logInfo(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, tableId)}] " +
            log"IcebergCompatV1 auto-supporting table features: " +
            log"${MDC(DeltaLogKeys.TABLE_FEATURES, tblFeatureUpdates.map(_.name))}")
          Some(newestProtocol.merge(tblFeatureUpdates.map(Protocol.forTableFeature).toSeq: _*))
        } else None

        val metadataResult = if (tblPropertyUpdates.nonEmpty) {
          logInfo(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, tableId)}] " +
            log"IcebergCompatV1 auto-setting table properties: " +
            log"${MDC(DeltaLogKeys.TBL_PROPERTIES, tblPropertyUpdates)}")
          val newConfiguration = newestMetadata.configuration ++ tblPropertyUpdates.toMap
          var tmpNewMetadata = newestMetadata.copy(configuration = newConfiguration)

          requiredTableProperties.foreach { tp =>
            tmpNewMetadata = tp.postProcess(prevMetadata, tmpNewMetadata, isCreatingOrReorgTable)
          }

          Some(tmpNewMetadata)
        } else None

        // Apply additional checks
        val context = IcebergCompatContext(prevSnapshot,
          protocolResult.getOrElse(newestProtocol),
          metadataResult.getOrElse(newestMetadata),
          isCreatingOrReorgTable, actions, tableId, version)
        checks.foreach(_.apply(context))

        (protocolResult, metadataResult)
    }
  }
}

/**
 * Util methods to manage between IcebergCompat versions
 */
object IcebergCompat extends DeltaLogging {

  val knownVersions = Seq(
    DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED -> 1,
    DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED -> 2)

  /**
   * Fetch from Metadata the current enabled IcebergCompat version.
   * @return a number indicate the version. E.g., 1 for CompatV1.
   *         None if no version enabled.
   */
  def getEnabledVersion(metadata: Metadata): Option[Int] =
    knownVersions
      .find{ case (config, _) => config.fromMetaData(metadata).getOrElse(false) }
      .map{ case (_, version) => version }

  /**
   * Get the DeltaConfig for the given IcebergCompat version. If version is not valid,
   * throw an exception.
   * @return the DeltaConfig for the given version. E.g.,
   *         [[DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED]] for version 1.
   */
  def getIcebergCompatVersionConfigForValidVersion(version: Int): DeltaConfig[Option[Boolean]] = {
    if (version <= 0 || version > knownVersions.length) {
      throw DeltaErrors.icebergCompatVersionNotSupportedException(
        version, knownVersions.length
      )
    }
    knownVersions(version - 1)._1
  }

  /**
   * @return true if any version of IcebergCompat is enabled
   */
  def isAnyEnabled(metadata: Metadata): Boolean =
    knownVersions.exists{ case (config, _) => config.fromMetaData(metadata).getOrElse(false) }

  /**
   * @return true if the target version is enabled on the table.
   */
  def isVersionEnabled(metadata: Metadata, version: Integer): Boolean =
    knownVersions.exists{ case (_, v) => v == version }
}

/**
 * Wrapper class for table property validation
 *
 * @param deltaConfig [[DeltaConfig]] we are checking
 * @param validator A generic method to validate the given value
 * @param autoSetValue The value to set if we can auto-set this value (e.g. during table creation)
 */
case class RequiredDeltaTableProperty[T](
    deltaConfig: DeltaConfig[T],
    validator: T => Boolean,
    autoSetValue: String) {
  /**
   * A callback after all required properties are added to the new metadata.
   * @return Updated metadata. None if no change
   */
  def postProcess(
      prevMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean) : Metadata = newMetadata
}

object RequireColumnMapping extends RequiredDeltaTableProperty(
    deltaConfig = DeltaConfigs.COLUMN_MAPPING_MODE,
    validator = (mode: DeltaColumnMappingMode) => (mode == NameMapping || mode == IdMapping),
    autoSetValue = NameMapping.name) {
  override def postProcess(
      prevMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {
    if (newMetadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key)) {
      assert(isCreatingNewTable, "we only auto-upgrade Column Mapping on new tables")
      val tmpNewMetadata = DeltaColumnMapping.assignColumnIdAndPhysicalName(
        newMetadata = newMetadata,
        oldMetadata = prevMetadata,
        isChangingModeOnExistingTable = false,
        isOverwritingSchema = false
      )
      DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(tmpNewMetadata)
      tmpNewMetadata
    } else {
      newMetadata
    }
  }
}

case class IcebergCompatContext(
    prevSnapshot: Snapshot,
    newestProtocol: Protocol,
    newestMetadata: Metadata,
    isCreatingOrReorgTable: Boolean,
    actions: Seq[Action],
    tableId: String,
    version: Integer) {
  def prevMetadata: Metadata = prevSnapshot.metadata

  def prevProtocol: Protocol = prevSnapshot.protocol
}

trait IcebergCompatCheck extends (IcebergCompatContext => Unit)

/**
 * Checks that ensures no more than one IcebergCompatVx is enabled.
 */
object CheckOnlySingleVersionEnabled extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    val numEnabled = IcebergCompat.knownVersions
      .map{ case (config, _) =>
        if (config.fromMetaData(context.newestMetadata).getOrElse(false)) 1 else 0 }
      .sum
    if (numEnabled > 1) {
      throw DeltaErrors.icebergCompatVersionMutualExclusive(context.version)
    }
  }
}

object CheckAddFileHasStats extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    // If this field is empty, then the AddFile is missing the `numRecords` statistic.
    context.actions.collect { case a: AddFile if a.numLogicalRecords.isEmpty =>
      throw new UnsupportedOperationException(s"[tableId=${context.tableId}] " +
        s"IcebergCompatV${context.version} requires all AddFiles to contain " +
        s"the numRecords statistic. AddFile ${a.path} is missing this statistic. " +
        s"Stats: ${a.stats}")
    }
  }
}

object CheckNoPartitionEvolution extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    // Note: Delta doesn't support partition evolution, but you can change the partitionColumns
    // by doing a REPLACE or DataFrame overwrite.
    //
    // Iceberg-Spark itself *doesn't* support the following cases
    // - CREATE TABLE partitioned by colA; REPLACE TABLE partitioned by colB
    // - CREATE TABLE partitioned by colA; REPLACE TABLE not partitioned
    //
    // While Iceberg-Spark *does* support
    // - CREATE TABLE not partitioned; REPLACE TABLE not partitioned
    // - CREATE TABLE not partitioned; REPLACE TABLE partitioned by colA
    // - CREATE TABLE partitioned by colA dataType1; REPLACE TABLE partitioned by colA dataType2
    if (context.prevMetadata.partitionColumns.nonEmpty &&
      context.prevMetadata.partitionColumns != context.newestMetadata.partitionColumns) {
      throw DeltaErrors.icebergCompatReplacePartitionedTableException(
        context.version,
        context.prevMetadata.partitionColumns,
        context.newestMetadata.partitionColumns)
    }
  }
}

object CheckNoListMapNullType extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    SchemaUtils.findAnyTypeRecursively(context.newestMetadata.schema) { f =>
      f.isInstanceOf[MapType] || f.isInstanceOf[ArrayType] || f.isInstanceOf[NullType]
    } match {
      case Some(unsupportedType) =>
        throw DeltaErrors.icebergCompatUnsupportedDataTypeException(
          context.version, unsupportedType, context.newestMetadata.schema)
      case _ =>
    }
  }
}

object CheckTypeInV2AllowList extends IcebergCompatCheck {
  private val allowTypes = Set[Class[_]] (
    ByteType.getClass, ShortType.getClass, IntegerType.getClass, LongType.getClass,
    FloatType.getClass, DoubleType.getClass, classOf[DecimalType],
    StringType.getClass, BinaryType.getClass,
    BooleanType.getClass,
    TimestampType.getClass, TimestampNTZType.getClass, DateType.getClass,
    classOf[ArrayType], classOf[MapType], classOf[StructType]
  )
  override def apply(context: IcebergCompatContext): Unit = {
    SchemaUtils
      .findAnyTypeRecursively(context.newestMetadata.schema)(t => !allowTypes.contains(t.getClass))
    match {
      case Some(unsupportedType) =>
        throw DeltaErrors.icebergCompatUnsupportedDataTypeException(
          context.version, unsupportedType, context.newestMetadata.schema)
      case _ =>
    }
  }
}

object CheckNoDeletionVector extends IcebergCompatCheck {

  override def apply(context: IcebergCompatContext): Unit = {
    // Check for incompatible table features;
    // Deletion Vectors cannot be writeable; Note that concurrent txns are also covered
    // to NOT write deletion vectors as that txn would need to make DVs writable, which
    // would conflict with current txn because of metadata change.
    if (DeletionVectorUtils.deletionVectorsWritable(
      context.newestProtocol, context.newestMetadata)) {
      throw DeltaErrors.icebergCompatDeletionVectorsShouldBeDisabledException(context.version)
    }
  }
}


/**
 * Check if change IcebergCompat version needs a REORG operation
 */
object CheckVersionChangeNeedsRewrite extends IcebergCompatCheck {

  private val versionChangesWithoutRewrite: Map[Int, Set[Int]] =
    Map(0 -> Set(0, 1), 1 -> Set(0, 1), 2 -> Set(0, 1, 2))
  override def apply(context: IcebergCompatContext): Unit = {
    if (!context.isCreatingOrReorgTable) {
      val oldVersion = IcebergCompat.getEnabledVersion(context.prevMetadata).getOrElse(0)
      val allowedChanges = versionChangesWithoutRewrite.getOrElse(oldVersion, Set.empty[Int])
      if (!allowedChanges.contains(context.version)) {
          throw DeltaErrors.icebergCompatChangeVersionNeedRewrite(oldVersion, context.version)
      }
    }
  }
}
