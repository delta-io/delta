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
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.sql.types.{ArrayType, MapType, NullType}

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
object IcebergCompatV1 extends DeltaLogging {

  val REQUIRED_TABLE_FEATURES = Seq(ColumnMappingTableFeature)

  val INCOMPATIBLE_TABLE_FEATURES = Seq(DeletionVectorsTableFeature)

  val REQUIRED_DELTA_TABLE_PROPERTIES = Seq(
    RequiredDeltaTableProperty(
      deltaConfig = DeltaConfigs.COLUMN_MAPPING_MODE,
      validator = (mode: DeltaColumnMappingMode) => (mode == NameMapping || mode == IdMapping),
      autoSetValue = NameMapping.name
    )
  )

  def isEnabled(metadata: Metadata): Boolean = {
    DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.fromMetaData(metadata).getOrElse(false)
  }

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
      prevProtocol: Protocol,
      prevMetadata: Metadata,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      isCreatingNewTable: Boolean,
      actions: Seq[Action]): (Option[Protocol], Option[Metadata]) = {
    val wasEnabled = IcebergCompatV1.isEnabled(prevMetadata)
    val isEnabled = IcebergCompatV1.isEnabled(newestMetadata)
    val tableId = newestMetadata.id

    (wasEnabled, isEnabled) match {
      case (false, false) => (None, None) // Ignore
      case (true, false) => // Disabling
        // UniversalFormat.validateIceberg should detect that IcebergCompatV1 is being disabled,
        // and automatically disable Universal Format (Iceberg)
        assert(!UniversalFormat.icebergEnabled(newestMetadata))
        (None, None)
      case (_, true) => // Enabling now or already-enabled
        val tblFeatureUpdates = scala.collection.mutable.Set.empty[TableFeature]
        val tblPropertyUpdates = scala.collection.mutable.Map.empty[String, String]

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
        if (prevMetadata.partitionColumns.nonEmpty &&
          prevMetadata.partitionColumns != newestMetadata.partitionColumns) {
          throw DeltaErrors.icebergCompatV1ReplacePartitionedTableException(
            prevMetadata.partitionColumns, newestMetadata.partitionColumns)
        }

        if (SchemaUtils.typeExistsRecursively(newestMetadata.schema) { f =>
          f.isInstanceOf[MapType] || f.isInstanceOf[ArrayType] || f.isInstanceOf[NullType]
        }) {
          throw DeltaErrors.icebergCompatV1UnsupportedDataTypeException(newestMetadata.schema)
        }

        // If this field is empty, then the AddFile is missing the `numRecords` statistic.
        actions.collect { case a: AddFile if a.numLogicalRecords.isEmpty =>
          throw new UnsupportedOperationException(s"[tableId=$tableId] IcebergCompatV1 requires " +
            s"all AddFiles to contain the numRecords statistic. AddFile ${a.path} is missing " +
            s"this statistic. Stats: ${a.stats}")
        }

        // Check we have all required table features
        REQUIRED_TABLE_FEATURES.foreach { f =>
          (prevProtocol.isFeatureSupported(f), newestProtocol.isFeatureSupported(f)) match {
            case (_, true) => // all good
            case (false, false) => // txn has not supported it!
              // Note: this code path should be impossible, since the IcebergCompatV1TableFeature
              //       specifies ColumnMappingTableFeature as a required table feature. Thus,
              //       it should already have been added during
              //       OptimisticTransaction::updateMetadataInternal
              if (isCreatingNewTable) {
                tblFeatureUpdates += f
              } else {
                throw DeltaErrors.icebergCompatV1MissingRequiredTableFeatureException(f)
              }
            case (true, false) => // txn is removing/un-supporting it!
              // Note: currently it is impossible to remove/un-support a table feature
              throw DeltaErrors.icebergCompatV1DisablingRequiredTableFeatureException(f)
          }
        }

        // Check we haven't added any incompatible table features
        INCOMPATIBLE_TABLE_FEATURES.foreach { f =>
          if (newestProtocol.isFeatureSupported(f)) {
            throw DeltaErrors.icebergCompatV1IncompatibleTableFeatureException(f)
          }
        }

        // Check we have all required delta table properties
        REQUIRED_DELTA_TABLE_PROPERTIES.foreach {
          case RequiredDeltaTableProperty(deltaConfig, validator, autoSetValue) =>
            val newestValue = deltaConfig.fromMetaData(newestMetadata)
            val newestValueOkay = validator(newestValue)
            val newestValueExplicitlySet = newestMetadata.configuration.contains(deltaConfig.key)

            val err = DeltaErrors.icebergCompatV1WrongRequiredTablePropertyException(
              deltaConfig.key, newestValue.toString, autoSetValue)

            if (!newestValueOkay) {
              if (!newestValueExplicitlySet && isCreatingNewTable) {
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

        val protocolResult = if (tblFeatureUpdates.nonEmpty) {
          logInfo(s"[tableId=$tableId] IcebergCompatV1 auto-supporting table features: " +
            s"${tblFeatureUpdates.map(_.name)}")
          Some(newestProtocol.merge(tblFeatureUpdates.map(Protocol.forTableFeature).toSeq: _*))
        } else None

        val metadataResult = if (tblPropertyUpdates.nonEmpty) {
          logInfo(s"[tableId=$tableId] IcebergCompatV1 auto-setting table properties: " +
            s"$tblPropertyUpdates")
          val newConfiguration = newestMetadata.configuration ++ tblPropertyUpdates.toMap
          var tmpNewMetadata = newestMetadata.copy(configuration = newConfiguration)

          if (tblPropertyUpdates.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key)) {
            assert(isCreatingNewTable, "we only auto-upgrade Column Mapping on new tables")
            tmpNewMetadata = DeltaColumnMapping.assignColumnIdAndPhysicalName(
              newMetadata = tmpNewMetadata,
              oldMetadata = prevMetadata,
              isChangingModeOnExistingTable = false,
              isOverwritingSchema = false
            )
            DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(tmpNewMetadata)
          }

          Some(tmpNewMetadata)
        } else None

        (protocolResult, metadataResult)
    }
  }
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
    autoSetValue: String)
