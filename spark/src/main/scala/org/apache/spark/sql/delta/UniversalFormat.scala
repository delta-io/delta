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

import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.{ArrayType, MapType, NullType}

/**
 * Utils to validate the Universal Format (UniForm) Delta feature (NOT a table feature).
 *
 * The UniForm Delta feature governs and implements the actual conversion of Delta metadata into
 * other formats.
 *
 * Currently, UniForm only supports Iceberg. When `delta.universalFormat.enabledFormats` contains
 * "iceberg", we say that Universal Format (Iceberg) is enabled.
 *
 * [[enforceInvariantsAndDependencies]] ensures that all of UniForm's requirements for the
 * specified format are met (e.g. for 'iceberg' that IcebergCompatV1 or V2 is enabled).
 * It doesn't verify that its nested requirements are met (e.g. IcebergCompat's requirements,
 * like Column Mapping). That is the responsibility of format-specific validations such as
 * [[IcebergCompatV1.enforceInvariantsAndDependencies]]
 * and [[IcebergCompatV2.enforceInvariantsAndDependencies]].
 *
 *
 * Note that UniForm (Iceberg) depends on IcebergCompat, but IcebergCompat does not
 * depend on or require UniForm (Iceberg). It is perfectly valid for a Delta table to have
 * IcebergCompatV1 or V2 enabled but UniForm (Iceberg) not enabled.
 */
object UniversalFormat extends DeltaLogging {

  val ICEBERG_FORMAT = "iceberg"
  val HUDI_FORMAT = "hudi"
  val SUPPORTED_FORMATS = Set(HUDI_FORMAT, ICEBERG_FORMAT)

  def icebergEnabled(metadata: Metadata): Boolean = {
    DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetaData(metadata).contains(ICEBERG_FORMAT)
  }

  def hudiEnabled(metadata: Metadata): Boolean = {
    DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetaData(metadata).contains(HUDI_FORMAT)
  }

  def hudiEnabled(properties: Map[String, String]): Boolean = {
    properties.get(DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key)
      .exists(value => value.contains(HUDI_FORMAT))
  }

  def icebergEnabled(properties: Map[String, String]): Boolean = {
    properties.get(DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key)
      .exists(value => value.contains(ICEBERG_FORMAT))
  }

  /**
   * Expected to be called after the newest metadata and protocol have been ~ finalized.
   *
   * @return tuple of options of (updatedProtocol, updatedMetadata). For either action, if no
   *         updates need to be applied, will return None.
   */
  def enforceInvariantsAndDependencies(
      snapshot: Snapshot,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      isCreatingOrReorgTable: Boolean,
      actions: Seq[Action]): (Option[Protocol], Option[Metadata]) = {
    enforceHudiDependencies(newestMetadata, snapshot)
    enforceIcebergInvariantsAndDependencies(
      snapshot, newestProtocol, newestMetadata, isCreatingOrReorgTable, actions)
  }

  /**
   * If you are enabling Hudi, this method ensures that Deletion Vectors are not enabled. New
   * conditions may be added here in the future to make sure the source is compatible with Hudi.
   * @param newestMetadata the newest metadata
   * @param snapshot current snapshot
   * @return N/A, throws exception if condition is not met
   */
  def enforceHudiDependencies(newestMetadata: Metadata, snapshot: Snapshot): Any = {
    if (hudiEnabled(newestMetadata)) {
      if (DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(newestMetadata)) {
        throw DeltaErrors.uniFormHudiDeleteVectorCompat()
      }
      // TODO: remove once map/list support is added https://github.com/delta-io/delta/issues/2738
      SchemaUtils.findAnyTypeRecursively(newestMetadata.schema) { f =>
        f.isInstanceOf[MapType] || f.isInstanceOf[ArrayType] || f.isInstanceOf[NullType]
      } match {
        case Some(unsupportedType) =>
          throw DeltaErrors.uniFormHudiSchemaCompat(unsupportedType)
        case _ =>
      }
    }
  }

  /**
   * If you are enabling Universal Format (Iceberg), this method ensures that at least one of
   * IcebergCompat is enabled. If you are disabling Universal Format (Iceberg), this method
   * will leave the current IcebergCompat version untouched.
   *
   * @return tuple of options of (updatedProtocol, updatedMetadata). For either action, if no
   *         updates need to be applied, will return None.
   */
  def enforceIcebergInvariantsAndDependencies(
      snapshot: Snapshot,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      isCreatingOrReorg: Boolean,
      actions: Seq[Action]): (Option[Protocol], Option[Metadata]) = {

    val prevMetadata = snapshot.metadata
    val uniformIcebergWasEnabled = UniversalFormat.icebergEnabled(prevMetadata)
    val uniformIcebergIsEnabled = UniversalFormat.icebergEnabled(newestMetadata)
    val tableId = newestMetadata.id
    var changed = false

    val (uniformProtocol, uniformMetadata) =
      (uniformIcebergWasEnabled, uniformIcebergIsEnabled) match {
        case (_, false) => (None, None) // Ignore
        case (_, true) => // Enabling now or already-enabled
          val icebergCompatWasEnabled = IcebergCompat.isAnyEnabled(prevMetadata)
          val icebergCompatIsEnabled = IcebergCompat.isAnyEnabled(newestMetadata)

          if (icebergCompatIsEnabled) {
            (None, None)
          } else if (icebergCompatWasEnabled) {
            // IcebergCompat is being disabled. We need to also disable Universal Format (Iceberg)
            val remainingSupportedFormats = DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS
              .fromMetaData(newestMetadata)
              .filterNot(_ == UniversalFormat.ICEBERG_FORMAT)

            val newConfiguration = if (remainingSupportedFormats.isEmpty) {
              newestMetadata.configuration - DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key
            } else {
              newestMetadata.configuration ++
                Map(DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key ->
                  remainingSupportedFormats.mkString(","))
            }

            logInfo(log"[${MDC(DeltaLogKeys.TABLE_ID, tableId)}] " +
              log"IcebergCompat is being disabled. Auto-disabling Universal Format (Iceberg), too.")

            (None, Some(newestMetadata.copy(configuration = newConfiguration)))
          } else {
            throw DeltaErrors.uniFormIcebergRequiresIcebergCompat()
          }
      }

    var protocolToCheck = uniformProtocol.getOrElse(newestProtocol)
    var metadataToCheck = uniformMetadata.getOrElse(newestMetadata)
    changed = uniformProtocol.nonEmpty || uniformMetadata.nonEmpty

    val (v1protocolUpdate, v1metadataUpdate) = IcebergCompatV1.enforceInvariantsAndDependencies(
      snapshot,
      newestProtocol = protocolToCheck,
      newestMetadata = metadataToCheck,
      isCreatingOrReorg,
      actions
    )
    protocolToCheck = v1protocolUpdate.getOrElse(protocolToCheck)
    metadataToCheck = v1metadataUpdate.getOrElse(metadataToCheck)
    changed ||= v1protocolUpdate.nonEmpty || v1metadataUpdate.nonEmpty

    val (v2protocolUpdate, v2metadataUpdate) = IcebergCompatV2.enforceInvariantsAndDependencies(
      snapshot,
      newestProtocol = protocolToCheck,
      newestMetadata = metadataToCheck,
      isCreatingOrReorg,
      actions
    )
    changed ||= v2protocolUpdate.nonEmpty || v2metadataUpdate.nonEmpty

    if (changed) {
      (
        v2protocolUpdate.orElse(Some(protocolToCheck)),
        v2metadataUpdate.orElse(Some(metadataToCheck))
      )
    } else {
      (None, None)
    }
  }

  /**
   * This method is used to build UniForm metadata dependencies closure.
   * It checks configuration conflicts and adds missing properties.
   * It will call [[enforceIcebergInvariantsAndDependencies]] to perform the actual check.
   * @param configuration the original metadata configuration.
   * @return updated configuration if any changes are required,
   *         otherwise the original configuration.
   */
  def enforceDependenciesInConfiguration(
      configuration: Map[String, String],
      snapshot: Snapshot): Map[String, String] = {
    var metadata = snapshot.metadata.copy(configuration = configuration)

    // Check UniversalFormat related property dependencies
    val (_, universalMetadata) = UniversalFormat.enforceInvariantsAndDependencies(
      snapshot,
      newestProtocol = snapshot.protocol,
      newestMetadata = metadata,
      isCreatingOrReorgTable = true,
      actions = Seq()
    )

    universalMetadata match {
      case Some(valid) => valid.configuration
      case _ => configuration
    }
  }

  val ICEBERG_TABLE_TYPE_KEY = "table_type"

  /**
   * Update CatalogTable to mark it readable by other table readers (iceberg for now).
   * This method ensures 'table_type' = 'ICEBERG' when uniform is enabled,
   * and ensure table_type is not 'ICEBERG' when uniform is not enabled
   * If the key has other values than 'ICEBERG', this method will not touch it for compatibility
   *
   * @param table    catalogTable before change
   * @param metadata snapshot metadata
   * @return the converted catalog, or None if no change is made
   */
  def enforceSupportInCatalog(table: CatalogTable, metadata: Metadata): Option[CatalogTable] = {
    val icebergInCatalog = table.properties.get(ICEBERG_TABLE_TYPE_KEY) match {
      case Some(value) => value.equalsIgnoreCase(ICEBERG_FORMAT)
      case _ => false
    }

    (icebergEnabled(metadata), icebergInCatalog) match {
      case (true, false) =>
        Some(table.copy(properties = table.properties
          + (ICEBERG_TABLE_TYPE_KEY -> ICEBERG_FORMAT)))
      case (false, true) =>
        Some(table.copy(properties =
          table.properties - ICEBERG_TABLE_TYPE_KEY))
      case _ => None
    }
  }
}
/** Class to facilitate the conversion of Delta into other table formats. */
abstract class UniversalFormatConverter(spark: SparkSession) {
  /**
   * Perform an asynchronous conversion.
   *
   * This will start an async job to run the conversion, unless there already is an async conversion
   * running for this table. In that case, it will queue up the provided snapshot to be run after
   * the existing job completes.
   */
  def enqueueSnapshotForConversion(
    snapshotToConvert: Snapshot,
    txn: OptimisticTransactionImpl): Unit

  /**
   * Perform a blocking conversion when performing an OptimisticTransaction
   * on a delta table.
   *
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txn the transaction that triggers the conversion. Used as a hint to
   *            avoid recomputing old metadata. It must contain the catalogTable
   *            this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  def convertSnapshot(
    snapshotToConvert: Snapshot, txn: OptimisticTransactionImpl): Option[(Long, Long)]

  /**
   * Perform a blocking conversion for the given catalogTable
   *
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param catalogTable the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  def convertSnapshot(
      snapshotToConvert: Snapshot, catalogTable: CatalogTable): Option[(Long, Long)]

  /**
   * Fetch the delta version corresponding to the latest conversion.
   * @param snapshot the snapshot to be converted
   * @param table the catalogTable with info of previous conversions
   * @return None if no previous conversion found
   */
  def loadLastDeltaVersionConverted(snapshot: Snapshot, table: CatalogTable): Option[Long]
}
