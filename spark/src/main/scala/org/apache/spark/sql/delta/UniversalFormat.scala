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
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Utils to validate the Universal Format (UniForm) Delta feature (NOT a table feature).
 *
 * The UniForm Delta feature governs and implements the actual conversion of Delta metadata into
 * other formats.
 *
 * Currently, UniForm only supports Iceberg. When `delta.universalFormat.enabledFormats` contains
 * "iceberg", we say that Universal Format (Iceberg) is enabled.
 *
 * [[enforceIcebergInvariantsAndDependencies]] ensures that all of UniForm (Iceberg)'s requirements
 * are met (i.e. that IcebergCompatV1 is enabled). It doesn't verify that its nested requirements
 * are met (i.e. IcebergCompatV1's requirements, like Column Mapping). That is the responsibility of
 * [[IcebergCompatV1.enforceInvariantsAndDependencies]].
 *
 *
 * Note that UniForm (Iceberg) depends on IcebergCompatV1, but IcebergCompatV1 does not depend on or
 * require UniForm (Iceberg). It is perfectly valid for a Delta table to have IcebergCompatV1
 * enabled but UniForm (Iceberg) not enabled.
 */
object UniversalFormat extends DeltaLogging {

  val ICEBERG_FORMAT = "iceberg"
  val SUPPORTED_FORMATS = Set(ICEBERG_FORMAT)

  def icebergEnabled(metadata: Metadata): Boolean = {
    DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetaData(metadata).contains(ICEBERG_FORMAT)
  }

  def icebergEnabled(properties: Map[String, String]): Boolean = {
    properties.get(DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key)
      .exists(value => value.contains(ICEBERG_FORMAT))
  }

  /**
   * Expected to be called after the newest metadata and protocol have been ~ finalized.
   *
   * Furthermore, this should be called *before*
   * [[IcebergCompatV1.enforceInvariantsAndDependencies]].
   *
   * If you are enabling Universal Format (Iceberg), this method ensures that IcebergCompatV1 is
   * supported and enabled. If this is a new table, IcebergCompatV1 will be automatically enabled.
   *
   * If you are disabling Universal Format (Iceberg), this method ensures that IcebergCompatV1 is
   * disabled. It may still be supported, however.
   *
   * @return tuple of options of (updatedProtocol, updatedMetadata). For either action, if no
   *         updates need to be applied, will return None.
   */
  def enforceIcebergInvariantsAndDependencies(
      prevProtocol: Protocol,
      prevMetadata: Metadata,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      isCreatingNewTable: Boolean): (Option[Protocol], Option[Metadata]) = {
    val uniformIcebergWasEnabled = UniversalFormat.icebergEnabled(prevMetadata)
    val uniformIcebergIsEnabled = UniversalFormat.icebergEnabled(newestMetadata)
    val tableId = newestMetadata.id

    (uniformIcebergWasEnabled, uniformIcebergIsEnabled) match {
      case (false, false) => (None, None) // Ignore
      case (true, false) => // Disabling!
        if (!IcebergCompatV1.isEnabled(newestMetadata)) {
          (None, None)
        } else {
          logInfo(s"[tableId=$tableId] Universal Format (Iceberg): This feature is being " +
            "disabled. Auto-disabling IcebergCompatV1, too.")

          val newConfiguration = newestMetadata.configuration ++
            Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "false")

          (None, Some(newestMetadata.copy(configuration = newConfiguration)))
        }
      case (_, true) => // Enabling now or already-enabled
        val icebergCompatV1WasEnabled = IcebergCompatV1.isEnabled(prevMetadata)
        val icebergCompatV1IsEnabled = IcebergCompatV1.isEnabled(newestMetadata)

        if (icebergCompatV1IsEnabled) {
          (None, None)
        } else if (isCreatingNewTable) {
          // We need to handle the isCreatingNewTable case first because in the the case of
          // a REPLACE TABLE, it could be that icebergCompatV1IsEnabled is false, if it
          // is not explicitly specified as part of the REPLACE command, but
          // icebergCompatV1WasEnabled is true, if it was set on the previous table. In this
          // case, we do not want to auto disable Uniform but rather set its dependencies
          // automatically, the same way as is done for CREATE.
          logInfo(s"[tableId=$tableId] Universal Format (Iceberg): Creating a new table " +
            s"with Universal Format (Iceberg) enabled, but IcebergCompatV1 is not yet enabled. " +
            s"Auto-supporting and enabling IcebergCompatV1 now.")
          val protocolResult = Some(
            newestProtocol.merge(Protocol.forTableFeature(IcebergCompatV1TableFeature))
          )
          val metadataResult = Some(
            newestMetadata.copy(
              configuration = newestMetadata.configuration ++
                Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "true")
            )
          )

          (protocolResult, metadataResult)
        } else if (icebergCompatV1WasEnabled) {
          // IcebergCompatV1 is being disabled. We need to also disable Universal Format (Iceberg)
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

          logInfo(s"[tableId=$tableId] IcebergCompatV1 is being disabled. Auto-disabling " +
            "Universal Format (Iceberg), too.")

          (None, Some(newestMetadata.copy(configuration = newConfiguration)))
        } else {
          throw DeltaErrors.uniFormIcebergRequiresIcebergCompat()
        }
    }
  }

  /**
   * This method should be called before CTAS writer writes the new table to disk.
   * It will call [[enforceIcebergInvariantsAndDependencies]] to perform the actual check.
   * @param configuration of delta writer used to write CTAS data.
   * @return updated configuration if any changes are required,
   *         otherwise the original configuration.
   */
  def enforceInvariantsAndDependenciesForCTAS(
      configuration: Map[String, String],
      snapshot: Snapshot): Map[String, String] = {
    var metadata = Metadata(configuration = configuration)

    // Check UniversalFormat related property dependencies
    val (_, universalMetadata) = UniversalFormat.enforceIcebergInvariantsAndDependencies(
      prevProtocol = Protocol(),
      prevMetadata = Metadata(),
      newestProtocol = Protocol(),
      newestMetadata = metadata,
      isCreatingNewTable = true
    )

    metadata = universalMetadata.getOrElse(metadata)

    // UniversalFormat relies on IcebergV1, check its dependencies
    val (_, icebergMetadata) = IcebergCompatV1.enforceInvariantsAndDependencies(
      prevSnapshot = snapshot,
      newestProtocol = Protocol(),
      newestMetadata = metadata,
      isCreatingNewTable = true,
      actions = Seq()
    )

    icebergMetadata
      .orElse(universalMetadata).map(_.configuration)
      .getOrElse(configuration)
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
