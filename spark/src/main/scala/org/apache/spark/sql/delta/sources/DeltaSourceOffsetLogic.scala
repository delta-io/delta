/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, DeltaOptions, SnapshotDescriptor, TypeWidening}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.StructType

/**
 * Offset logic shared by the Delta streaming sources: [[DeltaSource]] (V1) and
 * `DeltaV2MicroBatchStream` (V2). Both sources mix in this trait and run the same code.
 * The trait does not extend the Spark source interfaces; each source mixes them in itself.
 */
trait DeltaSourceOffsetLogic extends DeltaLogging {

  // ---------------------------------------------------------------------------------------------
  // Required from the source
  // ---------------------------------------------------------------------------------------------

  protected def spark: SparkSession

  protected def options: DeltaOptions

  /**
   * The snapshot of the table when the source starts. V2 passes a snapshot with no DeltaLog, so
   * do not call `deltaLog` on it.
   */
  protected def snapshotAtSourceInit: SnapshotDescriptor

  /**
   * The fixed source schema that later metadata must be read-compatible with, see
   * [[DeltaSource.sourceSchema]].
   */
  protected def schema: StructType

  /** Options that control how the source handles schema changes. */
  protected def schemaReadOptions: DeltaStreamUtils.SchemaReadOptions

  /** The metadata tracking log (schema tracking log) of the stream, if it has one. */
  protected def metadataTrackingLog: Option[DeltaSourceMetadataTrackingLog]

  /** An internal `latestOffsetInternal` to get the latest offset. */
  protected def latestOffsetInternal(
    startOffset: Option[DeltaSourceOffset], limit: ReadLimit): Option[DeltaSourceOffset]


  // ---------------------------------------------------------------------------------------------
  // Metadata tracking
  // ---------------------------------------------------------------------------------------------

  /** Whether the source reads with a schema from the metadata tracking log. */
  protected def trackingMetadataChange: Boolean =
    DeltaSourceMetadataEvolutionSupport.shouldTrackMetadataChange(
      schemaReadOptions, metadataTrackingLog)

  /**
   * Whether the tracking log is provided but still empty, so the source initializes it now.
   * Only consult it before the first write to the log.
   */
  protected def readyToInitializeMetadataTrackingEagerly: Boolean =
    DeltaSourceMetadataEvolutionSupport.shouldInitializeMetadataTrackingEagerly(
      schemaReadOptions, metadataTrackingLog)

  // ---------------------------------------------------------------------------------------------
  // Schema checks
  // ---------------------------------------------------------------------------------------------

  /**
   * Checks for non-additive schema changes (column renames, drops, type widening) and blocks
   * the stream by throwing an exception if detected.
   *
   * Blocks when type widening tracking is enabled and widening changes exist, or when column
   * mapping changes (rename/drop) are detected, unless `allowUnsafeStreamingReadOnColumnMapping
   * SchemaChanges` is enabled. Upon blocking, the error requests the user to provide a schema
   * tracking location to enable schema tracking. On restart, users must acknowledge changes via
   * reader options or SQL confs.
   * See [[DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked]].
   *
   * Note: Should not be called when schema tracking is active (trackingMetadataChange = true).
   *
   * @param oldMetadata Previous metadata (typically from stream initialization)
   * @param newMetadata New metadata with potential schema changes
   * @param validatedDuringStreamStart Whether validating during stream start vs. execution,
   *                                   which affects the error message.
   * @throws DeltaAnalysisException if non-additive schema changes require blocking
   */
  protected def checkNonAdditiveSchemaChanges(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      validatedDuringStreamStart: Boolean): Unit = {
    val shouldTrackSchema: Boolean =
      if (schemaReadOptions.typeWideningEnabled &&
          schemaReadOptions.enableSchemaTrackingForTypeWidening &&
        TypeWidening.containsWideningTypeChanges(oldMetadata.schema, newMetadata.schema)) {
        // If schema tracking is enabled for type widening, we will detect widening type changes and
        // block the stream until the user sets `allowSourceColumnTypeChange` - similar to handling
        // DROP/RENAME for column mapping.
        true
      } else if (schemaReadOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges) {
        false
      } else {
        // Column mapping schema changes
        assert(!trackingMetadataChange, "should not check schema change while tracking it")
        !DeltaColumnMapping.hasNoColumnMappingSchemaChanges(newMetadata, oldMetadata,
          schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges)
      }

    if (shouldTrackSchema) {
      throw DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
        spark,
        oldMetadata.schema,
        newMetadata.schema,
        detectedDuringStreaming = !validatedDuringStreamStart)
    }
  }

  /**
   * Narrow waist to verify a metadata action for read-incompatible schema changes, specifically:
   * 1. Any column mapping related schema changes (rename / drop) columns
   * 2. Standard read-compatibility changes including:
   *    a) No missing columns
   *    b) No data type changes
   *    c) No read-incompatible nullability changes
   * If the check fails, we throw an exception to exit the stream.
   * If lazy log initialization is required, we also run a one time scan to safely initialize the
   * metadata tracking log upon any non-additive schema change failures.
   * @param metadata Metadata that contains a potential schema change
   * @param version Version for the metadata action
   * @param validatedDuringStreamStart Whether this check is being done during stream start.
   */
  protected def checkReadIncompatibleSchemaChanges(
      metadata: Metadata,
      version: Long,
      batchStartVersion: Long,
      batchEndVersionOpt: Option[Long] = None,
      validatedDuringStreamStart: Boolean = false): Unit = {
    log.info(s"checking read incompatibility with schema at version $version, " +
      s"inside batch[$batchStartVersion, ${batchEndVersionOpt.getOrElse("latest")}]")

    val (newMetadata, oldMetadata) = if (version < snapshotAtSourceInit.version) {
      (snapshotAtSourceInit.metadata, metadata)
    } else {
      (metadata, snapshotAtSourceInit.metadata)
    }

    // Table ID has changed during streaming
    if (newMetadata.id != oldMetadata.id) {
      throw DeltaErrors.differentDeltaTableReadByStreamingSource(
        newTableId = newMetadata.id, oldTableId = oldMetadata.id)
    }

    checkNonAdditiveSchemaChanges(oldMetadata, newMetadata, validatedDuringStreamStart)

    // Other standard read compatibility changes
    if (!validatedDuringStreamStart ||
        !schemaReadOptions.
            forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {

      val schemaChange = if (options.readChangeFeed) {
        CDCReader.cdcReadSchema(metadata.schema)
      } else {
        metadata.schema
      }

      // There is a schema change. All of files after this commit will use `schemaChange`. Hence, we
      // check whether we can use `schema` (the fixed source schema we use in the same run of the
      // query) to read these new files safely.
      val backfilling = version < snapshotAtSourceInit.version
      // Partition column change will be ignored if user enable the unsafe flag
      val newPartitionColumns =
        if (schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges) Seq.empty
        else newMetadata.partitionColumns
      val oldPartitionColumns =
        if (schemaReadOptions.allowUnsafeStreamingReadOnPartitionColumnChanges) Seq.empty
        else oldMetadata.partitionColumns

      val checkResult = DeltaStreamUtils.checkSchemaChangesWhenNoSchemaTracking(
        schemaChange, schema,
        newPartitionColumns, oldPartitionColumns,
        backfilling,
        schemaReadOptions)

      if (!DeltaStreamUtils.SchemaCompatibilityResult.isCompatible(checkResult)) {
        val isRetryable =
          DeltaStreamUtils.SchemaCompatibilityResult.isRetryableIncompatible(checkResult)
        recordDeltaEvent(
          snapshotAtSourceInit,
          "delta.streaming.source.schemaChanged",
          data = Map(
            "currentVersion" -> snapshotAtSourceInit.version,
            "newVersion" -> version,
            "retryable" -> isRetryable,
            "backfilling" -> backfilling,
            "readChangeDataFeed" -> options.readChangeFeed,
            "typeWideningEnabled" -> schemaReadOptions.typeWideningEnabled,
            "enableSchemaTrackingForTypeWidening" ->
                schemaReadOptions.enableSchemaTrackingForTypeWidening,
            "containsWideningTypeChanges" ->
              TypeWidening.containsWideningTypeChanges(schema, schemaChange)
          )
        )

        throw DeltaErrors.schemaChangedException(
          schema,
          schemaChange,
          retryable = isRetryable,
          Some(version),
          includeStartingVersionOrTimestampMessage = options.containsStartingVersionOrTimestamp)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Trigger.AvailableNow
  // ---------------------------------------------------------------------------------------------

  /**
   * When `AvailableNow` is used, this offset will be the upper bound where this run of the query
   * will process up. We may run multiple micro batches, but the query will stop itself when it
   * reaches this offset.
   */
  protected var lastOffsetForTriggerAvailableNow: Option[DeltaSourceOffset] = None

  private var isLastOffsetForTriggerAvailableNowInitialized = false

  private var isTriggerAvailableNow = false

  /** Implements `SupportsTriggerAvailableNow.prepareForTriggerAvailableNow` for both sources. */
  def prepareForTriggerAvailableNow(): Unit = {
    logInfo(log"The streaming query reports to use Trigger.AvailableNow.")
    isTriggerAvailableNow = true
  }

  /**
   * initialize the internal states for AvailableNow if this method is called first time after
   * `prepareForTriggerAvailableNow`.
   */
  protected def initForTriggerAvailableNowIfNeeded(
    startOffsetOpt: Option[DeltaSourceOffset]): Unit = {
    if (isTriggerAvailableNow && !isLastOffsetForTriggerAvailableNowInitialized) {
      isLastOffsetForTriggerAvailableNowInitialized = true
      initLastOffsetForTriggerAvailableNow(startOffsetOpt)
    }
  }

  // TODO(#6817): Explore unifying TAN offset computation into latestOffsetInternal.
  // Blocked: latestOffset per-batch shares the same conditions but must NOT bypass rate limiting.
  protected def initLastOffsetForTriggerAvailableNow(
    startOffsetOpt: Option[DeltaSourceOffset]): Unit = {
    val offset = latestOffsetInternal(startOffsetOpt, ReadLimit.allAvailable())
    lastOffsetForTriggerAvailableNow = offset
    lastOffsetForTriggerAvailableNow.foreach { lastOffset =>

    logInfo(log"lastOffset for Trigger.AvailableNow has set to " +
      log"${MDC(DeltaLogKeys.OFFSET, lastOffset.json)}")
    }
  }
}
