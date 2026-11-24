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
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.ClosableIterator

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
   * The snapshot that the source reads with: [[snapshotAtSourceInit]] with the metadata from the
   * metadata tracking log, if the stream has one.
   */
  protected def readSnapshotDescriptor: SnapshotDescriptor

  /** The Delta log operations that the source uses to read the table. */
  protected def streamingBackend: DeltaStreamingBackend

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

  /** The deprecated `ignoreFileDeletion` option. */
  protected def ignoreFileDeletion: Boolean

  /** The `skipChangeCommits` option. */
  protected def skipChangeCommits: Boolean


  /**
   * When the source tracks metadata changes, returns a metadata change barrier [[IndexedFile]] if
   * the metadata or protocol change of `version` differs from the stream metadata, see
   * [[DeltaSourceMetadataEvolutionSupport.getMetadataOrProtocolChangeIndexedFileIterator]].
   */
  protected def getMetadataOrProtocolChangeIndexedFileIterator(
      metadataChangeOpt: Option[Metadata],
      protocolChangeOpt: Option[Protocol],
      version: Long): ClosableIterator[IndexedFile]

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
  // Commit validation
  // ---------------------------------------------------------------------------------------------

  /**
   * Check stream for violating any constraints.
   *
   * If verifyMetadataAction = true, we will break the stream when we detect any read-incompatible
   * metadata changes.
   *
   * @return (true if commit should be skipped, a metadata action if found)
   */
  protected def validateCommitAndDecideSkipping(
      actions: Iterator[Action],
      version: Long,
      batchStartVersion: Long,
      batchEndOffsetOpt: Option[DeltaSourceOffset] = None,
      verifyMetadataAction: Boolean = true
  ): (Boolean, Option[Metadata], Option[Protocol]) = {
    // If the batch end is at the beginning of this exact version, then we actually stop reading
    // just _before_ this version. So then we can ignore the version contents entirely.
    if (batchEndOffsetOpt.exists(end =>
      end.reservoirVersion == version && end.index == DeltaSourceOffset.BASE_INDEX)) {
      return (false, None, None)
    }

    /** A check on the source table that disallows changes on the source data. */
    val shouldAllowChanges = options.ignoreChanges || ignoreFileDeletion || skipChangeCommits
    /** A check on the source table that disallows commits that only include deletes to the data. */
    val shouldAllowDeletes = shouldAllowChanges || options.ignoreDeletes || ignoreFileDeletion

    var seenFileAdd = false
    var skippedCommit = false
    var metadataAction: Option[Metadata] = None
    var protocolAction: Option[Protocol] = None
    var removeFileActionPath: Option[String] = None
    var operation: Option[String] = None
    actions.foreach {
      case a: AddFile if a.dataChange =>
        seenFileAdd = true
      case r: RemoveFile if r.dataChange =>
        skippedCommit = skipChangeCommits
        if (removeFileActionPath.isEmpty) {
          removeFileActionPath = Some(r.path)
        }
      case m: Metadata =>
        if (verifyMetadataAction) {
          checkReadIncompatibleSchemaChanges(
            m, version, batchStartVersion, batchEndOffsetOpt.map(_.reservoirVersion))
        }
        assert(metadataAction.isEmpty,
          "Should not encounter two metadata actions in the same commit")
        metadataAction = Some(m)
      case protocol: Protocol =>
        streamingBackend.protocolRead(protocol, readSnapshot = readSnapshotDescriptor)
        assert(protocolAction.isEmpty,
          "Should not encounter two protocol actions in the same commit")
        protocolAction = Some(protocol)
      case commitInfo: CommitInfo =>
        operation = Some(s"${commitInfo.operation} (${commitInfo.operationParameters})")
      case _ => ()
    }
    if (removeFileActionPath.isDefined) {
      if (seenFileAdd && !shouldAllowChanges) {
        throw DeltaErrors.deltaSourceIgnoreChangesError(
          version,
          if (operation.nonEmpty) operation.get else removeFileActionPath.get,
          snapshotAtSourceInit.dataPath.toString
        )
      } else if (!seenFileAdd && !shouldAllowDeletes) {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(
          version,
          removeFileActionPath.get,
          snapshotAtSourceInit.dataPath.toString
        )
      }
    }
    (skippedCommit, metadataAction, protocolAction)
  }

  // ---------------------------------------------------------------------------------------------
  // Indexed files
  // ---------------------------------------------------------------------------------------------

  /**
   * Adds dummy BEGIN_INDEX and END_INDEX IndexedFiles for @version before and after the
   * contents of the iterator. The contents of the iterator must be the IndexedFiles that correspond
   * to this version.
   */
  protected def addBeginAndEndIndexOffsetsForVersion(
      version: Long, iterator: Iterator[IndexedFile]): Iterator[IndexedFile] = {
    Iterator.single(IndexedFile(version, DeltaSourceOffset.BASE_INDEX, add = null)) ++
      iterator ++
      Iterator.single(IndexedFile(version, DeltaSourceOffset.END_INDEX, add = null))
  }

  /**
   * Filter the iterator with only add files that contain data change and get indexed files.
   * @return indexed add files
   */
  protected def filterAndGetIndexedFiles(
      iterator: Iterator[Action],
      version: Long,
      shouldSkipCommit: Boolean,
      metadataOpt: Option[Metadata],
      protocolOpt: Option[Protocol]): Iterator[IndexedFile] = {
    val filteredIterator =
      if (shouldSkipCommit) {
        Iterator.empty
      } else {
        iterator.collect { case a: AddFile if a.dataChange => a }
      }

    var index = -1L
    val indexedFiles = new Iterator[IndexedFile] {
      override def hasNext: Boolean = filteredIterator.hasNext
      override def next(): IndexedFile = {
        index += 1 // pre-increment the index (so it starts from 0)
        val add = filteredIterator.next().copy(stats = null)
        IndexedFile(version, index, add)
      }
    }
    addBeginAndEndIndexOffsetsForVersion(
      version,
      getMetadataOrProtocolChangeIndexedFileIterator(metadataOpt, protocolOpt, version) ++
        indexedFiles)
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
