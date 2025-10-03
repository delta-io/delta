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

package org.apache.spark.sql.delta.sources

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.sql.Timestamp

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.DeltaSourceSnapshot
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.spark.sql.delta.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * A case class to help with `Dataset` operations regarding Offset indexing, representing AddFile
 * actions in a Delta log. For proper offset tracking (SC-19523), there are also special sentinel
 * values with negative index = [[DeltaSourceOffset.BASE_INDEX]] and add = null.
 *
 * This class is not designed to be persisted in offset logs or such.
 *
 * @param version The version of the Delta log containing this AddFile.
 * @param index The index of this AddFile in the Delta log.
 * @param add The AddFile.
 * @param remove The RemoveFile if any.
 * @param cdc the CDC File if any.
 * @param isLast A flag to indicate whether this is the last AddFile in the version. This is used
 *               to resolve an off-by-one issue in the streaming offset interface; once we've read
 *               to the end of a log version file, we check this flag to advance immediately to the
 *               next one in the persisted offset. Without this special case we would re-read the
 *               already completed log file.
 * @param shouldSkip A flag to indicate whether this IndexedFile should be skipped. Currently, we
 *                   skip processing an IndexedFile on no-op merges to avoid producing redundant
 *                   records.
 */
private[delta] case class IndexedFile(
    version: Long,
    index: Long,
    add: AddFile,
    remove: RemoveFile = null,
    cdc: AddCDCFile = null,
    shouldSkip: Boolean = false) {

  require(Option(add).size + Option(remove).size + Option(cdc).size <= 1,
    "IndexedFile must have at most one of add, remove, or cdc")

  def getFileAction: FileAction = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else {
      cdc
    }
  }

  def hasFileAction: Boolean = {
    getFileAction != null
  }

  def getFileSize: Long = {
    if (add != null) {
      add.size
    } else if (remove != null) {
      remove.size.getOrElse(0)
    } else {
      cdc.size
    }
  }
}

/**
 * Base trait for the Delta Source, that contains methods that deal with
 * getting changes from the delta log.
 */
trait DeltaSourceBase extends Source
    with SupportsAdmissionControl
    with SupportsTriggerAvailableNow
    with DeltaLogging { self: DeltaSource =>

  /**
   * Flag that allows user to force enable unsafe streaming read on Delta table with
   * column mapping enabled AND drop/rename actions.
   */
  protected lazy val allowUnsafeStreamingReadOnColumnMappingSchemaChanges: Boolean = {
    val unsafeFlagEnabled = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES)
    if (unsafeFlagEnabled) {
      recordDeltaEvent(
        deltaLog,
        "delta.unsafe.streaming.readOnColumnMappingSchemaChanges"
      )
    }
    unsafeFlagEnabled
  }

  protected lazy val allowUnsafeStreamingReadOnPartitionColumnChanges: Boolean =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE
    )

  /**
   * Flag that allows user to disable the read-compatibility check during stream start which
   * protects against an corner case in which verifyStreamHygiene could not detect.
   * This is a bug fix but yet a potential behavior change, so we add a flag to fallback.
   */
  protected lazy val forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_START)

  /**
   * Flag that allow user to fallback to the legacy behavior in which user can allow nullable=false
   * schema to read nullable=true data, which is incorrect but a behavior change regardless.
   */
  protected lazy val forceEnableUnsafeReadOnNullabilityChange =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STREAM_UNSAFE_READ_ON_NULLABILITY_CHANGE)

  /**
   * Whether we are streaming from a table with column mapping enabled
   */
  protected val isStreamingFromColumnMappingTable: Boolean =
    snapshotAtSourceInit.metadata.columnMappingMode != NoMapping

  /**
   * Whether we are streaming from a table that has the type widening table feature enabled.
   */
  protected lazy val typeWideningEnabled: Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE) &&
      TypeWidening.isSupported(snapshotAtSourceInit.protocol)

  /**
   * Whether we should track widening type changes to allow users to accept them and resume
   * stream processing.
   */
  protected lazy val enableSchemaTrackingForTypeWidening: Boolean =
    spark.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING)

  /**
   * The persisted schema from the schema log that must be used to read data files in this Delta
   * streaming source.
   */
  protected val persistedMetadataAtSourceInit: Option[PersistedMetadata] =
    metadataTrackingLog.flatMap(_.getCurrentTrackedMetadata)

  /**
   * The read schema for this source during initialization, taking in account of SchemaLog.
   */
  protected val readSchemaAtSourceInit: StructType = readSnapshotDescriptor.metadata.schema

  protected val readPartitionSchemaAtSourceInit: StructType =
    readSnapshotDescriptor.metadata.partitionSchema

  protected val readProtocolAtSourceInit: Protocol = readSnapshotDescriptor.protocol

  protected val readConfigurationsAtSourceInit: Map[String, String] =
    readSnapshotDescriptor.metadata.configuration

  /**
   * Create a snapshot descriptor, customizing its metadata using metadata tracking if necessary
   */
  protected lazy val readSnapshotDescriptor: SnapshotDescriptor =
    persistedMetadataAtSourceInit.map { customMetadata =>
      // Construct a snapshot descriptor with custom schema inline
      new SnapshotDescriptor {
        val deltaLog: DeltaLog = snapshotAtSourceInit.deltaLog
        val metadata: Metadata =
          snapshotAtSourceInit.metadata.copy(
            schemaString = customMetadata.dataSchemaJson,
            partitionColumns = customMetadata.partitionSchema.fieldNames,
            // Copy the configurations so the correct file format can be constructed
            configuration = customMetadata.tableConfigurations
              // Fallback for backward compat only, this should technically not be triggered
              .getOrElse {
                val config = snapshotAtSourceInit.metadata.configuration
                logWarning(log"Using snapshot's table configuration: " +
                  log"${MDC(DeltaLogKeys.CONFIG, config)}")
                config
              }
          )
        val protocol: Protocol = customMetadata.protocol.getOrElse {
          val protocol = snapshotAtSourceInit.protocol
          logWarning(log"Using snapshot's protocol: ${MDC(DeltaLogKeys.PROTOCOL, protocol)}")
          protocol
        }
        // The following are not important in stream reading
        val version: Long = customMetadata.deltaCommitVersion
        val numOfFilesIfKnown = snapshotAtSourceInit.numOfFilesIfKnown
        val sizeInBytesIfKnown = snapshotAtSourceInit.sizeInBytesIfKnown
      }
    }.getOrElse(snapshotAtSourceInit)

  /**
   * A global flag to mark whether we have done a per-stream start check for column mapping
   * schema changes (rename / drop).
   */
  @volatile protected var hasCheckedReadIncompatibleSchemaChangesOnStreamStart: Boolean = false

  override val schema: StructType = {
    val readSchemaWithCdc = if (options.readChangeFeed) {
      CDCReader.cdcReadSchema(readSchemaAtSourceInit)
    } else {
      readSchemaAtSourceInit
    }
    DeltaTableUtils.removeInternalDeltaMetadata(
      spark, DeltaTableUtils.removeInternalWriterMetadata(spark, readSchemaWithCdc))
  }

  // A dummy empty dataframe that can be returned at various point during streaming
  protected val emptyDataFrame: DataFrame =
    DataFrameUtils.ofRows(spark, LocalRelation(schema).copy(isStreaming = true))

  /**
   * When `AvailableNow` is used, this offset will be the upper bound where this run of the query
   * will process up. We may run multiple micro batches, but the query will stop itself when it
   * reaches this offset.
   */
  protected var lastOffsetForTriggerAvailableNow: Option[DeltaSourceOffset] = None

  private var isLastOffsetForTriggerAvailableNowInitialized = false

  private var isTriggerAvailableNow = false

  override def prepareForTriggerAvailableNow(): Unit = {
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

  protected def initLastOffsetForTriggerAvailableNow(
    startOffsetOpt: Option[DeltaSourceOffset]): Unit = {
    val offset = latestOffsetInternal(startOffsetOpt, ReadLimit.allAvailable())
    lastOffsetForTriggerAvailableNow = offset
    lastOffsetForTriggerAvailableNow.foreach { lastOffset =>

    logInfo(log"lastOffset for Trigger.AvailableNow has set to " +
      log"${MDC(DeltaLogKeys.OFFSET, lastOffset.json)}")
    }
  }

  /** An internal `latestOffsetInternal` to get the latest offset. */
  protected def latestOffsetInternal(
    startOffset: Option[DeltaSourceOffset], limit: ReadLimit): Option[DeltaSourceOffset]

  protected def getFileChangesWithRateLimit(
      fromVersion: Long,
      fromIndex: Long,
      isInitialSnapshot: Boolean,
      limits: Option[AdmissionLimits] = Some(AdmissionLimits())): ClosableIterator[IndexedFile] = {
    val iter = if (options.readChangeFeed) {
      // In this CDC use case, we need to consider RemoveFile and AddCDCFiles when getting the
      // offset.

      // This method is only used to get the offset so we need to return an iterator of IndexedFile.
      getFileChangesForCDC(fromVersion, fromIndex, isInitialSnapshot, limits, None).flatMap(_._2)
        .toClosable
    } else {
      val changes = getFileChanges(fromVersion, fromIndex, isInitialSnapshot)

      // Take each change until we've seen the configured number of addFiles. Some changes don't
      // represent file additions; we retain them for offset tracking, but they don't count towards
      // the maxFilesPerTrigger conf.
      if (limits.isEmpty) {
        changes
      } else {
        val admissionControl = limits.get
        changes.withClose { it => it.takeWhile { admissionControl.admit(_) }
        }
      }
    }
    // Stop before any schema change barrier if detected.
    stopIndexedFileIteratorAtSchemaChangeBarrier(iter)
  }

  /**
   * get the changes from startVersion, startIndex to the end
   * @param startVersion - calculated starting version
   * @param startIndex - calculated starting index
   * @param isInitialSnapshot - whether the stream has to return the initial snapshot or not
   * @param endOffset - Offset that signifies the end of the stream.
   * @return
   */
  protected def getFileChangesAndCreateDataFrame(
      startVersion: Long,
      startIndex: Long,
      isInitialSnapshot: Boolean,
      endOffset: DeltaSourceOffset): DataFrame = {
    if (options.readChangeFeed) {
      getCDCFileChangesAndCreateDataFrame(startVersion, startIndex, isInitialSnapshot, endOffset)
    } else {
      val fileActionsIter = getFileChanges(
        startVersion,
        startIndex,
        isInitialSnapshot,
        endOffset = Some(endOffset)
      )
      try {
        val filteredIndexedFiles = fileActionsIter.filter { indexedFile =>
          indexedFile.getFileAction != null &&
            excludeRegex.forall(_.findFirstIn(indexedFile.getFileAction.path).isEmpty)
        }

        val (result, duration) = Utils.timeTakenMs {
          createDataFrame(filteredIndexedFiles)
        }
        logInfo(log"Getting dataFrame for delta_log_path=" +
          log"${MDC(DeltaLogKeys.PATH, deltaLog.logPath)} with " +
          log"startVersion=${MDC(DeltaLogKeys.START_VERSION, startVersion)}, " +
          log"startIndex=${MDC(DeltaLogKeys.START_INDEX, startIndex)}, " +
          log"isInitialSnapshot=${MDC(DeltaLogKeys.IS_INIT_SNAPSHOT, isInitialSnapshot)}, " +
          log"endOffset=${MDC(DeltaLogKeys.END_INDEX, endOffset)} took timeMs=" +
          log"${MDC(DeltaLogKeys.DURATION, duration)} ms")
        result
      } finally {
        fileActionsIter.close()
      }
    }
  }

  /**
   * Given an iterator of file actions, create a DataFrame representing the files added to a table
   * Only AddFile actions will be used to create the DataFrame.
   * @param indexedFiles actions iterator from which to generate the DataFrame.
   */
  protected def createDataFrame(indexedFiles: Iterator[IndexedFile]): DataFrame = {
    val addFiles = indexedFiles
      .filter(_.getFileAction.isInstanceOf[AddFile])
      .toSeq
    val hasDeletionVectors =
      addFiles.exists(_.getFileAction.asInstanceOf[AddFile].deletionVector != null)
    if (hasDeletionVectors) {
      // Read AddFiles from different versions in different scans.
      // This avoids an issue where we might read the same file with different deletion vectors in
      // the same scan, which we cannot support as long we broadcast a map of DVs for lookup.
      // This code can be removed once we can pass the DVs into the scan directly together with the
      // AddFile/PartitionedFile entry.
      addFiles
        .groupBy(_.version)
        .values
        .map { addFilesList =>
          deltaLog.createDataFrame(
            readSnapshotDescriptor,
            addFilesList.map(_.getFileAction.asInstanceOf[AddFile]),
            isStreaming = true)
        }
        .reduceOption(_ union _)
        .getOrElse {
          // If we filtered out all the values before the groupBy, just return an empty DataFrame.
          deltaLog.createDataFrame(
            readSnapshotDescriptor,
            Seq.empty[AddFile],
            isStreaming = true)
        }
    } else {
      deltaLog.createDataFrame(
        readSnapshotDescriptor,
        addFiles.map(_.getFileAction.asInstanceOf[AddFile]),
        isStreaming = true)
    }
  }

  /**
   * Returns the offset that starts from a specific delta table version. This function is
   * called when starting a new stream query.
   *
   * @param fromVersion The version of the delta table to calculate the offset from.
   * @param isInitialSnapshot Whether the delta version is for the initial snapshot or not.
   * @param limits Indicates how much data can be processed by a micro batch.
   */
  protected def getStartingOffsetFromSpecificDeltaVersion(
      fromVersion: Long,
      isInitialSnapshot: Boolean,
      limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {
    // Initialize schema tracking log if possible, no-op if already initialized
    // This is one of the two places can initialize schema tracking.
    // This case specifically handles when we have a fresh stream.
    if (readyToInitializeMetadataTrackingEagerly) {
      initializeMetadataTrackingAndExitStream(fromVersion)
    }

    val changes = getFileChangesWithRateLimit(
      fromVersion,
      fromIndex = DeltaSourceOffset.BASE_INDEX,
      isInitialSnapshot = isInitialSnapshot,
      limits)

    val lastFileChange = DeltaSource.iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      None
    } else {
      // Block latestOffset() from generating an invalid offset by proactively verifying
      // incompatible schema changes under column mapping. See more details in the method doc.
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(fromVersion)
      buildOffsetFromIndexedFile(lastFileChange.get, fromVersion, isInitialSnapshot)
    }
  }

  /**
   * Return the next offset when previous offset exists.
   */
  protected def getNextOffsetFromPreviousOffset(
      previousOffset: DeltaSourceOffset,
      limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {
    if (trackingMetadataChange) {
      getNextOffsetFromPreviousOffsetIfPendingSchemaChange(previousOffset) match {
        case None =>
        case updatedPreviousOffsetOpt =>
          // Stop generating new offset if there were pending schema changes
          return updatedPreviousOffsetOpt
      }
    }

    val changes = getFileChangesWithRateLimit(
      previousOffset.reservoirVersion,
      previousOffset.index,
      previousOffset.isInitialSnapshot,
      limits)

    val lastFileChange = DeltaSource.iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      Some(previousOffset)
    } else {
      // Similarly, block latestOffset() from generating an invalid offset by proactively
      // verifying incompatible schema changes under column mapping. See more details in the
      // method scala doc.
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(previousOffset.reservoirVersion)
      buildOffsetFromIndexedFile(lastFileChange.get, previousOffset.reservoirVersion,
        previousOffset.isInitialSnapshot)
    }
  }

  /**
   * Build the latest offset based on the last indexedFile. The function also checks if latest
   * version is valid by comparing with previous version.
   * @param indexedFile The last indexed file used to build offset from.
   * @param version Previous offset reservoir version.
   * @param isInitialSnapshot Whether previous offset is starting version or not.
   */
  private def buildOffsetFromIndexedFile(
      indexedFile: IndexedFile,
      version: Long,
      isInitialSnapshot: Boolean): Option[DeltaSourceOffset] = {
    val (v, i) = (indexedFile.version, indexedFile.index)
    assert(v >= version,
      s"buildOffsetFromIndexedFile returns an invalid version: $v (expected: >= $version), " +
        s"tableId: $tableId")

    // If the last file in previous batch is the end index of that version, automatically bump
    // to next version to skip accessing that version file altogether. The END_INDEX should never
    // be returned as an offset.
    val offset = if (indexedFile.index == DeltaSourceOffset.END_INDEX) {
      // isInitialSnapshot must be false here as we have bumped the version.
      Some(DeltaSourceOffset(
        tableId,
        v + 1,
        index = DeltaSourceOffset.BASE_INDEX,
        isInitialSnapshot = false))
    } else {
      // isInitialSnapshot will be true only if previous isInitialSnapshot is true and the next file
      // is still at the same version (i.e v == version).
      Some(DeltaSourceOffset(
        tableId, v, i,
        isInitialSnapshot = v == version && isInitialSnapshot
      ))
    }
    offset
  }

  /**
   * Return the DataFrame between start and end offset.
   */
  protected def createDataFrameBetweenOffsets(
      startVersion: Long,
      startIndex: Long,
      isInitialSnapshot: Boolean,
      startOffsetOption: Option[DeltaSourceOffset],
      endOffset: DeltaSourceOffset): DataFrame = {
    getFileChangesAndCreateDataFrame(startVersion, startIndex, isInitialSnapshot, endOffset)
  }

  protected def cleanUpSnapshotResources(): Unit = {
    if (initialState != null) {
      initialState.close(unpersistSnapshot = initialStateVersion < snapshotAtSourceInit.version)
      initialState = null
    }
  }

  /**
   * Check read-incompatible schema changes during stream (re)start so we could fail fast.
   *
   * This only needs to be called ONCE in the life cycle of a stream, either at the very first
   * latestOffset, or the very first getBatch to make sure we have detected an incompatible
   * schema change.
   * Typically, the verifyStreamHygiene that was called maybe good enough to detect these
   * schema changes, there may be cases that wouldn't work, e.g. consider this sequence:
   * 1. User starts a new stream @ startingVersion 1
   * 2. latestOffset is called before getBatch() because there was no previous commits so
   * getBatch won't be called as a recovery mechanism.
   * Suppose there's a single rename/drop/nullability change S during computing next offset, S
   * would look exactly the same as the latest schema so verifyStreamHygiene would not work.
   * 3. latestOffset would return this new offset cross the schema boundary.
   *
   * If a schema log is already initialized, we don't have to run the initialization nor schema
   * checks any more.
   *
   * @param batchStartVersion Start version we want to verify read compatibility against
   * @param batchEndVersionOpt Optionally, if we are checking against an existing constructed batch
   *                           during streaming initialization, we would also like to verify all
   *                           schema changes in between as well before we can lazily initialize the
   *                           schema log if needed.
   */
  protected def checkReadIncompatibleSchemaChangeOnStreamStartOnce(
      batchStartVersion: Long,
      batchEndVersionOpt: Option[Long] = None): Unit = {
    if (trackingMetadataChange) return
    if (hasCheckedReadIncompatibleSchemaChangesOnStreamStart) return

    lazy val (startVersionSnapshotOpt, errOpt) =
      Try(deltaLog.getSnapshotAt(batchStartVersion, catalogTableOpt = catalogTableOpt)) match {
        case Success(snapshot) => (Some(snapshot), None)
        case Failure(exception) => (None, Some(exception))
      }

    // Cannot perfectly verify column mapping schema changes if we cannot compute a start snapshot.
    if (!allowUnsafeStreamingReadOnColumnMappingSchemaChanges &&
        isStreamingFromColumnMappingTable && errOpt.isDefined) {
      throw DeltaErrors.failedToGetSnapshotDuringColumnMappingStreamingReadCheck(errOpt.get)
    }

    // Perform schema check if we need to, considering all escape flags.
    if (!allowUnsafeStreamingReadOnColumnMappingSchemaChanges || typeWideningEnabled ||
        !forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {
      startVersionSnapshotOpt.foreach { snapshot =>
        checkReadIncompatibleSchemaChanges(
          snapshot.metadata,
          snapshot.version,
          batchStartVersion,
          batchEndVersionOpt,
          validatedDuringStreamStart = true
        )
        // If end version is defined (i.e. we have a pending batch), let's also eagerly check all
        // intermediate schema changes against the stream read schema to capture corners cases such
        // as rename and rename back.
        for {
          endVersion <- batchEndVersionOpt
          (version, metadata) <- collectMetadataActions(batchStartVersion, endVersion)
        } {
          checkReadIncompatibleSchemaChanges(
            metadata,
            version,
            batchStartVersion,
            Some(endVersion),
            validatedDuringStreamStart = true)
        }
      }
    }

    // Mark as checked
    hasCheckedReadIncompatibleSchemaChangesOnStreamStart = true
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

    def shouldTrackSchema: Boolean =
      if (typeWideningEnabled && enableSchemaTrackingForTypeWidening &&
        TypeWidening.containsWideningTypeChanges(oldMetadata.schema, newMetadata.schema)) {
        // If schema tracking is enabled for type widening, we will detect widening type changes and
        // block the stream until the user sets `allowSourceColumnTypeChange` - similar to handling
        // DROP/RENAME for column mapping.
        true
      } else if (allowUnsafeStreamingReadOnColumnMappingSchemaChanges) {
        false
      } else {
        // Column mapping schema changes
        assert(!trackingMetadataChange, "should not check schema change while tracking it")
        !DeltaColumnMapping.hasNoColumnMappingSchemaChanges(newMetadata, oldMetadata,
          allowUnsafeStreamingReadOnPartitionColumnChanges)
      }

    if (shouldTrackSchema) {
      throw DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
        spark,
        oldMetadata.schema,
        newMetadata.schema,
        detectedDuringStreaming = !validatedDuringStreamStart)
    }

    // Other standard read compatibility changes
    if (!validatedDuringStreamStart ||
        !forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {

      val schemaChange = if (options.readChangeFeed) {
        CDCReader.cdcReadSchema(metadata.schema)
      } else {
        metadata.schema
      }

      // There is a schema change. All of files after this commit will use `schemaChange`. Hence, we
      // check whether we can use `schema` (the fixed source schema we use in the same run of the
      // query) to read these new files safely.
      val backfilling = version < snapshotAtSourceInit.version
      // We forbid the case when the the schemaChange is nullable while the read schema is NOT
      // nullable, or in other words, `schema` should not tighten nullability from `schemaChange`,
      // because we don't ever want to read back any nulls when the read schema is non-nullable.
      val shouldForbidTightenNullability = !forceEnableUnsafeReadOnNullabilityChange
      // If schema tracking is disabled for type widening, we allow widening type changes to go
      // through without requiring the user to set `allowSourceColumnTypeChange`. The schema change
      // will cause the stream to fail with a retryable exception, and the stream will restart using
      // the new schema.
      val typeWideningMode =
        if (typeWideningEnabled && !enableSchemaTrackingForTypeWidening) {
          TypeWideningMode.AllTypeWidening
        } else {
         TypeWideningMode.NoTypeWidening
        }
      if (!SchemaUtils.isReadCompatible(
          schemaChange, schema,
          forbidTightenNullability = shouldForbidTightenNullability,
          // If a user is streaming from a column mapping table and enable the unsafe flag to ignore
          // column mapping schema changes, we can allow the standard check to allow missing columns
          // from the read schema in the schema change, because the only case that happens is when
          // user rename/drops column but they don't care so they enabled the flag to unblock.
          // This is only allowed when we are "backfilling", i.e. the stream progress is older than
          // the analyzed table version. Any schema change past the analysis should still throw
          // exception, because additive schema changes MUST be taken into account.
          allowMissingColumns =
            isStreamingFromColumnMappingTable &&
              allowUnsafeStreamingReadOnColumnMappingSchemaChanges &&
              backfilling,
          typeWideningMode = typeWideningMode,
          // Partition column change will be ignored if user enable the unsafe flag
          newPartitionColumns = if (allowUnsafeStreamingReadOnPartitionColumnChanges) Seq.empty
            else newMetadata.partitionColumns,
          oldPartitionColumns = if (allowUnsafeStreamingReadOnPartitionColumnChanges) Seq.empty
            else oldMetadata.partitionColumns
        )) {
        // Only schema change later than the current read snapshot/schema can be retried, in other
        // words, backfills could never be retryable, because we have no way to refresh
        // the latest schema to "catch up" when the schema change happens before than current read
        // schema version.
        // If not backfilling, we do another check to determine retryability, in which we assume
        // we will be reading using this later `schemaChange` back on the current outdated `schema`,
        // and if it works (including that `schemaChange` should not tighten the nullability
        // constraint from `schema`), it is a retryable exception.
        val retryable = !backfilling && SchemaUtils.isReadCompatible(
          schema,
          schemaChange,
          forbidTightenNullability = shouldForbidTightenNullability,
          typeWideningMode = typeWideningMode
        )
        throw DeltaErrors.schemaChangedException(
          schema,
          schemaChange,
          retryable = retryable,
          Some(version),
          includeStartingVersionOrTimestampMessage = options.containsStartingVersionOrTimestamp)
      }
    }
  }
}

/**
 * A streaming source for a Delta table.
 *
 * When a new stream is started, delta starts by constructing a
 * [[org.apache.spark.sql.delta.Snapshot]] at
 * the current version of the table. This snapshot is broken up into batches until
 * all existing data has been processed. Subsequent processing is done by tailing
 * the change log looking for new data. This results in the streaming query returning
 * the same answer as a batch query that had processed the entire dataset at any given point.
 */
case class DeltaSource(
    spark: SparkSession,
    deltaLog: DeltaLog,
    catalogTableOpt: Option[CatalogTable],
    options: DeltaOptions,
    snapshotAtSourceInit: SnapshotDescriptor,
    metadataPath: String,
    metadataTrackingLog: Option[DeltaSourceMetadataTrackingLog] = None,
    filters: Seq[Expression] = Nil)
  extends DeltaSourceBase
  with DeltaSourceCDCSupport
  with DeltaSourceMetadataEvolutionSupport {

  private val shouldValidateOffsets =
    spark.sessionState.conf.getConf(DeltaSQLConf.STREAMING_OFFSET_VALIDATION)

  // Deprecated. Please use `skipChangeCommits` from now on.
  private val ignoreFileDeletion = {
    if (options.ignoreFileDeletion) {
      logConsole(DeltaErrors.ignoreStreamingUpdatesAndDeletesWarning(spark))
      recordDeltaEvent(deltaLog, "delta.deprecation.ignoreFileDeletion")
    }
    options.ignoreFileDeletion
  }

  /** A check on the source table that skips commits that contain removes from the
   * set of files. */
  private val skipChangeCommits = options.skipChangeCommits

  protected val excludeRegex: Option[Regex] = options.excludeRegex

  // This was checked before creating ReservoirSource
  assert(schema.nonEmpty)

  protected val tableId = snapshotAtSourceInit.metadata.id

  // A metadata snapshot when starting the query.
  protected var initialState: DeltaSourceSnapshot = null
  protected var initialStateVersion: Long = -1L

  logInfo(log"Filters being pushed down: ${MDC(DeltaLogKeys.FILTER, filters)}")

  /**
   * Get the changes starting from (startVersion, startIndex). The start point should not be
   * included in the result.
   *
   * @param endOffset If defined, do not return changes beyond this offset.
   *                  If not defined, we must be scanning the log to find the next offset.
   * @param verifyMetadataAction If true, we will break the stream when we detect any
   *                             read-incompatible metadata changes.
   */
  protected def getFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isInitialSnapshot: Boolean,
      endOffset: Option[DeltaSourceOffset] = None,
      verifyMetadataAction: Boolean = true
  ): ClosableIterator[IndexedFile] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): ClosableIterator[IndexedFile] = {
      // TODO: handle the case when failOnDataLoss = false and we are missing change log files
      //    in that case, we need to recompute the start snapshot and evolve the schema if needed
      require(options.failOnDataLoss || !trackingMetadataChange,
        "Using schema from schema tracking log cannot tolerate missing commit files.")
      deltaLog.getChangeLogFiles(
        startVersion, catalogTableOpt, options.failOnDataLoss).flatMapWithClose {
        case (version, filestatus) =>
          // First pass reads the whole commit and closes the iterator.
          val iter = DeltaSource.createRewindableActionIterator(spark, deltaLog, filestatus)
          val (shouldSkipCommit, metadataOpt, protocolOpt) = iter
            .processAndClose { actionsIter =>
              validateCommitAndDecideSkipping(
                actionsIter, version,
                fromVersion, endOffset,
                verifyMetadataAction && !trackingMetadataChange
              )
            }
          // Rewind the iterator to the beginning, if the actions are cached in memory, they will
          // be reused again.
          iter.rewind()
          // Second pass reads the commit lazily.
          iter.withClose { actionsIter =>
            filterAndGetIndexedFiles(
              actionsIter, version, shouldSkipCommit, metadataOpt, protocolOpt)
          }
      }
    }

    val (result, duration) = Utils.timeTakenMs {
      var iter = if (isInitialSnapshot) {
        Iterator(1, 2).flatMapWithClose { // so that the filterAndIndexDeltaLogs call is lazy
          case 1 => getSnapshotAt(fromVersion)._1.toClosable
          case 2 => filterAndIndexDeltaLogs(fromVersion + 1)
        }
      } else {
        filterAndIndexDeltaLogs(fromVersion)
      }

      iter = iter.withClose { it =>
        it.filter { file =>
          file.version > fromVersion || file.index > fromIndex
        }
      }

      // If endOffset is provided, we are getting a batch on a constructed range so we should use
      // the endOffset as the limit.
      // Otherwise, we are looking for a new offset, so we try to use the latestOffset we found for
      // Trigger.availableNow() as limit. We know endOffset <= lastOffsetForTriggerAvailableNow.
        val lastOffsetForThisScan = endOffset.orElse(lastOffsetForTriggerAvailableNow)

        lastOffsetForThisScan.foreach { bound =>
          iter = iter.withClose { it =>
            it.takeWhile { file =>
              file.version < bound.reservoirVersion ||
                (file.version == bound.reservoirVersion && file.index <= bound.index)
            }
          }
        }
      iter
    }
    logInfo(log"Getting file changes for delta_log_path=" +
      log"${MDC(DeltaLogKeys.PATH, deltaLog.logPath)} with " +
      log"fromVersion=${MDC(DeltaLogKeys.START_VERSION, fromVersion)}, " +
      log"fromIndex=${MDC(DeltaLogKeys.START_INDEX, fromIndex)}, " +
      log"isInitialSnapshot=${MDC(DeltaLogKeys.IS_INIT_SNAPSHOT, isInitialSnapshot)} " +
      log"took timeMs=${MDC(DeltaLogKeys.DURATION, duration)} ms")
    result
  }

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
   * This method computes the initial snapshot to read when Delta Source was initialized on a fresh
   * stream.
   * @return A tuple where the first element is an iterator of IndexedFiles and the second element
   *         is the in-commit timestamp of the initial snapshot if available.
   */
  protected def getSnapshotAt(version: Long): (Iterator[IndexedFile], Option[Long]) = {
    if (initialState == null || version != initialStateVersion) {
      super[DeltaSourceBase].cleanUpSnapshotResources()
      val snapshot = getSnapshotFromDeltaLog(version)

      initialState = new DeltaSourceSnapshot(spark, snapshot, filters)
      initialStateVersion = version

      // This handle a special case for schema tracking log when it's initialized but the initial
      // snapshot's schema has changed, suppose:
      // 1. The stream starts and looks at the initial snapshot to compute the starting offset, say
      //    at version 0 with schema <a>
      // 2. User renames a column, creates version 1 with schema <b>
      // 3. The read compatibility check fails during scanning version 1, initializes schema log
      //    using the initial snapshot's schema (<a>, because that's the safest thing to do as we
      //    have not served any data from initial snapshot yet) and exits stream.
      // 4. Stream restarts, since no starting offset was generated, it will retry loading the
      //    initial snapshot, which is now at version 1, but the tracked schema <a> is now different
      //    from the "new" initial snapshot schema! Worse, since schema tracking ignores any schema
      //    changes inside initial snapshot, we will then be reading the files using a wrong schema!
      // The below logic allows us to detect any discrepancies when reading initial snapshot using
      // a tracked schema, and reinitialize the log if needed.
      if (trackingMetadataChange &&
          initialState.snapshot.version >= readSnapshotDescriptor.version) {
        updateMetadataTrackingLogAndFailTheStreamIfNeeded(
          Some(initialState.snapshot.metadata),
          Some(initialState.snapshot.protocol),
          initialState.snapshot.version,
          // The new schema should replace the previous initialized schema for initial snapshot
          replace = true
        )
      }
    }
    val inCommitTimestampOpt =
      Option.when(
          DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(initialState.snapshot.metadata)) {
        initialState.snapshot.timestamp
      }
    (addBeginAndEndIndexOffsetsForVersion(version, initialState.iterator()), inCommitTimestampOpt)
  }

  /**
   * Narrow-waist for generating snapshot from Delta Log within Delta Source
   */
  protected def getSnapshotFromDeltaLog(version: Long): Snapshot = {
    try {
      deltaLog.getSnapshotAt(version, catalogTableOpt = catalogTableOpt)
    } catch {
      case e: FileNotFoundException =>
        throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(e)
    }
  }

  private def getStartingOffset(limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {

    val (version, isInitialSnapshot) = getStartingVersion match {
      case Some(v) => (v, false)
      case None => (snapshotAtSourceInit.version, true)
    }
    if (version < 0) {
      return None
    }

    getStartingOffsetFromSpecificDeltaVersion(version, isInitialSnapshot, limits)
  }

  override def getDefaultReadLimit: ReadLimit = {
    AdmissionLimits().toReadLimit
  }

  def toDeltaSourceOffset(offset: streaming.Offset): DeltaSourceOffset = {
    DeltaSourceOffset(tableId, offset)
  }

  /**
   * This should only be called by the engine. Call `latestOffsetInternal` instead if you need to
   * get the latest offset.
   */
  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset =
    recordDeltaOperation(
      snapshotAtSourceInit.deltaLog, opType = "delta.streaming.source.latestOffset") {
    val deltaStartOffset = Option(startOffset).map(toDeltaSourceOffset)
    initForTriggerAvailableNowIfNeeded(deltaStartOffset)
    latestOffsetInternal(deltaStartOffset, limit).orNull
  }

  override protected def latestOffsetInternal(
    startOffset: Option[DeltaSourceOffset], limit: ReadLimit): Option[DeltaSourceOffset] = {
    val limits = AdmissionLimits(limit)

    val endOffset = startOffset.map(getNextOffsetFromPreviousOffset(_, limits))
      .getOrElse(getStartingOffset(limits))

    val startVersion = startOffset.map(_.reservoirVersion).getOrElse(-1L)
    val endVersion = endOffset.map(_.reservoirVersion).getOrElse(-1L)
    lazy val offsetRangeInfo = "(latestOffsetInternal)startOffset -> endOffset:" +
      s" $startOffset -> $endOffset"
    if (endVersion - startVersion > 1000L) {
      // Improve the log level if the source is processing a large batch.
      logInfo(offsetRangeInfo)
    } else {
      logDebug(offsetRangeInfo)
    }
    if (shouldValidateOffsets && startOffset.isDefined) {
      endOffset.foreach { endOffset =>
        DeltaSourceOffset.validateOffsets(startOffset.get, endOffset)
      }
    }
    endOffset
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  /**
   * Filter the iterator with only add files that contain data change and get indexed files.
   * @return indexed add files
   */
  private def filterAndGetIndexedFiles(
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
        deltaLog.protocolRead(protocol)
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
          deltaLog.dataPath.toString
        )
      } else if (!seenFileAdd && !shouldAllowDeletes) {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(
          version,
          removeFileActionPath.get,
          deltaLog.dataPath.toString
        )
      }
    }
    (skippedCommit, metadataAction, protocolAction)
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame =
    recordDeltaOperation(
      snapshotAtSourceInit.deltaLog, opType = "delta.streaming.source.getBatch") {
    val endOffset = toDeltaSourceOffset(end)
    val startDeltaOffsetOption = startOffsetOption.map(toDeltaSourceOffset)

    val (startVersion, startIndex, isInitialSnapshot) =
      extractStartingState(startDeltaOffsetOption, endOffset)

    if (startOffsetOption.contains(endOffset)) {
      // This happens only if we recover from a failure and `MicroBatchExecution` tries to call
      // us with the previous offsets. The returned DataFrame will be dropped immediately, so we
      // can return any DataFrame.
      return emptyDataFrame
    }

    val offsetRangeInfo = s"(getBatch)start: $startDeltaOffsetOption end: $end"
    if (endOffset.reservoirVersion - startVersion > 1000L) {
      // Improve the log level if the source is processing a large batch.
      logInfo(offsetRangeInfo)
    } else {
      logDebug(offsetRangeInfo)
    }

    // Initialize schema tracking log if possible, no-op if already initialized.
    // This is one of the two places can initialize schema tracking.
    // This case specifically handles initialization when we are already working with an initialized
    // stream.
    // Here we may have two conditions:
    // 1. We are dealing with the recovery getBatch() that gives us the previous committed offset
    // where start and end corresponds to the previous batch.
    // In this case, we should initialize the schema at the previous committed offset (endOffset),
    // which can be done using the same `initializeMetadataTrackingAndExitStream` method.
    // This also means we are caught up with the stream and we can start schema tracking in the
    // next latestOffset call.
    // 2. We are running an already-constructed batch, we need the schema to be compatible
    // with the entire batch, so we also pass the batch end offset. The schema tracking log will
    // only be initialized if there exists a consistent read schema for the entire batch. If such
    // a consistent schema does not exist, the stream will be broken. This case will be rare: it can
    // only happen for streams where the schema tracking log was added after the stream has already
    // been running, *and* the stream was running on an older version of the DeltaSource that did
    // not detect non-additive schema changes, *and* it was stopped while processing a batch that
    // contained such a schema change.
    // In either world, the initialization logic would find the superset compatible schema for this
    // batch by scanning Delta log.
    validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(startVersion, endOffset)

    val createdDf = createDataFrameBetweenOffsets(
      startVersion, startIndex, isInitialSnapshot, startDeltaOffsetOption, endOffset)

    createdDf
  }

  /**
   * Extracts the start state for a scan given an optional start offset and an end offset, so we
   * know exactly where we should scan from for a batch end at the `endOffset`, invoked when:
   *
   * 1. We are in `getBatch` given a startOffsetOption and endOffset from streaming engine.
   * 2. We are in the `init` method for every stream (re)start given a start offset for all pending
   *    batches and the latest planned offset, and trying to figure out if this range contains any
   *    non-additive schema changes.
   *
   * @param startOffsetOption Optional start offset, if not defined. This means we are trying to
   *                          scan the very first batch where endOffset is the very first offset
   *                          generated by `latestOffsets`, specifically `getStartingOffset`
   * @param endOffset The end offset for a batch.
   * @return (start commit version to scan from,
   *         start offset index to scan from,
   *         whether this version is part of the initial snapshot)
   */
  private def extractStartingState(
      startOffsetOption: Option[DeltaSourceOffset],
      endOffset: DeltaSourceOffset): (Long, Long, Boolean) = {
    val (startVersion, startIndex, isInitialSnapshot) = if (startOffsetOption.isEmpty) {
      getStartingVersion match {
        case Some(v) =>
          (v, DeltaSourceOffset.BASE_INDEX, false)

        case None =>
          if (endOffset.isInitialSnapshot) {
            (endOffset.reservoirVersion, DeltaSourceOffset.BASE_INDEX, true)
          } else {
            assert(
              endOffset.reservoirVersion > 0, s"invalid reservoirVersion in endOffset: $endOffset")
            // Load from snapshot `endOffset.reservoirVersion - 1L` so that `index` in `endOffset`
            // is still valid.
            // It's OK to use the previous version as the updated initial snapshot, even if the
            // initial snapshot might have been different from the last time when this starting
            // offset was computed.
            (endOffset.reservoirVersion - 1L, DeltaSourceOffset.BASE_INDEX, true)
          }
      }
    } else {
      val startOffset = startOffsetOption.get
      if (!startOffset.isInitialSnapshot) {
        // unpersist `snapshot` because it won't be used any more.
        cleanUpSnapshotResources()
      }
      (startOffset.reservoirVersion, startOffset.index, startOffset.isInitialSnapshot)
    }
    (startVersion, startIndex, isInitialSnapshot)
  }

  /**
   * Centralized place for validating and initializing schema log for all pending batch(es).
   * This is called only during stream start.
   *
   * @param startVersion Start version of the pending batch range
   * @param endOffset End offset for the pending batch range. end offset >= start offset
   */
  private def validateAndInitMetadataLogForPlannedBatchesDuringStreamStart(
      startVersion: Long,
      endOffset: DeltaSourceOffset): Unit = {
    // We don't have to include the end reservoir version when the end offset is a base index, i.e.
    // no data commit has been marked within a constructed batch, we can simply ignore end offset
    // version. This can help us avoid overblocking a potential ending offset right at a schema
    // change.
    val endVersionForMetadataLogInit = if (endOffset.index == DeltaSourceOffset.BASE_INDEX) {
      endOffset.reservoirVersion - 1
    } else {
      endOffset.reservoirVersion
    }
    // For eager initialization, we initialize the log right now.
    if (readyToInitializeMetadataTrackingEagerly) {
      initializeMetadataTrackingAndExitStream(startVersion, Some(endVersionForMetadataLogInit))
    }

    // Check for column mapping + streaming incompatible schema changes
    // Note for initial snapshot, the startVersion should be the same as the latestOffset's
    // version and therefore this check won't have any effect.
    // This method would also handle read-compatibility checks against the pending batch(es)
    // as well as lazy metadata log initialization.
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(
      startVersion,
      Some(endVersionForMetadataLogInit)
    )
  }

  override def stop(): Unit = {
    cleanUpSnapshotResources()
  }

  // Marks that the `end` offset is done and we can safely run any actions in response to that.
  // This happens AFTER `end` offset is committed by the streaming engine so we can safely fail this
  // if needed, e.g. for failing the stream to conduct schema evolution.
  override def commit(end: Offset): Unit =
    recordDeltaOperation(snapshotAtSourceInit.deltaLog, opType = "delta.streaming.source.commit") {
    super.commit(end)
    // IMPORTANT: for future developers, please place any work you would like to do in commit()
    // before `updateSchemaTrackingLogAndFailTheStreamIfNeeded(end)` as it may throw an exception.
    updateMetadataTrackingLogAndFailTheStreamIfNeeded(end)
  }

  override def toString(): String = s"DeltaSource[${deltaLog.dataPath}]"

  trait DeltaSourceAdmissionBase { self: AdmissionLimits =>
    // This variable indicates whether a commit has already been processed by a batch or not.
    var commitProcessedInBatch = false

    protected def take(files: Int, bytes: Long): Unit = {
      filesToTake -= files
      bytesToTake -= bytes
    }

    /**
     * This overloaded method checks if all the FileActions for a commit can be accommodated by
     * the rate limit.
     */
    def admit(indexedFiles: Seq[IndexedFile]): Boolean = {
      def getSize(actions: Seq[IndexedFile]): Long = {
        actions.filter(_.hasFileAction).foldLeft(0L) { (l, r) => l + r.getFileAction.getFileSize }
      }
      if (indexedFiles.isEmpty) {
        true
      } else {
        // if no files have been admitted, then admit all to avoid deadlock
        // else check if all of the files together satisfy the limit, only then admit
        val bytesInFiles = getSize(indexedFiles)
        val shouldAdmit = !commitProcessedInBatch ||
          (filesToTake - indexedFiles.size >= 0 && bytesToTake - bytesInFiles >= 0)

        commitProcessedInBatch = true
        take(files = indexedFiles.size, bytes = bytesInFiles)
        shouldAdmit
      }
    }

    /**
     * Whether to admit the next file. Dummy IndexedFile entries with no attached file action are
     * always admitted.
     */
    def admit(indexedFile: IndexedFile): Boolean = {
      commitProcessedInBatch = true

      if (!indexedFile.hasFileAction) {
        // Don't count placeholders. They are not files. If we have empty commits, then we should
        // not count the placeholders as files, or else we'll end up with under-filled batches.
        return true
      }

      // We always admit a file if we still have capacity _before_ we take it. This ensures that we
      // will even admit a file when it is larger than the remaining capacity, and that we will
      // admit at least one file.
      val shouldAdmit = hasCapacity
      take(files = 1, bytes = indexedFile.getFileAction.getFileSize)
      shouldAdmit
    }

    /** Returns whether admission limits has capacity to accept files or bytes */
    def hasCapacity: Boolean = {
      filesToTake > 0 && bytesToTake > 0
    }

  }

  /**
   * Class that helps controlling how much data should be processed by a single micro-batch.
   */
  case class AdmissionLimits(
      maxFiles: Option[Int] = options.maxFilesPerTrigger,
      var bytesToTake: Long = options.maxBytesPerTrigger.getOrElse(Long.MaxValue)
  ) extends DeltaSourceAdmissionBase {

    var filesToTake = maxFiles.getOrElse {
      if (options.maxBytesPerTrigger.isEmpty) {
        DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT
      } else {
        Int.MaxValue - 8 // - 8 to prevent JVM Array allocation OOM
      }
    }

    def toReadLimit: ReadLimit = {
      if (options.maxFilesPerTrigger.isDefined && options.maxBytesPerTrigger.isDefined) {
        CompositeLimit(
          ReadMaxBytes(options.maxBytesPerTrigger.get),
          ReadLimit.maxFiles(options.maxFilesPerTrigger.get).asInstanceOf[ReadMaxFiles])
      } else if (options.maxBytesPerTrigger.isDefined) {
        ReadMaxBytes(options.maxBytesPerTrigger.get)
      } else {
        ReadLimit.maxFiles(
          options.maxFilesPerTrigger.getOrElse(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT))
      }
    }
  }

  object AdmissionLimits {

    def apply(limit: ReadLimit): Option[AdmissionLimits] = limit match {
      case _: ReadAllAvailable => None
      case maxFiles: ReadMaxFiles => Some(new AdmissionLimits(Some(maxFiles.maxFiles())))
      case maxBytes: ReadMaxBytes => Some(new AdmissionLimits(None, maxBytes.maxBytes))
      case composite: CompositeLimit =>
        Some(new AdmissionLimits(Some(composite.maxFiles.maxFiles()), composite.bytes.maxBytes))
      case other => throw DeltaErrors.unknownReadLimit(other.toString())
    }
  }

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never been called.
   */
  protected lazy val getStartingVersion: Option[Long] = {
    // Note: returning a version beyond latest snapshot version won't be a problem as callers
    // of this function won't use the version to retrieve snapshot(refer to [[getStartingOffset]]).
    val allowOutOfRange =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP)
    /** DeltaOption validates input and ensures that only one is provided. */
    if (options.startingVersion.isDefined) {
      val v = options.startingVersion.get match {
        case StartingVersionLatest =>
          deltaLog.update(catalogTableOpt = catalogTableOpt).version + 1
        case StartingVersion(version) =>
          if (!DeltaSource.validateProtocolAt(spark, deltaLog, catalogTableOpt, version)) {
            // When starting from a given version, we don't require that the snapshot of this
            // version can be reconstructed, even though the input table is technically in an
            // inconsistent state. If the snapshot cannot be reconstructed, then the protocol
            // check is skipped, so this is technically not safe, but we keep it this way for
            // historical reasons.
            deltaLog.history.checkVersionExists(
              version, catalogTableOpt = None, mustBeRecreatable = false, allowOutOfRange)
          }
          version
      }
      Some(v)
    } else if (options.startingTimestamp.isDefined) {
      val tt: DeltaTimeTravelSpec = DeltaTimeTravelSpec(
        timestamp = options.startingTimestamp.map(Literal(_)),
        version = None,
        creationSource = Some("deltaSource"))
      Some(DeltaSource
        .getStartingVersionFromTimestamp(
          spark,
          deltaLog,
          catalogTableOpt,
          tt.getTimestamp(spark.sessionState.conf),
          allowOutOfRange))
    } else {
      None
    }
  }

}

object DeltaSource extends DeltaLogging {
  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than table feature exception, we suppress it. This allows to fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * Returns true when the validation was performed and succeeded.
   */
  def validateProtocolAt(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTableOpt: Option[CatalogTable],
      version: Long): Boolean = {
    val alwaysValidateProtocol = spark.sessionState.conf.getConf(
      DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL)
    if (!alwaysValidateProtocol) return false

    try {
      // We attempt to construct a snapshot at the startingVersion in order to validate the
      // protocol. If snapshot reconstruction fails, fall back to the old behavior where the
      // only requirement was for the commit to exist.
      deltaLog.getSnapshotAt(version, catalogTableOpt = catalogTableOpt)
      return true
    } catch {
      case e: DeltaUnsupportedTableFeatureException =>
        recordDeltaEvent(
          deltaLog = deltaLog,
          opType = "dropFeature.validateProtocolAt.unsupportedFeatureFound",
          data = Map("message" -> e.getMessage))
        throw e
      case NonFatal(e) => // Suppress rest errors.
        logWarning(log"Protocol validation failed with '${MDC(DeltaLogKeys.EXCEPTION, e)}'.")
        recordDeltaEvent(
          deltaLog = deltaLog,
          opType = "dropFeature.validateProtocolAt.error",
          data = Map("message" -> e.getMessage))
    }
    false
  }

  /**
   * - If a commit version exactly matches the provided timestamp, we return it.
   * - Otherwise, we return the earliest commit version
   *   with a timestamp greater than the provided one.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, and canExceedLatest is disabled we throw an error.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, and canExceedLatest is enabled we return a version that is greater
   *   than deltaLog.snapshot.version by one
   *
   * @param spark - current spark session
   * @param deltaLog - Delta log of the table for which we find the version.
   * @param catalogTableOpt - The CatalogTable for the Delta table.
   * @param timestamp - user specified timestamp
   * @param canExceedLatest - if true, version can be greater than the latest snapshot commit
   * @return - corresponding version number for timestamp
   */
  def getStartingVersionFromTimestamp(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTableOpt: Option[CatalogTable],
      timestamp: Timestamp,
      canExceedLatest: Boolean = false): Long = {
    val tz = spark.sessionState.conf.sessionLocalTimeZone
    val commit = deltaLog.history.getActiveCommitAtTime(
      timestamp,
      catalogTableOpt = catalogTableOpt,
      canReturnLastCommit = true,
      mustBeRecreatable = false,
      canReturnEarliestCommit = true)
    if (commit.timestamp >= timestamp.getTime) {
      validateProtocolAt(spark, deltaLog, catalogTableOpt, commit.version)
      // Find the commit at the `timestamp` or the earliest commit
      commit.version
    } else {
      // commit.timestamp is not the same, so this commit is a commit before the timestamp and
      // the next version if exists should be the earliest commit after the timestamp.
      // Note: `getActiveCommitAtTime` has called `update`, so we don't need to call it again.
      //
      // Note2: In the use case of [[CDCReader]] timestamp passed in can exceed the latest commit
      // timestamp, caller doesn't expect exception, and can handle the non-existent version.
      val latestNotExceeded = commit.version + 1 <= deltaLog.unsafeVolatileSnapshot.version
      if (latestNotExceeded || canExceedLatest) {
        if (latestNotExceeded) {
          validateProtocolAt(spark, deltaLog, catalogTableOpt, commit.version + 1)
        }
        commit.version + 1
      } else {
        val commitTs = new Timestamp(commit.timestamp)
        val timestampFormatter = TimestampFormatter(DateTimeUtils.getTimeZone(tz))
        val tsString = DateTimeUtils.timestampToString(
          timestampFormatter, DateTimeUtils.fromJavaTimestamp(commitTs))
        throw DeltaErrors.timestampGreaterThanLatestCommit(timestamp, commitTs, tsString)
      }
    }
  }

  /**
   * Read an [[ClosableIterator]] of Delta actions from file status, considering memory constraints
   */
  def createRewindableActionIterator(
      spark: SparkSession,
      deltaLog: DeltaLog,
      fileStatus: FileStatus): ClosableIterator[Action] with SupportsRewinding[Action] = {
    val threshold = spark.sessionState.conf.getConf(DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD)
    lazy val actions =
      deltaLog.store.read(fileStatus, deltaLog.newDeltaHadoopConf()).map(Action.fromJson)
    // Return a new [[CloseableIterator]] over the commit. If the commit is smaller than the
    // threshold, we will read it into memory once and iterate over that every time.
    // Otherwise, we read it again every time.
    val shouldLoadIntoMemory = fileStatus.getLen < threshold
    def createClosableIterator(): ClosableIterator[Action] = if (shouldLoadIntoMemory) {
      // Reuse in the memory actions
      actions.toIterator.toClosable
    } else {
      deltaLog.store.readAsIterator(fileStatus, deltaLog.newDeltaHadoopConf())
        .withClose {
          _.map(Action.fromJson)
        }
    }
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      var delegatedIterator: ClosableIterator[Action] = createClosableIterator()
      override def hasNext: Boolean = delegatedIterator.hasNext
      override def next(): Action = delegatedIterator.next()
      override def close(): Unit = delegatedIterator.close()
      override def rewind(): Unit = delegatedIterator = createClosableIterator()
    }
  }

  /**
   * Scan and get the last item of the iterator.
   */
  def iteratorLast[T](iter: ClosableIterator[T]): Option[T] = {
    try {
      var last: Option[T] = None
      while (iter.hasNext) {
        last = Some(iter.next())
      }
      last
    } finally {
      iter.close()
    }
  }
}

