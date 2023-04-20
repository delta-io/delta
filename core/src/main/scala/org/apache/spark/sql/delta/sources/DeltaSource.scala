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

import scala.collection.mutable
import scala.util.{Success, Try}
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.DeltaSourceSnapshot
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.spark.sql.delta.util.{DateTimeUtils, TimestampFormatter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType

/**
 * A case class to help with `Dataset` operations regarding Offset indexing, representing AddFile
 * actions in a Delta log. For proper offset tracking (SC-19523), there are also special sentinel
 * values with index = -1 and add = null.
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
    isLast: Boolean = false,
    shouldSkip: Boolean = false) {

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
  protected lazy val forceEnableStreamingReadOnColumnMappingSchemaChanges: Boolean =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES)

  /**
   * Flag that allows user to disable the read-compatibility check during stream start which
   * protects against an corner case in which verifyStreamHygiene could not detect.
   * This is a bug fix but yet a potential behavior change, so we add a flag to fallback.
   */
  protected lazy val forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_SATRT)

  /**
   * Flag that allow user to fallback to the legacy behavior in which user can allow nullable=false
   * schema to read nullable=true data, which is incorrect but a behavior change regardless.
   */
  protected lazy val forceEnableUnsafeReadOnNullabilityChange =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STREAM_UNSAFE_READ_ON_NULLABILITY_CHANGE)

  /**
   * The persisted schema from the schema log that must be used to read data files in this Delta
   * streaming source.
   */
  protected val persistedSchemaAtSourceInit: Option[PersistedSchema] =
    schemaLog.flatMap(_.getSchemaAtLogInit)

  /**
   * The read schema for this source during initialization, taking in account of SchemaLog.
   */
  protected val readSchemaAtSourceInit: StructType =
    persistedSchemaAtSourceInit.map(_.dataSchema).getOrElse {
      snapshotAtSourceInit.schema
    }


  /**
   * A global flag to mark whether we have done a per-stream start check for column mapping
   * schema changes (rename / drop).
   */
  protected var hasCheckedReadIncompatibleSchemaChangesOnStreamStart: Boolean = false

  override val schema: StructType = {
    val schemaWithoutCDC =
      ColumnWithDefaultExprUtils.removeDefaultExpressions(readSchemaAtSourceInit)
    if (options.readChangeFeed) {
      CDCReader.cdcReadSchema(schemaWithoutCDC)
    } else {
      schemaWithoutCDC
    }
  }

  /**
   * When `AvailableNow` is used, this offset will be the upper bound where this run of the query
   * will process up. We may run multiple micro batches, but the query will stop itself when it
   * reaches this offset.
   */
  protected var lastOffsetForTriggerAvailableNow: Option[DeltaSourceOffset] = None

  private var isLastOffsetForTriggerAvailableNowInitialized = false

  private var isTriggerAvailableNow = false

  override def prepareForTriggerAvailableNow(): Unit = {
    logInfo("The streaming query reports to use Trigger.AvailableNow.")
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
      logInfo("lastOffset for Trigger.AvailableNow has set to ${lastOffset.json}")
    }
  }

  /** An internal `latestOffsetInternal` to get the latest offset. */
  protected def latestOffsetInternal(
    startOffset: Option[DeltaSourceOffset], limit: ReadLimit): Option[DeltaSourceOffset]

  protected def getFileChangesWithRateLimit(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits] = Some(new AdmissionLimits())):
    ClosableIterator[IndexedFile] = {
    if (options.readChangeFeed) {
      // in this CDC use case, we need to consider RemoveFile and AddCDCFiles when getting the
      // offset.

      // This method is only used to get the offset so we need to return an iterator of IndexedFile.
      getFileChangesForCDC(fromVersion, fromIndex, isStartingVersion, limits, None).flatMap(_._2)
        .toClosable
    } else {
      val changes = getFileChanges(fromVersion, fromIndex, isStartingVersion)
      if (limits.isEmpty) return changes

      // Take each change until we've seen the configured number of addFiles. Some changes don't
      // represent file additions; we retain them for offset tracking, but they don't count towards
      // the maxFilesPerTrigger conf.
      val admissionControl = limits.get
      changes.withClose { it =>
        it.takeWhile { index =>
          admissionControl.admit(Option(index.add))
        }
      }
    }
  }

  /**
   * get the changes from startVersion, startIndex to the end
   * @param startVersion - calculated starting version
   * @param startIndex - calculated starting index
   * @param isStartingVersion - whether the stream has to return the initial snapshot or not
   * @param endOffset - Offset that signifies the end of the stream.
   * @return
   */
  protected def getFileChangesAndCreateDataFrame(
      startVersion: Long,
      startIndex: Long,
      isStartingVersion: Boolean,
      endOffset: DeltaSourceOffset): DataFrame = {
    if (options.readChangeFeed) {
      getCDCFileChangesAndCreateDataFrame(startVersion, startIndex, isStartingVersion, endOffset)
    } else {
      val changes = getFileChanges(startVersion, startIndex, isStartingVersion)
      try {
        val fileActionsIter = changes.takeWhile { case IndexedFile(version, index, _, _, _, _, _) =>
          version < endOffset.reservoirVersion ||
            (version == endOffset.reservoirVersion && index <= endOffset.index)
        }

        val filteredIndexedFiles = fileActionsIter.filter { indexedFile =>
          indexedFile.getFileAction != null &&
            excludeRegex.forall(_.findFirstIn(indexedFile.getFileAction.path).isEmpty)
        }

        createDataFrame(filteredIndexedFiles)
      } finally {
        changes.close()
      }
    }
  }

  /**
   * Given an iterator of file actions, create a DataFrame representing the files added to a table
   * Only AddFile actions will be used to create the DataFrame.
   * @param indexedFiles actions iterator from which to generate the DataFrame.
   */
  protected def createDataFrame(indexedFiles: Iterator[IndexedFile]): DataFrame = {
    val addFilesList = indexedFiles
        .map(_.getFileAction)
        .filter(_.isInstanceOf[AddFile])
        .asInstanceOf[Iterator[AddFile]].toArray

    deltaLog.createDataFrame(
      snapshotAtSourceInit,
      addFilesList,
      isStreaming = true,
      customReadSchema = persistedSchemaAtSourceInit
    )
  }

  /**
   * Returns the offset that starts from a specific delta table version. This function is
   * called when starting a new stream query.
   * @param fromVersion The version of the delta table to calculate the offset from.
   * @param isStartingVersion Whether the delta version is for the initial snapshot or not.
   * @param limits Indicates how much data can be processed by a micro batch.
   */
  protected def getStartingOffsetFromSpecificDeltaVersion(
      fromVersion: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {

    val changes = getFileChangesWithRateLimit(
      fromVersion,
      fromIndex = -1L,
      isStartingVersion = isStartingVersion,
      limits)
    val lastFileChange = iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      None
    } else {
      // Block latestOffset() from generating an invalid offset by proactively verifying
      // incompatible schema changes under column mapping. See more details in the method doc.
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(fromVersion)
      buildOffsetFromIndexedFile(lastFileChange.get, fromVersion, isStartingVersion)
    }
  }

  /**
   * Return the next offset when previous offset exists.
   */
  protected def getNextOffsetFromPreviousOffset(
      previousOffset: DeltaSourceOffset,
      limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {
    val changes = getFileChangesWithRateLimit(
      previousOffset.reservoirVersion,
      previousOffset.index,
      previousOffset.isStartingVersion,
      limits)
    val lastFileChange = iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      Some(previousOffset)
    } else {
      // Similarly, block latestOffset() from generating an invalid offset by proactively verifying
      // incompatible schema changes under column mapping. See more details in the method doc.
      checkReadIncompatibleSchemaChangeOnStreamStartOnce(previousOffset.reservoirVersion)
      buildOffsetFromIndexedFile(lastFileChange.get, previousOffset.reservoirVersion,
        previousOffset.isStartingVersion)
    }
  }

  /**
   * Build the latest offset based on the last indexedFile. The function also checks if latest
   * version is valid by comparing with previous version.
   * @param indexedFile The last indexed file used to build offset from.
   * @param version Previous offset reservoir version.
   * @param isStartingVersion Whether previous offset is starting version or not.
   */
  private def buildOffsetFromIndexedFile(
      indexedFile: IndexedFile,
      version: Long,
      isStartingVersion: Boolean): Option[DeltaSourceOffset] = {
    val IndexedFile(v, i, _, _, _, isLastFileInVersion, _) = indexedFile
    assert(v >= version,
      s"buildOffsetFromIndexedFile returns an invalid version: $v (expected: >= $version), " +
        s"tableId: $tableId")

    // If the last file in previous batch is the last file of that version, automatically bump
    // to next version to skip accessing that version file altogether.
    if (isLastFileInVersion) {
      // isStartingVersion must be false here as we have bumped the version.
      Some(DeltaSourceOffset(DeltaSourceOffset.VERSION_1, tableId, v + 1, index = -1,
        isStartingVersion = false))
    } else {
      // isStartingVersion will be true only if previous isStartingVersion is true and the next file
      // is still at the same version (i.e v == version).
      Some(DeltaSourceOffset(DeltaSourceOffset.VERSION_1, tableId, v, i,
        isStartingVersion = v == version && isStartingVersion))
    }
  }

  /**
   * Return the DataFrame between start and end offset.
   */
  protected def createDataFrameBetweenOffsets(
      startVersion: Long,
      startIndex: Long,
      isStartingVersion: Boolean,
      startSourceVersion: Option[Long],
      startOffsetOption: Option[DeltaSourceOffset],
      endOffset: DeltaSourceOffset): DataFrame = {
    getFileChangesAndCreateDataFrame(startVersion, startIndex, isStartingVersion, endOffset)
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
   * TODO: unblock the column mapping side after we roll out the proper semantics.
   */
  protected def checkReadIncompatibleSchemaChangeOnStreamStartOnce(startVersion: Long): Unit = {
    if (hasCheckedReadIncompatibleSchemaChangesOnStreamStart) {
      return
    }
    // Column-mapping related schema changes, all of them should not be retryable
    if (snapshotAtSourceInit.metadata.columnMappingMode != NoMapping &&
        !forceEnableStreamingReadOnColumnMappingSchemaChanges) {

      val snapshotAtStartVersionToScan = try {
        getSnapshotFromDeltaLog(startVersion)
      } catch {
        case NonFatal(e) =>
          // If we could not construct a snapshot, unfortunately there isn't much we could do
          // to completely ensure data consistency.
          throw DeltaErrors.failedToGetSnapshotDuringColumnMappingStreamingReadCheck(e)
      }

      // Compare stream metadata with that to detect column mapping schema changes
      if (!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
          snapshotAtSourceInit.metadata, snapshotAtStartVersionToScan.metadata)) {
        throw DeltaErrors.blockStreamingReadsOnColumnMappingEnabledTable(
          readSchema = snapshotAtSourceInit.schema,
          incompatibleSchema = snapshotAtStartVersionToScan.schema,
          isCdfRead = options.readChangeFeed,
          detectedDuringStreaming = false
        )
      }
    }

    if (!forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart) {
      // Other read-incompatible schema changes, some of them can be retryable
      Try(getSnapshotFromDeltaLog(startVersion)) match {
        case Success(startSnapshot) =>
          verifySchemaChange(startSnapshot.schema, startSnapshot.version)
        case _ =>
        // Don't regress and fail if we could not calculate such snapshot.
      }
    }
    // Mark as checked
    hasCheckedReadIncompatibleSchemaChangesOnStreamStart = true
  }

  /**
   * Check column mapping schema changes during stream execution. It does the following:
   * 1. Unifies the error messages thrown when encountering rename/drop column during streaming.
   * 2. Prevents a tricky case in which when a drop column happened, the read compatibility check
   *    could NOT detect that, so it would move PAST that version and later MicroBatchExecution
   *    would throw a different exception. BUT, if the stream restarts, it would start serving
   *    the batches POST the drop column and none of our checks will be able to capture that.
   *    This check, however, would make sure the stream to NOT move past the drop column change so
   *    next time when stream restarts, it would be blocked right again.
   *
   * We need to compare change versions so that we won't accidentally block an ADD column by
   * detecting it as a reverse DROP column.
   *
   * TODO: unblock this after we roll out the proper semantics.
   */
  protected def checkColumnMappingSchemaChangesDuringStreaming(
      curMetadata: Metadata,
      curVersion: Long): Unit = {

    val metadataAtSourceInit = snapshotAtSourceInit.metadata

    if (metadataAtSourceInit.columnMappingMode != NoMapping &&
      !forceEnableStreamingReadOnColumnMappingSchemaChanges) {
      if (curVersion < snapshotAtSourceInit.version) {
        // Stream version is newer, ensure there's no column mapping schema changes
        // from cur -> stream.
        if (!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
            metadataAtSourceInit, curMetadata)) {
          throw DeltaErrors.blockStreamingReadsOnColumnMappingEnabledTable(
            curMetadata.schema,
            metadataAtSourceInit.schema,
            isCdfRead = options.readChangeFeed,
            detectedDuringStreaming = true)
        }
      } else {
        // Current metadata action version is newer, ensure there's no column mapping schema changes
        // from stream -> cur.
        if (!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
            curMetadata, metadataAtSourceInit)) {
          throw DeltaErrors.blockStreamingReadsOnColumnMappingEnabledTable(
            metadataAtSourceInit.schema,
            curMetadata.schema,
            isCdfRead = options.readChangeFeed,
            detectedDuringStreaming = true)
        }
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
    options: DeltaOptions,
    snapshotAtSourceInit: Snapshot,
    schemaLog: Option[DeltaSourceSchemaLog] = None,
    filters: Seq[Expression] = Nil)
  extends DeltaSourceBase
  with DeltaSourceCDCSupport {

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

  logInfo(s"Filters being pushed down: $filters")

  /**
   * Get the changes starting from (startVersion, startIndex). The start point should not be
   * included in the result.
   *
   * If verifyMetadataAction = true, we will break the stream when we detect any read-incompatible
   * metadata changes.
   */
  protected def getFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      verifyMetadataAction: Boolean = true): ClosableIterator[IndexedFile] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): ClosableIterator[IndexedFile] = {
      deltaLog.getChangeLogFiles(startVersion, options.failOnDataLoss).flatMapWithClose {
        case (version, filestatus) =>
          lazy val inMemoryActions = deltaLog.store.read(filestatus, deltaLog.newDeltaHadoopConf())
            .map(Action.fromJson)
          val threshold = spark.sessionState.conf.getConf(DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD)

          // Return a new [[CloseableIterator]] over the commit. If the commit is smaller than the
          // threshold, we will read it into memory once and iterate over that every time.
          // Otherwise, we read it again every time.
          def createActionsIterator() =
            if (filestatus.getLen < threshold) {
              inMemoryActions.toIterator.toClosable
            } else {
              deltaLog.store.readAsIterator(filestatus, deltaLog.newDeltaHadoopConf())
                .withClose { _.map(Action.fromJson) }
            }

          // First pass reads the whole commit and closes the iterator.
          val shouldSkipCommit = createActionsIterator().processAndClose { actionsIter =>
            validateCommitAndDecideSkipping(actionsIter, version, verifyMetadataAction)
          }

          // Second pass reads the commit lazily.
          createActionsIterator().withClose { actionsIter =>
            filterAndGetIndexedFiles(actionsIter, version, shouldSkipCommit)
          }
      }
    }

    var iter = if (isStartingVersion) {
      Iterator(1, 2).flatMapWithClose {  // so that the filterAndIndexDeltaLogs call is lazy
        case 1 => getSnapshotAt(fromVersion).toClosable
        case 2 => filterAndIndexDeltaLogs(fromVersion + 1)
      }
    } else {
      filterAndIndexDeltaLogs(fromVersion)
    }

    iter = iter.withClose { it =>
      it.filter { case IndexedFile(version, index, _, _, _, _, _) =>
        version > fromVersion || index > fromIndex
      }
    }

    lastOffsetForTriggerAvailableNow.foreach { bound =>
      iter = iter.withClose { it =>
        it.filter { case file =>
          file.version < bound.reservoirVersion ||
            (file.version == bound.reservoirVersion && file.index <= bound.index)
        }
      }
    }
    iter
  }

  protected def getSnapshotAt(version: Long): Iterator[IndexedFile] = {
    if (initialState == null || version != initialStateVersion) {
      super[DeltaSourceBase].cleanUpSnapshotResources()
      val snapshot = getSnapshotFromDeltaLog(version)

      initialState = new DeltaSourceSnapshot(spark, snapshot, filters)
      initialStateVersion = version
    }
    initialState.iterator()
  }

  /**
   * Narrow-waist for generating snapshot from Delta Log within Delta Source
   */
  protected def getSnapshotFromDeltaLog(version: Long): Snapshot = {
    try {
      deltaLog.getSnapshotAt(version)
    } catch {
      case e: FileNotFoundException =>
        throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(e)
    }
  }

  protected def iteratorLast[T](iter: ClosableIterator[T]): Option[T] = {
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

  private def getStartingOffset(limits: Option[AdmissionLimits]): Option[DeltaSourceOffset] = {

    val (version, isStartingVersion) = getStartingVersion match {
      case Some(v) => (v, false)
      case None => (snapshotAtSourceInit.version, true)
    }
    if (version < 0) {
      return None
    }

    getStartingOffsetFromSpecificDeltaVersion(version, isStartingVersion, limits)
  }

  override def getDefaultReadLimit: ReadLimit = {
    new AdmissionLimits().toReadLimit
  }

  def toDeltaSourceOffset(offset: streaming.Offset): DeltaSourceOffset = {
    DeltaSourceOffset(tableId, offset)
  }

  /**
   * This should only be called by the engine. Call `latestOffsetInternal` instead if you need to
   * get the latest offset.
   */
  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
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
   * Verify whether the schema change in `version` is safe to continue. If not, throw an exception
   * to fail the query.
   */
  protected def verifySchemaChange(schemaChange: StructType, version: Long): Unit = {
    // There is a schema change. All of files after this commit will use `schemaChange`. Hence, we
    // check whether we can use `schema` (the fixed source schema we use in the same run of the
    // query) to read these new files safely.
    val backfilling = version < snapshotAtSourceInit.version
    // We forbid the case when the the schemaChange is nullable while the read schema is NOT
    // nullable, or in other words, `schema` should not tighten nullability from `schemaChange`,
    // because we don't ever want to read back any nulls when the read schema is non-nullable.
    val shouldForbidTightenNullability = !forceEnableUnsafeReadOnNullabilityChange
    if (!SchemaUtils.isReadCompatible(
        schemaChange, schema, forbidTightenNullability = shouldForbidTightenNullability)) {
      // Only schema change later than the current read snapshot/schema can be retried, in other
      // words, backfills could never be retryable, because we have no way to refresh
      // the latest schema to "catch up" when the schema change happens before than current read
      // schema version.
      // If not backfilling, we do another check to determine retryability, in which we assume that
      // we will be reading using this later `schemaChange` back on the current outdated `schema`,
      // and if it works (including that `schemaChange` should not tighten the nullability
      // constraint from `schema`), it is a retryable exception.
      val retryable = !backfilling && SchemaUtils.isReadCompatible(
        schema, schemaChange, forbidTightenNullability = shouldForbidTightenNullability)
      throw DeltaErrors.schemaChangedException(
        schema,
        schemaChange,
        retryable = retryable,
        Some(version),
        includeStartingVersionOrTimestampMessage = options.containsStartingVersionOrTimestamp)
    }
  }

  /**
   * Filter the iterator with only add files that contain data change and get indexed files.
   * @return indexed add files
   */
  private def filterAndGetIndexedFiles(
      iterator: Iterator[Action],
      version: Long,
      shouldSkipCommit: Boolean): Iterator[IndexedFile] = {
    val filteredIterator =
      if (shouldSkipCommit) {
        Iterator.empty.toClosable
      } else iterator.filter {
        case a: AddFile if a.dataChange => true
        case _ => false
      }
    Iterator.single(IndexedFile(version, -1, null)) ++ filteredIterator
      .map(_.asInstanceOf[AddFile])
      .zipWithIndex.map { case (action, index) =>
      IndexedFile(version, index.toLong, action, isLast = !iterator.hasNext)
    }
  }

  /**
   * Check stream for violating any constraints.
   *
   * If verifyMetadataAction = true, we will break the stream when we detect any read-incompatible
   * metadata changes.
   *
   * @return true if commit should be skipped
   */
  protected def validateCommitAndDecideSkipping(
      actions: Iterator[Action],
      version: Long,
      verifyMetadataAction: Boolean = true): Boolean = {
    /** A check on the source table that disallows changes on the source data. */
    val shouldAllowChanges = options.ignoreChanges || ignoreFileDeletion || skipChangeCommits
    /** A check on the source table that disallows commits that only include deletes to the data. */
    val shouldAllowDeletes = shouldAllowChanges || options.ignoreDeletes || ignoreFileDeletion

    var seenFileAdd = false
    var skippedCommit = false
    var removeFileActionPath: Option[String] = None
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
          checkColumnMappingSchemaChangesDuringStreaming(m, version)
          verifySchemaChange(m.schema, version)
        }
      case protocol: Protocol =>
        deltaLog.protocolRead(protocol)
      case _ => ()
    }
    if (removeFileActionPath.isDefined) {
      if (seenFileAdd && !shouldAllowChanges) {
        throw DeltaErrors.deltaSourceIgnoreChangesError(
          version,
          removeFileActionPath.get,
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
    skippedCommit
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    val endOffset = toDeltaSourceOffset(end)
    val startDeltaOffsetOption = startOffsetOption.map(toDeltaSourceOffset)

    val (startVersion,
        startIndex,
        isStartingVersion,
        startSourceVersion) = if (startOffsetOption.isEmpty) {
      getStartingVersion match {
        case Some(v) =>
          (v, -1L, false, None)

        case _ =>
          if (endOffset.isStartingVersion) {
            (endOffset.reservoirVersion, -1L, true, None)
          } else {
            assert(
              endOffset.reservoirVersion > 0, s"invalid reservoirVersion in endOffset: $endOffset")
            // Load from snapshot `endOffset.reservoirVersion - 1L` so that `index` in `endOffset`
            // is still valid.
            (endOffset.reservoirVersion - 1L, -1L, true, None)
          }
      }
    } else {
      val startOffset = startDeltaOffsetOption.get
      if (!startOffset.isStartingVersion) {
        // unpersist `snapshot` because it won't be used any more.
        cleanUpSnapshotResources()
      }
      if (startOffset == endOffset) {
        // This happens only if we recover from a failure and `MicroBatchExecution` tries to call
        // us with the previous offsets. The returned DataFrame will be dropped immediately, so we
        // can return any DataFrame.
        return spark.sqlContext.internalCreateDataFrame(
          spark.sparkContext.emptyRDD[InternalRow], schema, isStreaming = true)
      }
      (startOffset.reservoirVersion, startOffset.index, startOffset.isStartingVersion,
        Some(startOffset.sourceVersion))
    }
    val offsetRangeInfo = s"(getBatch)start: $startDeltaOffsetOption end: $end"
    if (endOffset.reservoirVersion - startVersion > 1000L) {
      // Improve the log level if the source is processing a large batch.
      logInfo(offsetRangeInfo)
    } else {
      logDebug(offsetRangeInfo)
    }

    // Check for column mapping + streaming incompatible schema changes
    // Note for initial snapshot, the startVersion should be the same as the latestOffset's version
    // and therefore this check won't have any effect.
    checkReadIncompatibleSchemaChangeOnStreamStartOnce(startVersion)

    val createdDf = createDataFrameBetweenOffsets(startVersion, startIndex, isStartingVersion,
      startSourceVersion, startDeltaOffsetOption, endOffset)

    createdDf
  }

  override def stop(): Unit = {
    cleanUpSnapshotResources()
  }

  override def toString(): String = s"DeltaSource[${deltaLog.dataPath}]"

  trait DeltaSourceAdmissionBase { self: AdmissionLimits =>
    // This variable indicates whether a commit has already been processed by a batch or not.
    var commitProcessedInBatch = false

    /**
     * This overloaded method checks if all the AddCDCFiles for a commit can be accommodated by
     * the rate limit.
     */
    def admit(fileActions: Seq[AddCDCFile]): Boolean = {
      def getSize(actions: Seq[AddCDCFile]): Long = {
        actions.foldLeft(0L) { (l, r) => l + r.size }
      }
      if (fileActions.isEmpty) {
        true
      } else {
        // if no files have been admitted, then admit all to avoid deadlock
        // else check if all of the files together satisfy the limit, only then admit
        val shouldAdmit = !commitProcessedInBatch ||
          (filesToTake - fileActions.size >= 0 && bytesToTake - getSize(fileActions) >= 0)

        commitProcessedInBatch = true
        filesToTake -= fileActions.size
        bytesToTake -= getSize(fileActions)
        shouldAdmit
      }
    }

    /** Whether to admit the next file */
    def admit(fileAction: Option[FileAction]): Boolean = {
      commitProcessedInBatch = true

      def getSize(action: FileAction): Long = {
        action match {
          case a: AddFile =>
            a.size
          case r: RemoveFile =>
            r.size.getOrElse(0L)
          case cdc: AddCDCFile =>
            cdc.size
        }
      }

      val shouldAdmit = hasCapacity

      if (fileAction.isEmpty) {
        return shouldAdmit
      }

      filesToTake -= 1

      bytesToTake -= getSize(fileAction.get)
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
  class AdmissionLimits(
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
          deltaLog.update().version + 1
        case StartingVersion(version) =>
          // when starting from a given version, we don't need the snapshot of this version. So
          // `mustBeRecreatable` is set to `false`.
          deltaLog.history.checkVersionExists(version, mustBeRecreatable = false, allowOutOfRange)
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
          tt.getTimestamp(spark.sessionState.conf),
          allowOutOfRange))
    } else {
      None
    }
  }
}

object DeltaSource {

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
   * @param timestamp - user specified timestamp
   * @param canExceedLatest - if true, version can be greater than the latest snapshot commit
   * @return - corresponding version number for timestamp
   */
  def getStartingVersionFromTimestamp(
      spark: SparkSession,
      deltaLog: DeltaLog,
      timestamp: Timestamp,
      canExceedLatest: Boolean = false): Long = {
    val tz = spark.sessionState.conf.sessionLocalTimeZone
    val commit = deltaLog.history.getActiveCommitAtTime(
      timestamp,
      canReturnLastCommit = true,
      mustBeRecreatable = false,
      canReturnEarliestCommit = true)
    if (commit.timestamp >= timestamp.getTime) {
      // Find the commit at the `timestamp` or the earliest commit
      commit.version
    } else {
      // commit.timestamp is not the same, so this commit is a commit before the timestamp and
      // the next version if exists should be the earliest commit after the timestamp.
      // Note: `getActiveCommitAtTime` has called `update`, so we don't need to call it again.
      //
      // Note2: In the use case of [[CDCReader]] timestamp passed in can exceed the latest commit
      // timestamp, caller doesn't expect exception, and can handle the non-existent version.
      if (commit.version + 1 <= deltaLog.unsafeVolatileSnapshot.version || canExceedLatest) {
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
}

