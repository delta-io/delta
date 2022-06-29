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

import scala.util.matching.Regex

import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaErrors, DeltaLog, DeltaOptions, DeltaTimeTravelSpec, GeneratedColumn, NoMapping, Snapshot, StartingVersion, StartingVersionLatest}
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
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl}
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
 */
private[delta] case class IndexedFile(
    version: Long,
    index: Long,
    add: AddFile,
    remove: RemoveFile = null,
    cdc: AddCDCFile = null,
    isLast: Boolean = false) {

  def getFileAction: FileAction = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else {
      cdc
    }
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
    with DeltaLogging { self: DeltaSource =>

  override val schema: StructType = {
    val schemaWithoutCDC =
      ColumnWithDefaultExprUtils.removeDefaultExpressions(deltaLog.snapshot.metadata.schema)
    if (options.readChangeFeed) {
      CDCReader.cdcReadSchema(schemaWithoutCDC)
    } else {
      schemaWithoutCDC
    }
  }

  protected var lastOffsetForTriggerAvailableNow: DeltaSourceOffset = _

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
      var admissionControl = limits.get
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
        val fileActionsIter = changes.takeWhile { case IndexedFile(version, index, _, _, _, _) =>
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
      deltaLog.snapshot,
      addFilesList,
      isStreaming = true)
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
      limits: Option[AdmissionLimits]): Option[Offset] = {
    val changes = getFileChangesWithRateLimit(
      fromVersion,
      fromIndex = -1L,
      isStartingVersion = isStartingVersion,
      limits)
    val lastFileChange = iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      None
    } else {
      buildOffsetFromIndexedFile(lastFileChange.get, fromVersion, isStartingVersion)
    }
  }

  /**
   * Return the next offset when previous offset exists.
   */
  protected def getNextOffsetFromPreviousOffset(
      previousOffset: DeltaSourceOffset,
      limits: Option[AdmissionLimits]): Option[Offset] = {
    val changes = getFileChangesWithRateLimit(
      previousOffset.reservoirVersion,
      previousOffset.index,
      previousOffset.isStartingVersion,
      limits)
    val lastFileChange = iteratorLast(changes)

    if (lastFileChange.isEmpty) {
      Some(previousOffset)
    } else {
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
    val IndexedFile(v, i, _, _, _, isLastFileInVersion) = indexedFile
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
      startOffsetOption: Option[Offset],
      endOffset: DeltaSourceOffset): DataFrame = {
    getFileChangesAndCreateDataFrame(startVersion, startIndex, isStartingVersion, endOffset)
  }

  protected def cleanUpSnapshotResources(): Unit = {
    if (initialState != null) {
      initialState.close(unpersistSnapshot = initialStateVersion < deltaLog.snapshot.version)
      initialState = null
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
    filters: Seq[Expression] = Nil)
  extends DeltaSourceBase
  with DeltaSourceCDCSupport {

  // Deprecated. Please use `ignoreDeletes` or `ignoreChanges` from now on.
  private val ignoreFileDeletion = {
    if (options.ignoreFileDeletion) {
      logConsole(DeltaErrors.ignoreStreamingUpdatesAndDeletesWarning(spark))
      recordDeltaEvent(deltaLog, "delta.deprecation.ignoreFileDeletion")
    }
    options.ignoreFileDeletion
  }

  /** A check on the source table that disallows deletes on the source data. */
  private val ignoreChanges = options.ignoreChanges || ignoreFileDeletion

  /** A check on the source table that disallows commits that only include deletes to the data. */
  private val ignoreDeletes = options.ignoreDeletes || ignoreFileDeletion || ignoreChanges

  protected val excludeRegex: Option[Regex] = options.excludeRegex

  // This was checked before creating ReservoirSource
  assert(schema.nonEmpty)

  protected val tableId = deltaLog.snapshot.metadata.id

  private var previousOffset: DeltaSourceOffset = null

  // A metadata snapshot when starting the query.
  protected var initialState: DeltaSourceSnapshot = null
  protected var initialStateVersion: Long = -1L

  /**
   * Get the changes starting from (startVersion, startIndex). The start point should not be
   * included in the result.
   */
  protected def getFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean): ClosableIterator[IndexedFile] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): ClosableIterator[IndexedFile] = {
      deltaLog.getChangeLogFiles(startVersion, options.failOnDataLoss).flatMapWithClose {
        case (version, filestatus) =>
          if (filestatus.getLen <
            spark.sessionState.conf.getConf(DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD)) {
            // entire file can be read into memory
            val actions = deltaLog.store.read(filestatus.getPath, deltaLog.newDeltaHadoopConf())
              .map(Action.fromJson)
            val addFiles = verifyStreamHygieneAndFilterAddFiles(actions, version)

            (Iterator.single(IndexedFile(version, -1, null)) ++ addFiles
              .map(_.asInstanceOf[AddFile])
              .zipWithIndex.map { case (action, index) =>
              IndexedFile(version, index.toLong, action, isLast = index + 1 == addFiles.size)
            }).toClosable

          } else { // file too large to read into memory
            var fileIterator = deltaLog.store.readAsIterator(
              filestatus.getPath,
              deltaLog.newDeltaHadoopConf())
            try {
              verifyStreamHygiene(fileIterator.map(Action.fromJson), version)
            } finally {
              fileIterator.close()
            }
            fileIterator = deltaLog.store.readAsIterator(
              filestatus.getPath,
              deltaLog.newDeltaHadoopConf())
            fileIterator.withClose { it =>
              val addFiles = it.map(Action.fromJson).filter {
                case a: AddFile if a.dataChange => true
                case _ => false
              }
              Iterator.single(IndexedFile(version, -1, null)) ++ addFiles
                .map(_.asInstanceOf[AddFile])
                .zipWithIndex
                .map { case (action, index) =>
                  IndexedFile(version, index.toLong, action, isLast = !addFiles.hasNext)
                }
            }
          }
      }
    }

    // If the table has column mapping enabled, throw an error. With column mapping, certain schema
    // changes are possible (rename a column or drop a column) which don't work well with streaming.
    if (deltaLog.snapshot.metadata.columnMappingMode != NoMapping) {
      throw DeltaErrors.blockStreamingReadsOnColumnMappingEnabledTable
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
      it.filter { case IndexedFile(version, index, _, _, _, _) =>
        version > fromVersion || (index == -1 || index > fromIndex)
      }
    }

    if (lastOffsetForTriggerAvailableNow != null) {
      iter = iter.withClose { it =>
        it.filter { case IndexedFile(version, index, _, _, _, _) =>
          version < lastOffsetForTriggerAvailableNow.reservoirVersion ||
            (version == lastOffsetForTriggerAvailableNow.reservoirVersion &&
              index <= lastOffsetForTriggerAvailableNow.index)
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

  private def getStartingOffset(
      limits: Option[AdmissionLimits] = Some(new AdmissionLimits())): Option[Offset] = {

    val (version, isStartingVersion) = getStartingVersion match {
      case Some(v) => (v, false)
      case None => (deltaLog.snapshot.version, true)
    }
    if (version < 0) {
      return None
    }

    getStartingOffsetFromSpecificDeltaVersion(version, isStartingVersion, limits)
  }

  override def getDefaultReadLimit: ReadLimit = {
    new AdmissionLimits().toReadLimit
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val limits = AdmissionLimits(limit)

    val currentOffset = if (previousOffset == null) {
      getStartingOffset(limits)
    } else {
      getNextOffsetFromPreviousOffset(previousOffset, limits)
    }
    logDebug(s"previousOffset -> currentOffset: $previousOffset -> $currentOffset")
    currentOffset.orNull
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  protected def verifyStreamHygiene(
      actions: Iterator[Action],
      version: Long): Unit = {
    var seenFileAdd = false
    var removeFileActionPath: Option[String] = None
    actions.foreach{
      case a: AddFile if a.dataChange =>
        seenFileAdd = true
      case r: RemoveFile if r.dataChange =>
        if (removeFileActionPath.isEmpty) {
          removeFileActionPath = Some(r.path)
        }
      case m: Metadata =>
        if (!SchemaUtils.isReadCompatible(m.schema, schema)) {
          throw DeltaErrors.schemaChangedException(schema, m.schema, false)
        }
      case protocol: Protocol =>
        deltaLog.protocolRead(protocol)
      case _ => ()
    }
    if (removeFileActionPath.isDefined) {
      if (seenFileAdd && !ignoreChanges) {
        throw DeltaErrors.deltaSourceIgnoreChangesError(version, removeFileActionPath.get)
      } else if (!seenFileAdd && !ignoreDeletes) {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFileActionPath.get)
      }
    }
  }

  protected def verifyStreamHygieneAndFilterAddFiles(
    actions: Seq[Action],
    version: Long): Seq[Action] = {
    var seenFileAdd = false
    var removeFileActionPath: Option[String] = None
    val filteredActions = actions.filter {
      case a: AddFile if a.dataChange =>
        seenFileAdd = true
        true
      case a: AddFile =>
        // Created by a data compaction
        false
      case r: RemoveFile if r.dataChange =>
        if (removeFileActionPath.isEmpty) {
          removeFileActionPath = Some(r.path)
        }
        false
      case _: AddCDCFile =>
        false
      case _: RemoveFile =>
        false
      case m: Metadata =>
        if (!SchemaUtils.isReadCompatible(m.schema, schema)) {
          throw DeltaErrors.schemaChangedException(schema, m.schema, false)
        }
        false
      case protocol: Protocol =>
        deltaLog.protocolRead(protocol)
        false
      case _: SetTransaction | _: CommitInfo =>
        false
      case null => // Some crazy future feature. Ignore
        false
    }
    if (removeFileActionPath.isDefined) {
      if (seenFileAdd && !ignoreChanges) {
        throw DeltaErrors.deltaSourceIgnoreChangesError(version, removeFileActionPath.get)
      } else if (!seenFileAdd && !ignoreDeletes) {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFileActionPath.get)
      }
    }

    filteredActions
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    val endOffset = DeltaSourceOffset(tableId, end)
    previousOffset = endOffset // For recovery

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
      val startOffset = DeltaSourceOffset(tableId, startOffsetOption.get)
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
    logDebug(s"start: $startOffsetOption end: $end")

    val createdDf = createDataFrameBetweenOffsets(startVersion, startIndex, isStartingVersion,
      startSourceVersion, startOffsetOption, endOffset)

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

      if (fileAction.isEmpty) return true
      val shouldAdmit = filesToTake > 0 && bytesToTake > 0
      filesToTake -= 1

      bytesToTake -= getSize(fileAction.get)
      shouldAdmit
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
        Some(new AdmissionLimits(Some(composite.files.maxFiles()), composite.bytes.maxBytes))
      case other => throw new UnsupportedOperationException(s"Unknown ReadLimit: $other")
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
      if (commit.version + 1 <= deltaLog.snapshot.version || canExceedLatest) {
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

