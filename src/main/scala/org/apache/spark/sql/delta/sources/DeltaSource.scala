/*
 * Copyright 2019 Databricks, Inc.
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

import scala.util.matching.Regex

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.files.DeltaSourceSnapshot
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
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
 * @param isLast A flag to indicate whether this is the last AddFile in the version. This is used
 *               to resolve an off-by-one issue in the streaming offset interface; once we've read
 *               to the end of a log version file, we check this flag to advance immediately to the
 *               next one in the persisted offset. Without this special case we would re-read the
 *               already completed log file.
 */
private[delta] case class IndexedFile(
    version: Long, index: Long, add: AddFile, isLast: Boolean = false)

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
    filters: Seq[Expression] = Nil) extends Source with DeltaLogging {

  private val maxFilesPerTrigger = options.maxFilesPerTrigger

  // Deprecated. Please use `ignoreDeletes` or `ignoreChanges` from now on.
  private val ignoreFileDeletion = {
    if (options.ignoreFileDeletion) {
      val docPage = DeltaErrors.baseDocsPath(spark.sparkContext.getConf) +
        "/delta/delta-streaming.html#ignoring-updates-and-deletes"
      logConsole(
        s"""WARNING: The 'ignoreFileDeletion' option is deprecated. Switch to using one of
           |'ignoreDeletes' or 'ignoreChanges'. Refer to $docPage for details.
         """.stripMargin)
      recordDeltaEvent(deltaLog, "delta.deprecation.ignoreFileDeletion")
    }
    options.ignoreFileDeletion
  }

  /** A check on the source table that disallows deletes on the source data. */
  private val ignoreChanges = options.ignoreChanges || ignoreFileDeletion

  /** A check on the source table that disallows commits that only include deletes to the data. */
  private val ignoreDeletes = options.ignoreDeletes || ignoreFileDeletion || ignoreChanges

  private val excludeRegex: Option[Regex] = options.excludeRegex

  override val schema: StructType = deltaLog.snapshot.metadata.schema

  // This was checked before creating ReservoirSource
  assert(!schema.isEmpty)

  private val tableId = deltaLog.snapshot.metadata.id

  private var previousOffset: DeltaSourceOffset = null

  // A metadata snapshot when starting the query.
  private var initialState: DeltaSourceSnapshot = null
  private var initialStateVersion: Long = -1L

  /**
   * Get the changes starting from (startVersion, startIndex). The start point should not be
   * included in the result.
   */
  private def getChanges(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean): Iterator[IndexedFile] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): Iterator[IndexedFile] = {
      deltaLog.getChanges(startVersion).flatMap { case (version, actions) =>
        val addFiles = verifyStreamHygieneAndFilterAddFiles(actions)
        Iterator.single(IndexedFile(version, -1, null)) ++ addFiles
          .map(_.asInstanceOf[AddFile])
          .zipWithIndex.map { case (action, index) =>
          IndexedFile(version, index.toLong, action, isLast = index + 1 == addFiles.size)
        }
      }
    }

    val iter = if (isStartingVersion) {
      getSnapshotAt(fromVersion) ++ filterAndIndexDeltaLogs(fromVersion + 1)
    } else {
      filterAndIndexDeltaLogs(fromVersion)
    }

    iter.filter { case IndexedFile(version, index, _, _) =>
      version > fromVersion || (index == -1 || index > fromIndex)
    }
  }

  private def getSnapshotAt(version: Long): Iterator[IndexedFile] = {
    if (initialState == null || version != initialStateVersion) {
      cleanUpSnapshotResources()
      val snapshot =
        try {
          deltaLog.getSnapshotAt(version)
        } catch {
          case e: FileNotFoundException =>
            throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(e)
        }
      initialState = new DeltaSourceSnapshot(spark, snapshot, filters)
      initialStateVersion = version
    }
    initialState.iterator()
  }

  private def iteratorLast[T](iter: Iterator[T]): Option[T] = {
    var last: Option[T] = None
    while (iter.hasNext) {
      last = Some(iter.next())
    }
    last
  }

  private def getChangesWithRateLimit(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean): Iterator[IndexedFile] = {

    val changes = getChanges(fromVersion, fromIndex, isStartingVersion)


    // Take each change until we've seen the configured number of addFiles. Some changes don't
    // represent file additions; we retain them for offset tracking, but they don't count towards
    // the maxFilesPerTrigger conf.
    var toTake = maxFilesPerTrigger.getOrElse(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT) + 1
    changes.takeWhile { file =>
      if (file.add != null) toTake -= 1

      toTake > 0
    }
  }

  private def getStartingOffset(): Option[Offset] = {
    val version = deltaLog.snapshot.version
    if (version < 0) {
      return None
    }
    val last = iteratorLast(getChangesWithRateLimit(version, -1L, isStartingVersion = true))
    if (last.isEmpty) {
      return None
    }
    val IndexedFile(v, i, _, lastOfVersion) = last.get
    assert(v >= version,
      s"getChangesWithRateLimit returns an invalid version: $v (expected: >= $version)")

    // If the last file of previous batch is the last file of its version, automatically bump
    // to next version to skip accessing that version file altogether.
    if (lastOfVersion) {
      // isStartingVersion must be false here as we have bumped the version
      Some(DeltaSourceOffset(tableId, v + 1, -1, isStartingVersion = false))
    } else {
      Some(DeltaSourceOffset(tableId, v, i, isStartingVersion = v == version))
    }
  }

  override def getOffset: Option[Offset] = {
    val currentOffset = if (previousOffset == null) {
      getStartingOffset()
    } else {
      val last = iteratorLast(getChangesWithRateLimit(
        previousOffset.reservoirVersion,
        previousOffset.index,
        previousOffset.isStartingVersion))
      if (last.isEmpty) {
        Some(previousOffset)
      } else {
        val IndexedFile(v, i, _, lastOfVersion) = last.get
        val isStartingVersion =
          v == previousOffset.reservoirVersion && previousOffset.isStartingVersion

        // If the last file in previous batch is the last file of that version, automatically bump
        // to next version to skip accessing that version file altogether.
        if (lastOfVersion) {
          // isStartingVersion must be false here as we have bumped the version
          Some(DeltaSourceOffset(tableId, v + 1, -1, isStartingVersion = false))
        } else {
          Some(DeltaSourceOffset(tableId, v, i, isStartingVersion))
        }
      }
    }
    logDebug(s"previousOffset -> currentOffset: $previousOffset -> $currentOffset")
    currentOffset
  }

  private def verifyStreamHygieneAndFilterAddFiles(actions: Seq[Action]): Seq[Action] = {
    var seenFileAdd = false
    var seenFileRemove = false
    val filteredActions = actions.filter {
      case a: AddFile if a.dataChange =>
        seenFileAdd = true
        true
      case a: AddFile =>
        // Created by a data compaction
        false
      case r: RemoveFile if r.dataChange =>
        seenFileRemove = true
        false
      case _: RemoveFile =>
        false
      case m: Metadata =>
        if (!SchemaUtils.isReadCompatible(m.schema, schema)) {
          throw DeltaErrors.schemaChangedException(schema, m.schema)
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
    if (seenFileRemove) {
      if (seenFileAdd && !ignoreChanges) {
        throw new UnsupportedOperationException(DeltaErrors.DeltaSourceIgnoreChangesErrorMessage)
      } else if (!seenFileAdd && !ignoreDeletes) {
        throw new UnsupportedOperationException(DeltaErrors.DeltaSourceIgnoreDeleteErrorMessage)
      }
    }

    filteredActions
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val endOffset = DeltaSourceOffset(tableId, end)
    previousOffset = endOffset // For recovery
    val changes = if (start.isEmpty) {
      if (endOffset.isStartingVersion) {
        getChanges(endOffset.reservoirVersion, -1L, isStartingVersion = true)
      } else {
        assert(endOffset.reservoirVersion > 0, s"invalid reservoirVersion in endOffset: $endOffset")
        // Load from snapshot `endOffset.reservoirVersion - 1L` so that `index` in `endOffset`
        // is still valid.
        getChanges(endOffset.reservoirVersion - 1L, -1L, isStartingVersion = true)
      }
    } else {
      val startOffset = DeltaSourceOffset(tableId, start.get)
      if (!startOffset.isStartingVersion) {
        // unpersist `snapshot` because it won't be used any more.
        cleanUpSnapshotResources()
      }
      getChanges(startOffset.reservoirVersion, startOffset.index, startOffset.isStartingVersion)
    }

    val addFilesIter = changes.takeWhile { case IndexedFile(version, index, _, _) =>
      version < endOffset.reservoirVersion ||
        (version == endOffset.reservoirVersion && index <= endOffset.index)
    }.collect { case i: IndexedFile if i.add != null => i.add }
    val addFiles =
      addFilesIter.filter(a => excludeRegex.forall(_.findFirstIn(a.path).isEmpty)).toSeq
    logDebug(s"start: $start end: $end ${addFiles.toList}")
    deltaLog.createDataFrame(deltaLog.snapshot, addFiles, isStreaming = true)
  }

  override def stop(): Unit = {
    cleanUpSnapshotResources()
  }

  private def cleanUpSnapshotResources(): Unit = {
    if (initialState != null) {
      initialState.close(unpersistSnapshot = initialStateVersion < deltaLog.snapshot.version)
      initialState = null
    }
  }

  override def toString(): String = s"DeltaSource[${deltaLog.dataPath}]"
}
