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

package org.apache.spark.sql.delta.rowid

import java.io.File

import scala.collection.JavaConverters._

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, CommitInfo, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.commands.backfill.{BackfillBatchStats, BackfillCommandStats}
import org.apache.spark.sql.delta.rowtracking.RowTrackingTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.datasources.FileFormat

trait RowIdTestUtils extends RowTrackingTestUtils with DeltaSQLCommandTest {
  val QUALIFIED_BASE_ROW_ID_COLUMN_NAME = s"${FileFormat.METADATA_NAME}.${RowId.BASE_ROW_ID}"

  protected def getRowIdRangeInclusive(f: AddFile): (Long, Long) = {
    val min = f.baseRowId.get
    val max = min + f.numPhysicalRecords.get - 1L
    (min, max)
  }

  def assertRowIdsDoNotOverlap(log: DeltaLog): Unit = {
    val files = log.update().allFiles.collect()

    val sortedRanges = files
      .map(f => (f.path, getRowIdRangeInclusive(f)))
      .sortBy { case (_, (min, _)) => min }

    for (i <- sortedRanges.indices.dropRight(1)) {
      val (curPath, (_, curMax)) = sortedRanges(i)
      val (nextPath, (nextMin, _)) = sortedRanges(i + 1)
      assert(curMax < nextMin, s"$curPath and $nextPath have overlapping row IDs")
    }
  }

  def assertHighWatermarkIsCorrect(log: DeltaLog): Unit = {
    val snapshot = log.update()
    val files = snapshot.allFiles.collect()

    val highWatermarkOpt = RowId.extractHighWatermark(snapshot)
    if (files.isEmpty) {
      assert(highWatermarkOpt.isDefined)
    } else {
      val maxAssignedRowId = files
        .map(a => a.baseRowId.get + a.numPhysicalRecords.get - 1L)
        .max
      assert(highWatermarkOpt.get == maxAssignedRowId)
    }
  }

  def assertRowIdsAreValid(log: DeltaLog): Unit = {
    assertRowIdsDoNotOverlap(log)
    assertHighWatermarkIsCorrect(log)
  }

  def assertHighWatermarkIsCorrectAfterUpdate(
      log: DeltaLog, highWatermarkBeforeUpdate: Long, expectedNumRecordsWritten: Long): Unit = {
    val highWaterMarkAfterUpdate = RowId.extractHighWatermark(log.update()).get
    assert((highWatermarkBeforeUpdate + expectedNumRecordsWritten) === highWaterMarkAfterUpdate)
    assertRowIdsAreValid(log)
  }

  def assertRowIdsAreNotSet(log: DeltaLog): Unit = {
    val snapshot = log.update()

    val highWatermarks = RowId.extractHighWatermark(snapshot)
    assert(highWatermarks.isEmpty)

    val files = snapshot.allFiles.collect()
    assert(files.forall(_.baseRowId.isEmpty))
  }

  def assertRowIdsAreLargerThanValue(log: DeltaLog, value: Long): Unit = {
    log.update().allFiles.collect().foreach { f =>
      val minRowId = getRowIdRangeInclusive(f)._1
      assert(minRowId > value, s"${f.toString} has a row id smaller or equal than $value")
    }
  }

  def extractMaterializedRowIdColumnName(log: DeltaLog): Option[String] = {
    log.update().metadata.configuration.get(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
  }

  protected def readRowGroupsPerFile(dir: File): Seq[Seq[BlockMetaData]] = {
    assert(dir.isDirectory)
    readAllFootersWithoutSummaryFiles(
      // scalastyle:off deltahadoopconfiguration
      new Path(dir.getAbsolutePath), spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      .map(_.getParquetMetadata.getBlocks.asScala.toSeq)
  }

  protected def checkFileLayout(
      dir: File,
      numFiles: Int,
      numRowGroupsPerFile: Int,
      rowCountPerRowGroup: Int): Unit = {
    val rowGroupsPerFile = readRowGroupsPerFile(dir)
    assert(numFiles === rowGroupsPerFile.size)
    for (rowGroups <- rowGroupsPerFile) {
      assert(numRowGroupsPerFile === rowGroups.size)
      for (rowGroup <- rowGroups) {
        assert(rowCountPerRowGroup === rowGroup.getRowCount)
      }
    }
  }

  // easily add a rowid column to a dataframe by calling [[df.withMaterializedRowIdColumn]]
  implicit class DataFrameRowIdColumn(df: DataFrame) {
    def withMaterializedRowIdColumn(
        materializedColumnName: String, rowIdColumn: Column): DataFrame =
      RowId.preserveRowIdsUnsafe(df, materializedColumnName, rowIdColumn)

    def withMaterializedRowCommitVersionColumn(
        materializedColumnName: String, rowCommitVersionColumn: Column): DataFrame =
      RowCommitVersion.preserveRowCommitVersionsUnsafe(
        df, materializedColumnName, rowCommitVersionColumn)
  }

  def extractMaterializedRowCommitVersionColumnName(log: DeltaLog): Option[String] = {
    log.update().metadata.configuration
      .get(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP)
  }

  /** Returns a Map of file path to base row ID from the AddFiles in a Snapshot. */
  private def getAddFilePathToBaseRowIdMap(snapshot: Snapshot): Map[String, Long] = {
    val allAddFiles = snapshot.allFiles.collect()
    allAddFiles.foreach(addFile => assert(addFile.baseRowId.isDefined,
      "Every AddFile should have a base row ID"))
    allAddFiles.map(a => a.path -> a.baseRowId.get).toMap
  }

  /** Returns a Map of file path to base row ID from the RemoveFiles in a Snapshot. */
  private def getRemoveFilePathToBaseRowIdMap(snapshot: Snapshot): Map[String, Long] = {
    val removeFiles = snapshot.tombstones.collect()
    removeFiles.foreach(removeFile => assert(removeFile.baseRowId.isDefined,
      "Every RemoveFile should have a base row ID"))
    removeFiles.map(r => r.path -> r.baseRowId.get).toMap
  }

  /** Check that the high watermark does not get updated if there aren't any new files */
  def checkHighWatermarkBeforeAndAfterOperation(log: DeltaLog)(operation: => Unit): Unit = {
    val prevSnapshot = log.update()
    val prevHighWatermark = RowId.extractHighWatermark(prevSnapshot)
    val prevAddFiles = getAddFilePathToBaseRowIdMap(prevSnapshot).keySet

    operation

    val newAddFiles = getAddFilePathToBaseRowIdMap(log.update()).keySet
    val newFilesAdded = newAddFiles.diff(prevAddFiles).nonEmpty
    val newHighWatermark = RowId.extractHighWatermark(log.update())

    if (newFilesAdded) {
      assert(prevHighWatermark.get < newHighWatermark.get,
        "The high watermark should have been updated after creating new files")
    } else {
      assert(prevHighWatermark === newHighWatermark,
        "The high watermark should not be updated when there are no new file")
    }
  }

  /**
   * Check that file actions do not violate Row ID invariants after an operation.
   * More specifically:
   *  - We do not reassign the base row ID to the same AddFile.
   *  - RemoveFiles have the same base row ID as the corresponding AddFile
   *    with the same file path.
   */
  def checkFileActionInvariantBeforeAndAfterOperation(log: DeltaLog)(operation: => Unit): Unit = {
    val prevAddFilePathToBaseRowId = getAddFilePathToBaseRowIdMap(log.update())

    operation

    val snapshot = log.update()
    val newAddFileBaseRowIdsMap = getAddFilePathToBaseRowIdMap(snapshot)
    val newRemoveFileBaseRowIds = getRemoveFilePathToBaseRowIdMap(snapshot)

    prevAddFilePathToBaseRowId.foreach { case (path, prevRowId) =>
      if (newAddFileBaseRowIdsMap.contains(path)) {
        val currRowId = newAddFileBaseRowIdsMap(path)
        assert(currRowId === prevRowId,
          "We should not reassign base row IDs if it's the same AddFile")
      } else if (newRemoveFileBaseRowIds.contains(path)) {
        assert(newRemoveFileBaseRowIds(path) === prevRowId,
          "No new base row ID should be assigned to RemoveFiles")
      }
    }
  }

  /**
   * Checks whether Row tracking is marked as preserved on the [[CommitInfo]] action
   * committed during `operation`.
   */
  def rowTrackingMarkedAsPreservedForCommit(log: DeltaLog)(operation: => Unit): Boolean = {
    val versionPriorToCommit = log.update().version

    operation

    val versionOfCommit = log.update().version
    assert(versionPriorToCommit < versionOfCommit)
    val commitInfos = log.getChanges(versionOfCommit).flatMap(_._2).flatMap {
      case commitInfo: CommitInfo => Some(commitInfo)
      case _ => None
    }.toList
    assert(commitInfos.size === 1)
    commitInfos.forall { commitInfo =>
      commitInfo.tags
        .getOrElse(Map.empty)
        .getOrElse(DeltaCommitTag.PreservedRowTrackingTag.key, "false").toBoolean
    }
  }

  def checkRowTrackingMarkedAsPreservedForCommit(log: DeltaLog)(operation: => Unit): Unit = {
    assert(rowTrackingMarkedAsPreservedForCommit(log) {
      operation
    })
  }

  /**
   * Capture backfill related metrics for basic validation.
   */
  def validateSuccessfulBackfillMetrics(
      expectedNumSuccessfulBatches: Int,
      nameOfTriggeringOperation: String = DeltaOperations.OP_SET_TBLPROPERTIES)
      (testBlock: => Unit): Unit = {
    val backfillUsageRecords = Log4jUsageLogger.track {
      testBlock
    }.filter(_.metric == "tahoeEvent")

    val backfillRecords = backfillUsageRecords
      .filter(_.tags.get("opType").contains(DeltaUsageLogsOpTypes.BACKFILL_COMMAND))
    assert(backfillRecords.size === 1, "Row Tracking Backfill should have " +
      "only been executed once.")

    val backfillStats = JsonUtils.fromJson[BackfillCommandStats](backfillRecords.head.blob)
    assert(backfillStats.wasSuccessful)
    assert(backfillStats.numFailedBatches === 0)
    assert(backfillStats.totalExecutionTimeMs > 0)
    val expectedMaxNumBatchesInParallel =
      spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_BATCHES_IN_PARALLEL)
    assert(backfillStats.maxNumBatchesInParallel === expectedMaxNumBatchesInParallel)
    assert(backfillStats.numSuccessfulBatches === expectedNumSuccessfulBatches)
    assert(backfillStats.nameOfTriggeringOperation === nameOfTriggeringOperation)

    val parentTxnId = backfillStats.transactionId

    val backfillBatchRecords = backfillUsageRecords
      .filter(_.tags.get("opType").contains(DeltaUsageLogsOpTypes.BACKFILL_BATCH))
    val backfillBatchStats = backfillBatchRecords.map { backfillBatchRecord =>
      JsonUtils.fromJson[BackfillBatchStats](backfillBatchRecord.blob)
    }
    // Sanity check that the individual child commits were successful.
    backfillBatchStats.foreach { backfillBatchStat =>
      assert(backfillBatchStat.wasSuccessful)
      assert(backfillBatchStat.totalExecutionTimeInMs > 0)
      assert(backfillBatchStat.initialNumFiles > 0)
      assert(backfillBatchStat.parentTransactionId === parentTxnId)
    }
  }

  /**
   * This triggers backfill on the test table in this suite by calling the user-facing syntax
   * `ALTER TABLE t SET TBLPROPERTIES()`. We check for proper protocol upgrade (if any) and
   * that the table has valid row IDs afterwards.
   */
  def triggerBackfillOnTestTableUsingAlterTable(
      targetTableName: String,
      numRowsInTable: Int,
      log: DeltaLog): Unit = {
    val prevMinReaderVersion = log.update().protocol.minReaderVersion
    val prevMinWriterVersion = log.update().protocol.minWriterVersion

    val rowIdPropertyKey = DeltaConfigs.ROW_TRACKING_ENABLED.key

    spark.sql(s"ALTER TABLE $targetTableName SET TBLPROPERTIES ('$rowIdPropertyKey'=true)")
    assert(lastCommitHasRowTrackingEnablementOnlyTag(log))

    // Check the protocol upgrade is as expected. We should only bump the minWriterVersion if
    // necessary and add the table feature support for row IDs.
    val snapshot = log.update()
    val newProtocol = snapshot.protocol
    assert(newProtocol.isFeatureSupported(RowTrackingFeature))
    assert(newProtocol.minReaderVersion === prevMinReaderVersion,
      "The reader version does not need to be upgraded")
    val expectedMinWriterVersion = Math.max(
      prevMinWriterVersion, TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(newProtocol.minWriterVersion === expectedMinWriterVersion)

    // Tables should have the table property enabled at the end of ALTER TABLE command.
    assert(RowId.isEnabled(newProtocol, snapshot.metadata))
    val highWaterMarkBefore = -1L
    assertRowIdsAreValid(log)
    assertRowIdsAreLargerThanValue(log, highWaterMarkBefore)
    assertHighWatermarkIsCorrectAfterUpdate(log, highWaterMarkBefore, numRowsInTable)
  }

  /**
   * Returns a Boolean indicating whether the last commit on a Delta table has the tag
   * [[DeltaCommitTag.RowTrackingEnablementOnlyTag.key]].
   */
  def lastCommitHasRowTrackingEnablementOnlyTag(log: DeltaLog): Boolean = {
    val lastTableVersion = log.update().version
    val (_, lastCommitActions) = log.getChanges(lastTableVersion).toList.last
    val findRowTrackingEnablementOnlyTag = lastCommitActions.collectFirst {
      case commitInfo: CommitInfo => DeltaCommitTag.getTagValueFromCommitInfo(
        Some(commitInfo), DeltaCommitTag.RowTrackingEnablementOnlyTag.key)
    }.flatten

    findRowTrackingEnablementOnlyTag.exists(_.toBoolean)
  }
}
