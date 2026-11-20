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

package org.apache.spark.sql.delta.amt

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{Checkpoints, DeltaLog, DeltaTestUtils, Snapshot, TruncationGranularity, TypeWideningTableFeature}
import org.apache.spark.sql.delta.actions.DropTableFeatureUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Metadata cleanup is not supported for AMT tables yet. Covers the direct `cleanUpExpiredLogs`
 * calls that bypass the AMT branch of `checkpointAndCleanUpDeltaLog` (DROP FEATURE ... TRUNCATE
 * HISTORY, clone history), which must be skipped.
 */
class AMTMetadataCleanupSuite extends AMTCheckpointTestBase {

  /** Far enough in the past to expire every retention period the cleanup paths use. */
  private val expiredAgeMillis = TimeUnit.DAYS.toMillis(7)

  /** Every file under `_delta_log`, including unbackfilled commits under `_staged_commits`. */
  private def logFilesOnDisk(deltaLog: DeltaLog): Set[Path] = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val files = ArrayBuffer.empty[Path]
    val it = fs.listFiles(deltaLog.logPath, true)
    while (it.hasNext) {
      files += it.next().getPath
    }
    files.toSet
  }

  /** Versions of the classic (single-part, multi-part or V2) checkpoint files on disk. */
  private def classicCheckpointVersions(deltaLog: DeltaLog): Set[Long] =
    logFilesOnDisk(deltaLog).filter(FileNames.isCheckpointFile).map(FileNames.getFileVersion)

  /**
   * Backdates the modification time of every versioned log file up to `untilVersion`, so metadata
   * cleanup sees those commits and checkpoints as expired.
   */
  private def expireLogFilesUntil(deltaLog: DeltaLog, untilVersion: Long): Unit = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val expiredTime = System.currentTimeMillis() - expiredAgeMillis
    logFilesOnDisk(deltaLog).foreach { path =>
      if (FileNames.getFileVersionOpt(path).exists(_ <= untilVersion)) {
        fs.setTimes(path, expiredTime, -1)
      }
    }
  }

  /** Runs metadata cleanup the way DROP FEATURE ... TRUNCATE HISTORY does. */
  private def cleanUpLikeTruncateHistory(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      catalogTableOpt: Option[CatalogTable]): Unit =
    deltaLog.cleanUpExpiredLogs(
      snapshotToCleanup = snapshot,
      catalogTableOpt = catalogTableOpt,
      deltaRetentionMillisOpt =
        Some(DropTableFeatureUtils.truncateHistoryLogRetentionMillis(snapshot.metadata)),
      cutoffTruncationGranularity = TruncationGranularity.MINUTE)

  /** Runs `body` and returns the usage records of the metadata cleanups that ran. */
  private def trackCleanups(body: => Unit): Seq[UsageRecord] =
    DeltaTestUtils.filterUsageRecords(Log4jUsageLogger.track(body), "delta.log.cleanup")

  /**
   * An AMT table with two stray classic checkpoints, at `strayVersions`, and no checkpoint at
   * `cutoffVersion`, which sits between them. The table holds ids 0 until `numRows`.
   */
  private case class AMTTableWithStrayClassicCheckpoints(
      deltaLog: DeltaLog,
      strayVersions: Set[Long],
      cutoffVersion: Long,
      numRows: Int)

  /**
   * Creates `tableName` as an AMT table with two stray classic checkpoints, written with the
   * classic checkpoint writer directly to model a bug in a path that bypasses the AMT checks. Each
   * sits at the content root version of an AMT checkpoint: a classic checkpoint newer than the AMT
   * content root makes snapshot discovery throw, so this is the only place one can sit without
   * breaking the table first. `_last_checkpoint` points at an AMT checkpoint newer than both.
   */
  private def createAMTTableWithStrayClassicCheckpoints(
      tableName: String): AMTTableWithStrayClassicCheckpoints = {
    createAMTTable(tableName, checkpointInterval = Int.MaxValue)
    val deltaLog = deltaLogForName(tableName)
    val catalogTable = Some(catalogTableFor(tableName))
    commitCheckpoint(deltaLog, incremental = false)
    var numRows = 0
    def append(numCommits: Int): Long = {
      (0 until numCommits).foreach { _ =>
        appendRowsAsSeparateFiles(tableName, numFiles = 1, startId = numRows)
        numRows += 1
      }
      deltaLog.update().version
    }
    def amtCheckpointWithStrayClassicCheckpoint(): Long = {
      val snapshotToCheckpoint = deltaLog.update()
      commitCheckpoint(deltaLog, incremental = true)
      Checkpoints.writeCheckpoint(spark, deltaLog, snapshotToCheckpoint, catalogTable)
      snapshotToCheckpoint.version
    }

    append(numCommits = 2)
    val firstStrayVersion = amtCheckpointWithStrayClassicCheckpoint()
    val cutoffVersion = append(numCommits = 2)
    append(numCommits = 2)
    val secondStrayVersion = amtCheckpointWithStrayClassicCheckpoint()
    append(numCommits = 1)
    commitCheckpoint(deltaLog, incremental = true)
    append(numCommits = 3)
    AMTTableWithStrayClassicCheckpoints(
      deltaLog, Set(firstStrayVersion, secondStrayVersion), cutoffVersion, numRows)
  }

  /** Registers `body` across the AMT checkpoint scenarios, on a table holding ids 1 to 4. */
  private def testAcrossScenariosWithHistory(testName: String, tableName: String)(
      body: AMTCheckpointScenarioContext => Unit): Unit =
    testAcrossAMTCheckpointScenarios(testName, tableName)(
      setup = name => (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (4)")))(body)

  testAcrossScenariosWithHistory(
      "cleanUpExpiredLogs is skipped for an AMT table",
      "amt_no_log_cleanup") { context =>
    val deltaLog = deltaLogForName(context.tableName)
    val snapshot = deltaLog.update()
    expireLogFilesUntil(deltaLog, snapshot.version)
    val filesBefore = logFilesOnDisk(deltaLog)

    val cleanupRecords = trackCleanups {
      cleanUpLikeTruncateHistory(deltaLog, snapshot, Some(catalogTableFor(context.tableName)))
    }

    assert(cleanupRecords.isEmpty, "metadata cleanup ran on an AMT table")
    assert(logFilesOnDisk(deltaLog) === filesBefore)
    checkAnswer(spark.read.table(context.tableName), (1 to 4).map(Row(_)))
  }

  test("cleanUpExpiredLogs is skipped for an AMT table that has stray classic checkpoints") {
    val tableName = "amt_stray_classic_checkpoints"
    withTable(tableName) {
      val table = createAMTTableWithStrayClassicCheckpoints(tableName)
      val deltaLog = table.deltaLog
      val snapshot = deltaLog.update()
      assert(classicCheckpointVersions(deltaLog) === table.strayVersions,
        "precondition: the stray classic checkpoints are on disk")
      assert(deltaLog.readLastCheckpointFile().exists(_.version > table.strayVersions.max),
        "precondition: _last_checkpoint points at an AMT checkpoint newer than the strays")

      // Before the fix, cleanup anchored on the strays: it deleted every commit up to the cutoff
      // and wrote its own classic checkpoint there.
      expireLogFilesUntil(deltaLog, table.cutoffVersion)
      val filesBefore = logFilesOnDisk(deltaLog)
      val cleanupRecords =
          trackCleanups(
            cleanUpLikeTruncateHistory(deltaLog, snapshot, Some(catalogTableFor(tableName))))

      assert(cleanupRecords.isEmpty, "metadata cleanup ran on an AMT table")
      assert(logFilesOnDisk(deltaLog) === filesBefore)
      checkAnswer(spark.read.table(tableName), (0 until table.numRows).map(Row(_)))
    }
  }

  testAcrossScenariosWithHistory(
      "DROP FEATURE ... TRUNCATE HISTORY skips metadata cleanup for an AMT table",
      "amt_drop_feature_truncate_history") { context =>
    val tableName = context.tableName
    val deltaLog = deltaLogForName(tableName)
    // A reader-writer feature with no traces in the data, so the drop goes straight to the
    // history truncation step.
    sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    expireLogFilesUntil(deltaLog, deltaLog.update().version)
    val filesBefore = logFilesOnDisk(deltaLog)

    val cleanupRecords = trackCleanups {
      sql(s"ALTER TABLE $tableName DROP FEATURE typeWidening TRUNCATE HISTORY")
    }

    assert(!deltaLog.update().protocol.isFeatureSupported(TypeWideningTableFeature))
    assert(cleanupRecords.isEmpty, "DROP FEATURE ran metadata cleanup on an AMT table")
    assert(classicCheckpointVersions(deltaLog).isEmpty)
    // DROP FEATURE adds commits and AMT checkpoints of its own, but must delete nothing.
    assert(filesBefore.subsetOf(logFilesOnDisk(deltaLog)))
    checkAnswer(spark.read.table(tableName), (1 to 4).map(Row(_)))
  }
}
