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

import java.io.File

import scala.language.postfixOps

import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RawLocalFileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.util.ManualClock

// scalastyle:off: removeFile
class DeltaRetentionSuite extends QueryTest
  with DeltaRetentionSuiteBase
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with CheckpointProtectionTestUtilsMixin {

  protected override def sparkConf: SparkConf = super.sparkConf

  override protected def getLogFiles(dir: File): Seq[File] =
    getDeltaFiles(dir) ++ getUnbackfilledDeltaFiles(dir) ++ getCheckpointFiles(dir)

  test("delete expired logs") {
    withTempDir { tempDir =>
      val startTime = getStartTimeForRetentionTest
      val clock = new ManualClock(startTime)
      val actualTestStartTime = System.currentTimeMillis()
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri)
      (1 to 5).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          val timestamp = startTime + (System.currentTimeMillis()-actualTestStartTime)
          RemoveFile(i - 1 toString, Some(timestamp), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, testOp)
      }

      val initialFiles = getLogFiles(logPath)
      // Shouldn't clean up, no checkpoint, no expired files
      log.cleanUpExpiredLogs(log.snapshot)

      assert(initialFiles === getLogFiles(logPath))

      clock.advance(intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) +
        intervalStringToMillis("interval 1 day"))

      // Shouldn't clean up, no checkpoint, although all files have expired
      log.cleanUpExpiredLogs(log.snapshot)
      assert(initialFiles === getLogFiles(logPath))

      log.checkpoint()

      val expectedFiles = Seq("04.json", "04.checkpoint.parquet")
      // after checkpointing, the files should be cleared
      log.cleanUpExpiredLogs(log.snapshot)
      val afterCleanup = getLogFiles(logPath)
      assert(initialFiles !== afterCleanup)
      assert(expectedFiles.forall(suffix => afterCleanup.exists(_.getName.endsWith(suffix))),
        s"${afterCleanup.mkString("\n")}\n didn't contain files with suffixes: $expectedFiles")
    }
  }

  test("log files being already deleted shouldn't fail log deletion job") {
    withTempDir { tempDir =>
      val startTime = getStartTimeForRetentionTest
      val clock = new ManualClock(startTime)
      val actualTestStartTime = System.currentTimeMillis()
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri)
      val iterationCount = (log.checkpointInterval() * 2) + 1

      (1 to iterationCount).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          val timestamp = startTime + (System.currentTimeMillis()-actualTestStartTime)
          RemoveFile(i - 1 toString, Some(timestamp), true) :: Nil
        } else {
          Nil
        }
        val version = txn.commit(delete ++ file, testOp)
        val deltaFile = new File(FileNames.unsafeDeltaFile(log.logPath, version).toUri)
        deltaFile.setLastModified(clock.getTimeMillis() + i * 10000)
        val crcFile = new File(FileNames.checksumFile(log.logPath, version).toUri)
        crcFile.setLastModified(clock.getTimeMillis() + i * 10000)
        val chk = new File(FileNames.checkpointFileSingular(log.logPath, version).toUri)
        if (chk.exists()) {
          chk.setLastModified(clock.getTimeMillis() + i * 10000)
        }
      }

      // delete some files in the middle
      val middleStartIndex = log.checkpointInterval() / 2
      getDeltaFiles(logPath).sortBy(_.getName).slice(
        middleStartIndex, middleStartIndex + log.checkpointInterval()).foreach(_.delete())
      clock.advance(intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) +
        intervalStringToMillis("interval 2 day"))
      log.cleanUpExpiredLogs(log.snapshot)

      val minDeltaFile =
        getDeltaFiles(logPath).map(f => FileNames.deltaVersion(new Path(f.toString))).min
      val maxChkFile = getCheckpointFiles(logPath).map(f =>
        FileNames.checkpointVersion(new Path(f.toString))).max

      assert(maxChkFile === minDeltaFile,
        "Delta files before the last checkpoint version should have been deleted")
      assert(getCheckpointFiles(logPath).length === 1,
        "There should only be the last checkpoint version")
    }
  }

  testQuietly(
    "RemoveFiles persist across checkpoints as tombstones if retention time hasn't expired") {
    withTempDir { tempDir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)

      val txn = startTxnWithManualLogCleanup(log1)
      val files1 = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files1, testOp)
      val txn2 = log1.startTransaction()
      val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
      txn2.commit(files2, testOp)
      log1.checkpoint()

      DeltaLog.clearCache()
      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      assert(log2.snapshot.tombstones.count() === 4)
      assert(log2.snapshot.allFiles.count() === 6)
    }
  }

  def removeFileCountFromUnderlyingCheckpoint(snapshot: Snapshot): Long = {
    val df = snapshot.checkpointProvider
      .allActionsFileIndexes()
      .map(snapshot.deltaLog.loadIndex(_))
      .reduce(_.union(_))
    df.where("remove is not null").count()
  }

  testQuietly("retention timestamp is picked properly by the cold snapshot initialization") {
    withTempDir { dir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      def deltaLog: DeltaLog = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath), clock)

      // Create table with 30 day tombstone retention.
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
       """.stripMargin)


      // 1st day - commit 10 new files and remove them also same day.
      clock.advance(intervalStringToMillis("interval 1 days"))
      val files1 = (1 to 4).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      deltaLog.startTransaction().commit(files1, testOp)
      val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
      deltaLog.startTransaction().commit(files2, testOp)

      // Advance clock by 10 days.
      clock.advance(intervalStringToMillis("interval 10 days"))
      DeltaLog.clearCache()
      deltaLog.checkpoint()
      DeltaLog.clearCache() // Clear cache and reinitialize snapshot with latest checkpoint.
      assert(removeFileCountFromUnderlyingCheckpoint(deltaLog.unsafeVolatileSnapshot) === 4)

      // Advance clock by 21 more days. Now checkpoint should stop tracking remove tombstones.
      clock.advance(intervalStringToMillis("interval 21 days"))
      deltaLog.startTransaction().commit(Seq.empty, testOp)
      DeltaLog.clearCache()
      deltaLog.checkpoint(deltaLog.unsafeVolatileSnapshot)
      DeltaLog.clearCache() // Clear cache and reinitialize snapshot with latest checkpoint.
      assert(removeFileCountFromUnderlyingCheckpoint(deltaLog.unsafeVolatileSnapshot) === 0)
    }
  }


  testQuietly("retention timestamp is lesser than the default value") {
    withTempDir { dir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      def deltaLog: DeltaLog = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath), clock)

      // Create table with 2 day tombstone retention.
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 2 days')
       """.stripMargin)


      // 1st day - commit 10 new files and remove them also same day.
      {
        clock.advance(intervalStringToMillis("interval 1 days"))
        val txn = deltaLog.startTransaction()
        val files1 = (1 to 4).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
        txn.commit(files1, testOp)
        val txn2 = deltaLog.startTransaction()
        val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
        txn2.commit(files2, testOp)
      }


      // Advance clock by 4 days.
      clock.advance(intervalStringToMillis("interval 4 days"))
      DeltaLog.clearCache()
      deltaLog.checkpoint(deltaLog.unsafeVolatileSnapshot)
      DeltaLog.clearCache() // Clear cache and reinitialize snapshot with latest checkpoint.
      assert(removeFileCountFromUnderlyingCheckpoint(deltaLog.unsafeVolatileSnapshot) === 0)
    }
  }

  testQuietly("RemoveFiles get deleted during checkpoint if retention time has passed") {
    withTempDir { tempDir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)

      val txn = startTxnWithManualLogCleanup(log1)
      val files1 = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files1, testOp)
      val txn2 = log1.startTransaction()
      val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
      txn2.commit(files2, testOp)

      clock.advance(
        intervalStringToMillis(DeltaConfigs.TOMBSTONE_RETENTION.defaultValue) + 1000000L)

      log1.checkpoint()

      DeltaLog.clearCache()
      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      assert(log2.snapshot.tombstones.count() === 0)
      assert(log2.snapshot.allFiles.count() === 6)
    }
  }

  test("the checkpoint file for version 0 should be cleaned") {
    withTempDir { tempDir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri)
      startTxnWithManualLogCleanup(log).commit(AddFile("0", Map.empty, 1, 1, true) :: Nil, testOp)
      log.checkpoint()

      val initialFiles = getLogFiles(logPath)
      clock.advance(intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) +
        intervalStringToMillis("interval 1 day"))

      // Create a new checkpoint so that the previous version can be deleted
      log.startTransaction().commit(AddFile("1", Map.empty, 1, 1, true) :: Nil, testOp)
      log.checkpoint()

      // despite our clock time being set in the future, this doesn't change the FileStatus
      // lastModified time. this can cause some flakiness during log cleanup. setting it fixes that.
      getLogFiles(logPath)
        .filterNot(f => initialFiles.contains(f))
        .foreach(f => f.setLastModified(clock.getTimeMillis()))

      log.cleanUpExpiredLogs(log.snapshot)
      val afterCleanup = getLogFiles(logPath)
      initialFiles.foreach { file =>
        assert(!afterCleanup.contains(file))
      }
    }
  }

  test("allow users to expire transaction identifiers from checkpoints") {
    withTempDir { dir =>
      val clock = new ManualClock(getStartTimeForRetentionTest)
      val log = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath), clock)
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES ('delta.setTransactionRetentionDuration' = 'interval 1 days')
       """.stripMargin)

      // commit at time < TRANSACTION_ID_RETENTION_DURATION
      log.startTransaction().commitManually(SetTransaction("app", 1, Some(clock.getTimeMillis())))
      assert(log.update().transactions == Map("app" -> 1))
      assert(log.update().numOfSetTransactions == 1)

      clock.advance(intervalStringToMillis("interval 1 days"))

      // query at time == TRANSACTION_ID_RETENTION_DURATION & NO new commit
      // No new commit has been made, so we will see expired transactions (this is not ideal, but
      // it's a tradeoff we've accepted)
      assert(log.update().transactions == Map("app" -> 1))
      assert(log.snapshot.numOfSetTransactions == 1)

      clock.advance(1)

      // query at time > TRANSACTION_ID_RETENTION_DURATION & NO new commit
      // we continue to see expired transactions
      assert(log.update().transactions == Map("app" -> 1))
      assert(log.snapshot.numOfSetTransactions == 1)

      // query at time > TRANSACTION_ID_RETENTION_DURATION & there IS a new commit
      // We will only filter expired transactions when time is >= TRANSACTION_ID_RETENTION_DURATION
      // and a new commit has been made
      val addFile = AddFile(
        path = "fake/path/1", partitionValues = Map.empty, size = 1,
        modificationTime = 1, dataChange = true)
      log.startTransaction().commitManually(addFile)
      assert(log.update().transactions.isEmpty)
      assert(log.snapshot.numOfSetTransactions == 0)
    }
  }

  protected def cleanUpExpiredLogs(log: DeltaLog): Unit = {
    val snapshot = log.update()

    val checkpointVersion = snapshot.logSegment.checkpointProvider.version
    logInfo(s"snapshot version: ${snapshot.version} checkpoint: $checkpointVersion")

    log.cleanUpExpiredLogs(snapshot)
  }

  for (v2CheckpointFormat <- V2Checkpoint.Format.ALL_AS_STRINGS)
  test(s"sidecar file cleanup [v2CheckpointFormat: $v2CheckpointFormat]") {
    val checkpointPolicy = CheckpointPolicy.V2.name
    withSQLConf((DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> v2CheckpointFormat)) {
      withTempDir { tempDir =>
        val startTime = getStartTimeForRetentionTest
        val clock = new ManualClock(startTime)
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
        val logPath = new File(log.logPath.toUri)
        val visitedFiles = scala.collection.mutable.Set.empty[String]

        spark.sql(s"""CREATE TABLE delta.`${tempDir.toString()}` (id Int) USING delta
                   | TBLPROPERTIES(
                   |-- Disable the async log cleanup as this test needs to manually trigger log
                   |-- clean up.
                   |'delta.enableExpiredLogCleanup' = 'false',
                   |'${DeltaConfigs.CHECKPOINT_POLICY.key}' = '$checkpointPolicy',
                   |'${DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key}' = 'false',
                   |'delta.checkpointInterval' = '100000',
                   |'delta.logRetentionDuration' = 'interval 6 days')
                    """.stripMargin)

        // day-1. Create a commit with 4 AddFiles.
        clock.setTime(day(startTime, day = 1))
        val file = (1 to 4).map(i => createTestAddFile(i.toString))
        log.startTransaction().commit(file, testOp)
        setModificationTimeOfNewFiles(log, clock, visitedFiles)

        // Trigger 1 commit and 1 checkpoint daily for next 8 days
        val sidecarFiles = scala.collection.mutable.Map.empty[Long, String]
        val oddCommitSidecarFile_1 = createSidecarFile(log, Seq(1))
        val evenCommitSidecarFile_1 = createSidecarFile(log, Seq(1))
        def commitAndCheckpoint(dayNumber: Int): Unit = {
          clock.setTime(day(startTime, dayNumber))

          // Write a new commit on each day
          log.startTransaction().commit(Seq(log.unsafeVolatileSnapshot.metadata), testOp)
          setModificationTimeOfNewFiles(log, clock, visitedFiles)

          // Write a new checkpoint on each day. Each checkpoint has 2 sodecars:
          // 1. Common sidecar - one of oddCommitSidecarFile_1/evenCommitSidecarFile_1
          // 2. A new sidecar just created for this checkpoint.
          val sidecarFile1 =
            if (dayNumber % 2 == 0) evenCommitSidecarFile_1 else oddCommitSidecarFile_1
          val sidecarFile2 = createSidecarFile(log, Seq(2, 3, 4))
          val checkpointVersion = log.update().version
          createV2CheckpointWithSidecarFile(
            log,
            checkpointVersion,
            sidecarFileNames = Seq(sidecarFile1, sidecarFile2))
          setModificationTimeOfNewFiles(log, clock, visitedFiles)
          sidecarFiles.put(checkpointVersion, sidecarFile2)
        }

        (2 to 9).foreach { dayNumber => commitAndCheckpoint(dayNumber) }
        clock.setTime(day(startTime, day = 10))
        log.update()

        // Assert all log files are present.
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 2 to 9)
        compareVersions(getDeltaVersions(logPath), "delta", 0 to 9)
        assert(
          getSidecarFiles(log) ===
            Set(
              evenCommitSidecarFile_1,
              oddCommitSidecarFile_1) ++ sidecarFiles.values.toIndexedSeq)

        // Trigger metadata cleanup and validate that only last 6 days of deltas and checkpoints
        // have been retained.
        cleanUpExpiredLogs(log)
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 4 to 9)
        compareVersions(getDeltaVersions(logPath), "delta", 4 to 9)
        // Check that all active sidecars are retained and expired ones are deleted.
        assert(
          getSidecarFiles(log) ===
            Set(evenCommitSidecarFile_1, oddCommitSidecarFile_1) ++
            (4 to 9).map(sidecarFiles(_)))

        // Advance 1 day and again run metadata cleanup.
        clock.setTime(day(startTime, day = 11))
        cleanUpExpiredLogs(log)
        setModificationTimeOfNewFiles(log, clock, visitedFiles)
        // Commit 4 and checkpoint 4 have expired and were deleted.
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 5 to 9)
        compareVersions(getDeltaVersions(logPath), "delta", 5 to 9)
        assert(
          getSidecarFiles(log) ===
            Set(evenCommitSidecarFile_1, oddCommitSidecarFile_1) ++
            (5 to 9).map(sidecarFiles(_)))

        // do 1 more commit and checkpoint on day 13 and run metadata cleanup.
        commitAndCheckpoint(dayNumber = 13) // commit and checkpoint 10
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 5 to 10)
        compareVersions(getDeltaVersions(logPath), "delta", 5 to 10)
        cleanUpExpiredLogs(log)
        setModificationTimeOfNewFiles(log, clock, visitedFiles)
        // Version 5 and 6 checkpoints and deltas have expired and were deleted.
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 7 to 10)
        compareVersions(getDeltaVersions(logPath), "delta", 7 to 10)

        assert(
          getSidecarFiles(log) ===
            Set(evenCommitSidecarFile_1, oddCommitSidecarFile_1) ++
            (7 to 10).map(sidecarFiles(_)))
      }
    }
  }

  for (v2CheckpointFormat <- V2Checkpoint.Format.ALL_AS_STRINGS)
  test(
    s"compat file created with metadata cleanup when checkpoints are deleted" +
      s" [v2CheckpointFormat: $v2CheckpointFormat]") {
    val checkpointPolicy = CheckpointPolicy.V2.name
    withSQLConf((DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> v2CheckpointFormat)) {
      withTempDir { tempDir =>
        val startTime = getStartTimeForRetentionTest
        val clock = new ManualClock(startTime)
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
        val logPath = new File(log.logPath.toUri)
        val visitedFiles = scala.collection.mutable.Set.empty[String]

        spark.sql(s"""CREATE TABLE delta.`${tempDir.toString()}` (id Int) USING delta
                     | TBLPROPERTIES(
                     |-- Disable the async log cleanup as this test needs to manually trigger log
                     |-- clean up.
                     |'delta.enableExpiredLogCleanup' = 'false',
                     |'${DeltaConfigs.CHECKPOINT_POLICY.key}' = '$checkpointPolicy',
                     |'${DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key}' = 'false',
                     |'delta.checkpointInterval' = '100000',
                     |'delta.logRetentionDuration' = 'interval 6 days')
        """.stripMargin)

        (1 to 10).foreach { dayNum =>
          clock.setTime(day(startTime, dayNum))
          log.startTransaction().commit(Seq(), testOp)
          setModificationTimeOfNewFiles(log, clock, visitedFiles)
          clock.setTime(day(startTime, dayNum) + 10)
          log.checkpoint(log.update())
          setModificationTimeOfNewFiles(log, clock, visitedFiles)
        }
        clock.setTime(day(startTime, 11))
        log.update()
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 1 to 10)
        compareVersions(getDeltaVersions(logPath), "delta", 0 to 10)

        // 11th day Run metadata cleanup.
        clock.setTime(day(startTime, 11))
        cleanUpExpiredLogs(log)
        compareVersions(getCheckpointVersions(logPath), "checkpoint", 5 to 10)
        compareVersions(getDeltaVersions(logPath), "delta", 5 to 10)
        val checkpointInstancesForV10 =
          getCheckpointFiles(logPath)
            .filter(f => getFileVersions(Seq(f)).head == 10)
            .map(f => new Path(f.getAbsolutePath))
            .sortBy(_.getName)
            .map(CheckpointInstance.apply)

        assert(checkpointInstancesForV10.size == 2)
        assert(
          checkpointInstancesForV10.map(_.format) ===
            Seq(CheckpointInstance.Format.V2, CheckpointInstance.Format.SINGLE))
      }
    }
  }

  test("Metadata cleanup respects requireCheckpointProtectionBeforeVersion") {
    withSQLConf(
        DeltaSQLConf.ALLOW_METADATA_CLEANUP_WHEN_ALL_PROTOCOLS_SUPPORTED.key -> "false",
        DeltaSQLConf.ALLOW_METADATA_CLEANUP_CHECKPOINT_EXISTENCE_CHECK_DISABLED.key -> "true") {
      // Commits should be cleaned up to the latest checkpoint.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 2,
        expectedCommitsAfterCleanup = (8 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Commits should be cleaned up to the latest checkpoint.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 6,
        expectedCommitsAfterCleanup = (8 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Commits should be cleaned up to the latest checkpoint.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 7,
        expectedCommitsAfterCleanup = (8 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Commits should be cleaned up to the latest checkpoint.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 8,
        expectedCommitsAfterCleanup = (8 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Commits should be cleaned up to the checkpoint 10.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 10,
        createNumCommitsWithinRetentionPeriod = 10,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 9,
        expectedCommitsAfterCleanup = (10 to 19),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(10))

      // Checkpoint 8 is within the retention period.
      // Cleanup should be skipped.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 9,
        expectedCommitsAfterCleanup = (0 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(6, 8, 10))

      // Cleanup should be skipped.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8),
        requireCheckpointProtectionBeforeVersion = 20,
        expectedCommitsAfterCleanup = (0 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(6, 8, 10))

      // With multiple checkpoints (8, 12, 14) within the retention period.
      // None of these should be cleaned up.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8, 12, 14),
        requireCheckpointProtectionBeforeVersion = 8,
        expectedCommitsAfterCleanup = (8 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(8, 10, 12, 14))

      // With multiple checkpoints (8, 12, 14) within the retention period.
      // requireCheckpointProtectionBeforeVersion = 9 is within the retention period.
      // Cleanup should be skipped.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(6, 8, 12, 14),
        requireCheckpointProtectionBeforeVersion = 9,
        expectedCommitsAfterCleanup = (0 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(6, 8, 10, 12, 14))

      // Corner cases.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 2,
        createNumCommitsWithinRetentionPeriod = 14,
        createCheckpoints = Set(1),
        requireCheckpointProtectionBeforeVersion = 0,
        expectedCommitsAfterCleanup = (2 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 2,
        createNumCommitsWithinRetentionPeriod = 14,
        createCheckpoints = Set(1),
        requireCheckpointProtectionBeforeVersion = 1,
        expectedCommitsAfterCleanup = (2 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 2,
        createNumCommitsWithinRetentionPeriod = 14,
        createCheckpoints = Set(1),
        requireCheckpointProtectionBeforeVersion = 2,
        expectedCommitsAfterCleanup = (2 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 2,
        createNumCommitsWithinRetentionPeriod = 14,
        createCheckpoints = Set(1),
        requireCheckpointProtectionBeforeVersion = 3,
        expectedCommitsAfterCleanup = (0 to 15),
        // Α checkpoint is automatically created every 10 commits.
        expectedCheckpointsAfterCleanup = Set(1, 10))
    }
  }

  test("Cleanup is allowed if a checkpoint already exists at the boundary") {
    withSQLConf(DeltaSQLConf.ALLOW_METADATA_CLEANUP_WHEN_ALL_PROTOCOLS_SUPPORTED.key -> "false") {
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        // Metadata cleanup should attempt to clean before version 8.
        createCheckpoints = Set(0, 8),
        requireCheckpointProtectionBeforeVersion = 10,
        unsupportedFeatureStartVersion = Some(8),
        expectedCommitsAfterCleanup = (8 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))
    }
  }

  test("Metadata cleanup protocol validation positive tests.") {
    withSQLConf(
        DeltaSQLConf.ALLOW_METADATA_CLEANUP_CHECKPOINT_EXISTENCE_CHECK_DISABLED.key -> "true") {
      // In all tests below, we cannot satisfy the version requirement and thus fallback
      // to protocol validations. We identify we support all features and proceed to
      // metadata cleanup.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        expectedCommitsAfterCleanup = (8 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // The protocol contains unsupported feature but at requireCheckpointProtectionBeforeVersion.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        unsupportedFeatureStartVersion = Some(10),
        expectedCommitsAfterCleanup = (8 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // The protocol contains unsupported feature but after
      // requireCheckpointProtectionBeforeVersion.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        unsupportedFeatureStartVersion = Some(11),
        expectedCommitsAfterCleanup = (8 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // The protocol contains unsupported feature before requireCheckpointProtectionBeforeVersion
      // but right after the boundary version where the cleanup ends.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        // Metadata cleanup should attempt to clean before version 8.
        createCheckpoints = Set(0, 8),
        requireCheckpointProtectionBeforeVersion = 10,
        unsupportedFeatureStartVersion = Some(9),
        expectedCommitsAfterCleanup = (8 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Other corner cases.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(1),
        requireCheckpointProtectionBeforeVersion = 10,
        expectedCommitsAfterCleanup = (8 to 15),
        // This is a bit weird. Cleanup should had created a checkpoint at 8.
        expectedCheckpointsAfterCleanup = Set(10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(0),
        requireCheckpointProtectionBeforeVersion = 10,
        expectedCommitsAfterCleanup = (8 to 15),
        // This is a bit weird. Cleanup should had created a checkpoint at 8.
        expectedCheckpointsAfterCleanup = Set(10))
    }
  }

  test("Metadata cleanup protocol validation negative tests.") {
    withSQLConf(
        DeltaSQLConf.ALLOW_METADATA_CLEANUP_CHECKPOINT_EXISTENCE_CHECK_DISABLED.key -> "true") {
      // In all tests below, we cannot satisfy the version requirement and thus fallback
      // to protocol validations. We should detect the start version version includes a
      // non-supported feature and skip the cleanup.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        // Unsupported feature in the first version.
        unsupportedFeatureStartVersion = Some(0),
        unsupportedFeatureEndVersion = Some(1),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(0, 8),
        requireCheckpointProtectionBeforeVersion = 10,
        // Unsupported feature right before the boundary version where the cleanup ends.
        unsupportedFeatureStartVersion = Some(7),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(0, 8, 10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        // Unsupported feature in intermediate versions.
        unsupportedFeatureStartVersion = Some(4),
        unsupportedFeatureEndVersion = Some(7),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        // Unsupported feature in dropped at the boundary version.
        unsupportedFeatureStartVersion = Some(4),
        unsupportedFeatureEndVersion = Some(8),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // The protocol contains unsupported feature before requireCheckpointProtectionBeforeVersion
      // but at the boundary version where the cleanup ends.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        // Metadata cleanup should attempt to clean before version 8.
        createCheckpoints = Set(0, 8),
        requireCheckpointProtectionBeforeVersion = 10,
        unsupportedFeatureStartVersion = Some(8),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(0, 8, 10))

      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        // Metadata cleanup should attempt to clean before version 8.
        createCheckpoints = Set(0, 8),
        requireCheckpointProtectionBeforeVersion = 10,
        // Make sure we correctly validate the protocol of the checkpoint version.
        unsupportedFeature = TestUnsupportedWriterFeature,
        unsupportedFeatureStartVersion = Some(8),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(0, 8, 10))
    }
  }

  test("Metadata cleanup protocol validation with incomplete CRCs.") {
    withSQLConf(
        DeltaSQLConf.ALLOW_METADATA_CLEANUP_CHECKPOINT_EXISTENCE_CHECK_DISABLED.key -> "true") {
      // We fall back to protocol validations which cannot be completed due to missing
      // protocol in one of the CRCs.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        incompleteCRCVersion = Some(3),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))

      // Similar to above but a CRC file is missing.
      testRequireCheckpointProtectionBeforeVersion(
        createNumCommitsOutsideRetentionPeriod = 8,
        createNumCommitsWithinRetentionPeriod = 8,
        createCheckpoints = Set(8),
        requireCheckpointProtectionBeforeVersion = 10,
        missingCRCVersion = Some(3),
        expectedCommitsAfterCleanup = (0 to 15),
        expectedCheckpointsAfterCleanup = Set(8, 10))
    }
  }
}

class DeltaRetentionWithCoordinatedCommitsBatch1Suite extends DeltaRetentionSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

/**
 * This test suite does not extend other tests of DeltaRetentionSuiteEdge because
 * DeltaRetentionSuiteEdge contain tests that rely on setting the file modification time for delta
 * files. However, in this suite, delta files might be backfilled asynchronously, which means
 * setting the modification time will not work as expected.
 */
class DeltaRetentionWithCoordinatedCommitsBatch2Suite extends QueryTest
    with DeltaSQLCommandTest
    with DeltaRetentionSuiteBase {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)

  override def getLogFiles(dir: File): Seq[File] =
    getDeltaFiles(dir) ++ getUnbackfilledDeltaFiles(dir) ++ getCheckpointFiles(dir)

  /**
   * This test verifies that unbackfilled versions, i.e., versions for which backfilled deltas do
   * not exist yet, are never considered for deletion, even if they fall outside the retention
   * window. The primary reason for not deleting these versions is that the CommitCoordinator might
   * be actively tracking those files, and currently, MetadataCleanup does not communicate with the
   * CommitCoordinator.
   *
   * Although the fact that they are unbackfilled is somewhat redundant since these versions are
   * currently already protected due to two additional reasons:
   * 1.They will always be part of the latest snapshot.
   * 2.They don't have two checkpoints after them.
   * However, this test helps ensure that unbackfilled deltas remain protected in the future, even
   * if the above two conditions are no longer triggered.
   *
   * Note: This test is too slow for batchSize = 100 and wouldn't necessarily work for batchSize = 1
   */
  test("unbackfilled expired commits are always retained") {
    withTempDir { tempDir =>
      val startTime = getStartTimeForRetentionTest
      val clock = new ManualClock(startTime)
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri.getPath)
      val fs = new RawLocalFileSystem()
      fs.initialize(tempDir.toURI, new Configuration())

      log.startTransaction().commitManually(createTestAddFile("1"))
      log.checkpoint()
      spark.sql(s"""ALTER TABLE delta.`${tempDir.toString}`
                   |SET TBLPROPERTIES(
                   |-- Trigger log clean up manually.
                   |'delta.enableExpiredLogCleanup' = 'false',
                   |'delta.checkpointInterval' = '10000',
                   |'delta.checkpointRetentionDuration' = 'interval 2 days',
                   |'delta.logRetentionDuration' = 'interval 30 days',
                   |'delta.enableFullRetentionRollback' = 'true')
        """.stripMargin)
      log.checkpoint()
      setModificationTime(log, startTime, 0, 0, fs)
      setModificationTime(log, startTime, 1, 0, fs)
      // Create commits [2, 6] with a checkpoint per commit
      2 to 6 foreach { i =>
        log.startTransaction().commitManually(createTestAddFile(s"$i"))
        log.checkpoint()
        setModificationTime(log, startTime, i, 0, fs)
      }
      // Create unbackfilled commit [7] with no checkpoints
      log.startTransaction().commitManually(createTestAddFile("7"))
      setModificationTime(log, startTime, 7, 0, fs)

      // Everything is eligible for deletion but we don't consider the unbackfilled commit,
      // i.e. [7], for  deletion because it is part of the current LogSegment.
      clock.setTime(day(startTime, 100))
      log.cleanUpExpiredLogs(log.update())
      // Since we also need a checkpoint, [6] is also protected.
      val firstProtectedVersion = 6
      compareVersions(
        getDeltaVersions(logPath),
        "backfilled delta",
        firstProtectedVersion to 6)
      compareVersions(
        getUnbackfilledDeltaVersions(logPath),
        "unbackfilled delta",
        firstProtectedVersion to 7)
    }
  }
}

