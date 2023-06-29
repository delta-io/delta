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

import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.ManualClock

// scalastyle:off: removeFile
class DeltaRetentionSuite extends QueryTest
  with DeltaRetentionSuiteBase
  with SQLTestUtils
  with DeltaSQLCommandTest {

  protected override def sparkConf: SparkConf = super.sparkConf

  override protected def getLogFiles(dir: File): Seq[File] =
    getDeltaFiles(dir) ++ getCheckpointFiles(dir)

  test("delete expired logs") {
    withTempDir { tempDir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri)
      (1 to 5).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
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
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val logPath = new File(log.logPath.toUri)
      val iterationCount = (log.checkpointInterval() * 2) + 1

      (1 to iterationCount).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        val version = txn.commit(delete ++ file, testOp)
        val deltaFile = new File(FileNames.deltaFile(log.logPath, version).toUri)
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
      val clock = new ManualClock(System.currentTimeMillis())
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
      val clock = new ManualClock(System.currentTimeMillis())
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
      val clock = new ManualClock(System.currentTimeMillis())
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
      val clock = new ManualClock(System.currentTimeMillis())
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
      val clock = new ManualClock(System.currentTimeMillis())
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
      val clock = new ManualClock(System.currentTimeMillis())
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
}
