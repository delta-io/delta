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

package org.apache.spark.sql.delta.managedcommit

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.{LogStore, LogStoreProvider}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class InMemoryCommitOwnerSuite extends QueryTest
  with DeltaSQLTestUtils
  with SharedSparkSession
  with LogStoreProvider
  with DeltaSQLCommandTest
  with ManagedCommitTestUtils {

  // scalastyle:off deltahadoopconfiguration
  def sessionHadoopConf: Configuration = spark.sessionState.newHadoopConf()
  // scalastyle:on deltahadoopconfiguration

  def store: LogStore = createLogStore(spark)

  private def withTempTableDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    val deltaLogDir = new File(dir, DeltaLog.LOG_DIR_NAME)
    deltaLogDir.mkdir()
    val commitLogDir = new File(deltaLogDir, FileNames.COMMIT_SUBDIR)
    commitLogDir.mkdir()
    try f(dir)
    finally {
      Utils.deleteRecursively(dir)
    }
  }

  protected def commit(
      version: Long,
      timestamp: Long,
      tableCommitOwnerClient: TableCommitOwnerClient): Commit = {
    val commitInfo = CommitInfo.empty(version = Some(version)).withTimestamp(timestamp)
    val updatedActions = if (version == 0) {
      getUpdatedActionsForZerothCommit(commitInfo)
    } else {
      getUpdatedActionsForNonZerothCommit(commitInfo)
    }
    tableCommitOwnerClient.commit(
      version,
      Iterator(s"$version", s"$timestamp"),
      updatedActions).commit
  }

  private def assertBackfilled(
      version: Long,
      logPath: Path,
      timestampOpt: Option[Long] = None): Unit = {
    val delta = FileNames.unsafeDeltaFile(logPath, version)
    if (timestampOpt.isDefined) {
      assert(store.read(delta, sessionHadoopConf) == Seq(s"$version", s"${timestampOpt.get}"))
    } else {
      assert(store.read(delta, sessionHadoopConf).take(1) == Seq(s"$version"))
    }
  }

  private def assertCommitFail(
      currentVersion: Long,
      expectedVersion: Long,
      retryable: Boolean,
      commitFunc: => Commit): Unit = {
    val e = intercept[CommitFailedException] {
      commitFunc
    }
    assert(e.retryable == retryable)
    assert(e.conflict == retryable)
    val expectedMessage = if (currentVersion == 0) {
      "Commit version 0 must go via filesystem."
    } else {
      s"Commit version $currentVersion is not valid. Expected version: $expectedVersion."
    }
    assert(e.getMessage === expectedMessage)
  }

  private def assertInvariants(
      logPath: Path,
      cs: InMemoryCommitOwner,
      commitTimestampsOpt: Option[Array[Long]] = None): Unit = {
    val maxUntrackedVersion: Int = cs.withReadLock[Int](logPath) {
      val tableData = cs.perTableMap.get(logPath)
      if (tableData.commitsMap.isEmpty) {
        tableData.maxCommitVersion.toInt
      } else {
        assert(
          tableData.commitsMap.last._1 == tableData.maxCommitVersion,
          s"Max version in commitMap ${tableData.commitsMap.last._1} must match max version in " +
            s"maxCommitVersionMap $tableData.maxCommitVersion.")
        val minVersion = tableData.commitsMap.head._1
        assert(
          tableData.maxCommitVersion - minVersion + 1 == tableData.commitsMap.size,
          "Commit map should have a contiguous range of unbackfilled commits.")
        minVersion.toInt - 1
      }
    }
    (0 to maxUntrackedVersion).foreach { version =>
      assertBackfilled(version, logPath, commitTimestampsOpt.map(_(version)))}
  }

  test("InMemoryCommitOwnerBuilder works as expected") {
    val builder1 = InMemoryCommitOwnerBuilder(5)
    val cs1 = builder1.build(Map.empty)
    assert(cs1.isInstanceOf[InMemoryCommitOwner])
    assert(cs1.asInstanceOf[InMemoryCommitOwner].batchSize == 5)

    val cs1_again = builder1.build(Map.empty)
    assert(cs1_again.isInstanceOf[InMemoryCommitOwner])
    assert(cs1 == cs1_again)

    val builder2 = InMemoryCommitOwnerBuilder(10)
    val cs2 = builder2.build(Map.empty)
    assert(cs2.isInstanceOf[InMemoryCommitOwner])
    assert(cs2.asInstanceOf[InMemoryCommitOwner].batchSize == 10)
    assert(cs2 ne cs1)

    val builder3 = InMemoryCommitOwnerBuilder(10)
    val cs3 = builder3.build(Map.empty)
    assert(cs3.isInstanceOf[InMemoryCommitOwner])
    assert(cs3.asInstanceOf[InMemoryCommitOwner].batchSize == 10)
    assert(cs3 ne cs2)
  }

  test("test basic commit and backfill functionality") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir)
      val logPath = log.logPath
      val cs = InMemoryCommitOwnerBuilder(batchSize = 3).build(Map.empty)
      val tcs = TableCommitOwnerClient(cs, log, Map.empty[String, String])

      cs.registerTable(logPath, currentVersion = -1L, Metadata(), Protocol(1, 1))
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, -1))

      // Commit 0 must be done by file-system
      val e = intercept[CommitFailedException] { commit(version = 0, timestamp = 0, tcs) }
      assert(e.getMessage === "Commit version 0 must go via filesystem.")
      store.write(FileNames.unsafeDeltaFile(logPath, 0), Iterator("0", "0"), overwrite = false)
      // Commit 0 doesn't go through commit-owner. So commit-owner is not aware of it in getCommits
      // response.
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, -1))
      assertBackfilled(0, logPath, Some(0))

      val c1 = commit(1, 1, tcs)
      val c2 = commit(2, 2, tcs)
      assert(tcs.getCommits(0).commits.takeRight(2) == Seq(c1, c2))

      // All 3 commits are backfilled since batchSize == 3
      val c3 = commit(3, 3, tcs)
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, 3))
      (1 to 3).foreach(i => assertBackfilled(i, logPath, Some(i)))

      // Test that startVersion and endVersion are respected in getCommits
      val c4 = commit(4, 4, tcs)
      val c5 = commit(5, 5, tcs)
      assert(tcs.getCommits(4) == GetCommitsResponse(Seq(c4, c5), 5))
      assert(tcs.getCommits(4, Some(4)) == GetCommitsResponse(Seq(c4), 5))
      assert(tcs.getCommits(5) == GetCommitsResponse(Seq(c5), 5))

      // Commit [4, 6] are backfilled since batchSize == 3
      val c6 = commit(6, 6, tcs)
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, 6))
      (4 to 6).foreach(i => assertBackfilled(i, logPath, Some(i)))
      assertInvariants(logPath, tcs.commitOwnerClient.asInstanceOf[InMemoryCommitOwner])
    }
  }

  test("test basic commit and backfill functionality with 1 batch size") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      val logPath = log.logPath
      val cs = InMemoryCommitOwnerBuilder(batchSize = 1).build(Map.empty)
      cs.registerTable(logPath, currentVersion = -1L, Metadata(), Protocol(1, 1))
      val tcs = TableCommitOwnerClient(cs, log, Map.empty[String, String])

      val e = intercept[CommitFailedException] { commit(version = 0, timestamp = 0, tcs) }
      assert(e.getMessage === "Commit version 0 must go via filesystem.")
      store.write(FileNames.unsafeDeltaFile(logPath, 0), Iterator("0", "0"), overwrite = false)
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, -1))
      assertBackfilled(version = 0, logPath, Some(0L))

      // Test that all commits are immediately backfilled
      (1 to 3).foreach { version =>
        commit(version, version, tcs)
        assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, version))
        assertBackfilled(version, logPath, Some(version))
      }

      // Test that out-of-order backfill is rejected
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitOwner]
          .registerBackfill(logPath, 5)
      }
      assertInvariants(logPath, cs.asInstanceOf[InMemoryCommitOwner])
    }
  }

  test("test out-of-order commits are rejected") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      val logPath = log.logPath
      val cs = InMemoryCommitOwnerBuilder(batchSize = 5).build(Map.empty)
      cs.registerTable(logPath, currentVersion = -1L, Metadata(), Protocol(1, 1))
      val tcs = TableCommitOwnerClient(cs, log, Map.empty[String, String])

      // Anything other than version-0 or version-1 should be rejected as the first commit
      // version-0 will be directly backfilled and won't be recorded in InMemoryCommitOwner.
      // version-1 is what commit-owner is accepting.
      assertCommitFail(2, 1, retryable = false, commit(2, 0, tcs))

      // commit-0 must be file system based
      store.write(FileNames.unsafeDeltaFile(logPath, 0), Iterator("0", "0"), overwrite = false)
      // Verify that conflict-checker rejects out-of-order commits.
      (1 to 4).foreach(i => commit(i, i, tcs))
      // A retry of commit 0 fails from commit-owner with a conflict and it can't be retried as
      // commit 0 is upgrading the commit-owner.
      assertCommitFail(0, 5, retryable = false, commit(0, 5, tcs))
      assertCommitFail(4, 5, retryable = true, commit(4, 6, tcs))

      // Verify that the conflict-checker still works even when everything has been backfilled
      commit(5, 5, tcs)
      assert(tcs.getCommits(0) == GetCommitsResponse(Seq.empty, 5))
      assertCommitFail(5, 6, retryable = true, commit(5, 5, tcs))
      assertCommitFail(7, 6, retryable = false, commit(7, 7, tcs))

      assertInvariants(logPath, cs.asInstanceOf[InMemoryCommitOwner])
    }
  }

  test("test out-of-order backfills are rejected") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      val logPath = log.logPath
      val cs = InMemoryCommitOwnerBuilder(batchSize = 5).build(Map.empty)
      val tcs = TableCommitOwnerClient(cs, log, Map.empty[String, String])
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitOwner].registerBackfill(logPath, 0)
      }
      cs.registerTable(logPath, currentVersion = -1L, Metadata(), Protocol(1, 1))
      // commit-0 must be file system based
      store.write(FileNames.unsafeDeltaFile(logPath, 0), Iterator("0", "0"), overwrite = false)
      (1 to 3).foreach(i => commit(i, i, tcs))

      // Test that backfilling is idempotent for already-backfilled commits.
      cs.asInstanceOf[InMemoryCommitOwner].registerBackfill(logPath, 2)
      cs.asInstanceOf[InMemoryCommitOwner].registerBackfill(logPath, 2)

      // Test that backfilling uncommited commits fail.
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitOwner].registerBackfill(logPath, 4)
      }
    }
  }

  test("should handle concurrent readers and writers") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val batchSize = 6
      val cs = InMemoryCommitOwnerBuilder(batchSize).build(Map.empty)
      val tcs =
        TableCommitOwnerClient(cs, DeltaLog.forTable(spark, tablePath), Map.empty[String, String])

      val numberOfWriters = 10
      val numberOfCommitsPerWriter = 10
      // scalastyle:off sparkThreadPools
      val executor = Executors.newFixedThreadPool(numberOfWriters)
      // scalastyle:on sparkThreadPools
      val runningTimestamp = new AtomicInteger(0)
      val commitFailedExceptions = new AtomicInteger(0)
      val totalCommits = numberOfWriters * numberOfCommitsPerWriter
      val commitTimestamp: Array[Long] = new Array[Long](totalCommits)

      try {
        (0 until numberOfWriters).foreach { i =>
          executor.submit(new Runnable {
            override def run(): Unit = {
              var currentWriterCommits = 0
              while (currentWriterCommits < numberOfCommitsPerWriter) {
                val nextVersion = tcs.getCommits(0).latestTableVersion + 1
                try {
                  val currentTimestamp = runningTimestamp.getAndIncrement()
                  val commitResponse = commit(nextVersion, currentTimestamp, tcs)
                  currentWriterCommits += 1
                  assert(commitResponse.commitTimestamp == currentTimestamp)
                  assert(commitResponse.version == nextVersion)
                  commitTimestamp(commitResponse.version.toInt) = commitResponse.commitTimestamp
                } catch {
                  case e: CommitFailedException =>
                    assert(e.conflict)
                    assert(e.retryable)
                    commitFailedExceptions.getAndIncrement()
                } finally {
                  assertInvariants(
                    logPath,
                    cs.asInstanceOf[InMemoryCommitOwner],
                    Some(commitTimestamp))
                }
              }
            }
          })
        }

        executor.shutdown()
        executor.awaitTermination(15, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          fail("Test interrupted: " + e.getMessage)
      }
    }
  }
}
