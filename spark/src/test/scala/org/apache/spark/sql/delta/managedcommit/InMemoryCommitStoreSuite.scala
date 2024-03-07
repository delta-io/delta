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
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.storage.{LogStore, LogStoreProvider}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class InMemoryCommitStoreSuite extends QueryTest
  with SharedSparkSession
  with LogStoreProvider {

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
      cs: CommitStore,
      logPath: Path): Commit = {
    val commitInfo = CommitInfo.empty(version = Some(version)).withTimestamp(timestamp)
    cs.commit(
      store,
      sessionHadoopConf,
      logPath,
      version,
      Iterator(s"$version", s"$timestamp"),
      UpdatedActions(commitInfo, None, None)).commit
  }

  private def assertBackfilled(
      version: Long,
      logPath: Path,
      timestampOpt: Option[Long] = None): Unit = {
    val delta = FileNames.deltaFile(logPath, version)
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
    assert(
      e.getMessage ==
      s"Commit version $currentVersion is not valid. Expected version: $expectedVersion.")
  }

  private def assertInvariants(
      logPath: Path,
      cs: InMemoryCommitStore,
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

  test("in-memory-commit-store-builder works as expected") {
    val builder1 = InMemoryCommitStoreBuilder(5)
    val cs1 = builder1.build(Map.empty)
    assert(cs1.isInstanceOf[InMemoryCommitStore])
    assert(cs1.asInstanceOf[InMemoryCommitStore].batchSize == 5)

    val cs1_again = builder1.build(Map.empty)
    assert(cs1_again.isInstanceOf[InMemoryCommitStore])
    assert(cs1 == cs1_again)

    val builder2 = InMemoryCommitStoreBuilder(10)
    val cs2 = builder2.build(Map.empty)
    assert(cs2.isInstanceOf[InMemoryCommitStore])
    assert(cs2.asInstanceOf[InMemoryCommitStore].batchSize == 10)
    assert(cs2 ne cs1)

    val builder3 = InMemoryCommitStoreBuilder(10)
    val cs3 = builder3.build(Map.empty)
    assert(cs3.isInstanceOf[InMemoryCommitStore])
    assert(cs3.asInstanceOf[InMemoryCommitStore].batchSize == 10)
    assert(cs3 ne cs2)
  }

  test("test basic commit and backfill functionality") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val cs = InMemoryCommitStoreBuilder(batchSize = 3).build(Map.empty)

      assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, -1))

      // Commit 0 is always immediately backfilled
      val c0 = commit(0, 0, cs, logPath)
      assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, 0))
      assertBackfilled(0, logPath, Some(0))

      val c1 = commit(1, 1, cs, logPath)
      val c2 = commit(2, 2, cs, logPath)
      assert(cs.getCommits(logPath, 0).commits.takeRight(2) == Seq(c1, c2))

      // All 3 commits are backfilled since batchSize == 3
      val c3 = commit(3, 3, cs, logPath)
      assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, 3))
      (1 to 3).foreach(i => assertBackfilled(i, logPath, Some(i)))

      // Test that startVersion and endVersion are respected in getCommits
      val c4 = commit(4, 4, cs, logPath)
      val c5 = commit(5, 5, cs, logPath)
      assert(cs.getCommits(logPath, 4) == GetCommitsResponse(Seq(c4, c5), 5))
      assert(cs.getCommits(logPath, 4, Some(4)) == GetCommitsResponse(Seq(c4), 5))
      assert(cs.getCommits(logPath, 5) == GetCommitsResponse(Seq(c5), 5))

      // Commit [4, 6] are backfilled since batchSize == 3
      val c6 = commit(6, 6, cs, logPath)
      assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, 6))
      (4 to 6).foreach(i => assertBackfilled(i, logPath, Some(i)))
      assertInvariants(logPath, cs.asInstanceOf[InMemoryCommitStore])
    }
  }

  test("test basic commit and backfill functionality with 1 batch size") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val cs = InMemoryCommitStoreBuilder(batchSize = 1).build(Map.empty)

      // Test that all commits are immediately backfilled
      (0 to 3).foreach { version =>
        commit(version, version, cs, logPath)
        assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, version))
        assertBackfilled(version, logPath, Some(version))
      }

      // Test that out-of-order backfill is rejected
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitStore]
          .registerBackfill(logPath, 5)
      }
      assertInvariants(logPath, cs.asInstanceOf[InMemoryCommitStore])
    }
  }

  test("test out-of-order commits are rejected") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val cs = InMemoryCommitStoreBuilder(batchSize = 5).build(Map.empty)

      // Anything other than version-0 should be rejected as the first commit
      assertCommitFail(1, 0, retryable = false, commit(1, 0, cs, logPath))

      // Verify that conflict-checker rejects out-of-order commits.
      (0 to 4).foreach(i => commit(i, i, cs, logPath))
      assertCommitFail(0, 5, retryable = true, commit(0, 5, cs, logPath))
      assertCommitFail(4, 5, retryable = true, commit(4, 6, cs, logPath))

      // Verify that the conflict-checker still works even when everything has been backfilled
      commit(5, 5, cs, logPath)
      assert(cs.getCommits(logPath, 0) == GetCommitsResponse(Seq.empty, 5))
      assertCommitFail(5, 6, retryable = true, commit(5, 5, cs, logPath))
      assertCommitFail(7, 6, retryable = false, commit(7, 7, cs, logPath))

      assertInvariants(logPath, cs.asInstanceOf[InMemoryCommitStore])
    }
  }

  test("test out-of-order backfills are rejected") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val cs = InMemoryCommitStoreBuilder(batchSize = 5).build(Map.empty)
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitStore].registerBackfill(logPath, 0)
      }
      (0 to 3).foreach(i => commit(i, i, cs, logPath))

      // Test that backfilling is idempotent for already-backfilled commits.
      cs.asInstanceOf[InMemoryCommitStore].registerBackfill(logPath, 2)
      cs.asInstanceOf[InMemoryCommitStore].registerBackfill(logPath, 2)

      // Test that backfilling uncommited commits fail.
      intercept[IllegalArgumentException] {
        cs.asInstanceOf[InMemoryCommitStore].registerBackfill(logPath, 4)
      }
    }
  }

  test("should handle concurrent readers and writers") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val batchSize = 6
      val cs = InMemoryCommitStoreBuilder(batchSize).build(Map.empty)

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
                val nextVersion = cs.getCommits(logPath, 0).latestTableVersion + 1
                try {
                  val currentTimestamp = runningTimestamp.getAndIncrement()
                  val commitResponse = commit(nextVersion, currentTimestamp, cs, logPath)
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
                    cs.asInstanceOf[InMemoryCommitStore],
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
