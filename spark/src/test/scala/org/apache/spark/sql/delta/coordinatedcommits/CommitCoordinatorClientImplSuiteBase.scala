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

package org.apache.spark.sql.delta.coordinatedcommits

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.{LogStore, LogStoreProvider}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import io.delta.dynamodbcommitcoordinator.DynamoDBCommitCoordinatorClient
import io.delta.storage.commit.{Commit => JCommit, CommitFailedException => JCommitFailedException, GetCommitsResponse => JGetCommitsResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{ThreadUtils, Utils}

trait CommitCoordinatorClientImplSuiteBase extends QueryTest
    with SharedSparkSession
    with LogStoreProvider
    with CoordinatedCommitsTestUtils
    with DeltaSQLTestUtils
    with DeltaSQLCommandTest {

  /**
   * Needs to be overwritten by implementing classes to provide a [[TableCommitCoordinatorClient]]
   * wrapping the commit coordinator client that should be tested.
   */
  protected def createTableCommitCoordinatorClient(deltaLog: DeltaLog): TableCommitCoordinatorClient

  /**
   * Needs to be overwritten by implementing classes to provide an implementation
   * of backfill registration.
   */
  protected def registerBackfillOp(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      deltaLog: DeltaLog,
      version: Long): Unit

  /**
   * Needs to be overwritten by implementing classes to provide a way of validating
   * that the commit coordinator client under test performs backfilling as expected at
   * the specified version.
   */
  protected def validateBackfillStrategy(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      logPath: Path,
      version: Long): Unit

  /**
   * Needs to be overwritten by implementing classes to provide a way of validating
   * the results of a getCommits call with the specified start and end versions,
   * where maxVersion is the current latest version of the table.
   */
  protected def validateGetCommitsResult(
    response: JGetCommitsResponse,
    startVersion: Option[Long],
    endVersion: Option[Long],
    maxVersion: Long): Unit

  /**
   * Checks that the commit coordinator state is correct in terms of
   *  - The latest table version in the commit coordinator is correct
   *  - All supposedly backfilled commits are indeed backfilled
   *  - The contents of the backfilled commits are correct (verified
   *     if commitTimestampOpt is provided)
   *
   * This can be overridden by implementing classes to implement
   * more specific invariants.
   */
  protected def assertInvariants(
       logPath: Path,
       tableCommitCoordinatorClient: TableCommitCoordinatorClient,
       commitTimestampsOpt: Option[Array[Long]] = None): Unit = {
    val maxUntrackedVersion: Int = {
      val commitResponse = tableCommitCoordinatorClient.getCommits()
      if (commitResponse.getCommits.isEmpty) {
        commitResponse.getLatestTableVersion.toInt
      } else {
        assert(commitResponse.getCommits.asScala.last.getVersion ==
            commitResponse.getLatestTableVersion,
          s"Max commit tracked by the commit coordinator " +
            s"${commitResponse.getCommits.asScala.last} must " +
            s"match latestTableVersion tracked by the commit coordinator " +
            s"${commitResponse.getLatestTableVersion}."
        )
        val minVersion = commitResponse.getCommits.asScala.head.getVersion
        assert(
          commitResponse.getLatestTableVersion - minVersion + 1 == commitResponse.getCommits.size,
          "Commit map should have a contiguous range of unbackfilled commits."
        )
        minVersion.toInt - 1
      }
    }
    (0 to maxUntrackedVersion).foreach { version =>
      assertBackfilled(version, logPath, commitTimestampsOpt.map(_(version)))
    }
  }

  protected def writeCommitZero(logPath: Path): Unit = {
    store.write(FileNames.unsafeDeltaFile(logPath, 0), Iterator("0", "0"), overwrite = false)
  }

  /**
   * The metadata that should be passed to the registerTable call. By default, this
   * is empty but implementing classes can overwrite this method to provide custom
   * metadata.
   */
  protected def initMetadata(): Metadata = Metadata()

  // scalastyle:off deltahadoopconfiguration
  protected def sessionHadoopConf: Configuration = spark.sessionState.newHadoopConf()
  // scalastyle:on deltahadoopconfiguration

  protected def store: LogStore = createLogStore(spark)

  protected def withTempTableDir(f: File => Unit): Unit = {
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
      tableCommitCoordinatorClient: TableCommitCoordinatorClient): JCommit = {
    val commitInfo = CommitInfo.empty(version = Some(version)).withTimestamp(timestamp)
      .copy(inCommitTimestamp = Some(timestamp))
    val updatedActions = if (version == 0) {
      getUpdatedActionsForZerothCommit(commitInfo)
    } else {
      getUpdatedActionsForNonZerothCommit(commitInfo)
    }
    tableCommitCoordinatorClient.commit(
      version,
      Iterator(s"$version", s"$timestamp"),
      updatedActions).getCommit
  }

  protected def assertBackfilled(
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

  protected def assertCommitFail(
      currentVersion: Long,
      expectedVersion: Long,
      retryable: Boolean,
      commitFunc: => JCommit): Unit = {
    val e = intercept[JCommitFailedException] {
      commitFunc
    }
    assert(e.getRetryable == retryable)
    assert(e.getConflict == retryable)
    val expectedMessage = if (currentVersion == 0) {
      "Commit version 0 must go via filesystem."
    } else {
      s"Commit version $currentVersion is not valid. Expected version: $expectedVersion."
    }
    assert(e.getMessage === expectedMessage)
  }

  protected def assertResponseEquals(
      resp1: JGetCommitsResponse,
      resp2: JGetCommitsResponse): Unit = {
    assert(resp1.getLatestTableVersion == resp2.getLatestTableVersion)
    assert(resp1.getCommits == resp2.getCommits)
  }

  test("test basic commit and backfill functionality") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)

      val e = intercept[JCommitFailedException] {
        commit(version = 0, timestamp = 0, tableCommitCoordinatorClient)
      }
      assert(e.getMessage === "Commit version 0 must go via filesystem.")
      writeCommitZero(logPath)
      assertResponseEquals(tableCommitCoordinatorClient.getCommits(),
        new JGetCommitsResponse(Seq.empty.asJava, -1))
      assertBackfilled(version = 0, logPath, Some(0L))

      // Test backfilling functionality for commits 1 - 8
      (1 to 8).foreach { version =>
        commit(version, version, tableCommitCoordinatorClient)
        validateBackfillStrategy(tableCommitCoordinatorClient, logPath, version)
        assert(tableCommitCoordinatorClient.getCommits().getLatestTableVersion == version)
      }

      // Test that out-of-order backfill is rejected
      intercept[IllegalArgumentException] {
        registerBackfillOp(tableCommitCoordinatorClient, log, 10)
      }
      assertInvariants(logPath, tableCommitCoordinatorClient)
    }
  }

  test("startVersion and endVersion are respected in getCommits") {
    def runGetCommitsAndValidate(
        client: TableCommitCoordinatorClient,
        startVersion: Option[Long],
        endVersion: Option[Long],
        maxVersion: Long): Unit = {
      val result = client.getCommits(startVersion, endVersion)
      validateGetCommitsResult(result, startVersion, endVersion, maxVersion)
    }

    withTempTableDir { tempDir =>
      // prepare a table with 15 commits
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)
      val maxVersion = 15
      (1 to maxVersion).foreach { version =>
        commit(version, version, tableCommitCoordinatorClient)
      }

      runGetCommitsAndValidate(tableCommitCoordinatorClient, None, None, maxVersion)
      runGetCommitsAndValidate(tableCommitCoordinatorClient, Some(9), None, maxVersion)
      runGetCommitsAndValidate(tableCommitCoordinatorClient, Some(11), Some(14), maxVersion)
      runGetCommitsAndValidate(tableCommitCoordinatorClient, Some(12), Some(12), maxVersion)
      runGetCommitsAndValidate(tableCommitCoordinatorClient, None, Some(14), maxVersion)
    }
  }

  test("test out-of-order backfills are rejected") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.getPath)
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
      // commit-0 must be file system based
      writeCommitZero(logPath)
      (1 to 3).foreach(i => commit(i, i, tableCommitCoordinatorClient))

      // Test that backfilling is idempotent for already-backfilled commits.
      registerBackfillOp(tableCommitCoordinatorClient, log, 2)
      registerBackfillOp(tableCommitCoordinatorClient, log, 2)

      // Test that backfilling uncommited commits fail.
      intercept[IllegalArgumentException] {
        registerBackfillOp(tableCommitCoordinatorClient, log, 4)
      }
    }
  }

  test("test out-of-order commits are rejected") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)

      // commit-0 must be file system based
      writeCommitZero(logPath)
      // Verify that conflict-checker rejects out-of-order commits.
      (1 to 4).foreach(i => commit(i, i, tableCommitCoordinatorClient))
      // A retry of commit 0 fails from commit coordinator client with a conflict and it can't be
      // retried as commit 0 is upgrading the commit coordinator client.
      assertCommitFail(0, 5, retryable = false, commit(0, 5, tableCommitCoordinatorClient))
      assertCommitFail(4, 5, retryable = true, commit(4, 6, tableCommitCoordinatorClient))

      commit(5, 5, tableCommitCoordinatorClient)
      validateGetCommitsResult(tableCommitCoordinatorClient.getCommits(), None, None, 5)
      assertCommitFail(5, 6, retryable = true, commit(5, 5, tableCommitCoordinatorClient))
      assertCommitFail(7, 6, retryable = false, commit(7, 7, tableCommitCoordinatorClient))

      assertInvariants(logPath, tableCommitCoordinatorClient)
    }
  }

  test("should handle concurrent readers and writers") {
    withTempTableDir { tempDir =>
      val tablePath = new Path(tempDir.getCanonicalPath)
      val logPath = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
      val tcs = createTableCommitCoordinatorClient(DeltaLog.forTable(spark, tablePath))

      val numberOfWriters = 11
      val numberOfCommitsPerWriter = 11
      // scalastyle:off sparkThreadPools
      val executor = DeltaThreadPool("commitCoordinatorSuite", numberOfWriters)
      // scalastyle:on sparkThreadPools
      val runningTimestamp = new AtomicInteger(0)
      val commitFailedExceptions = new AtomicInteger(0)
      // commit-0 must be file system based
      writeCommitZero(logPath)

      try {
        val tasks = (0 until numberOfWriters).map { i =>
          executor.submit(spark) {
              var currentWriterCommits = 0
              while (currentWriterCommits < numberOfCommitsPerWriter) {
                val nextVersion = math.max(tcs.getCommits().getLatestTableVersion + 1, 1)
                try {
                  val currentTimestamp = runningTimestamp.getAndIncrement()
                  val commitResponse = commit(nextVersion, currentTimestamp, tcs)
                  currentWriterCommits += 1
                  assert(commitResponse.getCommitTimestamp == currentTimestamp)
                  assert(commitResponse.getVersion == nextVersion)
                } catch {
                  case e: JCommitFailedException =>
                    assert(e.getConflict)
                    assert(e.getRetryable)
                    commitFailedExceptions.getAndIncrement()
                } finally {
                  assertInvariants(logPath, tcs)
                }
              }
            }
        }
        tasks.foreach(ThreadUtils.awaitResult(_, 150.seconds))
      } catch {
        case e: InterruptedException =>
          fail("Test interrupted: " + e.getMessage)
      }
    }
  }
}
