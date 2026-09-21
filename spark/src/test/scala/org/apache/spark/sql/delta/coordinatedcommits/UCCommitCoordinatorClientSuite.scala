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

import java.io.IOException
import java.lang.{Long => JLong}
import java.util.{Collections, List => JList, Optional, UUID}

import scala.collection.JavaConverters._
import scala.jdk.OptionConverters._
import scala.reflect.ClassTag

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaIllegalArgumentException, DeltaLog, LogSegment, Snapshot}
import org.apache.spark.sql.delta.CommitCoordinatorGetCommitsFailedException
import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{CommitInfo, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.coordinatedcommits.CatalogTrackedInfo
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStoreInverseAdaptor
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.storage.LogStore
import io.delta.storage.commit.{
  Commit => JCommit,
  CommitFailedException => JCommitFailedException,
  CoordinatedCommitsUtils => JCoordinatedCommitsUtils,
  GetCommitsResponse => JGetCommitsResponse,
  TableDescriptor,
  TableIdentifier => JTableIdentifier,
  UpdatedActions
}
import io.delta.storage.commit.actions.{AbstractDomainMetadata, AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{
  CommitCompletionUnknownException,
  CommitOutcomeUnknownException,
  UCClient,
  UCCommitCoordinatorClient,
  UCCoordinatedCommitsUsageLogs}
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path}
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.{mock, when}
import org.scalatest.PrivateMethodTester
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SystemClock

class UCCommitCoordinatorClientSuite extends UCCommitCoordinatorClientSuiteBase
    with PrivateMethodTester
{
  protected override def sparkConf = super.sparkConf
      .set("spark.sql.catalog.main", "io.unitycatalog.spark.UCSingleCatalog")
      .set("spark.sql.catalog.main.uri", "https://test-uri.com")
      .set("spark.sql.catalog.main.token", "test-token")
      .set("spark.hadoop.fs.file.impl", classOf[LocalFileSystem].getCanonicalName)

  override protected def commit(
      version: Long,
      timestamp: Long,
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      tableIdentifier: Option[TableIdentifier] = None): JCommit = {
    val commitResult = super.commit(
      version, timestamp, tableCommitCoordinatorClient, tableIdentifier)
    // As backfilling for UC happens after every commit asynchronously, we block here until
    // the current in-progress backfill has completed in order to make tests deterministic.
    waitForBackfill(version, tableCommitCoordinatorClient)
    commitResult
  }
  protected def assertUsageLogsContains(usageLogs: Seq[UsageRecord], opType: String): Unit = {
    assert(usageLogs.exists { record =>
      record.tags.get("opType").contains(opType)
    })
  }

  test("getCommits forwards table identifier to UCClient") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      var capturedTableIdentifier: JTableIdentifier = null
      val capturingUCClient = new InMemoryUCClient(metastoreId.toString, ucCommitCoordinator) {
        override def getCommits(
            tableId: String,
            tableUri: java.net.URI,
            tableIdentifier: JTableIdentifier,
            startVersion: Optional[JLong],
            endVersion: Optional[JLong]): JGetCommitsResponse = {
          capturedTableIdentifier = tableIdentifier
          new JGetCommitsResponse(Collections.emptyList(), 0L)
        }
      }
      val tableCommitCoordinatorClient = TableCommitCoordinatorClient(
        new UCCommitCoordinatorClient(Map.empty[String, String].asJava, capturingUCClient),
        log,
        Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableUUID.toString)
      )
      val tableIdentifier = TableIdentifier("tbl", Some("default"), Some("main"))

      tableCommitCoordinatorClient.getCommits(Some(tableIdentifier))

      assert(capturedTableIdentifier != null)
      assert(capturedTableIdentifier.getNamespace.toSeq == Seq("main", "default"))
      assert(capturedTableIdentifier.getName == "tbl")
    }
  }

  test("direct UC commit forwards raw domain metadata intent from CatalogTrackedInfo") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      var capturedDomainMetadata: JList[AbstractDomainMetadata] = null
      val capturingUCClient = new InMemoryUCClient(metastoreId.toString, ucCommitCoordinator) {
        // scalastyle:off argcount
        override def commit(
            tableId: String,
            tableUri: java.net.URI,
            tableIdentifier: JTableIdentifier,
            commit: Optional[JCommit],
            lastKnownBackfilledVersion: Optional[JLong],
            oldMetadata: Optional[AbstractMetadata],
            newMetadata: Optional[AbstractMetadata],
            oldProtocol: Optional[AbstractProtocol],
            newProtocol: Optional[AbstractProtocol],
            transactionDomainMetadata: JList[AbstractDomainMetadata],
            uniform: Optional[UniformMetadata]): Unit = {
          capturedDomainMetadata = transactionDomainMetadata
        }
        // scalastyle:on argcount
      }
      val commitCoordinatorClient =
        new UCCommitCoordinatorClient(Map.empty[String, String].asJava, capturingUCClient)
      writeCommitZero(logPath)
      val tableDesc = new TableDescriptor(
        logPath,
        Optional.empty(),
        Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableUUID.toString).asJava)
      val commitInfo = CommitInfo.empty(version = Some(1)).withTimestamp(1)
        .copy(inCommitTimestamp = Some(1))
      val updatedActions = getUpdatedActionsForNonZerothCommit(commitInfo)
      val setDomainMetadata =
        DomainMetadata("delta.clustering", """{"clusteringColumns":[["id"]]}""", removed = false)
      val removeDomainMetadata = DomainMetadata("delta.rowTracking", "{}", removed = true)
      val catalogTrackedInfo = new CatalogTrackedInfo(
        Optional.empty(),
        Seq(setDomainMetadata, removeDomainMetadata)
          .map(dm => dm: AbstractDomainMetadata)
          .asJava)

      commitCoordinatorClient.commit(
        LogStoreInverseAdaptor(log.store, log.newDeltaHadoopConf()),
        log.newDeltaHadoopConf(),
        tableDesc,
        1L,
        Iterator(commitInfo.json).asJava,
        catalogTrackedInfo,
        updatedActions)

      assert(capturedDomainMetadata.asScala.map(_.getDomain) ===
        Seq("delta.clustering", "delta.rowTracking"))
      assert(capturedDomainMetadata.asScala.map(_.isRemoved) === Seq(false, true))
    }
  }

  test("incorrect last known backfilled version") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
      tableCommitCoordinatorClient.commitCoordinatorClient.registerTable(
        logPath, Optional.empty(), -1L, initMetadata, Protocol(1, 1))
      // Write 11 commits.
      writeCommitZero(logPath)
      (1 to 10).foreach(i => commit(i, i, tableCommitCoordinatorClient))
      // Now delete some backfilled versions
      val fs = logPath.getFileSystem(log.newDeltaHadoopConf())
      fs.delete(FileNames.unsafeDeltaFile(logPath, 8), false)
      fs.delete(FileNames.unsafeDeltaFile(logPath, 9), false)
      fs.delete(FileNames.unsafeDeltaFile(logPath, 10), false)
      // Backfill with the wrong specified last version
      val e = intercept[IllegalStateException] {
        tableCommitCoordinatorClient.backfillToVersion(10L, Some(9L))
      }
      assert(e.getMessage.contains("Last known backfilled version 9 doesn't exist"))
      // Backfill with the correct version
      tableCommitCoordinatorClient.backfillToVersion(10L, Some(7L))
      // Everything should be backfilled now
      validateBackfillStrategy(tableCommitCoordinatorClient, logPath, 10)
    }
  }

  test("test getLastKnownBackfilledVersion") {
    withTempTableDir { tempDir =>
      val backfillListingOffset = 5
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      UCCommitCoordinatorClient.BACKFILL_LISTING_OFFSET = backfillListingOffset
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
      tableCommitCoordinatorClient.commitCoordinatorClient.registerTable(
        logPath, Optional.empty(), -1L, initMetadata, Protocol(1, 1))
      val hadoopConf = log.newDeltaHadoopConf()
      val fs = logPath.getFileSystem(hadoopConf)

      writeCommitZero(logPath)
      val backfillThreshold = 5
      (1 to backfillThreshold + backfillListingOffset + 5).foreach {
          commitVersion =>
        commit(commitVersion, commitVersion, tableCommitCoordinatorClient)
        if (commitVersion > backfillThreshold) {
          // After x = backfillThreshold commits, delete all backfilled files to simulate
          // backfill failing. This means UC should keep track of all commits starting
          // from x and nothing >= x should be backfilled.
          (backfillThreshold + 1 to commitVersion).foreach { deleteVersion =>
              fs.delete(FileNames.unsafeDeltaFile(logPath, deleteVersion), false)
            }
          val tableDesc = new TableDescriptor(
            logPath, Optional.empty(), tableCommitCoordinatorClient.tableConf.asJava)

          val ucCommitCoordinatorClient = tableCommitCoordinatorClient.commitCoordinatorClient
            .asInstanceOf[UCCommitCoordinatorClient]
          assert(
            ucCommitCoordinatorClient.getLastKnownBackfilledVersion(
              commitVersion,
              hadoopConf,
              LogStoreInverseAdaptor(log.store, hadoopConf),
              tableDesc
            ) == backfillThreshold
          )
        }
      }
    }
  }

  test("commit-limit-reached exception handling") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      // Create a client that does not register backfills to keep accumulating
      // commits in the commit coordinator.
      val noBackfillRegistrationClient =
        new UCCommitCoordinatorClient(Map.empty[String, String].asJava, ucClient)
          with DeltaLogging {
          override def backfillToVersion(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              version: Long,
              lastKnownBackfilledVersion: JLong): Unit = {
            throw new IOException("Simulated exception")
          }

          override protected def recordDeltaEvent(opType: String, data: Any, path: Path): Unit = {
            data match {
              case ref: AnyRef =>
                recordDeltaEvent(null, opType = opType, data = ref, path = Some(path))
            }
          }
        }
      // Client 1 performs backfills correctly.
      val tcc1 = createTableCommitCoordinatorClient(log)
      // Client 2 does not backfill.
      val tcc2 = tcc1.copy(commitCoordinatorClient = noBackfillRegistrationClient)

      // Write 10 commits to fill up the commit coordinator (MAX_NUM_COMMITS is set to 10
      // in the InMemoryUCCommitCoordinator).
      writeCommitZero(logPath)
      // We use super.commit here because tco2 does not backfill so the local override of
      // commit would fail waiting for the commits to be backfilled. This also applies
      // to the retry of commit 11 with tco2 below.
      (1 to 10).foreach(i =>
        super.commit(version = i, timestamp = i, tableCommitCoordinatorClient = tcc2)
      )
      // Commit 11 should trigger an exception and a full backfill should be attempted.
      // With tcc2, this backfill attempt should again fail, leading to a user facing
      // CommitLimitReachedException, along with the usage logs.
      var usageLogs = Log4jUsageLogger.track {
        val e1 = intercept[JCommitFailedException] {
          super.commit(version = 11, timestamp = 11, tableCommitCoordinatorClient = tcc2)
        }
        val tableId = tcc2.tableConf(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
        assert(e1.getMessage.contains(s"Too many unbackfilled commits for $tableId."))
        assert(e1.getMessage.contains(s"A full backfill attempt failed due to: " +
          "java.io.IOException: Simulated exception"))
      }
      assertUsageLogsContains(
        usageLogs, UCCoordinatedCommitsUsageLogs.UC_FULL_BACKFILL_ATTEMPT_FAILED)
      // Retry commit 11 with tcc1. This should again trigger an exception and a full
      // backfill should be attempted but the backfill should succeed this time. The
      // commit is then retried automatically and should succeed. We use the local
      // override of commit here to ensure that we only return once commit 11 has
      // been backfilled and the remaining asserts pass.
      usageLogs = Log4jUsageLogger.track {
        commit(version = 11, timestamp = 11, tableCommitCoordinatorClient = tcc1)
      }
      assertUsageLogsContains(usageLogs, UCCoordinatedCommitsUsageLogs.UC_ATTEMPT_FULL_BACKFILL)
      validateBackfillStrategy(tcc1, logPath, version = 11)
    }
  }

  Seq(None, Some(0L)).foreach { baseConvertedDeltaVersion =>
    val suffix = baseConvertedDeltaVersion.map(
      v => s"baseConvertedDeltaVersion=$v").getOrElse("no baseConvertedDeltaVersion"
    )
    test(s"Support UniForm update for tableCommitCoordinatorClient - $suffix") {
      withTempTableDir { tempDir =>
        val log = DeltaLog.forTable(spark, tempDir.toString)
        val logPath = log.logPath
        val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
        writeCommitZero(logPath)

        val baseVersionInJava: java.util.Optional[java.lang.Long] =
          baseConvertedDeltaVersion.map(Long.box).toJava
        val icebergMetadata =
          new IcebergMetadata("s3://bucket/metadata/v1.json", 1L, "2025-01-01", baseVersionInJava)
        val uniformMetadata = new UniformMetadata(icebergMetadata)
        val catalogTrackedInfo = new CatalogTrackedInfo(Optional.of(uniformMetadata))
        val commitInfo = CommitInfo
          .empty(version = Some(1)).withTimestamp(1).copy(inCommitTimestamp = Some(1))
        val updatedActions = getUpdatedActionsForNonZerothCommit(commitInfo)
        tableCommitCoordinatorClient.commit(
          1L,
          Iterator(commitInfo.json),
          updatedActions,
          tableIdentifierOpt = None,
          catalogTrackedInfo)
        waitForBackfill(1, tableCommitCoordinatorClient)

        val stored = ucCommitCoordinator.getUniformMetadata(tableUUID.toString)
        assert(stored.isDefined)
        assert(stored.get.getIcebergMetadata.isPresent)
        val storedIceberg = stored.get.getIcebergMetadata.get
        assert(storedIceberg.getMetadataLocation == "s3://bucket/metadata/v1.json")
        assert(storedIceberg.getConvertedDeltaVersion == 1L)
        assert(storedIceberg.getConvertedDeltaTimestamp == "2025-01-01")
        assert(storedIceberg.getBaseConvertedDeltaVersion == baseVersionInJava)
      }
    }
  }

  test("usage logs in commit calls are emitted correctly") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val eventLoggerClient =
        new UCCommitCoordinatorClient(Map.empty[String, String].asJava, ucClient)
          with DeltaLogging {
          override protected def recordDeltaEvent(opType: String, data: Any, path: Path): Unit = {
            data match {
              case ref: AnyRef =>
                recordDeltaEvent(null, opType = opType, data = ref, path = Some(path))
            }
          }
        }
      val logPath = log.logPath
      val tableCommitCoordinatorClient = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = eventLoggerClient)
      writeCommitZero(logPath)
      // A normal commit should emit one usage log.
      val usageLogs = Log4jUsageLogger.track {
        commit(version = 1, timestamp = 1, tableCommitCoordinatorClient)
      }
      assertUsageLogsContains(usageLogs, UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS)
    }
  }

  /**
   * A UC client that reports an unknown commit outcome, the way UC does when it cannot tell
   * whether an add-commit it received already landed. `acceptCommit` decides whether UC actually
   * took the proposal before the answer was lost -- the very thing the client cannot know from
   * the error alone.
   */
  private class UnknownCommitStateUCClient(
      acceptCommit: Boolean,
      unknownResponses: Int = 1,
      beforeReportingUnknownState: JCommit => Unit = _ => (),
      failTableReload: Boolean = false,
      useDeprecatedSignal: Boolean = false)
    extends InMemoryUCClient(metastoreId.toString, ucCommitCoordinator) {

    /** Number of add-commit calls this client has received. */
    var commitAttempts = 0

    // scalastyle:off argcount
    override def commit(
        tableId: String,
        tableUri: java.net.URI,
        tableIdentifier: JTableIdentifier,
        commit: Optional[JCommit],
        lastKnownBackfilledVersion: Optional[JLong],
        oldMetadata: Optional[AbstractMetadata],
        newMetadata: Optional[AbstractMetadata],
        oldProtocol: Optional[AbstractProtocol],
        newProtocol: Optional[AbstractProtocol],
        transactionDomainMetadata: JList[AbstractDomainMetadata],
        uniform: Optional[UniformMetadata]): Unit = {
      commitAttempts += 1
      val reportUnknownState = commitAttempts <= unknownResponses
      if (acceptCommit || !reportUnknownState) {
        super.commit(tableId, tableUri, tableIdentifier, commit, lastKnownBackfilledVersion,
          oldMetadata, newMetadata, oldProtocol, newProtocol, transactionDomainMetadata, uniform)
      }
      if (reportUnknownState) {
        commit.ifPresent(c => beforeReportingUnknownState(c))
        val message =
          s"Could not determine whether commit version ${commit.get.getVersion} is a replay: " +
            "unable to read the staged or published commit file; retry the request."
        if (useDeprecatedSignal) {
          throw new CommitCompletionUnknownException(message)
        }
        throw new CommitOutcomeUnknownException(message)
      }
    }
    // scalastyle:on argcount

    override def getCommits(
        tableId: String,
        tableUri: java.net.URI,
        tableIdentifier: JTableIdentifier,
        startVersion: Optional[JLong],
        endVersion: Optional[JLong]): JGetCommitsResponse = {
      if (failTableReload) {
        throw new IOException("Simulated UC outage")
      }
      super.getCommits(tableId, tableUri, tableIdentifier, startVersion, endVersion)
    }
  }

  private def ucCoordinatorClientFor(client: UCClient): UCCommitCoordinatorClient =
    new UCCommitCoordinatorClient(Map.empty[String, String].asJava, client) with DeltaLogging {
      override protected def recordDeltaEvent(opType: String, data: Any, path: Path): Unit = {
        data match {
          case ref: AnyRef => recordDeltaEvent(null, opType = opType, data = ref, path = Some(path))
        }
      }

      // These tests assert which requests are sent, not how long the client waits between them,
      // and the real backoff runs for minutes once the retry budget is exhausted.
      override protected def backOffBeforeResend(retryCount: Int, reason: String): Unit = {}
    }

  /** Ratifies `version` in UC under a staged file name that this writer did not generate. */
  private def ratifyCommitFromOtherWriter(log: DeltaLog, version: Long): Unit = {
    val fileName = f"$version%020d.${UUID.randomUUID()}.json"
    val fileStatus = new FileStatus(
      1L, false, 0, 0, 1L, new Path(FileNames.commitDirPath(log.logPath), fileName))
    ucClient.commit(
      tableUUID.toString,
      JCoordinatedCommitsUtils.getTablePath(log.logPath).toUri,
      null, // tableIdentifier
      Optional.of(new JCommit(version, fileStatus, 1L)),
      Optional.empty(), // lastKnownBackfilledVersion
      Optional.empty(), // oldMetadata
      Optional.empty(), // newMetadata
      Optional.empty(), // oldProtocol
      Optional.empty(), // newProtocol
      Collections.emptyList[AbstractDomainMetadata](),
      Optional.empty() /* uniform */)
  }

  test("unknown commit state: UC holds this writer's commit, so publishing resumes") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val ucClientWithLostAck = new UnknownCommitStateUCClient(acceptCommit = true)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))
      writeCommitZero(logPath)

      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      // The reload told the client it had already won version 1, so the add-commit was not
      // re-sent even once.
      assert(ucClientWithLostAck.commitAttempts == 1)
      validateBackfillStrategy(tcc, logPath, version = 1)
    }
  }

  test("unknown commit state: UC never took the proposal, so the add-commit is re-sent") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val ucClientRejectingFirstAttempt = new UnknownCommitStateUCClient(acceptCommit = false)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientRejectingFirstAttempt))
      writeCommitZero(logPath)

      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      assert(ucClientRejectingFirstAttempt.commitAttempts == 2)
      validateBackfillStrategy(tcc, logPath, version = 1)
    }
  }

  test("unknown commit state: a concurrent writer won the version, so the client rebases") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val ucClientWithLostAck = new UnknownCommitStateUCClient(acceptCommit = false)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))
      writeCommitZero(logPath)
      ratifyCommitFromOtherWriter(log, version = 1)

      val e = intercept[JCommitFailedException] {
        super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      }

      assert(e.getRetryable && e.getConflict)
      assert(e.getMessage.contains("concurrent writer"))
      assert(ucClientWithLostAck.commitAttempts == 1)
    }
  }

  test("unknown commit state: the deprecated signal runs the same recovery") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val ucClientWithLostAck = new UnknownCommitStateUCClient(
        acceptCommit = true, useDeprecatedSignal = true)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))
      writeCommitZero(logPath)

      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      // Recovery found the commit in UC, so the client stopped rather than propagating the
      // deprecated exception's retryable-conflict flags, which would have driven a rebase.
      assert(ucClientWithLostAck.commitAttempts == 1)
      assert(tcc.getCommits().getCommits.asScala.map(_.getVersion) == Seq(1L))
    }
  }

  test("unknown commit state: a published commit with this writer's content resumes") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      // Once the published file is registered, UC stops tracking the staged file name for
      // version 1, so the reload can only answer via the published commit's content.
      registerBackfillOp(tcc, log, version = 1)

      val ucClientWithLostAck = new UnknownCommitStateUCClient(acceptCommit = false)
      val retryTcc =
        tcc.copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))

      super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = retryTcc)

      assert(ucClientWithLostAck.commitAttempts == 1)
    }
  }

  test("unknown commit state: an unverifiable commit fails instead of rebasing") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val fs = logPath.getFileSystem(log.newDeltaHadoopConf())
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      registerBackfillOp(tcc, log, version = 1)

      // With the staged file gone there is nothing left to compare the published commit
      // against. Rebasing here would re-commit version 1's contents as version 2.
      val ucClientWithLostAck = new UnknownCommitStateUCClient(
        acceptCommit = false,
        beforeReportingUnknownState = c => fs.delete(c.getFileStatus.getPath, false))
      val retryTcc =
        tcc.copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))

      val e = intercept[JCommitFailedException] {
        super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = retryTcc)
      }

      assert(!e.getRetryable && !e.getConflict)
      assert(ucClientWithLostAck.commitAttempts == 1)
    }
  }

  test("lost add-commit response: UC never stored the commit, so the resend is safe") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)

      ucCommitCoordinator.throwIOExceptionBeforeCommit = true
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      assert(tcc.getCommits().getCommits.asScala.map(_.getVersion) == Seq(1L))
    }
  }

  test("lost add-commit response: UC stored the commit, so the resend must not rebase") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)

      // UC took the commit and then the connection dropped, so the client cannot tell the
      // difference between this and the test above from the IOException alone. Reporting a
      // conflict here would make the caller rebase and re-commit version 1's data as version 2.
      ucCommitCoordinator.throwIOExceptionAfterCommit = true
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      assert(tcc.getCommits().getCommits.asScala.map(_.getVersion) == Seq(1L))
    }
  }

  test("unknown commit state: a failed table reload fails the commit instead of re-sending") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val unreachableUCClient =
        new UnknownCommitStateUCClient(acceptCommit = false, failTableReload = true)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(unreachableUCClient))
      writeCommitZero(logPath)

      val e = intercept[JCommitFailedException] {
        super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      }

      assert(!e.getRetryable && !e.getConflict)
      assert(unreachableUCClient.commitAttempts == 1)
    }
  }

  test("unknown commit state: a published commit with another writer's content rebases") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val fs = logPath.getFileSystem(log.newDeltaHadoopConf())
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      registerBackfillOp(tcc, log, version = 1)

      // The published version 1 is readable but holds different bytes from what this writer
      // staged, which is the one case where rebasing is the correct answer.
      val ucClientWithLostAck = new UnknownCommitStateUCClient(
        acceptCommit = false,
        beforeReportingUnknownState = c => {
          val out = fs.create(c.getFileStatus.getPath, true /* overwrite */)
          try {
            out.write("{\"commitInfo\":{\"someOtherWriter\":true}}\n".getBytes)
          } finally {
            out.close()
          }
        })
      val retryTcc =
        tcc.copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))

      val e = intercept[JCommitFailedException] {
        super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = retryTcc)
      }

      assert(e.getRetryable && e.getConflict)
      assert(e.getMessage.contains("concurrent writer"))
    }
  }

  test("unknown commit state: UC trailing the commit's base version cannot be reasoned about") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcc = createTableCommitCoordinatorClient(log)
      writeCommitZero(logPath)
      commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)

      // Proposing version 3 against a UC that reports version 1 leaves latestTableVersion below
      // the version this commit was built on, a state that says nothing about the proposal.
      val ucClientWithLostAck = new UnknownCommitStateUCClient(acceptCommit = false)
      val retryTcc =
        tcc.copy(commitCoordinatorClient = ucCoordinatorClientFor(ucClientWithLostAck))

      val e = intercept[JCommitFailedException] {
        super.commit(version = 3, timestamp = 3, tableCommitCoordinatorClient = retryTcc)
      }

      assert(!e.getRetryable && !e.getConflict)
      assert(ucClientWithLostAck.commitAttempts == 1)
    }
  }

  test("unknown commit state: a server stuck on unknown exhausts the retry budget") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      // Never accept, and keep answering "unknown" past the transient-error budget so the
      // client gives up instead of re-sending forever.
      val alwaysUnknownUCClient =
        new UnknownCommitStateUCClient(acceptCommit = false, unknownResponses = Int.MaxValue)
      val tcc = createTableCommitCoordinatorClient(log)
        .copy(commitCoordinatorClient = ucCoordinatorClientFor(alwaysUnknownUCClient))
      writeCommitZero(logPath)

      val e = intercept[JCommitFailedException] {
        super.commit(version = 1, timestamp = 1, tableCommitCoordinatorClient = tcc)
      }

      assert(e.getRetryable && !e.getConflict)
      assert(alwaysUnknownUCClient.commitAttempts > 1)
    }
  }
}
