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
import java.net.URI
import java.util.{List => JList, Optional}

import scala.collection.JavaConverters._
import scala.jdk.OptionConverters._
import scala.reflect.ClassTag

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaIllegalArgumentException, DeltaLog, LogSegment, Snapshot}
import org.apache.spark.sql.delta.CommitCoordinatorGetCommitsFailedException
import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
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
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{
  UCClient,
  UCCommitCoordinatorClient,
  UCCoordinatedCommitsUsageLogs,
  UCDeltaClient,
  UCDeltaModels}
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

  private class RecordingDrcUCClient extends UCDeltaClient {
    var catalog: String = _
    var schema: String = _
    var table: String = _
    var request: UCDeltaModels.UpdateTableRequest = _

    override def supportsUCDeltaRestCatalogApi(): Boolean = true

    override def updateTable(
        catalog: String,
        schema: String,
        table: String,
        request: UCDeltaModels.UpdateTableRequest): AbstractMetadata = {
      this.catalog = catalog
      this.schema = schema
      this.table = table
      this.request = request
      null
    }

    override def getMetastoreId(): String = metastoreId.toString

    override def commit(
        tableId: String,
        tableUri: URI,
        tableIdentifier: JTableIdentifier,
        commit: Optional[JCommit],
        lastKnownBackfilledVersion: Optional[JLong],
        oldMetadata: Optional[AbstractMetadata],
        newMetadata: Optional[AbstractMetadata],
        oldProtocol: Optional[AbstractProtocol],
        newProtocol: Optional[AbstractProtocol],
        uniform: Optional[UniformMetadata]): Unit = {
      throw new RuntimeException("legacy UC commit should not be called")
    }

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[JLong],
        endVersion: Optional[JLong]): JGetCommitsResponse = null

    override def finalizeCreate(
        tableName: String,
        catalogName: String,
        schemaName: String,
        storageLocation: String,
        columns: JList[UCClient.ColumnDef],
        properties: java.util.Map[String, String]): Unit = {}

    override def close(): Unit = {}
  }

  private class TestableUCCommitCoordinatorClient(ucClient: UCClient)
    extends UCCommitCoordinatorClient(Map.empty[String, String].asJava, ucClient) {

    def callCommitToUC(
        tableDesc: TableDescriptor,
        logPath: Path,
        commitFile: Optional[FileStatus],
        commitVersion: Optional[JLong],
        commitTimestamp: Optional[JLong],
        lastKnownBackfilledVersion: Optional[JLong],
        oldMetadata: Optional[AbstractMetadata],
        newMetadata: Optional[AbstractMetadata],
        oldProtocol: Optional[AbstractProtocol],
        newProtocol: Optional[AbstractProtocol]): Unit = {
      commitToUC(
        tableUUID.toString,
        tableDesc,
        commitFile,
        commitVersion,
        commitTimestamp,
        lastKnownBackfilledVersion,
        CatalogTrackedInfo.EMPTY,
        /* disown = */ false,
        oldMetadata,
        newMetadata,
        oldProtocol,
        newProtocol)
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

  test("commitToUC uses UC Delta updateTable when DRC is supported") {
    val recordingClient = new RecordingDrcUCClient
    val client = new TestableUCCommitCoordinatorClient(recordingClient)
    val logPath = new Path("file:/tmp/uc-table/_delta_log")
    val tableDesc = new TableDescriptor(
      logPath,
      Optional.of(new JTableIdentifier(Array("main", "default"), "tbl")),
      Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableUUID.toString).asJava)
    val commitFile = new FileStatus(
      32L,
      false,
      1,
      1024L,
      200L,
      new Path("file:/tmp/uc-table/_delta_log/_staged_commits/1.uuid.json"))
    val oldMetadata = Metadata(
      description = "old comment",
      schemaString = """{"type":"struct","fields":[]}""",
      partitionColumns = Nil,
      configuration = Map("old.prop" -> "remove", "same.prop" -> "same"))
    val newMetadata = oldMetadata.copy(
      description = "new comment",
      schemaString =
        """{"type":"struct","fields":[""" +
          """{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
      partitionColumns = Seq("id"),
      configuration = Map("new.prop" -> "set", "same.prop" -> "same"))

    client.callCommitToUC(
      tableDesc,
      logPath,
      Optional.of(commitFile),
      Optional.of(JLong.valueOf(1L)),
      Optional.of(JLong.valueOf(100L)),
      Optional.of(JLong.valueOf(0L)),
      Optional.of(oldMetadata),
      Optional.of(newMetadata),
      Optional.of(Protocol(1, 1)),
      Optional.of(Protocol(1, 7)))

    assert(recordingClient.catalog === "main")
    assert(recordingClient.schema === "default")
    assert(recordingClient.table === "tbl")
    val request = recordingClient.request
    assert(request.getRequirements.asScala.map(_.getType) ===
      Seq(UCDeltaModels.TableRequirement.Type.ASSERT_TABLE_UUID))
    assert(request.getRequirements.get(0).getUuid === tableUUID)
    val updates = request.getUpdates.asScala
    assert(updates.map(_.getAction) === Seq(
      UCDeltaModels.TableUpdate.Action.ADD_COMMIT,
      UCDeltaModels.TableUpdate.Action.SET_LATEST_BACKFILLED_VERSION,
      UCDeltaModels.TableUpdate.Action.SET_COLUMNS,
      UCDeltaModels.TableUpdate.Action.SET_PARTITION_COLUMNS,
      UCDeltaModels.TableUpdate.Action.SET_TABLE_COMMENT,
      UCDeltaModels.TableUpdate.Action.SET_PROPERTIES,
      UCDeltaModels.TableUpdate.Action.REMOVE_PROPERTIES,
      UCDeltaModels.TableUpdate.Action.SET_PROTOCOL))
    assert(updates(0).getCommit.getVersion === 1L)
    assert(updates(0).getCommit.getFileName === "1.uuid.json")
    assert(updates(1).getLatestPublishedVersion === 0L)
    assert(updates(2).getSchemaString.contains("\"name\":\"id\""))
    assert(updates(3).getPartitionColumns.asScala === Seq("id"))
    assert(updates(4).getComment === "new comment")
    assert(updates(5).getPropertyUpdates.asScala === Map("new.prop" -> "set"))
    assert(updates(6).getPropertyRemovals.asScala === Seq("old.prop"))
    assert(updates(7).getProtocol.getMinWriterVersion === 7)
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

}
