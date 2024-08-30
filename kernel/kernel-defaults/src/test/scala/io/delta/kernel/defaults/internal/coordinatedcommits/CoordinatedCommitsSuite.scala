/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultCommitCoordinatorClientHandler
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol, SingleAction}
import io.delta.kernel.internal.actions.SingleAction.{createMetadataSingleAction, FULL_SCHEMA}
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.snapshot.{SnapshotManager, TableCommitCoordinatorClientHandler}
import io.delta.kernel.internal.util.{CoordinatedCommitsUtils, FileNames}
import io.delta.kernel.internal.util.Preconditions.checkArgument
import io.delta.kernel.internal.util.Utils.{closeCloseables, singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, InMemoryCommitCoordinator, TableDescriptor, TableIdentifier, UpdatedActions, CoordinatedCommitsUtils => CCU}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.{lang, util}
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.JavaConverters._
import scala.math

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {

  private val trackingInMemoryBatchSize10Config = Map(
    CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
    InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10")

  def setupCoordinatedCommitFilesForTest(
    engine: Engine,
    tablePath: String,
    coordinatorName: String = "tracking-in-memory",
    coordinatorConf: String = "{}",
    tableConfToOverwrite: String = null,
    versionConvertToCC: Long = 0L,
    coordinatedCommitNum: Long = 3L,
    checkpointVersion: Long = -1L,
    deleteVersion: Long = -1L): Unit = {
    assert(checkpointVersion < versionConvertToCC)
    val versionToDelete = math.max(versionConvertToCC + 1, deleteVersion)

    val handler =
      engine.getCommitCoordinatorClientHandler(
        coordinatorName, OBJ_MAPPER.readValue(coordinatorConf, classOf[util.Map[String, String]]))
    val logPath = new Path("file:" + tablePath, "_delta_log")
    val tableSpark = Table.forPath(engine, tablePath)
    val totalCommitNum = coordinatedCommitNum + versionConvertToCC

    (0L until totalCommitNum).foreach(version => {
      spark.range(
        version * 10, version * 10 + 10).write.format("delta").mode("append").save(tablePath)
    })
    checkAnswer(
      spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
      (0L until totalCommitNum * 10L).map(TestRow(_)))

    var tableConf: util.Map[String, String] = null

    /** Rewrite the FS to CC conversion commit and move coordinated commits to _commits folder */
    (0L until totalCommitNum).foreach{ version =>
      val commitFilePath = getHadoopDeltaFile(logPath, version)

      if (version == versionConvertToCC) {
        tableConf = handler.registerTable(
          logPath.toString,
          version - 1L,
          CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(getEmptyMetadata),
          CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(getProtocol(1, 1)))
        val tableConfString = if (tableConfToOverwrite != null) {
          tableConfToOverwrite
        } else {
          OBJ_MAPPER.writeValueAsString(tableConf)
        }
        val rows = addCoordinatedCommitToMetadataRow(
          engine,
          commitFilePath,
          tableSpark.getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl],
          Map(
            TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> coordinatorName,
            TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> coordinatorConf,
            TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey -> tableConfString))
        writeConvertToCCCommit(engine, logPath, rows, version)
      } else if (version > versionConvertToCC) {
        val rows = getRowsFromFile(engine, commitFilePath)
        commit(logPath.toString, tableConf, version, version, rows, handler)
        if (version >= versionToDelete) {
          logPath.getFileSystem(hadoopConf).delete(commitFilePath)
        }
      } else if (version == checkpointVersion) {
        tableSpark.checkpoint(engine, version)
      }
    }
  }

  def testWithCoordinatorCommits(
    testName: String,
    hadoopConf: Map[String, String] = Map.empty)(f: (String, Engine) => Unit): Unit = {
    test(testName) {
      InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
      withTempDirAndEngine(f, hadoopConf)
    }
  }

  testWithCoordinatorCommits("cold snapshot initialization", trackingInMemoryBatchSize10Config) {
    (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(engine, tablePath)

      val table = Table.forPath(engine, tablePath)
      for (version <- 0L to 1L) {
        val snapshot = table.getSnapshotAsOfVersion(engine, version)
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, (0L to version * 10L + 9L).map(TestRow(_)))
      }

      TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot2 = table.getLatestSnapshot(engine)
      val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
      checkAnswer(result2, (0L to 29L).map(TestRow(_)))
      assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "snapshot read should use coordinated commit related properties properly",
    Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("test-coordinator") ->
      classOf[TestCommitCoordinatorBuilder].getName)) { (tablePath, engine) =>
        setupCoordinatedCommitFilesForTest(
          engine,
          tablePath,
          coordinatorName = "test-coordinator",
          coordinatorConf = OBJ_MAPPER.writeValueAsString(
            TestCommitCoordinator.EXP_COORDINATOR_CONF))

        val table = Table.forPath(engine, tablePath)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, (0L to 29L).map(TestRow(_)))
        assert(snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        assert(
          snapshot
            .getTableCommitCoordinatorClientHandlerOpt(engine)
            .get()
            .semanticEquals(
              engine.getCommitCoordinatorClientHandler(
                "test-coordinator", TestCommitCoordinator.EXP_COORDINATOR_CONF)))
  }

  testWithCoordinatorCommits(
    "snapshot read fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF",
    trackingInMemoryBatchSize10Config) { (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(
        engine,
        tablePath,
        tableConfToOverwrite = """{"key1": "string_value", "key2Int": "2""")

      val table = Table.forPath(engine, tablePath)
      intercept[RuntimeException] {
        table.getLatestSnapshot(engine)
      }
  }

  testWithCoordinatorCommits(
    "snapshot read with checkpoint before table converted to coordinated commit table",
    trackingInMemoryBatchSize10Config) { (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(
        engine,
        tablePath,
        versionConvertToCC = 2L,
        coordinatedCommitNum = 2L,
        checkpointVersion = 1L)

      val table = Table.forPath(engine, tablePath)
      for (version <- 0L to 2L) {
        val snapshot = table.getSnapshotAsOfVersion(engine, version)
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, (0L to version * 10L + 9L).map(TestRow(_)))
      }

      TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot3 = table.getLatestSnapshot(engine)
      val result3 = readSnapshot(snapshot3, snapshot3.getSchema(engine), null, null, engine)
      checkAnswer(result3, (0L to 39L).map(TestRow(_)))
      assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "snapshot read with overlap between filesystem based commits and coordinated commits",
    trackingInMemoryBatchSize10Config) { (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(
        engine,
        tablePath,
        versionConvertToCC = 2L,
        coordinatedCommitNum = 4L,
        deleteVersion = 4L)

      val table = Table.forPath(engine, tablePath)
      for (version <- 0L to 4L) {
        val snapshot = table.getSnapshotAsOfVersion(engine, version)
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, (0L to version * 10L + 9L).map(TestRow(_)))
      }

      TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot5 = table.getLatestSnapshot(engine)
      val result5 = readSnapshot(snapshot5, snapshot5.getSchema(engine), null, null, engine)
      checkAnswer(result5, (0L to 59L).map(TestRow(_)))
      assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "getSnapshotAt with coordinated commits enabled", trackingInMemoryBatchSize10Config) {
      (tablePath, engine) =>
        setupCoordinatedCommitFilesForTest(
          engine, tablePath, versionConvertToCC = 2L)

        val table = Table.forPath(engine, tablePath)
        for (version <- 0L to 4L) {
          val snapshot = table.getSnapshotAsOfVersion(engine, version)
          val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
          checkAnswer(result, (0L to version * 10L + 9L).map(TestRow(_)))
        }
  }

  testWithCoordinatorCommits(
    "versionToLoad higher than possible", trackingInMemoryBatchSize10Config) {
      (tablePath, engine) =>
        setupCoordinatedCommitFilesForTest(
          engine, tablePath, versionConvertToCC = 2L)
        val table = Table.forPath(engine, tablePath)
        val e = intercept[RuntimeException] {
          table.getSnapshotAsOfVersion(engine, 5L)
        }
        assert(e.getMessage.contains(
          "Cannot load table version 5 as it does not exist. The latest available version is 4"))
  }

  def getRowsFromFile(engine: Engine, delta: Path): CloseableIterator[Row] = {
    val file = FileStatus.of(delta.toString, 0, 0)
    val columnarBatches =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty())


    var allRowsIterators = List.empty[Row]

    while (columnarBatches.hasNext) {
      val batch = columnarBatches.next()
      val rows = batch.getRows
      while (rows.hasNext) {
        val row = rows.next()
        allRowsIterators = allRowsIterators :+ row
      }
    }

   toCloseableIterator(allRowsIterators.iterator.asJava)
  }

  def addCoordinatedCommitToMetadataRow(
    engine: Engine,
    delta: Path,
    snapshot: SnapshotImpl,
    configurations: Map[String, String]): CloseableIterator[Row] = {
    var rows = getRowsFromFile(engine, delta)
    val metadata = snapshot.getMetadata.withNewConfiguration(configurations.asJava)
    var hasMetadataRow = false
    rows = rows.map(row => {
      val metadataOrd = row.getSchema.indexOf("metaData")
      if (row.isNullAt(metadataOrd)) {
        row
      } else {
        hasMetadataRow = true
        createMetadataSingleAction(metadata.toRow)
      }
    })
    if (!hasMetadataRow) {
      toCloseableIterator((rows.toIterator ++ singletonCloseableIterator(
        createMetadataSingleAction(metadata.toRow)).toIterator).asJava)
    } else {
      rows
    }
  }
}

object TestCommitCoordinator {
  val EXP_TABLE_CONF: util.Map[String, String] = Map(
    "tableKey1" -> "string_value",
    "tableKey2Int" -> "2",
    "tableKey3ComplexStr" -> "\"hello\""
  ).asJava

  val EXP_COORDINATOR_CONF: util.Map[String, String] = Map(
    "coordinatorKey1" -> "string_value",
    "coordinatorKey2Int" -> "2",
    "coordinatorKey3ComplexStr" -> "\"hello\"").asJava

  val COORDINATOR = new TestCommitCoordinatorClient()
}

/**
 * A [[CommitCoordinatorClient]] that tests can use to check the coordinator configuration and
 * table configuration.
 */
class TestCommitCoordinatorClient extends InMemoryCommitCoordinator(10) {
  override def registerTable(
    logPath: Path,
    tableIdentifier: Optional[TableIdentifier],
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol): util.Map[String, String] = {
    super.registerTable(logPath, tableIdentifier, currentVersion, currentMetadata, currentProtocol)
    TestCommitCoordinator.EXP_TABLE_CONF
  }
  override def getCommits(
    tableDesc: TableDescriptor,
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse = {
    checkArgument(tableDesc.getTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.getCommits(tableDesc, startVersion, endVersion)
  }
  override def commit(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    commitVersion: Long,
    actions: util.Iterator[String],
    updatedActions: UpdatedActions): CommitResponse = {
    checkArgument(tableDesc.getTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.commit(logStore, hadoopConf, tableDesc, commitVersion, actions, updatedActions)
  }
}

class TestCommitCoordinatorBuilder(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    checkArgument(conf == TestCommitCoordinator.EXP_COORDINATOR_CONF)
    TestCommitCoordinator.COORDINATOR
  }
  override def getName: String = "test-coordinator"
}
