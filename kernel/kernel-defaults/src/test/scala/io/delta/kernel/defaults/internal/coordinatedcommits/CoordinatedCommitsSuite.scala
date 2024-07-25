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
import io.delta.kernel.internal.util.{CoordinatedCommitsUtils, FileNames}
import io.delta.kernel.internal.util.Preconditions.checkArgument
import io.delta.kernel.internal.util.Utils.{singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, InMemoryCommitCoordinator, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.{lang, util}
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.JavaConverters._

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {

  def setupCoordinatedCommitFilesForTest(
    engine: Engine,
    tablePath: String,
    coordinatorName: String = "tracking-in-memory",
    coordinatorConf: String = "{}",
    tableConfToOverwrite: String = null): Unit = {
    val handler =
      engine.getCommitCoordinatorClientHandler(
        coordinatorName, OBJ_MAPPER.readValue(coordinatorConf, classOf[util.Map[String, String]]))
    val logPath = new Path("file:" + tablePath, "_delta_log")
    val tableSpark = Table.forPath(engine, tablePath)

    spark.range(0, 10).write.format("delta").mode("overwrite").save(tablePath) // version 0
    spark.range(10, 20).write.format("delta").mode("append").save(tablePath) // version 1
    spark.range(20, 30).write.format("delta").mode("append").save(tablePath) // version 2
    checkAnswer(
      spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
      (0L to 29L).map(TestRow(_)))

    var tableConf: util.Map[String, String] = null

    (0 to 2).foreach{ version =>
      val delta = getHadoopDeltaFile(logPath, version)

      if (version == 0) {
        tableConf = handler.registerTable(
          logPath.toString,
          -1L,
          CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(getEmptyMetadata),
          CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(getProtocol(1, 1)))
        val tableConfString = if (tableConfToOverwrite != null) {
          tableConfToOverwrite
        } else {
          OBJ_MAPPER.writeValueAsString(tableConf)
        }
        val rows = addCoordinatedCommitToMetadataRow(
          engine,
          delta,
          tableSpark.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl],
          Map(
            TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> coordinatorName,
            TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> coordinatorConf,
            TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey -> tableConfString))
        writeCommitZero(engine, logPath, rows)
      } else {
        val rows = getRowsFromFile(engine, delta)
        commit(logPath.toString, tableConf, version, version, rows, handler)
        logPath.getFileSystem(hadoopConf).delete(delta)
      }
    }
  }

  test("cold snapshot initialization") {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine ({ (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(engine, tablePath)

      val table = Table.forPath(engine, tablePath)
      val snapshot0 = table.getSnapshotAsOfVersion(engine, 0)
      val result0 = readSnapshot(snapshot0, snapshot0.getSchema(engine), null, null, engine)
      checkAnswer(result0, (0L to 9L).map(TestRow(_)))

      val snapshot1 = table.getSnapshotAsOfVersion(engine, 1)
      val result1 = readSnapshot(snapshot1, snapshot1.getSchema(engine), null, null, engine)
      checkAnswer(result1, (0L to 19L).map(TestRow(_)))

      TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot2 = table.getLatestSnapshot(engine)
      val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
      checkAnswer(result2, (0L to 29L).map(TestRow(_)))
      assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
    }, Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      TrackingInMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10"))
  }

  test("snapshot read should use coordinated commit related properties properly") {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine( { (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(
        engine,
        tablePath,
        coordinatorName = "test-coordinator",
        coordinatorConf = OBJ_MAPPER.writeValueAsString(TestCommitCoordinator.expCoordinatorConf))

      val table = Table.forPath(engine, tablePath)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      checkAnswer(result, (0L to 29L).map(TestRow(_)))
      assert(snapshot.getCommitCoordinatorClientHandlerOpt(engine).isPresent)
      assert(
        snapshot
          .getCommitCoordinatorClientHandlerOpt(engine)
          .get()
          .semanticEquals(
            engine.getCommitCoordinatorClientHandler(
              "test-coordinator", TestCommitCoordinator.expCoordinatorConf)))
    }, Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("test-coordinator") ->
      classOf[TestCommitCoordinatorBuilder].getName))
  }

  test("snapshot read fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF") {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine( { (tablePath, engine) =>
      setupCoordinatedCommitFilesForTest(
        engine,
        tablePath,
        tableConfToOverwrite = """{"key1": "string_value", "key2Int": "2""")

      val table = Table.forPath(engine, tablePath)
      intercept[RuntimeException] {
        table.getLatestSnapshot(engine)
      }
    }, Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      TrackingInMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10"))
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
    val rows = getRowsFromFile(engine, delta)
    val metadata = snapshot.getMetadata.withNewConfiguration(configurations.asJava)
    rows.map(row => {
      val metadataOrd = row.getSchema.indexOf("metaData")
      if (row.isNullAt(metadataOrd)) {
        row
      } else {
        createMetadataSingleAction(metadata.toRow)
      }
    })
  }
}

object TestCommitCoordinator {
  val expTableConf: util.Map[String, String] = Map(
    "tableKey1" -> "string_value",
    "tableKey2Int" -> "2",
    "tableKey3ComplexStr" -> "\"hello\""
  ).asJava

  val expCoordinatorConf: util.Map[String, String] = Map(
    "coordinatorKey1" -> "string_value",
    "coordinatorKey2Int" -> "2",
    "coordinatorKey3ComplexStr" -> "\"hello\"").asJava

  val coordinator = new TestCommitCoordinatorClient()
}

class TestCommitCoordinatorClient extends InMemoryCommitCoordinator(10) {
  override def registerTable(
    logPath: Path,
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol): util.Map[String, String] = {
    super.registerTable(logPath, currentVersion, currentMetadata, currentProtocol)
    TestCommitCoordinator.expTableConf
  }
  override def getCommits(
    logPath: Path,
    coordinatedCommitsTableConf: util.Map[String, String],
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse = {
    checkArgument(coordinatedCommitsTableConf == TestCommitCoordinator.expTableConf)
    super.getCommits(logPath, coordinatedCommitsTableConf, startVersion, endVersion)
  }
  override def commit(
    logStore: LogStore,
    hadoopConf: Configuration,
    logPath: Path,
    coordinatedCommitsTableConf: util.Map[String, String],
    commitVersion: Long,
    actions: util.Iterator[String],
    updatedActions: UpdatedActions): CommitResponse = {
    checkArgument(coordinatedCommitsTableConf == TestCommitCoordinator.expTableConf)
    super.commit(logStore, hadoopConf, logPath, coordinatedCommitsTableConf,
      commitVersion, actions, updatedActions)
  }
}

class TestCommitCoordinatorBuilder(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    checkArgument(conf == TestCommitCoordinator.expCoordinatorConf)
    TestCommitCoordinator.coordinator
  }
  override def getName: String = "test-coordinator"
}
