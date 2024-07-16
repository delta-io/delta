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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.Table
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, UpdatedActions}
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

  test("helper method that recovers config from abstract metadata works properly") {
    val m1 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "string_value").asJava
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(m1),
      COORDINATED_COMMITS_COORDINATOR_NAME) === Optional.of("string_value"))

    val m2 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "").asJava
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(m2),
      COORDINATED_COMMITS_COORDINATOR_NAME) === Optional.of(""))

    val m3 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""").asJava
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(m3),
      COORDINATED_COMMITS_COORDINATOR_CONF) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)
  }

  test("cold snapshot initialization") {
    CommitCoordinatorProvider.clearBuilders()
    val builder = TrackingInMemoryCommitCoordinatorBuilder(10)
    val commitCoordinatorClient =
      builder.build(Collections.emptyMap()).asInstanceOf[TrackingCommitCoordinatorClient]
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      val logPath = new Path("file:" + tablePath, "_delta_log")
      val table = Table.forPath(engine, tablePath)

      spark.range(0, 10).write.format("delta").mode("overwrite").save(tablePath) // version 0
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (0L to 9L).map(TestRow(_)))
      spark.range(10, 20).write.format("delta").mode("overwrite").save(tablePath) // version 1
      spark.range(20, 30).write.format("delta").mode("append").save(tablePath) // version 2
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (10L to 29L).map(TestRow(_)))

      var tableConf: util.Map[String, String] = null
      val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)

      (0 to 2).foreach{ version =>
        val delta = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version)

        var rows = logStore.read(delta, hadoopConf).toList

        if (version == 0) {
          rows = addCoordinatedCommitToMetadataRow(
            logStore.read(delta, hadoopConf).toList,
            Map(
              TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
              TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}"))
          tableConf = commitCoordinatorClient.registerTable(
            logPath,
            -1L,
            CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(Metadata.empty()),
            CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(new Protocol(1, 1)))
          writeCommitZero(engine, logPath, rows.asJava)
        } else {
          commit(logPath, tableConf, version, version, rows.asJava, commitCoordinatorClient)
          logPath.getFileSystem(hadoopConf).delete(
            CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version))
        }
      }
      val snapshot0 = table.getSnapshotAsOfVersion(engine, 0)
      val result0 = readSnapshot(snapshot0, snapshot0.getSchema(engine), null, null, engine)
      checkAnswer(result0, (0L to 9L).map(TestRow(_)))

      val snapshot1 = table.getSnapshotAsOfVersion(engine, 1)
      val result1 = readSnapshot(snapshot1, snapshot1.getSchema(engine), null, null, engine)
      checkAnswer(result1, (10L to 19L).map(TestRow(_)))

      commitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot2 = table.getLatestSnapshot(engine)
      val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
      checkAnswer(result2, (10L to 29L).map(TestRow(_)))
      assert(commitCoordinatorClient.numGetCommitsCalled.get === 1)
    }
  }

  test("snapshot read should use coordinated commit related properties properly") {
    CommitCoordinatorProvider.clearBuilders()
    val expTableConf: util.Map[String, String] = Map(
      "tableKey1" -> "string_value",
      "tableKey2Int" -> "2",
      "tableKey3ComplexStr" -> "\"hello\"").asJava

    val expCoordinatorConf: util.Map[String, String] = Map(
      "coordinatorKey1" -> "string_value",
      "coordinatorKey2Int" -> "2",
      "coordinatorKey3ComplexStr" -> "\"hello\"").asJava
    class TestCommitCoordinatorClient extends InMemoryCommitCoordinator(10) {
      override def registerTable(
        logPath: Path,
        currentVersion: Long,
        currentMetadata: AbstractMetadata,
        currentProtocol: AbstractProtocol): util.Map[String, String] = {
        super.registerTable(logPath, currentVersion, currentMetadata, currentProtocol)
        expTableConf
      }
      override def getCommits(
        logPath: Path,
        coordinatedCommitsTableConf: util.Map[String, String],
        startVersion: lang.Long,
        endVersion: lang.Long = null): GetCommitsResponse = {
        assert(coordinatedCommitsTableConf === expTableConf)
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
        assert(coordinatedCommitsTableConf === expTableConf)
        super.commit(logStore, hadoopConf, logPath, coordinatedCommitsTableConf,
          commitVersion, actions, updatedActions)
      }
    }

    object Builder extends CommitCoordinatorBuilder {
      private lazy val coordinator = new TestCommitCoordinatorClient()
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        assert(conf === expCoordinatorConf)
        coordinator
      }
      override def getName: String = "test-coordinator"
    }
    CommitCoordinatorProvider.registerBuilder(Builder)
    val commitCoordinatorClient = Builder.build(expCoordinatorConf)
    withTempDirAndEngine { (tablePath, engine) =>
      val logPath = new Path("file:" + tablePath, "_delta_log")
      val table = Table.forPath(engine, tablePath)

      spark.range(0, 10).write.format("delta").mode("overwrite").save(tablePath) // version 0
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (0L to 9L).map(TestRow(_)))
      spark.range(10, 20).write.format("delta").mode("overwrite").save(tablePath) // version 1
      spark.range(20, 30).write.format("delta").mode("append").save(tablePath) // version 2
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (10L to 29L).map(TestRow(_)))

      var tableConf: util.Map[String, String] = null
      val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)

      (0 to 2).foreach{ version =>
        val delta = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version)

        var rows = logStore.read(delta, hadoopConf).toList

        if (version == 0) {
          rows = addCoordinatedCommitToMetadataRow(
            rows,
            Map(
              TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "test-coordinator",
              TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
                OBJ_MAPPER.writeValueAsString(expCoordinatorConf),
              TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
                OBJ_MAPPER.writeValueAsString(expTableConf)))
          tableConf = commitCoordinatorClient.registerTable(
            logPath,
            -1L,
            CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(Metadata.empty()),
            CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(new Protocol(1, 1)))
          writeCommitZero(engine, logPath, rows.asJava)
          assert (tableConf === expTableConf)
        } else {
          commit(logPath, tableConf, version, version, rows.asJava, commitCoordinatorClient)
          logPath.getFileSystem(hadoopConf).delete(
            CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version))
        }
      }
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      checkAnswer(result, (10L to 29L).map(TestRow(_)))
      assert(snapshot.getCommitCoordinatorClientHandlerOpt(engine).isPresent)
      assert(
        snapshot
          .getCommitCoordinatorClientHandlerOpt(engine)
          .get()
          .semanticEquals(
            engine.getCommitCoordinatorClientHandler("test-coordinator", expCoordinatorConf)))
    }
  }

  test("snapshot read fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF") {
    CommitCoordinatorProvider.clearBuilders()
    val builder = TrackingInMemoryCommitCoordinatorBuilder(10)
    val commitCoordinatorClient =
      builder.build(Collections.emptyMap()).asInstanceOf[TrackingCommitCoordinatorClient]
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      val logPath = new Path("file:" + tablePath, "_delta_log")
      val table = Table.forPath(engine, tablePath)

      spark.range(0, 10).write.format("delta").mode("overwrite").save(tablePath) // version 0
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (0L to 9L).map(TestRow(_)))
      spark.range(10, 20).write.format("delta").mode("overwrite").save(tablePath) // version 1
      spark.range(20, 30).write.format("delta").mode("append").save(tablePath) // version 2
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (10L to 29L).map(TestRow(_)))

      var tableConf: util.Map[String, String] = null
      val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)

      (0 to 2).foreach{ version =>
        val delta = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version)

        var rows = logStore.read(delta, hadoopConf).toList

        if (version == 0) {
          rows = addCoordinatedCommitToMetadataRow(
            logStore.read(delta, hadoopConf).toList,
            Map(
              TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
              TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}",
              TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
              """{"key1": "string_value", "key2Int": "2"""))
          tableConf = commitCoordinatorClient.registerTable(
            logPath,
            -1L,
            CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(Metadata.empty()),
            CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(new Protocol(1, 1)))
          writeCommitZero(engine, logPath, rows.asJava)
        } else {
          commit(logPath, tableConf, version, version, rows.asJava, commitCoordinatorClient)
          logPath.getFileSystem(hadoopConf).delete(
            CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version))
        }
      }
      intercept[RuntimeException] {
        table.getLatestSnapshot(engine)
      }
    }
  }

  def addCoordinatedCommitToMetadataRow(
    rows: List[String], configurations: Map[String, String]): List[String] = rows.map(row => {
    if (row.contains("metaData")) row.replace(
        "\"configuration\":{}",
        "\"configuration\":" + OBJ_MAPPER.writeValueAsString(configurations.asJava)) else row
  })
}
