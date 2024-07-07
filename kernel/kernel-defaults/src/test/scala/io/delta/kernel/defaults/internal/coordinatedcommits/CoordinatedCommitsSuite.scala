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
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.Table
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration

import java.{lang, util}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import org.apache.hadoop.fs.{Path => HadoopPath}

import java.io.File
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterator asScala`}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {

  test("helper method that recovers config from abstract metadata works properly") {
    val m1 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "string_value").asJava
    )
    assert(CoordinatedCommitsDefaultUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsDefaultUtils.convertMetadataToAbstractMetadata(m1),
      COORDINATED_COMMITS_COORDINATOR_NAME) === Optional.of("string_value"))

    val m2 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "").asJava
    )
    assert(CoordinatedCommitsDefaultUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsDefaultUtils.convertMetadataToAbstractMetadata(m2),
      COORDINATED_COMMITS_COORDINATOR_NAME) === Optional.of(""))

    val m3 = Metadata.empty.withNewConfiguration(
      Map(COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
        """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""").asJava
    )
    assert(CoordinatedCommitsDefaultUtils.fromAbstractMetadataAndTableConfig(
      CoordinatedCommitsDefaultUtils.convertMetadataToAbstractMetadata(m3),
      COORDINATED_COMMITS_COORDINATOR_CONF) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)
  }

  test("0th commit happens via filesystem") {
    CommitCoordinatorProvider.clearBuilders()
    val commitCoordinatorName = "nobackfilling-commit-coordinator"
    object NoBackfillingCommitCoordinatorBuilder$ extends CommitCoordinatorBuilder {
      override def getName: String = commitCoordinatorName
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient =
        new InMemoryCommitCoordinator(5) {
          override def commit(
            logStore: LogStore,
            hadoopConf: Configuration,
            logPath: HadoopPath,
            coordinatedCommitsTableConf: util.Map[String, String],
            commitVersion: Long,
            actions: util.Iterator[String],
            updatedActions: UpdatedActions): CommitResponse = {
            throw new IllegalStateException("Fail commit request")
          }
        }
    }

    CommitCoordinatorProvider.registerBuilder(NoBackfillingCommitCoordinatorBuilder$)
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val logPath = new Path(table.getPath(engine), "_delta_log")
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> commitCoordinatorName,
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )

      assert(engine.getFileSystemClient.listFrom(FileNames.listingPrefix(logPath, 0L)).exists { f =>
        new Path(f.getPath).getName === "00000000000000000000.json"
      })
    }
  }

  test("basic write") {
    CommitCoordinatorProvider.clearBuilders()
    CommitCoordinatorProvider
      .registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val logPath = new Path(table.getPath(engine), "_delta_log")
      val commitsDir = new File(FileNames.commitDirPath(logPath).toUri)
      val deltaDir = new File(logPath.toUri)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      )

      assert(getCommitVersions(commitsDir) === Array(1))
      assert(getCommitVersions(deltaDir) === Array(0))

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      assert(getCommitVersions(commitsDir) === Array(1, 2))
      assert(getCommitVersions(deltaDir) === Array(0, 1, 2))

      val snapshot = table.getLatestSnapshot(engine)
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows) ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows)
      checkAnswer(result, expectedAnswer)
    }
  }

  test("cold snapshot initialization") {
    CommitCoordinatorProvider.clearBuilders()
    val builder = TrackingInMemoryCommitCoordinatorBuilder(10)
    val commitCoordinatorClient =
      builder.build(Collections.emptyMap()).asInstanceOf[TrackingCommitCoordinatorClient]
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      val snapshot0 = table.getSnapshotAsOfVersion(engine, 0)
      val result0 = readSnapshot(snapshot0, snapshot0.getSchema(engine), null, null, engine)
      val expectedAnswer0 = dataBatches1.flatMap(_.toTestRows)
      checkAnswer(result0, expectedAnswer0)

      val snapshot1 = table.getSnapshotAsOfVersion(engine, 1)
      val result1 = readSnapshot(snapshot1, snapshot1.getSchema(engine), null, null, engine)
      val expectedAnswer1 = expectedAnswer0 ++ dataBatches2.flatMap(_.toTestRows)
      checkAnswer(result1, expectedAnswer1)

      commitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot2 = table.getLatestSnapshot(engine)
      val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
      val expectedAnswer2 = expectedAnswer1 ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows)
      checkAnswer(result2, expectedAnswer2)
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
        logPath: HadoopPath,
        currentVersion: Long,
        currentMetadata: AbstractMetadata,
        currentProtocol: AbstractProtocol): util.Map[String, String] = {
        super.registerTable(logPath, currentVersion, currentMetadata, currentProtocol)
        expTableConf
      }
      override def getCommits(
        logPath: HadoopPath,
        coordinatedCommitsTableConf: util.Map[String, String],
        startVersion: lang.Long,
        endVersion: lang.Long = null): GetCommitsResponse = {
        assert(coordinatedCommitsTableConf === expTableConf)
        super.getCommits(logPath, coordinatedCommitsTableConf, startVersion, endVersion)
      }
      override def commit(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: HadoopPath,
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
    withTempDirAndEngine { (tablePath, engine) =>
      val logPath = new Path("file:" + tablePath, "_delta_log")
      val table = Table.forPath(engine, tablePath)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "test-coordinator",
          TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
            OBJ_MAPPER.writeValueAsString(expCoordinatorConf),
          TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
            OBJ_MAPPER.writeValueAsString(expTableConf))
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows) ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows)
      checkAnswer(result, expectedAnswer)
      assert(snapshot.getCommitCoordinatorClientHandlerOpt(engine).isPresent)
      assert(
        snapshot
          .getCommitCoordinatorClientHandlerOpt(engine)
          .get()
          .semanticEquals(
            engine.getCommitCoordinatorClientHandler("test-coordinator", expCoordinatorConf)))
    }
  }

  test("commit fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF") {
    CommitCoordinatorProvider.clearBuilders()
    val builder = TrackingInMemoryCommitCoordinatorBuilder(10)
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      intercept[RuntimeException] {
        appendData(
          engine,
          tablePath,
          isNewTable = true,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1),
          tableProperties = Map(
            TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
            TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}",
            TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
              """{"key1": "string_value", "key2Int": "2""")
        )
      }
    }
  }

  test("snapshot is updated recursively when FS table is converted to commit-coordinator table") {
    CommitCoordinatorProvider.clearBuilders()
    val commitCoordinatorClient =
      new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(10))
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10, Some(commitCoordinatorClient))
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      )
      val snapshotV1 = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(snapshotV1.getVersion(engine) === 1)
      assert(!snapshotV1.getCommitCoordinatorClientHandlerOpt(engine).isPresent)

      // Add new commit to convert FS table to coordinated-commits table
      createTxn(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        Seq.empty,
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")).commit(engine, emptyIterable())

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      createTxn(engine, tablePath).commit(engine, emptyIterable())

      val snapshotV4 = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(snapshotV4.getVersion(engine) === 4)
      assert(snapshotV4.getCommitCoordinatorClientHandlerOpt(engine).isPresent)
      // only delta 3/4 will be un-backfilled and should have two dots in filename (x.uuid.json)
      assert(
        snapshotV4
          .getLogSegment.deltas.count(f => new Path(f.getPath).getName.count(_ == '.') == 2) === 2)
    }
  }

  def getCommitVersions(dir: File): Array[Long] = {
    dir
      .listFiles()
      .filterNot(f => f.getName.startsWith(".") && f.getName.endsWith(".crc"))
      .filterNot(f => f.getName.equals("_commits"))
      .map(_.getAbsolutePath)
      .sortBy(path => path).map { commitPath =>
        assert(FileNames.isCommitFile(commitPath))
        FileNames.deltaVersion(new Path(commitPath))
      }
  }
}
