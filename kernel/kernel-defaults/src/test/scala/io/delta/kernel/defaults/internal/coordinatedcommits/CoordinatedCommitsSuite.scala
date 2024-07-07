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
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultCommitCoordinatorClientHandler
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.ConcurrentWriteException
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol, SingleAction}
import io.delta.kernel.internal.actions.SingleAction.{createMetadataSingleAction, FULL_SCHEMA}
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.snapshot.{SnapshotManager, TableCommitCoordinatorClientHandler}
import io.delta.kernel.internal.util.{CoordinatedCommitsUtils, FileNames, ManualClock}
import io.delta.kernel.internal.util.Preconditions.checkArgument
import io.delta.kernel.internal.util.Utils.{closeCloseables, singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse, InMemoryCommitCoordinator, UpdatedActions, CoordinatedCommitsUtils => CCU}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration

import java.{lang, util}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import org.apache.hadoop.fs.{Path => HadoopPath}

import java.io.File
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterator asScala`}
import scala.collection.JavaConverters._
import scala.math
import scala.collection.immutable.Seq

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {
  private val trackingInMemoryBatchSize10Config = Map(
    CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
    InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10")
  def setupCoordinatedCommitsForTest(
    engine: Engine,
    tablePath: String,
    commitDatas: Seq[Seq[FilteredColumnarBatch]],
    coordinatorName: String = "tracking-in-memory",
    coordinatorConf: String = "{}",
    versionConvertToCC: Long = 0L,
    coordinatedCommitNum: Long = 3L,
    checkpointVersion: Long = -1L,
    checkpointInterval: Long = -1L,
    backfillVersion: Long = -1L): Unit = {
    val table = Table.forPath(engine, tablePath)
    val totalCommitNum = coordinatedCommitNum + versionConvertToCC
    val handler = engine.getCommitCoordinatorClientHandler(
      coordinatorName, OBJ_MAPPER.readValue(coordinatorConf, classOf[util.Map[String, String]]))
    val logPath = new Path(table.getPath(engine), "_delta_log")

    /** Rewrite the FS to CC conversion commit and move coordinated commits to _commits folder */
    (0L until totalCommitNum).foreach{ version =>

      if (version == versionConvertToCC) {
        appendData(
          engine,
          tablePath,
          isNewTable = version == 0L,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> commitDatas(version.toInt)),
          tableProperties = Map(
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> coordinatorName,
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> coordinatorConf)
        )
      } else {
        appendData(
          engine,
          tablePath,
          isNewTable = version == 0L,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] ->  commitDatas(version.toInt))
        )
      }
      if (version == checkpointVersion) {
        table.checkpoint(engine, version)
      }
      if (checkpointInterval != -1 && version % checkpointInterval == 0) {
        table.checkpoint(engine, version)
      }
      if (version == backfillVersion) {
        handler.backfillToVersion(logPath.toString, Collections.emptyMap(), version, null)
      }
    }
  }

  def testWithCoordinatorCommits(
    hadoopConf: Map[String, String] = Map.empty, f: (String, Engine) => Unit): Unit = {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine(f, hadoopConf)
  }

  test("0th commit happens via filesystem") {
    val config = Map(CommitCoordinatorProvider.
      getCommitCoordinatorNameConfKey("nobackfilling-commit-coordinator") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "5")
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
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
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "nobackfilling-commit-coordinator",
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
        )

        assert(
          engine.getFileSystemClient.listFrom(FileNames.listingPrefix(logPath, 0L)).exists { f =>
          new Path(f.getPath).getName === "00000000000000000000.json"
        })
    })
  }

  test("basic write") {
    val config =
      Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
        InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "2")
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val logPath = new Path(table.getPath(engine), "_delta_log")
        val commitsDir = new File(FileNames.commitDirPath(logPath).toUri)
        val deltaDir = new File(logPath.toUri)

        val commitDatas = Seq.fill(2)(dataBatches1)
        setupCoordinatedCommitsForTest(engine, tablePath, commitDatas, coordinatedCommitNum = 2L)

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
          dataBatches1.flatMap(_.toTestRows) ++
          dataBatches1.flatMap(_.toTestRows) ++
          dataBatches2.flatMap(_.toTestRows)
        checkAnswer(result, expectedAnswer)
    })
  }

  test("cold snapshot initialization") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val commitDatas = Seq.fill(3)(dataBatches1)
        setupCoordinatedCommitsForTest(engine, tablePath, commitDatas)

        var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
        for (version <- 0L to 1L) {
          val snapshot = table.getSnapshotAsOfVersion(engine, version)
          val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
          checkAnswer(result, expectedAnswer)
          expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
        }

        TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
        val snapshot2 = table.getLatestSnapshot(engine)
        val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
        checkAnswer(result2, expectedAnswer)
        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
    })
  }

  test("snapshot read should use coordinated commit related properties properly") {
    val config = Map(
      CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("test-coordinator") ->
        classOf[TestCommitCoordinatorBuilder].getName)
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(3)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          coordinatorName = "test-coordinator",
          coordinatorConf =
            OBJ_MAPPER.writeValueAsString(TestCommitCoordinator.EXP_COORDINATOR_CONF))
        val table = Table.forPath(engine, tablePath)

        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
          dataBatches1.flatMap(_.toTestRows) ++
          dataBatches1.flatMap(_.toTestRows)
        checkAnswer(result, expectedAnswer)

        assert(snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        assert(
          snapshot
            .getTableCommitCoordinatorClientHandlerOpt(engine)
            .get()
            .semanticEquals(
              engine.getCommitCoordinatorClientHandler(
                "test-coordinator", TestCommitCoordinator.EXP_COORDINATOR_CONF)))
        assert(COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine, snapshot.getMetadata) ===
          TestCommitCoordinator.EXP_TABLE_CONF)

        assert(TrackingCommitCoordinatorClient.numCommitsCalled.get > 0)
        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get > 0)
        assert(TestCommitCoordinator.numBackfillToVersionCalled > 0)
    })
  }

  test("commit fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
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
    })
  }

  test("snapshot read with checkpoint before table converted to coordinated commit table") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(4)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 2L,
          coordinatedCommitNum = 2L,
          checkpointVersion = 1L)

        val table = Table.forPath(engine, tablePath)
        var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
        for (version <- 0L to 2L) {
          val snapshot = table.getSnapshotAsOfVersion(engine, version)
          val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
          checkAnswer(result, expectedAnswer)
          expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
        }

        TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
        val snapshot3 = table.getLatestSnapshot(engine)
        val result3 = readSnapshot(snapshot3, snapshot3.getSchema(engine), null, null, engine)
        checkAnswer(result3, expectedAnswer)
        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
    })
  }

  test("snapshot read with overlap between filesystem based commits and coordinated commits") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(6)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 2L,
          coordinatedCommitNum = 4L,
          backfillVersion = 4L)

        val table = Table.forPath(engine, tablePath)
        var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
        for (version <- 0L to 4L) {
          val snapshot = table.getSnapshotAsOfVersion(engine, version)
          val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
          checkAnswer(result, expectedAnswer)
          expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
        }

        TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
        val snapshot5 = table.getLatestSnapshot(engine)
        val result5 = readSnapshot(snapshot5, snapshot5.getSchema(engine), null, null, engine)
        checkAnswer(result5, expectedAnswer)
        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
    })
  }

  test("getSnapshotAt with coordinated commits enabled") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(5)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine, tablePath, commitDatas, versionConvertToCC = 2L)

        val table = Table.forPath(engine, tablePath)
        var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
        for (version <- 0L to 4L) {
          val snapshot = table.getSnapshotAsOfVersion(engine, version)
          val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
          checkAnswer(result, expectedAnswer)
          if (version != 4L) {
            expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
          }
        }
    })
  }

  test("versionToLoad higher than possible") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(5)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine, tablePath, commitDatas, versionConvertToCC = 2L)
        val table = Table.forPath(engine, tablePath)
        val e = intercept[RuntimeException] {
          table.getSnapshotAsOfVersion(engine, 5L)
        }
        assert(e.getMessage.contains(
          "Cannot load table version 5 as it does not exist. The latest available version is 4"))
    })
  }

  test("snapshot is updated recursively when FS table is converted to commit-coordinator table") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val commitDatas = Seq.fill(5)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 2L)

        val table = Table.forPath(engine, tablePath)

        val snapshotV1 = table.getSnapshotAsOfVersion(engine, 1L).asInstanceOf[SnapshotImpl]
        assert(snapshotV1.getVersion(engine) === 1L)
        assert(!snapshotV1.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)

        val snapshotV4 = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(snapshotV4.getVersion(engine) === 4)
        assert(snapshotV4.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        // only delta 3/4 will be un-backfilled and should have two dots in filename (x.uuid.json)
        assert(
          snapshotV4
            .getLogSegment
            .deltas.count(f => new Path(f.getPath).getName.count(_ == '.') == 2) === 2)
    })
  }

  testWithDifferentBackfillInterval("post commit snapshot creation") { backfillInterval =>
    val config =
      Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
        InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> backfillInterval.toString)
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        def getDeltasInPostCommitSnapshot(table: Table): Seq[String] = {
          table
            .getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
            .getLogSegment.deltas
            .map(f => new Path(f.getPath).getName.replace("0000000000000000000", "")).toList
        }

        val table = Table.forPath(engine, tablePath)
        // Commit 0
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
        assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json"))

        // Commit 1
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches2)
        ) // version 1
        val commit1 = if (backfillInterval < 2) "1.json" else "1.uuid-1.json"
        assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json", commit1))

        // Commit 2
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        ) // version 2
        if (backfillInterval <= 2) {
          // backfill would have happened at commit 2. Next deltaLog.update will pickup the
          // backfilled files.
          assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json", "1.json", "2.json"))
        } else {
          assert(getDeltasInPostCommitSnapshot(table) ===
            Seq("0.json", "1.uuid-1.json", "2.uuid-2.json"))
        }

        // Commit 3
        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches2)
        ) // version 3
        val commit3 = if (backfillInterval < 2) "3.json" else "3.uuid-3.json"
        if (backfillInterval <= 2) {
          assert(
            getDeltasInPostCommitSnapshot(table) === Seq("0.json", "1.json", "2.json", commit3))
        } else {
          assert(getDeltasInPostCommitSnapshot(table) ===
            Seq("0.json", "1.uuid-1.json", "2.uuid-2.json", commit3))
        }

        val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
          dataBatches2.flatMap(_.toTestRows) ++
          dataBatches1.flatMap(_.toTestRows) ++
          dataBatches2.flatMap(_.toTestRows)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        checkAnswer(
          readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
    })
  }

  testWithDifferentBackfillInterval("Snapshot.ensureCommitFilesBackfilled") { backfillBatchSize =>
    val config = Map(
      CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> backfillBatchSize.toString)
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val logPath = new Path(table.getPath(engine), "_delta_log")
        val commitDatas = Seq.fill(10)(dataBatches1)
        // Add 10 commits to the table
        setupCoordinatedCommitsForTest(
          engine, tablePath, commitDatas, coordinatedCommitNum = 10L)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

        snapshot.ensureCommitFilesBackfilled(engine)

        val commitFiles =
          engine.getFileSystemClient.listFrom(FileNames.listingPrefix(logPath, 0L))
            .filterNot(f => f.getPath.endsWith("_commits"))
            .map(_.getPath)
        val backfilledCommitFiles = (0 to 9).map(
          version => FileNames.deltaFile(logPath, version))
        assert(commitFiles.toSeq == backfilledCommitFiles)
    })
  }

  testWithDifferentCheckpointVersion("checkpoint with coordinated commit") {checkpointInterval =>
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val commitDatas = Seq.fill(20)(dataBatches1)
        // Add 20 commits to the table
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas, coordinatedCommitNum = 20L, checkpointInterval = checkpointInterval)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        val expectedAnswer = commitDatas.flatMap(_.flatMap(_.toTestRows))
        checkAnswer(
          readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
    })
  }

  test("table.getSnapshotAsOfVersion") {
    def checkGetSnapshotAt(engine: Engine, table: Table, version: Long): Unit = {
      var snapshot: SnapshotImpl = null

      snapshot = table.getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl]
      assert(snapshot.getVersion(engine) === version)

      val versionsInLogSegment = snapshot.getLogSegment.deltas.map(
        f => FileNames.deltaVersion(f.getPath))
      assert(versionsInLogSegment === (0L to version))
      val expectedAnswer = (1L to version + 1L).flatMap(_ => dataBatches1.flatMap(_.toTestRows))
      checkAnswer(
        readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
    }
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val commitDatas = Seq.fill(5)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 3L,
          coordinatedCommitNum = 2L)
        for (version <- 0L to 4L) {
          checkGetSnapshotAt(engine, table, version)
        }
    })
  }

  test("transfer from one commit-coordinator to another commit-coordinator fails " +
    "[CC-1 -> CC-2 fails]") {
    val config = Map(
      CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      CommitCoordinatorProvider.
        getCommitCoordinatorNameConfKey("nobackfilling-commit-coordinator") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10")
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        enableCoordinatedCommits(engine, tablePath, "tracking-in-memory", isNewTable = true)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(snapshot.getVersion(engine) === 0L)
        assert(snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)

        // Change commit-coordinator
        val ex = intercept[IllegalStateException] {
          enableCoordinatedCommits(engine, tablePath, "nobackfilling-commit-coordinator")
        }
        assert(ex.getMessage.contains(
          "from one commit-coordinator to another commit-coordinator is not allowed"))
    })
  }

  test("FS -> CC upgrade is not retried on a conflict") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val txn = createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testSchema,
          Seq.empty,
          tableProperties = Map(
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}"))

        appendData(
          engine,
          tablePath,
          isNewTable = true,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1))

        intercept[ConcurrentWriteException] {
          txn.commit(engine, emptyIterable()) // upgrade txn committed
        }
    })
  }

  test("Conflict resolution should work with coordinated commits") {
    val config = trackingInMemoryBatchSize10Config
    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = TableImpl.forPath(engine, tablePath, () => System.currentTimeMillis)

        val commitDatas = Seq.fill(4)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 2L,
          coordinatedCommitNum = 2L)

        val startTime = System.currentTimeMillis()
        val clock = new ManualClock(startTime)
        val txn1 = createTxn(
          engine,
          tablePath,
          schema = testSchema,
          partCols = Seq.empty,
          clock = clock
        )
        clock.setTime(startTime)
        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataBatches2),
          clock = clock
        )
        clock.setTime(startTime - 1000)
        commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
        val ver4Snapshot = table.getSnapshotAsOfVersion(engine, 4L).asInstanceOf[SnapshotImpl]
        val ver5Snapshot = table.getSnapshotAsOfVersion(engine, 5L).asInstanceOf[SnapshotImpl]
        assert(
          ver5Snapshot.getTimestamp(engine) === ver4Snapshot.getTimestamp(engine) + 1)
    })
  }

  // This test has the following setup:
  // Setup:
  // 1. Make 2 commits on the table with CS1 as owner.
  // 2. Make 2 new commits to change the owner back to FS and then from FS to CS2.
  // 3. Do cold read from table and confirm we can construct snapshot v3 automatically. This will
  //    need multiple snapshot update internally and both CS1 and CS2 will be contacted one
  //    after the other.
  // 4. Write commit 4/5 using new commit-coordinator.
  // 5. Read the table again and make sure right APIs are called:
  //    a) If read query is run in scala, we do listing 2 times. So CS2.getCommits will be called
  //       twice. We should not be contacting CS1 anymore.
  //    b) If read query is run on SQL, we do listing only once. So CS2.getCommits will be called
  //       only once.
  test("snapshot is updated properly when owner changes multiple times") {
    val config = Map(
      CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory-1") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder1].getName,
      CommitCoordinatorProvider.
        getCommitCoordinatorNameConfKey("tracking-in-memory-2") ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder2].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10")

    def verifyBackfilledList(
      engine: Engine, logPath: Path, backfilledList: List[String]): Unit = {
      assert(
        engine
          .getFileSystemClient
          .listFrom(FileNames.listingPrefix(logPath, 0L))
          .map(f => new Path(f.getPath).getName).toList ==
          backfilledList ++ List("_commits"))
    }

    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        val table = Table.forPath(engine, tablePath)
        val logPath = new Path(table.getPath(engine), "_delta_log")
        // Step-1: Make 2 commits on the table with CS1 as owner.
        appendData(
          engine,
          tablePath,
          isNewTable = true,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1),
          tableProperties = Map(
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory-1",
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

        var expectedAnswer =
          dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)
        val ver1Snapshot = table.getLatestSnapshot(engine)
        checkAnswer(
          readSnapshot(ver1Snapshot, ver1Snapshot.getSchema(engine), null, null, engine),
          expectedAnswer)
        verifyBackfilledList(engine, logPath, (0 to 0).map(i => f"$i%020d.json").toList)

        // Step-2: Add commit 2: change the table owner from "tracking-in-memory-1" to FS.
        //         Add commit 3: change the table owner from FS to "tracking-in-memory-2".
        // Both of these commits should be FS based as the spec mandates an atomic backfill when
        // the commit-coordinator changes.
        {
          enableCoordinatedCommits(engine, tablePath, null)

          // Commit 0/1/2 should already be backfilled -- which the spec mandates when the
          // commit-coordinator changes
          verifyBackfilledList(engine, logPath, (0 to 2).map(i => f"$i%020d.json").toList)

          enableCoordinatedCommits(engine, tablePath, "tracking-in-memory-2")
          verifyBackfilledList(engine, logPath, (0 to 3).map(i => f"$i%020d.json").toList)
        }

        // Step-3: Confirm we can construct snapshot v3 automatically.
        TrackingInMemoryCommitCoordinatorBuilderX.resetMetrics()
        val ver3Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        checkAnswer(
          readSnapshot(ver3Snapshot, ver3Snapshot.getSchema(engine), null, null, engine),
          expectedAnswer)
        assert(TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled1 == 0)
        assert(TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled2 == 1)

        assert(
          ver3Snapshot
            .getTableCommitCoordinatorClientHandlerOpt(engine)
            .get
            .semanticEquals(engine.getCommitCoordinatorClientHandler(
              "tracking-in-memory-2", Collections.emptyMap())))
        assert(ver3Snapshot.getVersion(engine) === 3)

        // Step-4: Write more commits using new owner
        TrackingInMemoryCommitCoordinatorBuilderX.resetMetrics()
        appendData(
          engine,
          tablePath,
          isNewTable = false,
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

        assert(TrackingCommitCoordinatorClient.numCommitsCalled.get === 2)
        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 2)

        // Step-5: Read the table again and assert that the right APIs are used
        TrackingInMemoryCommitCoordinatorBuilderX.resetMetrics()
        expectedAnswer = expectedAnswer ++ dataBatches1.flatMap(_.toTestRows) ++
          dataBatches2.flatMap(_.toTestRows)
        val ver5Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        checkAnswer(
          readSnapshot(ver5Snapshot, ver5Snapshot.getSchema(engine), null, null, engine),
          expectedAnswer)

        assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
        assert((
          TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled1,
          TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled2) === (0, 1))
    })
  }

  test("Incomplete backfills are handled properly by next commit after CC to FS conversion") {
    val config = Map(
      CommitCoordinatorProvider.
        getCommitCoordinatorNameConfKey("never-backfilling-commit-coordinator") ->
        classOf[NeverBackfillingCommitCoordinatorBuilder].getName)

    testWithCoordinatorCommits(config, {
      (tablePath, engine) =>
        appendData(
          engine,
          tablePath,
          isNewTable = true,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1),
          tableProperties = Map(
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "never-backfilling-commit-coordinator",
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
        ) // v0

        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        ) // v1

        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        ) // v2

        val table = Table.forPath(engine, tablePath)
        val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(ver2Snapshot.getVersion(engine) === 2)
        assert(ver2Snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        assert(
          ver2Snapshot.getLogSegment.deltas.count(
            f => new Path(f.getPath).getName.count(_ == '.') == 2) === 2)

        enableCoordinatedCommits(engine, tablePath, null)

        val snapshotAfterDowngrade = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(snapshotAfterDowngrade.getVersion(engine) === 3)
        assert(!snapshotAfterDowngrade.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        assert(
          snapshotAfterDowngrade.getLogSegment.deltas.count(f =>
            FileNames.getUnbackfilledDeltaFile(new Path(f.getPath)).isPresent) === 3)

        appendData(
          engine,
          tablePath,
          isNewTable = false,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        ) // v4

        val ver4Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(ver4Snapshot.getVersion(engine) === 4)
        assert(
          ver4Snapshot.getLogSegment.deltas.count(f =>
            FileNames.getUnbackfilledDeltaFile(new Path(f.getPath)).isPresent) === 0)

        val expectedAnswer = (1 to 4).flatMap(_ => dataBatches1.flatMap(_.toTestRows))
        checkAnswer(
          readSnapshot(
            ver4Snapshot, ver4Snapshot.getSchema(engine), null, null, engine), expectedAnswer)
    })
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

  val COORDINATOR = new TrackingCommitCoordinatorClient(new TestCommitCoordinatorClient())

  var numBackfillToVersionCalled = 0
}

/**
 * A [[CommitCoordinatorClient]] that tests can use to check the coordinator configuration and
 * table configuration.
 *
 * @param EXP_TABLE_CONF The expected table configuration that the builder should receive.
 * @param EXP_COORDINATOR_CONF The expected coordinator configuration that the builder should
 *                             receive.
 * @param coordinatedCommitsTableConf The table configuration that the coordinator receives.
 * @param conf The coordinator configuration that the builder receives.
 */
class TestCommitCoordinatorClient extends InMemoryCommitCoordinator(2) {
  override def registerTable(
    logPath: HadoopPath,
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol): util.Map[String, String] = {
    super.registerTable(logPath, currentVersion, currentMetadata, currentProtocol)
    TestCommitCoordinator.EXP_TABLE_CONF
  }
  override def getCommits(
    logPath: HadoopPath,
    coordinatedCommitsTableConf: util.Map[String, String],
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse = {
    checkArgument(coordinatedCommitsTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
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
    checkArgument(coordinatedCommitsTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.commit(logStore, hadoopConf, logPath, coordinatedCommitsTableConf,
      commitVersion, actions, updatedActions)
  }

  override def backfillToVersion(
    logStore: LogStore,
    hadoopConf: Configuration,
    logPath: HadoopPath,
    coordinatedCommitsTableConf: util.Map[String, String],
    version: Long,
    lastKnownBackfilledVersion: lang.Long): Unit = {
    TestCommitCoordinator.numBackfillToVersionCalled += 1
    checkArgument(coordinatedCommitsTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.backfillToVersion(
      logStore,
      hadoopConf,
      logPath,
      coordinatedCommitsTableConf,
      version,
      lastKnownBackfilledVersion)
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

class NoBackfillingCommitCoordinatorBuilder(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def getName: String = "nobackfilling-commit-coordinator"
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    val coordinator = new InMemoryCommitCoordinator(5) {
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
    InMemoryCommitCoordinatorBuilder.batchSizeMap.put(5L, coordinator)
    coordinator
  }
}

object TrackingInMemoryCommitCoordinatorBuilderX {
  val batchSize = 10
  val cc1 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize))
  val cc2 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize))
  var numBuildCalled1 = 0
  var numBuildCalled2 = 0

  def resetMetrics(): Unit = {
    numBuildCalled1 = 0
    numBuildCalled2 = 0
    Seq(cc1, cc2).foreach(_.reset())
  }
}

class TrackingInMemoryCommitCoordinatorBuilder1(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled1 += 1
    TrackingInMemoryCommitCoordinatorBuilderX.cc1
  }
  override def getName: String = "tracking-in-memory-1"
}

class TrackingInMemoryCommitCoordinatorBuilder2(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    TrackingInMemoryCommitCoordinatorBuilderX.numBuildCalled2 += 1
    TrackingInMemoryCommitCoordinatorBuilderX.cc2
  }
  override def getName: String = "tracking-in-memory-2"
}

object NeverBackfillingCommitCoordinatorClient {
  val cc = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(10) {
    override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: HadoopPath,
      coordinatedCommitsTableConf: util.Map[String, String],
      version: Long,
      lastKnownBackfilledVersion: lang.Long): Unit = { }
  })
}

class NeverBackfillingCommitCoordinatorBuilder(
  hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    NeverBackfillingCommitCoordinatorClient.cc
  }
  override def getName: String = "never-backfilling-commit-coordinator"
}
