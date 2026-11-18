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
import java.util.{Iterator => JIterator, Optional}

import scala.collection.JavaConverters._

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, CheckpointPolicy, DeltaOperations, DeltaTestUtilsBase, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.DeltaConfigs.{CHECKPOINT_POLICY, COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, IN_COMMIT_TIMESTAMPS_ENABLED}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.DummySnapshot
import org.apache.spark.sql.delta.LogSegment
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaExceptionTestUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.FileNames.UnbackfilledDeltaFile
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, CoordinatedCommitsUtils => JCoordinatedCommitsUtils, GetCommitsResponse => JGetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.{TableIdentifier => CatalystTableIdentifier}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class CoordinatedCommitsSuite
  extends CommitCoordinatorSuiteBase
  with CoordinatedCommitsBaseSuite {

  import testImplicits._

  override def sparkConf: SparkConf = {
    // Make sure all new tables in tests use tracking-in-memory commit-coordinator by default.
    super.sparkConf
      .set(COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey, "tracking-in-memory")
      .set(COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey, JsonUtils.toJson(Map()))
  }

  test("helper method that recovers config from abstract metadata works properly") {
    val m1 = Metadata(
      configuration = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> "string_value")
    )
    assert(JCoordinatedCommitsUtils.getCoordinatorName(m1) === Optional.of("string_value"))

    val m2 = Metadata(
      configuration = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> "")
    )
    assert(JCoordinatedCommitsUtils.getCoordinatorName(m2)=== Optional.of(""))

    val m3 = Metadata(
      configuration = Map(
        COORDINATED_COMMITS_COORDINATOR_CONF.key ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
    )
    assert(JCoordinatedCommitsUtils.getCoordinatorConf(m3) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)

    val m4 = Metadata()
    assert(JCoordinatedCommitsUtils.getCoordinatorConf(m4) === Map.empty.asJava)
  }

  test("tableConf returned from registration API is recorded in deltaLog and passed " +
    "to CommitCoordinatorClient in future for all the APIs") {
    val tableConf = Map("tableID" -> "random-u-u-i-d", "1" -> "2").asJava
    val trackingCommitCoordinatorClient = new TrackingCommitCoordinatorClient(
      new InMemoryCommitCoordinator(batchSize = 10) {
        override def registerTable(
            logPath: Path,
            tableIdentifier: Optional[TableIdentifier],
            currentVersion: Long,
            currentMetadata: AbstractMetadata,
            currentProtocol: AbstractProtocol): java.util.Map[String, String] = {
          super.registerTable(
            logPath, tableIdentifier, currentVersion, currentMetadata, currentProtocol)
          tableConf
        }

        override def getCommits(
            tableDesc: TableDescriptor,
            startVersion: java.lang.Long,
            endVersion: java.lang.Long): JGetCommitsResponse = {
          assert(tableDesc.getTableConf === tableConf)
          super.getCommits(tableDesc, startVersion, endVersion)
        }

        override def commit(
            logStore: LogStore,
            hadoopConf: Configuration,
            tableDesc: TableDescriptor,
            commitVersion: Long,
            actions: java.util.Iterator[String],
            updatedActions: UpdatedActions): CommitResponse = {
          assert(tableDesc.getTableConf === tableConf)
          super.commit(logStore, hadoopConf, tableDesc, commitVersion, actions, updatedActions)
        }

        override def backfillToVersion(
            logStore: LogStore,
            hadoopConf: Configuration,
            tableDesc: TableDescriptor,
            version: Long,
            lastKnownBackfilledVersionOpt: java.lang.Long): Unit = {
          assert(tableDesc.getTableConf === tableConf)
          super.backfillToVersion(
            logStore,
            hadoopConf,
            tableDesc,
            version,
            lastKnownBackfilledVersionOpt)
        }
      }
    )
    val builder = TrackingInMemoryCommitCoordinatorBuilder(
      batchSize = 10, Some(trackingCommitCoordinatorClient))
    registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      val commitCoordinatorConf = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder.getName)
      val newMetadata = Metadata().copy(configuration = commitCoordinatorConf)
      log.startTransaction().commitManually(newMetadata)
      assert(log.unsafeVolatileSnapshot.version === 0)
      assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf.asJava === tableConf)

      log.startTransaction().commitManually(createTestAddFile("f1"))
      log.startTransaction().commitManually(createTestAddFile("f2"))
      log.checkpoint()
      log.startTransaction().commitManually(createTestAddFile("f2"))

      assert(trackingCommitCoordinatorClient.numCommitsCalled.get > 0)
      assert(trackingCommitCoordinatorClient.numGetCommitsCalled.get > 0)
      assert(trackingCommitCoordinatorClient.numBackfillToVersionCalled.get > 0)
    }
  }

  test("transfer from one commit-coordinator to another commit-coordinator fails " +
    "[CC-1 -> CC-2 fails]") {
    clearBuilders()
    val builder1 = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)
    val builder2 = new TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10) {
      override def getName: String = "tracking-in-memory-2"
    }
    Seq(builder1, builder2).foreach(registerBuilder(_))

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      // A new table will automatically get `tracking-in-memory` as the whole suite is configured to
      // use it as default commit-coordinator via
      // [[COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey]].
      log.startTransaction().commitManually(Metadata())
      assert(log.unsafeVolatileSnapshot.version === 0L)
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)

      // Change commit-coordinator
      val newCommitCoordinatorConf =
        Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder2.getName)
      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val newMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration ++ newCommitCoordinatorConf)
      val ex = intercept[IllegalStateException] {
        log.startTransaction().commitManually(newMetadata)
      }
      assert(ex.getMessage.contains(
        "from one commit-coordinator to another commit-coordinator is not allowed"))
    }
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
    val batchSize = 10
    val cs1 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize))
    val cs2 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize))

    case class TrackingInMemoryCommitCoordinatorBuilder(
        name: String,
        commitCoordinatorClient: CommitCoordinatorClient) extends CommitCoordinatorBuilder {
      var numBuildCalled = 0
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
        numBuildCalled += 1
        commitCoordinatorClient
      }

      override def getName: String = name
    }
    val builder1 = TrackingInMemoryCommitCoordinatorBuilder(name = "tracking-in-memory-1", cs1)
    val builder2 = TrackingInMemoryCommitCoordinatorBuilder(name = "tracking-in-memory-2", cs2)
    Seq(builder1, builder2).foreach(CommitCoordinatorProvider.registerBuilder)

    def resetMetrics(): Unit = {
      Seq(builder1, builder2).foreach { b => b.numBuildCalled = 0 }
      Seq(cs1, cs2).foreach(_.reset())
    }

    withSQLConf(
        COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey -> builder1.name) {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        // Step-1: Make 2 commits on the table with CS1 as owner.
        Seq(0).toDF.write.format("delta").mode("append").save(tablePath) // version 0
        Seq(1).toDF.write.format("delta").mode("append").save(tablePath) // version 1
        DeltaLog.clearCache()
        checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(0), Row(1)))

        // Step-2: Add commit 2: change the table owner from "tracking-in-memory-1" to FS.
        //         Add commit 3: change the table owner from FS to "tracking-in-memory-2".
        // Both of these commits should be FS based as the spec mandates an atomic backfill when
        // the commit-coordinator changes.
        {
          val log = DeltaLog.forTable(spark, tablePath)
          val conf = log.newDeltaHadoopConf()
          val segment = log.unsafeVolatileSnapshot.logSegment
          (0 to 1).foreach { version =>
            assert(FileNames.deltaVersion(segment.deltas(version).getPath) === version)
          }
          val oldMetadata = log.unsafeVolatileMetadata
          val oldMetadataConf = oldMetadata.configuration
          val newMetadata1 = oldMetadata.copy(
            configuration = oldMetadataConf - COORDINATED_COMMITS_COORDINATOR_NAME.key)
          val newMetadata2 = oldMetadata.copy(
            configuration = oldMetadataConf + (
              COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder2.name))
          log.startTransaction().commitManually(newMetadata1)
          log.startTransaction().commitManually(newMetadata2)

          // Also backfill commit 0, 1 -- which the spec mandates when the commit-coordinator
          // changes.
          // commit 0 should already be backfilled
          assert(segment.deltas(0).getPath.getName === "00000000000000000000.json")
          log.store.write(
            path = FileNames.unsafeDeltaFile(log.logPath, 1),
            actions = log.store.read(segment.deltas(1).getPath, conf).toIterator,
            overwrite = true,
            conf)
        }

        // Step-3: Do cold read from table and confirm we can construct snapshot v3 automatically.
        // This will update snapshot twice and both CS1 and CS2 will be contacted one after the
        // other. Do cold read from the table and confirm things works as expected.
        DeltaLog.clearCache()
        resetMetrics()
        checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(0), Row(1)))
        assert(builder1.numBuildCalled == 0)
        assert(builder2.numBuildCalled == 1)
        val snapshotV3 = DeltaLog.forTable(spark, tablePath).unsafeVolatileSnapshot
        assert(
          snapshotV3.tableCommitCoordinatorClientOpt.map(_.commitCoordinatorClient) === Some(cs2))
        assert(snapshotV3.version === 3)

        // Step-4: Write more commits using new owner
        resetMetrics()
        Seq(2).toDF.write.format("delta").mode("append").save(tablePath) // version 4
        Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 5
        assert((cs1.numCommitsCalled.get, cs2.numCommitsCalled.get) === (0, 2))
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (0, 2))

        // Step-5: Read the table again and assert that the right APIs are used
        resetMetrics()
        assert(
          sql(s"SELECT * FROM delta.`$tablePath`").collect().toSet === (0 to 3).map(Row(_)).toSet)
        // since this was hot query, so no new snapshot was created as part of this
        // deltaLog.update() and so commit-coordinator is not initialized again.
        assert((builder1.numBuildCalled, builder2.numBuildCalled) === (0, 0))
        // Since this is dataframe read, so we invoke deltaLog.update() twice and so GetCommits API
        // is called twice.
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (0, 2))

        // Step-6: Clear cache and simulate cold read again.
        // We will firstly create snapshot from listing: 0.json, 1.json, 2.json.
        // We create Snapshot v2 and find about owner CS2.
        // Then we contact CS2 to update snapshot and find that v3, v4 exist.
        // We create snapshot v4 and find that owner doesn't change.
        DeltaLog.clearCache()
        resetMetrics()
        assert(
          sql(s"SELECT * FROM delta.`$tablePath`").collect().toSet === (0 to 3).map(Row(_)).toSet)
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (0, 2))
        assert((builder1.numBuildCalled, builder2.numBuildCalled) === (0, 2))
      }
    }
  }
}

class CatalogOwnedSuite
  extends CommitCoordinatorSuiteBase
  with CatalogOwnedTestBaseSuite {

  override def sparkConf: SparkConf = {
    // Make sure all new tables in tests use CatalogOwned table feature by default.
    super.sparkConf.set(defaultCatalogOwnedFeatureEnabledKey, "supported")
  }
}

abstract class CommitCoordinatorSuiteBase
  extends QueryTest
  with CommitCoordinatorUtilBase
  with DeltaSQLTestUtils
  with DeltaTestUtilsBase
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaExceptionTestUtils {

  import testImplicits._

  test("0th commit happens via filesystem") {
    val commitCoordinatorName = "tracking-in-memory"
    object NoBackfillingCommitCoordinatorBuilder$
        extends CatalogOwnedCommitCoordinatorBuilder {

      override def getName: String = commitCoordinatorName

      override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
        new InMemoryCommitCoordinator(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              commitVersion: Long,
              actions: JIterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            throw new IllegalStateException("Fail commit request")
          }
        }

      override def buildForCatalog(spark: SparkSession, name: String): CommitCoordinatorClient =
        new InMemoryCommitCoordinator(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tableDesc: TableDescriptor,
              commitVersion: Long,
              actions: JIterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            throw new IllegalStateException("Fail commit request")
          }
        }
    }

    registerBuilder(NoBackfillingCommitCoordinatorBuilder$)
    withDefaultCCTableFeature {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        Seq(1).toDF.write.format("delta").save(tablePath)
        val log = DeltaLog.forTable(spark, tablePath)
        assert(log.store.listFrom(FileNames.listingPrefix(log.logPath, 0L)).exists { f =>
          f.getPath.getName === "00000000000000000000.json"
        })
      }
    }
  }

  test("basic write") {
    registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 0
      Seq(2).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 1
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2

      val log = DeltaLog.forTable(spark, tablePath)
      val commitsDir = new File(FileNames.commitDirPath(log.logPath).toUri)
      val unbackfilledCommitVersions =
        commitsDir
          .listFiles()
          .filterNot(f => f.getName.startsWith(".") && f.getName.endsWith(".crc"))
          .map(_.getAbsolutePath)
          .sortBy(path => path).map { commitPath =>
            assert(FileNames.isDeltaFile(new Path(commitPath)))
            FileNames.deltaVersion(new Path(commitPath))
          }
      assert(unbackfilledCommitVersions === Array(1, 2))
      checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(2), Row(3)))
    }
  }

  test("cold snapshot initialization") {
    val builder = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)
    val commitCoordinatorClient =
      builder.build(spark, Map.empty).asInstanceOf[TrackingCommitCoordinatorClient]
    registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 0
      DeltaLog.clearCache()
      val usageLogs1 = Log4jUsageLogger.track {
        checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(1)))
      }
      val getCommitsUsageLogs1 = filterUsageRecords(
        usageLogs1,
        CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_CLIENT_GET_COMMITS)
      val getCommitsEventData1 = JsonUtils.fromJson[Map[String, Any]](getCommitsUsageLogs1(0).blob)
      assert(getCommitsEventData1("startVersion") === 0)
      assert(getCommitsEventData1("versionToLoad") === -1)
      assert(getCommitsEventData1("async") === "true")
      assert(getCommitsEventData1("responseCommitsSize") === 0)
      assert(getCommitsEventData1("responseLatestTableVersion") === -1)

      Seq(2).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 1
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2
      DeltaLog.clearCache()
      commitCoordinatorClient.numGetCommitsCalled.set(0)
      val result1 = sql(s"SELECT * FROM delta.`$tablePath`").collect()
      assert(result1.length === 2 && result1.toSet === Set(Row(2), Row(3)))
      assert(commitCoordinatorClient.numGetCommitsCalled.get === 2)
    }
  }

  test("update works correctly with InitialSnapshot") {
    registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLog.forTable(spark, new Path(tablePath), clock)
      assert(log.unsafeVolatileSnapshot.isInstanceOf[DummySnapshot])
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)
      assert(log.getCapturedSnapshot().updateTimestamp == clock.getTimeMillis())
      clock.advance(500)
      log.update()
      assert(log.unsafeVolatileSnapshot.isInstanceOf[DummySnapshot])
      assert(log.getCapturedSnapshot().updateTimestamp == clock.getTimeMillis())
    }
  }

  testWithDifferentBackfillInterval("post commit snapshot creation") { backfillInterval =>
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath

      def getDeltasInPostCommitSnapshot(log: DeltaLog): Seq[String] = {
        log
          .unsafeVolatileSnapshot
          .logSegment.deltas
          .map(_.getPath.getName.replace("0000000000000000000", ""))
      }

      // Commit 0
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath)
      val log = DeltaLog.forTable(spark, tablePath)
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json"))
      assert(log.unsafeVolatileSnapshot.getLastKnownBackfilledVersion == 0)
      var snapshot = log.update()
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json"))
      assert(snapshot.getLastKnownBackfilledVersion == 0)

      // Commit 1
      Seq(2).toDF.write.format("delta").mode("append").save(tablePath) // version 1
      // Note: The assert for backfillInterval = 1 only works because with a batch
      // size of 1, the AbstractBatchBackfillingCommitCoordinator synchronously
      // backfills a commit and then directly returns the backfilled commit information
      // from the commit() call so the post commit snapshot creation directly appends
      // the backfilled commit to the LogSegment.
      //
      // The expected behavior before update() is:
      // Backfill interval 1 : post commit snapshot contains 0.json, 1.json, lkbv = 1
      // Backfill interval 2 : post commit snapshot contains 0.json, 1.uuid.json, lkbv = 0
      // Backfill interval 10: post commit snapshot contains 0.json, 1.uuid.json, lkbv = 0
      val commit1 = if (backfillInterval < 2) "1.json" else "1.uuid-1.json"
      var backfillVersion = if (backfillInterval < 2) 1 else 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1))
      assert(log.unsafeVolatileSnapshot.getLastKnownBackfilledVersion == backfillVersion)
      snapshot = log.update()
      // The expected behavior after update() is:
      // Backfill interval 1 : post commit snapshot contains 0.json, 1.json, lkbv = 1
      // Backfill interval 2 : post commit snapshot contains 0.json, 1.uuid.json, lkbv = 0
      // Backfill interval 10: post commit snapshot contains 0.json, 1.uuid.json, lkbv = 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1))
      assert(snapshot.getLastKnownBackfilledVersion == backfillVersion)

      // Commit 2
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2
      // The expected behavior before update() is:
      // Backfill interval 1 : post commit snapshot contains
      //   0.json, 1.json, 2.json lkbv = 2
      // Backfill interval 2 : post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json lkbv = 0
      // Backfill interval 10: post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json, lkbv = 0
      val commit2 = if (backfillInterval < 2) "2.json" else "2.uuid-2.json"
      backfillVersion = if (backfillInterval < 2) 2 else 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1, commit2))
      assert(log.unsafeVolatileSnapshot.getLastKnownBackfilledVersion == backfillVersion)
      snapshot = log.update()
      // backfill would have happened at commit 2 for batchSize = 2 but we do not swap
      // the snapshot that contains the unbackfilled commits with the updated snapshot
      // (which contains the backfilled commits) during update because they are identical
      // and swapping would lead to losing the cached state. However, we update the
      // effective last known backfilled version on the snapshot.
      //
      // The expected behavior after update is
      // Backfill interval 1 : post commit snapshot contains
      //   0.json, 1.json, 2.json lkbv = 2
      // Backfill interval 2 : post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json lkbv = 0 lkbv = 2
      // Backfill interval 10: post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json lkbv = 0
      backfillVersion = if (backfillInterval <= 2) 2 else 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1, commit2))
      assert(snapshot.getLastKnownBackfilledVersion == backfillVersion)

      // Commit 3
      Seq(4).toDF.write.format("delta").mode("append").save(tablePath)
      val commit3 = if (backfillInterval < 2) "3.json" else "3.uuid-3.json"
      // The post commit snapshot is a new snapshot and so its lastKnownBackfilledVersion
      // member is calculated from the LogSegment. Given that the LogSegment for
      // batchInterval > 1 only contains 0 as the only backfilled commit, we need
      // to set the expected backfillVersion to 0 here for backfillIntervals > 1.
      //
      // The expected behavior before update() is:
      // Backfill interval 1 : post commit snapshot contains
      //   0.json, 1.json, 2.json, 3.json lkbv = 3
      // Backfill interval 2 : post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json, 3.uuid.json lkbv = 0
      // Backfill interval 10: post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json, 3.uuid.json lkbv = 0
      backfillVersion = if (backfillInterval < 2) 3 else 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1, commit2, commit3))
      assert(log.unsafeVolatileSnapshot.getLastKnownBackfilledVersion == backfillVersion)
      snapshot = log.update()
      // The expected behavior after update() is:
      // Backfill interval 1 : post commit snapshot contains
      //   0.json, 1.json, 2.json, 3.json lkbv = 3
      // Backfill interval 2 : post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json, 3.uuid.json lkbv = 2
      // Backfill interval 10: post commit snapshot contains
      //   0.json, 1.uuid.json, 2.uuid.json, 3.uuid.json lkbv = 0
      backfillVersion = if (backfillInterval < 2) 3 else if (backfillInterval == 2) 2 else 0
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1, commit2, commit3))
      assert(snapshot.getLastKnownBackfilledVersion == backfillVersion)

      checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(1), Row(2), Row(3), Row(4)))
    }
  }

  testWithDifferentBackfillInterval("Snapshot.ensureCommitFilesBackfilled") { _ =>
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath

      // Add 10 commits to the table
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath)
      2 to 10 foreach { i =>
        Seq(i).toDF().write.format("delta").mode("append").save(tablePath)
      }
      val log = DeltaLog.forTable(spark, tablePath)
      val snapshot = log.update()
      snapshot.ensureCommitFilesBackfilled()

      val commitFiles = log.listFrom(0).filter(FileNames.isDeltaFile).map(_.getPath)
      val backfilledCommitFiles = (0 to 9).map(
        version => FileNames.unsafeDeltaFile(log.logPath, version))
      assert(commitFiles.toSeq == backfilledCommitFiles)
    }
  }


  test("LogSegment comparison does not swap snapshots that only differ in " +
    "backfilled/unbackfilled commits") {
    // Use a batch size of two so we don't immediately backfill in
    // the AbstractBatchBackfillingCommitCoordinatorClient and so the
    // CommitResponse contains the UUID-based commit.
    registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)

      withSQLConf(
        CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.Classic.name) {
        // Version 0 -- backfilled by default
        makeCommitAndAssertSnapshotState(
          data = Seq(0),
          expectedLastKnownBackfilledVersion = 0,
          expectedNumUnbackfilledCommits = 0,
          expectedLastKnownBackfilledFile = FileNames.unsafeDeltaFile(log.logPath, 0),
          log, tablePath)
        // Version 1 -- not backfilled immediately because of batchSize = 2
        makeCommitAndAssertSnapshotState(
          data = Seq(1),
          expectedLastKnownBackfilledVersion = 0,
          expectedNumUnbackfilledCommits = 1,
          expectedLastKnownBackfilledFile = FileNames.unsafeDeltaFile(log.logPath, 0),
          log, tablePath)
        // Version 2 -- backfills versions 1 and 2
        makeCommitAndAssertSnapshotState(
          data = Seq(2),
          expectedLastKnownBackfilledVersion = 2,
          expectedNumUnbackfilledCommits = 2,
          expectedLastKnownBackfilledFile = FileNames.unsafeDeltaFile(log.logPath, 0),
          log, tablePath)
        // Version 3 -- not backfilled immediately because of batchSize = 2
        makeCommitAndAssertSnapshotState(
          data = Seq(3),
          expectedLastKnownBackfilledVersion = 2,
          expectedNumUnbackfilledCommits = 3,
          expectedLastKnownBackfilledFile = FileNames.unsafeDeltaFile(log.logPath, 0),
          log, tablePath)
        // Version 4 -- backfills versions 3 and 4
        makeCommitAndAssertSnapshotState(
          data = Seq(4),
          expectedLastKnownBackfilledVersion = 4,
          expectedNumUnbackfilledCommits = 4,
          expectedLastKnownBackfilledFile = FileNames.unsafeDeltaFile(log.logPath, 0),
          log, tablePath)
        // Trigger a checkpoint
        log.checkpoint(log.update())
        // Version 5 -- not backfilled immediately because of batchSize 2
        makeCommitAndAssertSnapshotState(
          data = Seq(5),
          expectedLastKnownBackfilledVersion = 4,
          expectedNumUnbackfilledCommits = 1,
          expectedLastKnownBackfilledFile = FileNames.checkpointFileSingular(log.logPath, 4),
          log, tablePath)
        // Version 6 -- backfills versions 5 and 6
        makeCommitAndAssertSnapshotState(
          data = Seq(6),
          expectedLastKnownBackfilledVersion = 6,
          expectedNumUnbackfilledCommits = 2,
          expectedLastKnownBackfilledFile = FileNames.checkpointFileSingular(log.logPath, 4),
          log, tablePath)
      }
    }
  }

  private def makeCommitAndAssertSnapshotState(
      data: Seq[Long],
      expectedLastKnownBackfilledVersion: Long,
      expectedNumUnbackfilledCommits: Long,
      expectedLastKnownBackfilledFile: Path,
      log: DeltaLog,
      tablePath: String): Unit = {
    data.toDF().write.format("delta").mode("overwrite").save(tablePath)
    val snapshot = log.update()
    val segment = snapshot.logSegment
    var numUnbackfilledCommits = 0
    segment.deltas.foreach {
      case UnbackfilledDeltaFile(_, _, _) => numUnbackfilledCommits += 1
      case _ => // do nothing
    }
    assert(snapshot.getLastKnownBackfilledVersion == expectedLastKnownBackfilledVersion)
    assert(numUnbackfilledCommits == expectedNumUnbackfilledCommits)
    val lastKnownBackfilledFile = CoordinatedCommitsUtils
      .getLastBackfilledFile(segment.deltas).getOrElse(
        segment.checkpointProvider.topLevelFiles.head
      )
    assert(lastKnownBackfilledFile.getPath == expectedLastKnownBackfilledFile,
      s"$lastKnownBackfilledFile did not equal $expectedLastKnownBackfilledFile")
  }

  test("During REPLACE, for non-CC tables, default CC configurations are ignored, but default " +
      "ICT confs are retained, and existing ICT confs are discarded") {
    // Non-CC table, REPLACE with default CC and ICT confs => Non-CC, but with ICT confs.
    withTempDir { tempDir =>
      withoutDefaultCCTableFeature {
        sql(s"CREATE TABLE delta.`${tempDir.getAbsolutePath}` (id LONG) USING delta")
      }
      withSQLConf(IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "true") {
        sql(s"REPLACE TABLE delta.`${tempDir.getAbsolutePath}` (id STRING) USING delta")
      }
      assert(DeltaLog.forTable(spark, tempDir).snapshot.tableCommitCoordinatorClientOpt.isEmpty)
      assert(!DeltaLog.forTable(spark, tempDir).snapshot.isCatalogOwned)
      assert(DeltaLog.forTable(spark, tempDir).snapshot.metadata.configuration.contains(
        IN_COMMIT_TIMESTAMPS_ENABLED.key))
    }

    // Non-CC table with ICT confs, REPLACE with only default CC confs => Non-CC, also no ICT confs.
    withTempDir { tempDir =>
      withoutDefaultCCTableFeature {
        withSQLConf(IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "true") {
          sql(s"CREATE TABLE delta.`${tempDir.getAbsolutePath}` (id LONG) USING delta")
        }
      }
      sql(s"REPLACE TABLE delta.`${tempDir.getAbsolutePath}` (id STRING) USING delta")
      val snapshot = DeltaLog.forTable(spark, tempDir).unsafeVolatileSnapshot
      assert(snapshot.tableCommitCoordinatorClientOpt.isEmpty)
      assert(!snapshot.isCatalogOwned)
      assert(!snapshot.metadata.configuration.contains(IN_COMMIT_TIMESTAMPS_ENABLED.key))
    }
  }

  test("During REPLACE, for CC tables, existing CC and ICT configurations are both retained.") {
    registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(1))

    withTable("t1") {
      if (isCatalogOwnedTest) {
        sql(s"CREATE TABLE t1 (id LONG) USING delta")
        sql(s"INSERT INTO t1 VALUES (0)")
      } else {
        withoutDefaultCCTableFeature {
          sql(s"CREATE TABLE t1 (id LONG) USING delta")
          sql(s"INSERT INTO t1 VALUES (0)")
          sql(s"ALTER TABLE t1 SET TBLPROPERTIES " + propertiesString)
        }
      }
      withoutDefaultCCTableFeature {
        // All three ICT configurations should be set because CC feature is enabled later.
        // REPLACE w/o default CC confs => CC, and all ICT confs.
        sql(s"REPLACE TABLE t1 (id STRING) USING delta")
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, CatalystTableIdentifier("t1"))
        if (isCatalogOwnedTest) {
          assert(snapshot.isCatalogOwned)
          // Only [[IN_COMMIT_TIMESTAMPS_ENABLED]] should be set for CatalogOwned
          // since we don't support upgrade yet.
          assert(snapshot.metadata.configuration.contains(IN_COMMIT_TIMESTAMPS_ENABLED.key))
        } else {
          assert(snapshot.tableCommitCoordinatorClientOpt.nonEmpty)
          CoordinatedCommitsUtils.ICT_TABLE_PROPERTY_KEYS.foreach { key =>
            assert(snapshot.metadata.configuration.contains(key))
          }
        }
      }
    }
  }

  test("CREATE LIKE does not copy commit coordinated related feature config " +
    "from the source table.") {
    registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(1))

    val source = "sourcetable"
    val target = "targettable"
    sql(s"CREATE TABLE $source (id LONG) USING delta TBLPROPERTIES" + propertiesString)
    sql(s"CREATE TABLE $target LIKE $source USING DELTA")
    val snapshot = DeltaLog.forTable(spark, target).unsafeVolatileSnapshot
    assert(snapshot.tableCommitCoordinatorClientOpt.isEmpty)
    assert(!snapshot.isCatalogOwned)
  }

}
