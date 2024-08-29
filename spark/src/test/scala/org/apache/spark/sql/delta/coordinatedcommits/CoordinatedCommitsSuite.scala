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
import java.lang.{Long => JLong}
import java.util.{Iterator => JIterator, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.databricks.spark.util.Log4jUsageLogger
import com.databricks.spark.util.UsageRecord
import org.apache.spark.sql.delta.{CommitStats, CoordinatedCommitsStats, CoordinatedCommitsTableFeature, DeltaOperations, DeltaUnsupportedOperationException, V2CheckpointTableFeature}
import org.apache.spark.sql.delta.{CommitCoordinatorGetCommitsFailedException, DeltaIllegalArgumentException}
import org.apache.spark.sql.delta.CoordinatedCommitType._
import org.apache.spark.sql.delta.DeltaConfigs.{CHECKPOINT_INTERVAL, COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.InitialSnapshot
import org.apache.spark.sql.delta.LogSegment
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.FileNames.{CompactedDeltaFile, DeltaFile, UnbackfilledDeltaFile}
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse => JGetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class CoordinatedCommitsSuite
    extends QueryTest
    with DeltaSQLTestUtils
    with SharedSparkSession
    with DeltaSQLCommandTest
    with CoordinatedCommitsTestUtils {

  import testImplicits._

  override def sparkConf: SparkConf = {
    // Make sure all new tables in tests use tracking-in-memory commit-coordinator by default.
    super.sparkConf
      .set(COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey, "tracking-in-memory")
      .set(COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey, JsonUtils.toJson(Map()))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
  }

  test("helper method that recovers config from abstract metadata works properly") {
    val m1 = Metadata(
      configuration = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> "string_value")
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndDeltaConfig(
      m1, COORDINATED_COMMITS_COORDINATOR_NAME) === Some("string_value"))

    val m2 = Metadata(
      configuration = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> "")
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndDeltaConfig(
      m2, COORDINATED_COMMITS_COORDINATOR_NAME) === Some(""))

    val m3 = Metadata(
      configuration = Map(
        COORDINATED_COMMITS_COORDINATOR_CONF.key ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
    )
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndDeltaConfig(
      m3, COORDINATED_COMMITS_COORDINATOR_CONF) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\""))

    val m4 = Metadata()
    assert(CoordinatedCommitsUtils.fromAbstractMetadataAndDeltaConfig(
      m4, COORDINATED_COMMITS_TABLE_CONF) === Map.empty)
  }


  test("0th commit happens via filesystem") {
    val commitCoordinatorName = "nobackfilling-commit-coordinator"
    object NoBackfillingCommitCoordinatorBuilder$ extends CommitCoordinatorBuilder {

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
    }

    CommitCoordinatorProvider.registerBuilder(NoBackfillingCommitCoordinatorBuilder$)
    withSQLConf(
      COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey -> commitCoordinatorName) {
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
    CommitCoordinatorProvider.registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
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
    CommitCoordinatorProvider.registerBuilder(builder)
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
      import testImplicits._
      val result1 = sql(s"SELECT * FROM delta.`$tablePath`").collect()
      assert(result1.length === 2 && result1.toSet === Set(Row(2), Row(3)))
      assert(commitCoordinatorClient.numGetCommitsCalled.get === 2)
    }
  }

  // Test commit-coordinator changed on concurrent cluster
    testWithDefaultCommitCoordinatorUnset("snapshot is updated recursively when FS table" +
      " is converted to commit-coordinator table on a concurrent cluster") {
    val commitCoordinatorClient =
      new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize = 10))
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10, Some(commitCoordinatorClient))
    CommitCoordinatorProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val deltaLog1 = DeltaLog.forTable(spark, tablePath)
      deltaLog1.startTransaction().commitManually(Metadata())
      deltaLog1.startTransaction().commitManually(createTestAddFile("f1"))
      deltaLog1.startTransaction().commitManually()
      val snapshotV2 = deltaLog1.update()
      assert(snapshotV2.version === 2)
      assert(snapshotV2.tableCommitCoordinatorClientOpt.isEmpty)
      DeltaLog.clearCache()

      // Add new commit to convert FS table to coordinated-commits table
      val deltaLog2 = DeltaLog.forTable(spark, tablePath)
      enableCoordinatedCommits(deltaLog2, commitCoordinator = "tracking-in-memory")
      deltaLog2.startTransaction().commitManually(createTestAddFile("f2"))
      deltaLog2.startTransaction().commitManually()
      val snapshotV5 = deltaLog2.unsafeVolatileSnapshot
      assert(snapshotV5.version === 5)
      assert(snapshotV5.tableCommitCoordinatorClientOpt.nonEmpty)
      // only delta 4/5 will be un-backfilled and should have two dots in filename (x.uuid.json)
      assert(snapshotV5.logSegment.deltas.count(_.getPath.getName.count(_ == '.') == 2) === 2)

      val usageRecords = Log4jUsageLogger.track {
        val newSnapshotV5 = deltaLog1.update()
        assert(newSnapshotV5.version === 5)
        assert(newSnapshotV5.logSegment.deltas === snapshotV5.logSegment.deltas)
      }
      assert(filterUsageRecords(usageRecords, "delta.readChecksum").size === 2)
    }
  }

  test("update works correctly with InitialSnapshot") {
    CommitCoordinatorProvider.registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLog.forTable(spark, new Path(tablePath), clock)
      assert(log.unsafeVolatileSnapshot.isInstanceOf[InitialSnapshot])
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)
      assert(log.getCapturedSnapshot().updateTimestamp == clock.getTimeMillis())
      clock.advance(500)
      log.update()
      assert(log.unsafeVolatileSnapshot.isInstanceOf[InitialSnapshot])
      assert(log.getCapturedSnapshot().updateTimestamp == clock.getTimeMillis())
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

  // This test has the following setup:
  // 1. Table is created with CS1 as commit-coordinator.
  // 2. Do another commit (v1) on table.
  // 3. Take a reference to current DeltaLog and clear the cache. This deltaLog object currently
  //    points to the latest table snapshot i.e. v1.
  // 4. Do commit v2 on the table.
  // 5. Do commit v3 on table. As part of this, change commit-coordinator to FS. Do v4 on table and
  //    change owner to CS2.
  // 6. Do commit v5 on table. This will happen via CS2.
  // 7. Invoke deltaLog.update() on the old deltaLog object which is still pointing to v1.
  //    - While doing this, we will inject failure in CS2 so that it fails twice when cs2.getCommits
  //      is called.
  //    - Because of this old delta log will firstly contact cs1 and get newer commits i.e. v2/v3
  //      and create a snapshot out of it. Then it will contact cs2 and fail. So deltaLog.update()
  //      won't succeed and throw exception. It also won't install the intermediate snapshot. The
  //      older delta log will still be pointing to v1.
  // 8. Invoke deltaLog.update() two more times. 3rd attempt will succeed.
  //    - the recorded timestamp for this should be clock timestamp.
  test("failures inside getCommits, correct timestamp is added in CapturedSnapshot") {
    val batchSize = 10
    val cs1 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize))
    val cs2 = new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize)) {
      var failAttempts = Set[Int]()

      override def getCommits(
          tableDesc: TableDescriptor,
          startVersion: java.lang.Long,
          endVersion: java.lang.Long): JGetCommitsResponse = {
        if (failAttempts.contains(numGetCommitsCalled.get + 1)) {
          numGetCommitsCalled.incrementAndGet()
          throw new IllegalStateException("Injected failure")
        }
        super.getCommits(tableDesc, startVersion, endVersion)
      }
    }
    case class TrackingInMemoryCommitCoordinatorClientBuilder(
        name: String,
        commitCoordinatorClient: CommitCoordinatorClient) extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession,
          conf: Map[String, String]): CommitCoordinatorClient = commitCoordinatorClient
      override def getName: String = name
    }
    val builder1 = TrackingInMemoryCommitCoordinatorClientBuilder(name = "in-memory-1", cs1)
    val builder2 = TrackingInMemoryCommitCoordinatorClientBuilder(name = "in-memory-2", cs2)
    Seq(builder1, builder2).foreach(CommitCoordinatorProvider.registerBuilder)

    def resetMetrics(): Unit = {
      cs1.reset()
      cs2.reset()
      cs2.failAttempts = Set()
    }

    withSQLConf(COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey -> "in-memory-1") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        // Step-1
        Seq(0).toDF.write.format("delta").mode("append").save(tablePath) // version 0
        DeltaLog.clearCache()

        // Step-2
        Seq(1).toDF.write.format("delta").mode("append").save(tablePath) // version 1

        // Step-3
        DeltaLog.clearCache()
        val clock = new ManualClock(System.currentTimeMillis())
        val oldDeltaLog = DeltaLog.forTable(spark, new Path(tablePath), clock)
        DeltaLog.clearCache()

        // Step-4
        Seq(2).toDF.write.format("delta").mode("append").save(tablePath) // version 2

        // Step-5
        val log = DeltaLog.forTable(spark, new Path(tablePath), clock)
        val oldMetadata = log.update().metadata
        assert(log.unsafeVolatileSnapshot.version === 2)
        val oldMetadataConf = log.update().metadata.configuration
        val newMetadata1 = oldMetadata.copy(
          configuration = oldMetadataConf - COORDINATED_COMMITS_COORDINATOR_NAME.key)
        val newMetadata2 = oldMetadata.copy(
          configuration = oldMetadataConf + (
            COORDINATED_COMMITS_COORDINATOR_NAME.key -> "in-memory-2"))
        assert(log.update().tableCommitCoordinatorClientOpt.get.commitCoordinatorClient === cs1)
        log.startTransaction().commitManually(newMetadata1) // version 3
        (1 to 3).foreach { v =>
          // backfill commit 1 and 2 also as 3/4 are written directly to FS.
          val segment = log.unsafeVolatileSnapshot.logSegment
          log.store.write(
            path = FileNames.unsafeDeltaFile(log.logPath, v),
            actions = log.store.read(segment.deltas(v).getPath).toIterator,
            overwrite = true)
        }
        assert(log.update().tableCommitCoordinatorClientOpt === None)
        log.startTransaction().commitManually(newMetadata2) // version 4
        assert(log.update().tableCommitCoordinatorClientOpt.get.commitCoordinatorClient === cs2)

        // Step-6
        Seq(4).toDF.write.format("delta").mode("append").save(tablePath) // version 5
        assert(log.unsafeVolatileSnapshot.version === 5L)
        assert(FileNames.deltaVersion(log.unsafeVolatileSnapshot.logSegment.deltas(5)) === 5)

        // Step-7
        // Invoke deltaLog.update() on older copy of deltaLog which is still pointing to version 1
        // Attempt-1
        assert(oldDeltaLog.unsafeVolatileSnapshot.version === 1)
        clock.setTime(System.currentTimeMillis())
        resetMetrics()
        cs2.failAttempts = Set(1, 2) // fail 0th and 1st attempt, 2nd attempt will succeed.
        val ex1 = intercept[CommitCoordinatorGetCommitsFailedException] { oldDeltaLog.update() }
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (1, 1))
        assert(ex1.getMessage.contains("Injected failure"))
        assert(oldDeltaLog.unsafeVolatileSnapshot.version == 1)
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp != clock.getTimeMillis())

        // Attempt-2
        // 2nd update also fails
        val ex2 = intercept[CommitCoordinatorGetCommitsFailedException] { oldDeltaLog.update() }
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (2, 2))
        assert(ex2.getMessage.contains("Injected failure"))
        assert(oldDeltaLog.unsafeVolatileSnapshot.version == 1)
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp != clock.getTimeMillis())

        // Attempt-3: 3rd update succeeds
        clock.advance(500)
        assert(oldDeltaLog.update().version === 5)
        assert((cs1.numGetCommitsCalled.get, cs2.numGetCommitsCalled.get) === (3, 3))
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp == clock.getTimeMillis())
      }
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

  testWithDefaultCommitCoordinatorUnset("DeltaLog.getSnapshotAt") {
    val commitCoordinatorClient =
      new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize = 10))
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10, Some(commitCoordinatorClient))
    CommitCoordinatorProvider.registerBuilder(builder)
    def checkGetSnapshotAt(
        deltaLog: DeltaLog,
        version: Long,
        expectedUpdateCount: Int,
        expectedListingCount: Int): Snapshot = {
      var snapshot: Snapshot = null

      val usageRecords = Log4jUsageLogger.track {
        snapshot = deltaLog.getSnapshotAt(version)
        assert(snapshot.version === version)
      }
      assert(filterUsageRecords(usageRecords, "deltaLog.update").size === expectedUpdateCount)
      // deltaLog.update() will internally do listing
      assert(filterUsageRecords(usageRecords, "delta.deltaLog.listDeltaAndCheckpointFiles").size
        === expectedListingCount)
      val versionsInLogSegment = if (version < 6) {
        snapshot.logSegment.deltas.map(FileNames.deltaVersion(_))
      } else {
        snapshot.logSegment.deltas.flatMap {
          case DeltaFile(_, deltaVersion) => Seq(deltaVersion)
          case CompactedDeltaFile(_, startVersion, endVersion) => (startVersion to endVersion)
        }
      }
      assert(versionsInLogSegment === (0L to version))
      snapshot
    }

    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      // Part-1: Validate getSnapshotAt API works as expected for non-coordinated commits tables
      // commit 0, 1, 2 on FS table
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v0
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v1
      val deltaLog1 = DeltaLog.forTable(spark, tablePath)
      DeltaLog.clearCache()
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v2
      assert(deltaLog1.unsafeVolatileSnapshot.version === 1)

      checkGetSnapshotAt(deltaLog1, version = 1, expectedUpdateCount = 0, expectedListingCount = 0)
      // deltaLog1 still points to version 1. So, we will do listing to get v0.
      checkGetSnapshotAt(deltaLog1, version = 0, expectedUpdateCount = 0, expectedListingCount = 1)
      // deltaLog1 still points to version 1 although we are asking for v2 So we do a
      // deltaLog.update - the update will internally do listing.Since the updated snapshot is same
      // as what we want, so we won't create another snapshot and do another listing.
      checkGetSnapshotAt(deltaLog1, version = 2, expectedUpdateCount = 1, expectedListingCount = 1)
      var deltaLog2 = DeltaLog.forTable(spark, tablePath)
      Seq(deltaLog1, deltaLog2).foreach { log => assert(log.unsafeVolatileSnapshot.version === 2) }
      DeltaLog.clearCache()

      // Part-2: Validate getSnapshotAt API works as expected for coordinated commits tables when
      // the switch is made
      // commit 3
      enableCoordinatedCommits(DeltaLog.forTable(spark, tablePath), "tracking-in-memory")
      // commit 4
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath)
      // the old deltaLog objects still points to version 2
      Seq(deltaLog1, deltaLog2).foreach { log => assert(log.unsafeVolatileSnapshot.version === 2) }
      // deltaLog1 points to version 2. So, we will do listing to get v1. Snapshot update not
      // needed as what we are looking for is less than what deltaLog1 points to.
      checkGetSnapshotAt(deltaLog1, version = 1, expectedUpdateCount = 0, expectedListingCount = 1)
      // deltaLog1.unsafeVolatileSnapshot.version points to v2 - return it directly.
      checkGetSnapshotAt(deltaLog1, version = 2, expectedUpdateCount = 0, expectedListingCount = 0)
      // We are asking for v3 although the deltaLog1.unsafeVolatileSnapshot is for v2. So this will
      // need deltaLog.update() to get the latest snapshot first - this update itself internally
      // will do 2 round of listing as we are discovering a commit-coordinator after first round of
      // listing. Once the update finishes, deltaLog1 will point to v4. So we need another round of
      // listing to get just v3.
      checkGetSnapshotAt(deltaLog1, version = 3, expectedUpdateCount = 1, expectedListingCount = 3)
      // Ask for v3 again - this time deltaLog1.unsafeVolatileSnapshot points to v4.
      // So we don't need deltaLog.update as version which we are asking is less than pinned
      // version. Just do listing and get the snapshot.
      checkGetSnapshotAt(deltaLog1, version = 3, expectedUpdateCount = 0, expectedListingCount = 1)
      // deltaLog1.unsafeVolatileSnapshot.version points to v4 - return it directly.
      checkGetSnapshotAt(deltaLog1, version = 4, expectedUpdateCount = 0, expectedListingCount = 0)
      // We are asking for v3 although the deltaLog2.unsafeVolatileSnapshot is for v2. So this will
      // need deltaLog.update() to get the latest snapshot first - this update itself internally
      // will do 2 round of listing as we are discovering a commit-coordinator after first round of
      // listing. Once the update finishes, deltaLog2 will point to v4. It can be returned directly.
      checkGetSnapshotAt(deltaLog2, version = 4, expectedUpdateCount = 1, expectedListingCount = 2)

      // Part-2: Validate getSnapshotAt API works as expected for coordinated commits tables
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v5
      deltaLog2 = DeltaLog.forTable(spark, tablePath)
      DeltaLog.clearCache()
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v6
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v7
      assert(deltaLog2.unsafeVolatileSnapshot.version === 5)
      checkGetSnapshotAt(deltaLog2, version = 1, expectedUpdateCount = 0, expectedListingCount = 1)
      checkGetSnapshotAt(deltaLog2, version = 2, expectedUpdateCount = 0, expectedListingCount = 1)
      checkGetSnapshotAt(deltaLog2, version = 4, expectedUpdateCount = 0, expectedListingCount = 1)
      checkGetSnapshotAt(deltaLog2, version = 5, expectedUpdateCount = 0, expectedListingCount = 0)
      checkGetSnapshotAt(deltaLog2, version = 6, expectedUpdateCount = 1, expectedListingCount = 2)
    }
  }

  private def enableCoordinatedCommits(deltaLog: DeltaLog, commitCoordinator: String): Unit = {
    val oldMetadata = deltaLog.update().metadata
    val commitCoordinatorConf = (COORDINATED_COMMITS_COORDINATOR_NAME.key -> commitCoordinator)
    val newMetadata =
      oldMetadata.copy(configuration = oldMetadata.configuration + commitCoordinatorConf)
    deltaLog.startTransaction().commitManually(newMetadata)
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
    CommitCoordinatorProvider.registerBuilder(builder)
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

  for (upgradeExistingTable <- BOOLEAN_DOMAIN)
  testWithDifferentBackfillInterval("upgrade + downgrade [FS -> CC1 -> FS -> CC2]," +
      s" upgradeExistingTable = $upgradeExistingTable") { backfillInterval =>
    withoutCoordinatedCommitsDefaultTableProperties {
      CommitCoordinatorProvider.clearNonDefaultBuilders()
      val builder1 = TrackingInMemoryCommitCoordinatorBuilder(batchSize = backfillInterval)
      val builder2 = new TrackingInMemoryCommitCoordinatorBuilder(batchSize = backfillInterval) {
        override def getName: String = "tracking-in-memory-2"
      }

      Seq(builder1, builder2).foreach(CommitCoordinatorProvider.registerBuilder(_))
      val cs1 = builder1
        .trackingInMemoryCommitCoordinatorClient
        .asInstanceOf[TrackingCommitCoordinatorClient]
      val cs2 = builder2
        .trackingInMemoryCommitCoordinatorClient
        .asInstanceOf[TrackingCommitCoordinatorClient]

      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val log = DeltaLog.forTable(spark, tablePath)
        val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())

        var upgradeStartVersion = 0L
        // Create a non-coordinated commits table if we are testing upgrade for existing tables
        if (upgradeExistingTable) {
          log.startTransaction().commitManually(Metadata())
          assert(log.unsafeVolatileSnapshot.version === 0)
          log.startTransaction().commitManually(createTestAddFile("1"))
          assert(log.unsafeVolatileSnapshot.version === 1)
          assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)
          upgradeStartVersion = 2L
        }

        // Upgrade the table
        // [upgradeExistingTable = false] Commit-0
        // [upgradeExistingTable = true] Commit-2
        val commitCoordinatorConf =
          Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder1.getName)
        val newMetadata = Metadata().copy(configuration = commitCoordinatorConf)
        val usageLogs1 = Log4jUsageLogger.track {
          log.startTransaction().commitManually(newMetadata)
        }
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorName ===
          Some(builder1.getName))
        assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)
        // upgrade commit always filesystem based
        assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, upgradeStartVersion)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled.get) == Seq(0, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled.get) == Seq(1, 0))
        // Check usage logs for upgrade commit
        val commitStatsUsageLogs1 = filterUsageRecords(usageLogs1, "delta.commit.stats")
        val commitStats1 = JsonUtils.fromJson[CommitStats](commitStatsUsageLogs1.head.blob)
        assert(commitStats1.coordinatedCommitsInfo ===
          CoordinatedCommitsStats(FS_TO_CC_UPGRADE_COMMIT.toString, builder1.getName, Map.empty))

        // Do couple of commits on the coordinated-commits table
        // [upgradeExistingTable = false] Commit-1/2
        // [upgradeExistingTable = true] Commit-3/4
        (1 to 2).foreach { versionOffset =>
          val version = upgradeStartVersion + versionOffset
          val usageLogs2 = Log4jUsageLogger.track {
            log.startTransaction().commitManually(createTestAddFile(s"$versionOffset"))
          }
          assert(log.unsafeVolatileSnapshot.version === version)
          assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)
          assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorName.nonEmpty)
          assert(
            log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorConf === Map.empty)
          assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)
          assert(cs1.numCommitsCalled.get === versionOffset)
          val backfillExpected = if (version % backfillInterval == 0) true else false
          assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, version)) == backfillExpected)
          // Check usage logs for INSERT commits on this coordinated-commits table.
          val commitStatsUsageLogs2 = filterUsageRecords(usageLogs2, "delta.commit.stats")
          val commitStats2 = JsonUtils.fromJson[CommitStats](commitStatsUsageLogs2.head.blob)
          assert(commitStats2.coordinatedCommitsInfo ===
            CoordinatedCommitsStats(CC_COMMIT.toString, builder1.getName, Map.empty))
        }

        // Downgrade the table
        // [upgradeExistingTable = false] Commit-3
        // [upgradeExistingTable = true] Commit-5
        val commitCoordinatorConfKeys = Seq(
          COORDINATED_COMMITS_COORDINATOR_NAME.key,
          COORDINATED_COMMITS_COORDINATOR_CONF.key,
          COORDINATED_COMMITS_TABLE_CONF.key
        )
        val newConfig = log.snapshot.metadata.configuration
          .filterKeys(!commitCoordinatorConfKeys.contains(_)) ++ Map("downgraded_at" -> "v2")
        val newMetadata2 = log.snapshot.metadata.copy(configuration = newConfig.toMap)
        val usageLogs3 = Log4jUsageLogger.track {
          log.startTransaction().commitManually(newMetadata2)
        }
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 3)
        assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorName.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata === newMetadata2)
        // This must have increased by 1 as downgrade commit happens via CommitCoordinatorClient.
        assert(Seq(cs1, cs2).map(_.numCommitsCalled.get) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled.get) == Seq(1, 0))
        (0 to 3).foreach { version =>
          assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, version)))
        }
        // Check usage logs for downgrade commit
        val commitStatsUsageLogs3 = filterUsageRecords(usageLogs3, "delta.commit.stats")
        val commitStats3 = JsonUtils.fromJson[CommitStats](commitStatsUsageLogs3.head.blob)
        assert(commitStats3.coordinatedCommitsInfo ===
          CoordinatedCommitsStats(CC_TO_FS_DOWNGRADE_COMMIT.toString, builder1.getName, Map.empty))


        // Do commit after downgrade is over
        // [upgradeExistingTable = false] Commit-4
        // [upgradeExistingTable = true] Commit-6
        val usageLogs4 = Log4jUsageLogger.track {
          log.startTransaction().commitManually(createTestAddFile("post-upgrade-file"))
        }
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 4)
        // no commit-coordinator after downgrade
        assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)
        // Metadata is same as what we added at time of downgrade
        assert(log.unsafeVolatileSnapshot.metadata === newMetadata2)
        // State reconstruction should give correct results
        var expectedFileNames = Set("1", "2", "post-upgrade-file")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        // commit-coordinator should not be invoked for commit API.
        // Register table API should not be called until the end
        assert(Seq(cs1, cs2).map(_.numCommitsCalled.get) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled.get) == Seq(1, 0))
        // 4th file is directly written to FS in backfilled way.
        assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, upgradeStartVersion + 4)))
        // Check usage logs for normal FS commit
        val commitStatsUsageLogs4 = filterUsageRecords(usageLogs4, "delta.commit.stats")
        val commitStats4 = JsonUtils.fromJson[CommitStats](commitStatsUsageLogs4.head.blob)
        assert(commitStats4.coordinatedCommitsInfo ===
          CoordinatedCommitsStats(FS_COMMIT.toString, "", Map.empty))

        // Now transfer the table to another commit-coordinator
        // [upgradeExistingTable = false] Commit-5
        // [upgradeExistingTable = true] Commit-7
        val commitCoordinatorConf2 =
          Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder2.getName)
        val oldMetadata3 = log.unsafeVolatileSnapshot.metadata
        val newMetadata3 = oldMetadata3.copy(
          configuration = oldMetadata3.configuration ++ commitCoordinatorConf2)
        val usageLogs5 = Log4jUsageLogger.track {
          log.startTransaction().commitManually(newMetadata3, createTestAddFile("upgrade-2-file"))
        }
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 5)
        assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorName ===
          Some(builder2.getName))
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)
        expectedFileNames = Set("1", "2", "post-upgrade-file", "upgrade-2-file")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled.get) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled.get) == Seq(1, 1))
        // Check usage logs for 2nd upgrade commit
        val commitStatsUsageLogs5 = filterUsageRecords(usageLogs5, "delta.commit.stats")
        val commitStats5 = JsonUtils.fromJson[CommitStats](commitStatsUsageLogs5.head.blob)
        assert(commitStats5.coordinatedCommitsInfo ===
          CoordinatedCommitsStats(FS_TO_CC_UPGRADE_COMMIT.toString, builder2.getName, Map.empty))

        // Make 1 more commit, this should go to new owner
        log.startTransaction().commitManually(newMetadata3, createTestAddFile("4"))
        expectedFileNames = Set("1", "2", "post-upgrade-file", "upgrade-2-file", "4")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled.get) == Seq(3, 1))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled.get) == Seq(1, 1))
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 6)
      }
    }
  }

  test("transfer from one commit-coordinator to another commit-coordinator fails " +
    "[CC-1 -> CC-2 fails]") {
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    val builder1 = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)
    val builder2 = new TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10) {
      override def getName: String = "tracking-in-memory-2"
    }
    Seq(builder1, builder2).foreach(CommitCoordinatorProvider.registerBuilder(_))

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

  testWithDefaultCommitCoordinatorUnset("FS -> CC upgrade is not retried on a conflict") {
    val builder = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)
    CommitCoordinatorProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      val commitCoordinatorConf = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder.getName)
      val newMetadata = Metadata().copy(configuration = commitCoordinatorConf)
      val txn = log.startTransaction() // upgrade txn started
      log.startTransaction().commitManually(createTestAddFile("f1"))
      intercept[io.delta.exceptions.ConcurrentWriteException] {
        txn.commitManually(newMetadata) // upgrade txn committed
      }
    }
  }

  testWithDefaultCommitCoordinatorUnset("FS -> CC upgrade with commitLarge API") {
    val builder = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)
    val cs =
      builder.trackingInMemoryCommitCoordinatorClient.asInstanceOf[TrackingCommitCoordinatorClient]
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").save(tablePath)
      Seq(1).toDF.write.mode("overwrite").format("delta").save(tablePath)
      var log = DeltaLog.forTable(spark, tablePath)
      assert(log.unsafeVolatileSnapshot.version === 1L)
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.isEmpty)

      val commitCoordinatorConf = Map(COORDINATED_COMMITS_COORDINATOR_NAME.key -> builder.getName)
      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val newMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration ++ commitCoordinatorConf)
      val oldProtocol = log.unsafeVolatileSnapshot.protocol
      assert(!oldProtocol.readerAndWriterFeatures.contains(V2CheckpointTableFeature))
      val newProtocol =
        oldProtocol.copy(
          minReaderVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures =
            Some(oldProtocol.readerFeatures.getOrElse(Set.empty) + V2CheckpointTableFeature.name),
          writerFeatures =
            Some(
              oldProtocol.writerFeatures.getOrElse(Set.empty) + CoordinatedCommitsTableFeature.name)
            )
      assert(cs.numRegisterTableCalled.get === 0)
      assert(cs.numCommitsCalled.get === 0)

      val txn = log.startTransaction()
      txn.updateMetadataForNewTable(newMetadata)
      txn.commitLarge(
        spark,
        Seq(SetTransaction("app-1", 1, None)).toIterator,
        Some(newProtocol),
        DeltaOperations.TestOperation("TEST"),
        Map.empty,
        Map.empty)
      log = DeltaLog.forTable(spark, tablePath)
      assert(cs.numRegisterTableCalled.get === 1)
      assert(cs.numCommitsCalled.get === 0)
      assert(log.unsafeVolatileSnapshot.version === 2L)

      Seq(V2CheckpointTableFeature, CoordinatedCommitsTableFeature).foreach { feature =>
        assert(log.unsafeVolatileSnapshot.protocol.isFeatureSupported(feature))
      }

      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)
      assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorName ===
        Some(builder.getName))
      assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsCoordinatorConf === Map.empty)
      assert(log.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf === Map.empty)

      Seq(3).toDF.write.mode("append").format("delta").save(tablePath)
      assert(cs.numRegisterTableCalled.get === 1)
      assert(cs.numCommitsCalled.get === 1)
      assert(log.unsafeVolatileSnapshot.version === 3L)
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)

    }
  }

  test("Incomplete backfills are handled properly by next commit after CC to FS conversion") {
    val batchSize = 10
    val neverBackfillingCommitCoordinator =
      new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(batchSize) {
        override def backfillToVersion(
          logStore: LogStore,
          hadoopConf: Configuration,
          tableDesc: TableDescriptor,
          version: Long,
          lastKnownBackfilledVersionOpt: JLong): Unit = { }
      })
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(batchSize, Some(neverBackfillingCommitCoordinator))
    CommitCoordinatorProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v0
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v1
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v2

      val log = DeltaLog.forTable(spark, tablePath)
      assert(log.unsafeVolatileSnapshot.tableCommitCoordinatorClientOpt.nonEmpty)
      assert(log.unsafeVolatileSnapshot.version === 2)
      assert(
        log.unsafeVolatileSnapshot.logSegment.deltas.count(FileNames.isUnbackfilledDeltaFile) == 2)

      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val downgradeMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration - COORDINATED_COMMITS_COORDINATOR_NAME.key)
      log.startTransaction().commitManually(downgradeMetadata)
      log.update()
      val snapshotAfterDowngrade = log.unsafeVolatileSnapshot
      assert(snapshotAfterDowngrade.version === 3)
      assert(snapshotAfterDowngrade.tableCommitCoordinatorClientOpt.isEmpty)
      assert(snapshotAfterDowngrade.logSegment.deltas.count(FileNames.isUnbackfilledDeltaFile) == 3)

      val records = Log4jUsageLogger.track {
        // commit 4
        Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath)
      }
      val filteredUsageLogs = filterUsageRecords(
        records, "delta.coordinatedCommits.backfillWhenCoordinatedCommitsSupportedAndDisabled")
      assert(filteredUsageLogs.size === 1)
      val usageObj = JsonUtils.fromJson[Map[String, Any]](filteredUsageLogs.head.blob)
      assert(usageObj("numUnbackfilledFiles").asInstanceOf[Int] === 3)
      assert(usageObj("numAlreadyBackfilledFiles").asInstanceOf[Int] === 0)
    }
  }

  test("LogSegment comparison does not swap snapshots that only differ in " +
    "backfilled/unbackfilled commits") {
    // Use a batch size of two so we don't immediately backfill in
    // the AbstractBatchBackfillingCommitCoordinatorClient and so the
    // CommitResponse contains the UUID-based commit.
    CommitCoordinatorProvider.registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)

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

  for (ignoreMissingCCImpl <- BOOLEAN_DOMAIN)
  test(s"missing coordinator implementation [ignoreMissingCCImpl = $ignoreMissingCCImpl]") {
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    CommitCoordinatorProvider.registerBuilder(
      TrackingInMemoryCommitCoordinatorBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(0).toDF.write.format("delta").save(tablePath)
      (1 to 3).foreach { v =>
        Seq(v).toDF.write.mode("append").format("delta").save(tablePath)
      }
      // The table has 3 backfilled commits [0, 1, 2] and 1 unbackfilled commit [3]
      CommitCoordinatorProvider.clearNonDefaultBuilders()

      def getUsageLogsAndEnsurePresenceOfMissingCCImplLog(
          expectedFailIfImplUnavailable: Boolean)(f: => Unit): Seq[UsageRecord] = {
        val usageLogs = Log4jUsageLogger.track {
          f
        }
        val filteredLogs = filterUsageRecords(
          usageLogs, CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_MISSING_IMPLEMENTATION)
        assert(filteredLogs.nonEmpty)
        val usageObj = JsonUtils.fromJson[Map[String, Any]](filteredLogs.head.blob)
        assert(usageObj("commitCoordinatorName") === "tracking-in-memory")
        assert(usageObj("registeredCommitCoordinators") ===
          CommitCoordinatorProvider.getRegisteredCoordinatorNames.mkString(", "))
        assert(usageObj("failIfImplUnavailable") === expectedFailIfImplUnavailable.toString)
        usageLogs
      }
      withSQLConf(
        DeltaSQLConf.COORDINATED_COMMITS_IGNORE_MISSING_COORDINATOR_IMPLEMENTATION.key ->
          ignoreMissingCCImpl.toString) {
        DeltaLog.clearCache()
        if (!ignoreMissingCCImpl) {
          getUsageLogsAndEnsurePresenceOfMissingCCImplLog(expectedFailIfImplUnavailable = true) {
            val e = intercept[IllegalArgumentException] {
              DeltaLog.forTable(spark, tablePath)
            }
            assert(e.getMessage.contains("Unknown commit-coordinator"))
          }
        } else {
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          assert(deltaLog.snapshot.tableCommitCoordinatorClientOpt.isEmpty)
          // This will create a stale deltaLog as the commit-coordinator is missing.
          assert(deltaLog.snapshot.version === 2L)
          DeltaLog.clearCache()
          getUsageLogsAndEnsurePresenceOfMissingCCImplLog(expectedFailIfImplUnavailable = false) {
            checkAnswer(spark.read.format("delta").load(tablePath), Seq(0, 1, 2).toDF())
          }
          // Writes and checkpoints should still fail.
          val createCheckpointFn = () => (deltaLog.checkpoint())
          val writeDataFn =
            () => Seq(4).toDF.write.format("delta").mode("append").save(tablePath)
          for (tableMutationFn <- Seq(createCheckpointFn, writeDataFn)) {
            DeltaLog.clearCache()
            val usageLogs = Log4jUsageLogger.track {
              val e = intercept[DeltaUnsupportedOperationException] {
                tableMutationFn()
              }
              checkError(e,
                errorClass = "DELTA_UNSUPPORTED_WRITES_WITHOUT_COORDINATOR",
                sqlState = "0AKDC",
                parameters = Map("coordinatorName" -> "tracking-in-memory")
              )
              assert(e.getMessage.contains(
                "no implementation of this coordinator is available in the current environment"))
            }
            val filteredLogs = filterUsageRecords(
              usageLogs,
              CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_MISSING_IMPLEMENTATION_WRITE)
            val usageObj = JsonUtils.fromJson[Map[String, Any]](filteredLogs.head.blob)
            assert(usageObj("commitCoordinatorName") === "tracking-in-memory")
            assert(usageObj("readVersion") === "2")
          }
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  //           Test coordinated-commits with DeltaLog.getChangeLogFile API starts                //
  /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Helper method which generates a delta table with `totalCommits`.
   * The `upgradeToCoordinatedCommitsVersion`th commit version upgrades this table to coordinated
   * commits and it uses `backfillInterval` for backfilling.
   * This method returns a mapping of version to DeltaLog for the versions in
   * `requiredDeltaLogVersions`. Each of this deltaLog object has a Snapshot as per what is
   * mentioned in the `requiredDeltaLogVersions`.
   */
  private def generateDataForGetChangeLogFilesTest(
      dir: File,
      totalCommits: Int,
      upgradeToCoordinatedCommitsVersion: Int,
      backfillInterval: Int,
      requiredDeltaLogVersions: Set[Int]): Map[Int, DeltaLog] = {
    val commitCoordinatorClient =
      new TrackingCommitCoordinatorClient(new InMemoryCommitCoordinator(backfillInterval))
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(backfillInterval, Some(commitCoordinatorClient))
    CommitCoordinatorProvider.registerBuilder(builder)
    val versionToDeltaLogMapping = collection.mutable.Map.empty[Int, DeltaLog]
    withSQLConf(
      CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      val tablePath = dir.getAbsolutePath

      (0 to totalCommits).foreach { v =>
        if (v === upgradeToCoordinatedCommitsVersion) {
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          val oldMetadata = deltaLog.unsafeVolatileSnapshot.metadata
          val commitCoordinator = (COORDINATED_COMMITS_COORDINATOR_NAME.key -> "tracking-in-memory")
          val newMetadata =
            oldMetadata.copy(configuration = oldMetadata.configuration + commitCoordinator)
          deltaLog.startTransaction().commitManually(newMetadata)
        } else {
          Seq(v).toDF().write.format("delta").mode("append").save(tablePath)
        }
        if (requiredDeltaLogVersions.contains(v)) {
          versionToDeltaLogMapping.put(v, DeltaLog.forTable(spark, tablePath))
          DeltaLog.clearCache()
        }
      }
    }
    versionToDeltaLogMapping.toMap
  }

  def runGetChangeLogFiles(
      deltaLog: DeltaLog,
      startVersion: Long,
      endVersionOpt: Option[Long] = None,
      totalCommitsOnTable: Long = 8,
      expectedLastBackfilledCommit: Long,
      updateExpected: Boolean): Unit = {
    val usageRecords = Log4jUsageLogger.track {
      val iter = endVersionOpt match {
        case Some(endVersion) =>
          deltaLog.getChangeLogFiles(startVersion, endVersion, failOnDataLoss = false)
        case None =>
          deltaLog.getChangeLogFiles(startVersion)
      }
      val paths = iter.map(_._2.getPath).toIndexedSeq
      val (backfilled, unbackfilled) = paths.partition(_.getParent === deltaLog.logPath)
      val expectedBackfilledCommits = startVersion to expectedLastBackfilledCommit
      val expectedUnbackfilledCommits = {
        val firstUnbackfilledVersion = (expectedLastBackfilledCommit + 1).max(startVersion)
        val expectedEndVersion = endVersionOpt.getOrElse(totalCommitsOnTable)
        firstUnbackfilledVersion to expectedEndVersion
      }
      assert(backfilled.map(FileNames.deltaVersion) === expectedBackfilledCommits)
      assert(unbackfilled.map(FileNames.deltaVersion) === expectedUnbackfilledCommits)
    }
    val updateCountEvents = if (updateExpected) 1 else 0
    assert(filterUsageRecords(usageRecords, "delta.log.update").size === updateCountEvents)
  }

  testWithDefaultCommitCoordinatorUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [No Coordinated Commits]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 4,
        upgradeToCoordinatedCommitsVersion = -1,
        backfillInterval = -1,
        requiredDeltaLogVersions = Set(2, 4))

      // We are asking for changes between 0 and 0 to a DeltaLog(unsafeVolatileSnapshot = 2).
      // So we should not need an update() as all the required files are on filesystem.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        endVersionOpt = Some(0),
        expectedLastBackfilledCommit = 0,
        updateExpected = false)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 2).
      // Since the commits in filesystem are more than what unsafeVolatileSnapshot has, we should
      // need an update() to get the latest snapshot and see if coordinated commits was enabled on
      // the table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 4).
      // The latest commit from filesystem listing is 4 -- same as unsafeVolatileSnapshot and this
      // unsafeVolatileSnapshot doesn't have coordinated commits enabled. So we should not need an
      // update().
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = false)
    }
  }

  testWithDefaultCommitCoordinatorUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [Coordinated Commits backfill size 1]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 4,
        upgradeToCoordinatedCommitsVersion = 2,
        backfillInterval = 1,
        requiredDeltaLogVersions = Set(0, 2, 4))

      // We are asking for changes between 0 and 0 to a DeltaLog(unsafeVolatileSnapshot = 2).
      // So we should not need an update() as all the required files are on filesystem.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        endVersionOpt = Some(0),
        expectedLastBackfilledCommit = 0,
        updateExpected = false)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 2).
      // Since the commits in filesystem are more than what unsafeVolatileSnapshot has, we should
      // need an update() to get the latest snapshot and see if coordinated commits was enabled on
      // the table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)

      // We are asking for changes between 0 to 4 to a DeltaLog(unsafeVolatileSnapshot = 4).
      // Since the commits in filesystem are between 0 to 4, so we don't need to update() to get
      // the latest snapshot and see if coordinated commits was enabled on the table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        endVersionOpt = Some(4),
        expectedLastBackfilledCommit = 4,
        updateExpected = false)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 4).
      // The latest commit from filesystem listing is 4 -- same as unsafeVolatileSnapshot and this
      // unsafeVolatileSnapshot has coordinated commits enabled. So we should need an update() to
      // find out latest commits from Commit Coordinator.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)
    }
  }

  testWithDefaultCommitCoordinatorUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [Coordinated Commits backfill size 10]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 8,
        upgradeToCoordinatedCommitsVersion = 2,
        backfillInterval = 10,
        requiredDeltaLogVersions = Set(2, 3, 4, 8))

      // We are asking for changes between 0 and 1 to a DeltaLog(unsafeVolatileSnapshot = 2/4).
      // So we should not need an update() as all the required files are on filesystem.
      Seq(2, 3, 4).foreach { version =>
        runGetChangeLogFiles(
          versionsToDeltaLogMapping(version),
          totalCommitsOnTable = 8,
          startVersion = 0,
          endVersionOpt = Some(1),
          expectedLastBackfilledCommit = 1,
          updateExpected = false)
      }

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 2/4).
      // Since the unsafeVolatileSnapshot has coordinated-commits enabled, so we need to trigger an
      // update to find the latest commits from Commit Coordinator.
      Seq(2, 3, 4).foreach { version =>
        runGetChangeLogFiles(
          versionsToDeltaLogMapping(version),
          totalCommitsOnTable = 8,
          startVersion = 0,
          expectedLastBackfilledCommit = 2,
          updateExpected = true)
      }

      // We are asking for changes between 0 to `4` to a DeltaLog(unsafeVolatileSnapshot = 8).
      // The filesystem has only commit 0/1/2.
      // After that we need to rely on deltaLog.update() to get the latest snapshot and return the
      // files until 8.
      // Ideally the unsafeVolatileSnapshot should have the info to generate files from 0 to 4 and
      // an update() should not be needed. This is an optimization that can be done in future.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(8),
        totalCommitsOnTable = 8,
        startVersion = 0,
        endVersionOpt = Some(4),
        expectedLastBackfilledCommit = 2,
        updateExpected = true)
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  //           Test coordinated-commits with DeltaLog.getChangeLogFile API ENDS                  //
  /////////////////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////////////////
  //     Test CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl STARTS    //
  /////////////////////////////////////////////////////////////////////////////////////////////

  def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  private val cNameKey = COORDINATED_COMMITS_COORDINATOR_NAME.key
  private val cConfKey = COORDINATED_COMMITS_COORDINATOR_CONF.key
  private val tableConfKey = COORDINATED_COMMITS_TABLE_CONF.key
  private val cName = cNameKey -> "some-cc-name"
  private val cConf = cConfKey -> "some-cc-conf"
  private val tableConf = tableConfKey -> "some-table-conf"

  private val cNameDefaultKey = COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey
  private val cConfDefaultKey = COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey
  private val tableConfDefaultKey = COORDINATED_COMMITS_TABLE_CONF.defaultTablePropertyKey
  private val cNameDefault = cNameDefaultKey -> "some-cc-name"
  private val cConfDefault = cConfDefaultKey -> "some-cc-conf"
  private val tableConfDefault = tableConfDefaultKey -> "some-table-conf"

  private val command = "CLONE"

  private val errCannotOverride = new DeltaIllegalArgumentException(
    "DELTA_CANNOT_OVERRIDE_COORDINATED_COMMITS_CONFS", Array(command))

  private def errMissingConfInCommand(key: String) = new DeltaIllegalArgumentException(
      "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_COMMAND", Array(command, key))

  private def errMissingConfInSession(key: String) = new DeltaIllegalArgumentException(
    "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_SESSION", Array(command, key))

  private def errTableConfInCommand = new DeltaIllegalArgumentException(
    "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_COMMAND", Array(command, tableConfKey))

  private def errTableConfInSession = new DeltaIllegalArgumentException(
    "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_SESSION",
    Array(command, tableConfDefaultKey, tableConfDefaultKey))

  private def testValidation(
      tableExists: Boolean,
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]): Unit = {
    withoutCoordinatedCommitsDefaultTableProperties {
      withSQLConf(defaultConfs: _*) {
        if (errorOpt.isDefined) {
          val e = intercept[DeltaIllegalArgumentException] {
            CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl(
              spark, propertyOverrides, tableExists, command)
          }
          assert(e.getMessage.contains(errorOpt.get.getMessage))
        } else {
          CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl(
            spark, propertyOverrides, tableExists, command)
        }
      }
    }
  }

  // tableExists: True
  //            | False
  //
  // propertyOverrides: Map.empty
  //                  | Map(cName)
  //                  | Map(cName, cConf)
  //                  | Map(cName, cConf, tableConf)
  //                  | Map(tableConf)
  //
  // defaultConf: Seq.empty
  //            | Seq(cNameDefault)
  //            | Seq(cNameDefault, cConfDefault)
  //            | Seq(cNameDefault, cConfDefault, tableConfDefault)
  //            | Seq(tableConfDefault)
  //
  // errorOpt: None
  //         | Some(errCannotOverride)
  //         | Some(errMissingConfInCommand(cConfKey))
  //         | Some(errMissingConfInSession(cConfKey))
  //         | Some(errTableConfInCommand)
  //         | Some(errTableConfInSession)

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "passes for existing target tables with no explicit Coordinated Commits Configurations.") (
    Seq(
      Seq.empty,
      // Not having any explicit Coordinated Commits configurations, but having an illegal
      // combination of Coordinated Commits configurations in default: pass.
      // This is because we don't consider default configurations when the table exists.
      Seq(cNameDefault),
      Seq(cNameDefault, cConfDefault),
      Seq(cNameDefault, cConfDefault, tableConfDefault),
      Seq(tableConfDefault)
    )
  ) { defaultConfs: Seq[(String, String)] =>
    testValidation(
      tableExists = true,
      propertyOverrides = Map.empty,
      defaultConfs,
      errorOpt = None)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "fails for existing target tables with any explicit Coordinated Commits Configurations.") (
    Seq(
      (Map(cName), Seq.empty),
      (Map(cName), Seq(cNameDefault)),
      (Map(cName), Seq(cNameDefault, cConfDefault)),
      (Map(cName), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName), Seq(tableConfDefault)),

      (Map(cName, cConf), Seq.empty),
      (Map(cName, cConf), Seq(cNameDefault)),
      (Map(cName, cConf), Seq(cNameDefault, cConfDefault)),
      (Map(cName, cConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName, cConf), Seq(tableConfDefault)),

      (Map(cName, cConf, tableConf), Seq.empty),
      (Map(cName, cConf, tableConf), Seq(cNameDefault)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName, cConf, tableConf), Seq(tableConfDefault)),

      (Map(tableConf), Seq.empty),
      (Map(tableConf), Seq(cNameDefault)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(tableConf), Seq(tableConfDefault))
    )
  ) { case (
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)]) =>
    testValidation(
      tableExists = true,
      propertyOverrides,
      defaultConfs,
      errorOpt = Some(errCannotOverride))
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "works correctly for new target tables with default Coordinated Commits Configurations.") (
    Seq(
      (Seq.empty, None),
      (Seq(cNameDefault), Some(errMissingConfInSession(cConfDefaultKey))),
      (Seq(cNameDefault, cConfDefault), None),
      (Seq(cNameDefault, cConfDefault, tableConfDefault), Some(errTableConfInSession)),
      (Seq(tableConfDefault), Some(errTableConfInSession))
    )
  ) { case (
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]) =>
    testValidation(
      tableExists = false,
      propertyOverrides = Map.empty,
      defaultConfs,
      errorOpt)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "fails for new target tables with any illegal explicit Coordinated Commits Configurations.") (
    Seq(
      (Map(cName), Seq.empty, Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault), Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault, cConfDefault), Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(tableConfDefault), Some(errMissingConfInCommand(cConfKey))),

      (Map(cName, cConf, tableConf), Seq.empty, Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault), Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault), Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(tableConfDefault), Some(errTableConfInCommand)),

      (Map(tableConf), Seq.empty, Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault), Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault), Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errTableConfInCommand)),
      (Map(tableConf), Seq(tableConfDefault), Some(errTableConfInCommand))
    )
  ) { case (
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]) =>
    testValidation(
      tableExists = false,
      propertyOverrides,
      defaultConfs,
      errorOpt)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "passes for new target tables with legal explicit Coordinated Commits Configurations.") (
    Seq(
      // Having exactly Coordinator Name and Coordinator Conf explicitly, but having an illegal
      // combination of Coordinated Commits configurations in default: pass.
      // This is because we don't consider default configurations when explicit ones are provided.
      Seq.empty,
      Seq(cNameDefault),
      Seq(cNameDefault, cConfDefault),
      Seq(cNameDefault, cConfDefault, tableConfDefault),
      Seq(tableConfDefault)
    )
  ) { defaultConfs: Seq[(String, String)] =>
    testValidation(
      tableExists = false,
      propertyOverrides = Map(cName, cConf),
      defaultConfs,
      errorOpt = None)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  //      Test CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl ENDS     //
  /////////////////////////////////////////////////////////////////////////////////////////////
}
