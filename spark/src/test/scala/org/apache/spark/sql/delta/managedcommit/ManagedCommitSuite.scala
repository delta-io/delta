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

package org.apache.spark.sql.delta.managedcommit

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.{DeltaOperations, ManagedCommitTableFeature, V2CheckpointTableFeature}
import org.apache.spark.sql.delta.CommitStoreGetCommitsFailedException
import org.apache.spark.sql.delta.DeltaConfigs.{CHECKPOINT_INTERVAL, MANAGED_COMMIT_OWNER_CONF, MANAGED_COMMIT_OWNER_NAME}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.InitialSnapshot
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.FileNames.{CompactedDeltaFile, DeltaFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class ManagedCommitSuite
    extends QueryTest
    with DeltaSQLTestUtils
    with SharedSparkSession
    with DeltaSQLCommandTest
    with ManagedCommitTestUtils {

  import testImplicits._

  override def sparkConf: SparkConf = {
    // Make sure all new tables in tests use tracking-in-memory commit store by default.
    super.sparkConf
      .set(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey, "tracking-in-memory")
      .set(MANAGED_COMMIT_OWNER_CONF.defaultTablePropertyKey, JsonUtils.toJson(Map()))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitStoreProvider.clearNonDefaultBuilders()
  }

  test("0th commit happens via filesystem") {
    val commitStoreName = "nobackfilling-commit-store"
    object NoBackfillingCommitStoreBuilder extends CommitStoreBuilder {

      override def name: String = commitStoreName

      override def build(conf: Map[String, String]): CommitStore =
        new InMemoryCommitStore(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              logPath: Path,
              managedCommitTableConf: Map[String, String],
              commitVersion: Long,
              actions: Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            throw new IllegalStateException("Fail commit request")
          }
        }
    }

    CommitStoreProvider.registerBuilder(NoBackfillingCommitStoreBuilder)
    withSQLConf(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> commitStoreName) {
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
    CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(batchSize = 2))
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
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10)
    val commitStore = builder.build(Map.empty).asInstanceOf[TrackingCommitStore]
    CommitStoreProvider.registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 0
      DeltaLog.clearCache()
      checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(1)))

      Seq(2).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 1
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2
      DeltaLog.clearCache()
      commitStore.numGetCommitsCalled = 0
      import testImplicits._
      val result1 = sql(s"SELECT * FROM delta.`$tablePath`").collect()
      assert(result1.length === 2 && result1.toSet === Set(Row(2), Row(3)))
      assert(commitStore.numGetCommitsCalled === 2)
    }
  }

  // Test commit owner changed on concurrent cluster
  testWithDefaultCommitOwnerUnset("snapshot is updated recursively when FS table is converted to" +
      " commit owner table on a concurrent cluster") {
    val commitStore = new TrackingCommitStore(new InMemoryCommitStore(batchSize = 10))
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10, Some(commitStore))
    CommitStoreProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val deltaLog1 = DeltaLog.forTable(spark, tablePath)
      deltaLog1.startTransaction().commitManually(Metadata())
      deltaLog1.startTransaction().commitManually(createTestAddFile("f1"))
      deltaLog1.startTransaction().commitManually()
      val snapshotV2 = deltaLog1.update()
      assert(snapshotV2.version === 2)
      assert(snapshotV2.tableCommitStoreOpt.isEmpty)
      DeltaLog.clearCache()

      // Add new commit to convert FS table to managed-commit table
      val deltaLog2 = DeltaLog.forTable(spark, tablePath)
      enableManagedCommit(deltaLog2, commitOwner = "tracking-in-memory")
      deltaLog2.startTransaction().commitManually(createTestAddFile("f2"))
      deltaLog2.startTransaction().commitManually()
      val snapshotV5 = deltaLog2.unsafeVolatileSnapshot
      assert(snapshotV5.version === 5)
      assert(snapshotV5.tableCommitStoreOpt.nonEmpty)
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
    CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLog.forTable(spark, new Path(tablePath), clock)
      assert(log.unsafeVolatileSnapshot.isInstanceOf[InitialSnapshot])
      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.isEmpty)
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
  // 4. Write commit 4/5 using new commit owner.
  // 5. Read the table again and make sure right APIs are called:
  //    a) If read query is run in scala, we do listing 2 times. So CS2.getCommits will be called
  //       twice. We should not be contacting CS1 anymore.
  //    b) If read query is run on SQL, we do listing only once. So CS2.getCommits will be called
  //       only once.
  test("snapshot is updated properly when owner changes multiple times") {
    val batchSize = 10
    val cs1 = new TrackingCommitStore(new InMemoryCommitStore(batchSize))
    val cs2 = new TrackingCommitStore(new InMemoryCommitStore(batchSize))

    case class TrackingInMemoryCommitStoreBuilder(
        override val name: String,
        commitStore: CommitStore) extends CommitStoreBuilder {
      var numBuildCalled = 0
      override def build(conf: Map[String, String]): CommitStore = {
        numBuildCalled += 1
        commitStore
      }
    }
    val builder1 = TrackingInMemoryCommitStoreBuilder(name = "tracking-in-memory-1", cs1)
    val builder2 = TrackingInMemoryCommitStoreBuilder(name = "tracking-in-memory-2", cs2)
    Seq(builder1, builder2).foreach(CommitStoreProvider.registerBuilder)

    def resetMetrics(): Unit = {
      Seq(builder1, builder2).foreach { b => b.numBuildCalled = 0 }
      Seq(cs1, cs2).foreach(_.reset())
    }

    withSQLConf(
        MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> builder1.name) {
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
        // the commit owner changes.
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
            configuration = oldMetadataConf - MANAGED_COMMIT_OWNER_NAME.key)
          val newMetadata2 = oldMetadata.copy(
            configuration = oldMetadataConf + (MANAGED_COMMIT_OWNER_NAME.key -> builder2.name))
          log.startTransaction().commitManually(newMetadata1)
          log.startTransaction().commitManually(newMetadata2)

          // Also backfill commit 0, 1 -- which the spec mandates when the commit owner changes.
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
        assert(snapshotV3.tableCommitStoreOpt.map(_.commitStore) === Some(cs2))
        assert(snapshotV3.version === 3)

        // Step-4: Write more commits using new owner
        resetMetrics()
        Seq(2).toDF.write.format("delta").mode("append").save(tablePath) // version 4
        Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 5
        assert((cs1.numCommitsCalled, cs2.numCommitsCalled) === (0, 2))
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (0, 2))

        // Step-5: Read the table again and assert that the right APIs are used
        resetMetrics()
        assert(
          sql(s"SELECT * FROM delta.`$tablePath`").collect().toSet === (0 to 3).map(Row(_)).toSet)
        // since this was hot query, so no new snapshot was created as part of this
        // deltaLog.update() and so commit-store is not initialized again.
        assert((builder1.numBuildCalled, builder2.numBuildCalled) === (0, 0))
        // Since this is dataframe read, so we invoke deltaLog.update() twice and so GetCommits API
        // is called twice.
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (0, 2))

        // Step-6: Clear cache and simulate cold read again.
        // We will firstly create snapshot from listing: 0.json, 1.json, 2.json.
        // We create Snapshot v2 and find about owner CS2.
        // Then we contact CS2 to update snapshot and find that v3, v4 exist.
        // We create snapshot v4 and find that owner doesn't change.
        DeltaLog.clearCache()
        resetMetrics()
        assert(
          sql(s"SELECT * FROM delta.`$tablePath`").collect().toSet === (0 to 3).map(Row(_)).toSet)
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (0, 2))
        assert((builder1.numBuildCalled, builder2.numBuildCalled) === (0, 2))
      }
    }
  }

  // This test has the following setup:
  // 1. Table is created with CS1 as commit-owner.
  // 2. Do another commit (v1) on table.
  // 3. Take a reference to current DeltaLog and clear the cache. This deltaLog object currently
  //    points to the latest table snapshot i.e. v1.
  // 4. Do commit v2 on the table.
  // 5. Do commit v3 on table. As part of this, change commit-owner to FS. Do v4 on table and change
  //    owner to CS2.
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
    val cs1 = new TrackingCommitStore(new InMemoryCommitStore(batchSize))
    val cs2 = new TrackingCommitStore(new InMemoryCommitStore(batchSize)) {
      var failAttempts = Set[Int]()

      override def getCommits(
        logPath: Path,
        managedCommitTableConf: Map[String, String],
        startVersion: Long,
        endVersion: Option[Long]): GetCommitsResponse = {
        if (failAttempts.contains(numGetCommitsCalled + 1)) {
          numGetCommitsCalled += 1
          throw new IllegalStateException("Injected failure")
        }
        super.getCommits(logPath, managedCommitTableConf, startVersion, endVersion)
      }
    }
    case class TrackingInMemoryCommitStoreBuilder(
        override val name: String,
        commitStore: CommitStore) extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = commitStore
    }
    val builder1 = TrackingInMemoryCommitStoreBuilder(name = "in-memory-1", cs1)
    val builder2 = TrackingInMemoryCommitStoreBuilder(name = "in-memory-2", cs2)
    Seq(builder1, builder2).foreach(CommitStoreProvider.registerBuilder)

    def resetMetrics(): Unit = {
      cs1.reset()
      cs2.reset()
      cs2.failAttempts = Set()
    }

    withSQLConf(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> "in-memory-1") {
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
          configuration = oldMetadataConf - MANAGED_COMMIT_OWNER_NAME.key)
        val newMetadata2 = oldMetadata.copy(
          configuration = oldMetadataConf + (MANAGED_COMMIT_OWNER_NAME.key -> "in-memory-2"))
        assert(log.update().tableCommitStoreOpt.get.commitStore === cs1)
        log.startTransaction().commitManually(newMetadata1) // version 3
        (1 to 3).foreach { v =>
          // backfill commit 1 and 2 also as 3/4 are written directly to FS.
          val segment = log.unsafeVolatileSnapshot.logSegment
          log.store.write(
            path = FileNames.unsafeDeltaFile(log.logPath, v),
            actions = log.store.read(segment.deltas(v).getPath).toIterator,
            overwrite = true)
        }
        assert(log.update().tableCommitStoreOpt === None)
        log.startTransaction().commitManually(newMetadata2) // version 4
        assert(log.update().tableCommitStoreOpt.get.commitStore === cs2)

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
        val ex1 = intercept[CommitStoreGetCommitsFailedException] { oldDeltaLog.update() }
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (1, 1))
        assert(ex1.getMessage.contains("Injected failure"))
        assert(oldDeltaLog.unsafeVolatileSnapshot.version == 1)
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp != clock.getTimeMillis())

        // Attempt-2
        // 2nd update also fails
        val ex2 = intercept[CommitStoreGetCommitsFailedException] { oldDeltaLog.update() }
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (2, 2))
        assert(ex2.getMessage.contains("Injected failure"))
        assert(oldDeltaLog.unsafeVolatileSnapshot.version == 1)
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp != clock.getTimeMillis())

        // Attempt-3: 3rd update succeeds
        clock.advance(500)
        assert(oldDeltaLog.update().version === 5)
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (3, 3))
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
      log.update()
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json"))

      // Commit 1
      Seq(2).toDF.write.format("delta").mode("append").save(tablePath) // version 1
      val commit1 = if (backfillInterval < 2) "1.json" else "1.uuid-1.json"
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1))
      log.update()
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1))

      // Commit 2
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2
      val commit2 = if (backfillInterval < 2) "2.json" else "2.uuid-2.json"
      assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", commit1, commit2))
      log.update()
      if (backfillInterval <= 2) {
        // backfill would have happened at commit 2. Next deltaLog.update will pickup the backfilled
        // files.
        assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", "1.json", "2.json"))
      } else {
        assert(getDeltasInPostCommitSnapshot(log) ===
          Seq("0.json", "1.uuid-1.json", "2.uuid-2.json"))
      }

      // Commit 3
      Seq(4).toDF.write.format("delta").mode("append").save(tablePath)
      val commit3 = if (backfillInterval < 2) "3.json" else "3.uuid-3.json"
      if (backfillInterval <= 2) {
        assert(getDeltasInPostCommitSnapshot(log) === Seq("0.json", "1.json", "2.json", commit3))
      } else {
        assert(getDeltasInPostCommitSnapshot(log) ===
          Seq("0.json", "1.uuid-1.json", "2.uuid-2.json", commit3))
      }

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

  testWithDefaultCommitOwnerUnset("DeltaLog.getSnapshotAt") {
    val commitStore = new TrackingCommitStore(new InMemoryCommitStore(batchSize = 10))
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10, Some(commitStore))
    CommitStoreProvider.registerBuilder(builder)
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
      // Part-1: Validate getSnapshotAt API works as expected for non-managed commit tables
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

      // Part-2: Validate getSnapshotAt API works as expected for managed commit tables when the
      // switch is made
      // commit 3
      enableManagedCommit(DeltaLog.forTable(spark, tablePath), "tracking-in-memory")
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
      // will do 2 round of listing as we are discovering a commit store after first round of
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
      // will do 2 round of listing as we are discovering a commit store after first round of
      // listing. Once the update finishes, deltaLog2 will point to v4. It can be returned directly.
      checkGetSnapshotAt(deltaLog2, version = 4, expectedUpdateCount = 1, expectedListingCount = 2)

      // Part-2: Validate getSnapshotAt API works as expected for managed commit tables
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

  private def enableManagedCommit(deltaLog: DeltaLog, commitOwner: String): Unit = {
    val oldMetadata = deltaLog.update().metadata
    val commitOwnerConf = (MANAGED_COMMIT_OWNER_NAME.key -> commitOwner)
    val newMetadata = oldMetadata.copy(configuration = oldMetadata.configuration + commitOwnerConf)
    deltaLog.startTransaction().commitManually(newMetadata)
  }

  test("tableConf returned from registration API is recorded in deltaLog and passed " +
      "to CommitStore in future for all the APIs") {
    val tableConf = Map("tableID" -> "random-u-u-i-d", "1" -> "2")
    val trackingCommitStore = new TrackingCommitStore(new InMemoryCommitStore(batchSize = 10) {
      override def registerTable(
          logPath: Path,
          currentVersion: Long,
          currentMetadata: Metadata,
          currentProtocol: Protocol): Map[String, String] = {
        super.registerTable(logPath, currentVersion, currentMetadata, currentProtocol)
        tableConf
      }

      override def getCommits(
          logPath: Path,
          managedCommitTableConf: Map[String, String],
          startVersion: Long,
          endVersion: Option[Long]): GetCommitsResponse = {
        assert(managedCommitTableConf === tableConf)
        super.getCommits(logPath, managedCommitTableConf, startVersion, endVersion)
      }

      override def commit(
          logStore: LogStore,
          hadoopConf: Configuration,
          logPath: Path,
          managedCommitTableConf: Map[String, String],
          commitVersion: Long,
          actions: Iterator[String],
          updatedActions: UpdatedActions): CommitResponse = {
        assert(managedCommitTableConf === tableConf)
        super.commit(logStore, hadoopConf, logPath, managedCommitTableConf,
          commitVersion, actions, updatedActions)
      }

      override def backfillToVersion(
          logStore: LogStore,
          hadoopConf: Configuration,
          logPath: Path,
          managedCommitTableConf: Map[String, String],
          startVersion: Long,
          endVersionOpt: Option[Long]): Unit = {
        assert(managedCommitTableConf === tableConf)
        super.backfillToVersion(
          logStore, hadoopConf, logPath, managedCommitTableConf, startVersion, endVersionOpt)
      }
    })
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10, Some(trackingCommitStore))
    CommitStoreProvider.registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      val commitOwnerConf = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder.name)
      val newMetadata = Metadata().copy(configuration = commitOwnerConf)
      log.startTransaction().commitManually(newMetadata)
      assert(log.unsafeVolatileSnapshot.version === 0)
      assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === tableConf)

      log.startTransaction().commitManually(createTestAddFile("f1"))
      log.startTransaction().commitManually(createTestAddFile("f2"))
      log.checkpoint()
      log.startTransaction().commitManually(createTestAddFile("f2"))

      assert(trackingCommitStore.numCommitsCalled > 0)
      assert(trackingCommitStore.numGetCommitsCalled > 0)
      assert(trackingCommitStore.numBackfillToVersionCalled > 0)
    }
  }

  for (upgradeExistingTable <- BOOLEAN_DOMAIN)
  testWithDifferentBackfillInterval("upgrade + downgrade [FS -> MC1 -> FS -> MC2]," +
      s" upgradeExistingTable = $upgradeExistingTable") { backfillInterval =>
    withoutManagedCommitsDefaultTableProperties {
      CommitStoreProvider.clearNonDefaultBuilders()
      val builder1 = TrackingInMemoryCommitStoreBuilder(batchSize = backfillInterval)
      val builder2 = new TrackingInMemoryCommitStoreBuilder(batchSize = backfillInterval) {
        override def name: String = "tracking-in-memory-2"
      }

      Seq(builder1, builder2).foreach(CommitStoreProvider.registerBuilder(_))
      val cs1 = builder1.trackingInMemoryCommitStore.asInstanceOf[TrackingCommitStore]
      val cs2 = builder2.trackingInMemoryCommitStore.asInstanceOf[TrackingCommitStore]

      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val log = DeltaLog.forTable(spark, tablePath)
        val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())

        var upgradeStartVersion = 0L
        // Create a non-managed commit table if we are testing upgrade for existing tables
        if (upgradeExistingTable) {
          log.startTransaction().commitManually(Metadata())
          assert(log.unsafeVolatileSnapshot.version === 0)
          log.startTransaction().commitManually(createTestAddFile("1"))
          assert(log.unsafeVolatileSnapshot.version === 1)
          assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.isEmpty)
          upgradeStartVersion = 2L
        }

        // Upgrade the table
        // [upgradeExistingTable = false] Commit-0
        // [upgradeExistingTable = true] Commit-2
        val commitOwnerConf = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder1.name)
        val newMetadata = Metadata().copy(configuration = commitOwnerConf)
        log.startTransaction().commitManually(newMetadata)
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerName === Some(builder1.name))
        assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)
        // upgrade commit always filesystem based
        assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, upgradeStartVersion)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled) == Seq(0, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled) == Seq(1, 0))

        // Do couple of commits on the managed-commit table
        // [upgradeExistingTable = false] Commit-1/2
        // [upgradeExistingTable = true] Commit-3/4
        (1 to 2).foreach { versionOffset =>
          val version = upgradeStartVersion + versionOffset
          log.startTransaction().commitManually(createTestAddFile(s"$versionOffset"))
          assert(log.unsafeVolatileSnapshot.version === version)
          assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)
          assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerName.nonEmpty)
          assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerConf === Map.empty)
          assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)
          assert(cs1.numCommitsCalled === versionOffset)
          val backfillExpected = if (version % backfillInterval == 0) true else false
          assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, version)) == backfillExpected)
        }

        // Downgrade the table
        // [upgradeExistingTable = false] Commit-3
        // [upgradeExistingTable = true] Commit-5
        val newMetadata2 = Metadata().copy(configuration = Map("downgraded_at" -> "v2"))
        log.startTransaction().commitManually(newMetadata2)
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 3)
        assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerName.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata === newMetadata2)
        // This must have increased by 1 as downgrade commit happens via CommitStore.
        assert(Seq(cs1, cs2).map(_.numCommitsCalled) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled) == Seq(1, 0))
        (0 to 3).foreach { version =>
          assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, version)))
        }

        // Do commit after downgrade is over
        // [upgradeExistingTable = false] Commit-4
        // [upgradeExistingTable = true] Commit-6
        log.startTransaction().commitManually(createTestAddFile("post-upgrade-file"))
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 4)
        // no commit store after downgrade
        assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.isEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)
        // Metadata is same as what we added at time of downgrade
        assert(log.unsafeVolatileSnapshot.metadata === newMetadata2)
        // State reconstruction should give correct results
        var expectedFileNames = Set("1", "2", "post-upgrade-file")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        // Commit Store should not be invoked for commit API.
        // Register table API should not be called until the end
        assert(Seq(cs1, cs2).map(_.numCommitsCalled) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled) == Seq(1, 0))
        // 4th file is directly written to FS in backfilled way.
        assert(fs.exists(FileNames.unsafeDeltaFile(log.logPath, upgradeStartVersion + 4)))

        // Now transfer the table to another commit owner
        // [upgradeExistingTable = false] Commit-5
        // [upgradeExistingTable = true] Commit-7
        val commitOwnerConf2 = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder2.name)
        val oldMetadata3 = log.unsafeVolatileSnapshot.metadata
        val newMetadata3 = oldMetadata3.copy(
          configuration = oldMetadata3.configuration ++ commitOwnerConf2)
        log.startTransaction().commitManually(newMetadata3, createTestAddFile("upgrade-2-file"))
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 5)
        assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerName === Some(builder2.name))
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerConf === Map.empty)
        assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)
        expectedFileNames = Set("1", "2", "post-upgrade-file", "upgrade-2-file")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled) == Seq(3, 0))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled) == Seq(1, 1))

        // Make 1 more commit, this should go to new owner
        log.startTransaction().commitManually(newMetadata3, createTestAddFile("4"))
        expectedFileNames = Set("1", "2", "post-upgrade-file", "upgrade-2-file", "4")
        assert(log.unsafeVolatileSnapshot.allFiles.collect().toSet ===
          expectedFileNames.map(name => createTestAddFile(name, dataChange = false)))
        assert(Seq(cs1, cs2).map(_.numCommitsCalled) == Seq(3, 1))
        assert(Seq(cs1, cs2).map(_.numRegisterTableCalled) == Seq(1, 1))
        assert(log.unsafeVolatileSnapshot.version === upgradeStartVersion + 6)
      }
    }
  }

  test("transfer from one commit-owner to another commit-owner fails [MC-1 -> MC-2 fails]") {
    CommitStoreProvider.clearNonDefaultBuilders()
    val builder1 = TrackingInMemoryCommitStoreBuilder(batchSize = 10)
    val builder2 = new TrackingInMemoryCommitStoreBuilder(batchSize = 10) {
      override def name: String = "tracking-in-memory-2"
    }
    Seq(builder1, builder2).foreach(CommitStoreProvider.registerBuilder(_))

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      // A new table will automatically get `tracking-in-memory` as the whole suite is configured to
      // use it as default commit store via [[MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey]].
      log.startTransaction().commitManually(Metadata())
      assert(log.unsafeVolatileSnapshot.version === 0L)
      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)

      // Change commit owner
      val newCommitOwnerConf = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder2.name)
      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val newMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration ++ newCommitOwnerConf)
      val ex = intercept[IllegalStateException] {
        log.startTransaction().commitManually(newMetadata)
      }
      assert(ex.getMessage.contains("from one commit-owner to another commit-owner is not allowed"))
    }
  }

  testWithDefaultCommitOwnerUnset("FS -> MC upgrade is not retried on a conflict") {
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10)
    CommitStoreProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val log = DeltaLog.forTable(spark, tablePath)
      val commitOwnerConf = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder.name)
      val newMetadata = Metadata().copy(configuration = commitOwnerConf)
      val txn = log.startTransaction() // upgrade txn started
      log.startTransaction().commitManually(createTestAddFile("f1"))
      intercept[io.delta.exceptions.ConcurrentWriteException] {
        txn.commitManually(newMetadata) // upgrade txn committed
      }
    }
  }

  testWithDefaultCommitOwnerUnset("FS -> MC upgrade with commitLarge API") {
    val builder = TrackingInMemoryCommitStoreBuilder(batchSize = 10)
    val cs = builder.trackingInMemoryCommitStore.asInstanceOf[TrackingCommitStore]
    CommitStoreProvider.registerBuilder(builder)
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").save(tablePath)
      Seq(1).toDF.write.mode("overwrite").format("delta").save(tablePath)
      var log = DeltaLog.forTable(spark, tablePath)
      assert(log.unsafeVolatileSnapshot.version === 1L)
      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.isEmpty)

      val commitOwnerConf = Map(MANAGED_COMMIT_OWNER_NAME.key -> builder.name)
      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val newMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration ++ commitOwnerConf)
      val oldProtocol = log.unsafeVolatileSnapshot.protocol
      assert(!oldProtocol.readerAndWriterFeatures.contains(V2CheckpointTableFeature))
      val newProtocol =
        oldProtocol.copy(
          minReaderVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures =
            Some(oldProtocol.readerFeatures.getOrElse(Set.empty) + V2CheckpointTableFeature.name),
          writerFeatures =
            Some(oldProtocol.writerFeatures.getOrElse(Set.empty) + ManagedCommitTableFeature.name))
      assert(cs.numRegisterTableCalled === 0)
      assert(cs.numCommitsCalled === 0)

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
      assert(cs.numRegisterTableCalled === 1)
      assert(cs.numCommitsCalled === 0)
      assert(log.unsafeVolatileSnapshot.version === 2L)

      Seq(V2CheckpointTableFeature, ManagedCommitTableFeature).foreach { feature =>
        assert(log.unsafeVolatileSnapshot.protocol.isFeatureSupported(feature))
      }

      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)
      assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerName === Some(builder.name))
      assert(log.unsafeVolatileSnapshot.metadata.managedCommitOwnerConf === Map.empty)
      assert(log.unsafeVolatileSnapshot.metadata.managedCommitTableConf === Map.empty)

      Seq(3).toDF.write.mode("append").format("delta").save(tablePath)
      assert(cs.numRegisterTableCalled === 1)
      assert(cs.numCommitsCalled === 1)
      assert(log.unsafeVolatileSnapshot.version === 3L)
      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)

    }
  }

  test("Incomplete backfills are handled properly by next commit after MC to FS conversion") {
    val batchSize = 10
    val neverBackfillingCommitStore = new TrackingCommitStore(new InMemoryCommitStore(batchSize) {
      override def backfillToVersion(
          logStore: LogStore,
          hadoopConf: Configuration,
          logPath: Path,
          managedCommitTableConf: Map[String, String],
          startVersion: Long,
          endVersion: Option[Long]): Unit = { }
    })
    CommitStoreProvider.clearNonDefaultBuilders()
    val builder =
      TrackingInMemoryCommitStoreBuilder(batchSize, Some(neverBackfillingCommitStore))
    CommitStoreProvider.registerBuilder(builder)

    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v0
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v1
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // v2

      val log = DeltaLog.forTable(spark, tablePath)
      assert(log.unsafeVolatileSnapshot.tableCommitStoreOpt.nonEmpty)
      assert(log.unsafeVolatileSnapshot.version === 2)
      assert(
        log.unsafeVolatileSnapshot.logSegment.deltas.count(FileNames.isUnbackfilledDeltaFile) == 2)

      val oldMetadata = log.unsafeVolatileSnapshot.metadata
      val downgradeMetadata = oldMetadata.copy(
        configuration = oldMetadata.configuration - MANAGED_COMMIT_OWNER_NAME.key)
      log.startTransaction().commitManually(downgradeMetadata)
      log.update()
      val snapshotAfterDowngrade = log.unsafeVolatileSnapshot
      assert(snapshotAfterDowngrade.version === 3)
      assert(snapshotAfterDowngrade.tableCommitStoreOpt.isEmpty)
      assert(snapshotAfterDowngrade.logSegment.deltas.count(FileNames.isUnbackfilledDeltaFile) == 3)

      val records = Log4jUsageLogger.track {
        // commit 4
        Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath)
      }
      val filteredUsageLogs = filterUsageRecords(
        records, "delta.managedCommit.backfillWhenManagedCommitSupportedAndDisabled")
      assert(filteredUsageLogs.size === 1)
      val usageObj = JsonUtils.fromJson[Map[String, Any]](filteredUsageLogs.head.blob)
      assert(usageObj("numUnbackfilledFiles").asInstanceOf[Int] === 3)
      assert(usageObj("numAlreadyBackfilledFiles").asInstanceOf[Int] === 0)
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  //           Test managed-commits with DeltaLog.getChangeLogFile API starts                //
  /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Helper method which generates a delta table with `totalCommits`.
   * The `upgradeToManagedCommitVersion`th commit version upgrades this table to managed commit
   * and it uses `backfillInterval` for backfilling.
   * This method returns a mapping of version to DeltaLog for the versions in
   * `requiredDeltaLogVersions`. Each of this deltaLog object has a Snapshot as per what is
   * mentioned in the `requiredDeltaLogVersions`.
   */
  private def generateDataForGetChangeLogFilesTest(
      dir: File,
      totalCommits: Int,
      upgradeToManagedCommitVersion: Int,
      backfillInterval: Int,
      requiredDeltaLogVersions: Set[Int]): Map[Int, DeltaLog] = {
    val commitStore = new TrackingCommitStore(new InMemoryCommitStore(backfillInterval))
    val builder = TrackingInMemoryCommitStoreBuilder(backfillInterval, Some(commitStore))
    CommitStoreProvider.registerBuilder(builder)
    val versionToDeltaLogMapping = collection.mutable.Map.empty[Int, DeltaLog]
    withSQLConf(
      CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      val tablePath = dir.getAbsolutePath

      (0 to totalCommits).foreach { v =>
        if (v === upgradeToManagedCommitVersion) {
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          val oldMetadata = deltaLog.unsafeVolatileSnapshot.metadata
          val commitOwner = (MANAGED_COMMIT_OWNER_NAME.key -> "tracking-in-memory")
          val newMetadata =
            oldMetadata.copy(configuration = oldMetadata.configuration + commitOwner)
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

  testWithDefaultCommitOwnerUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [No Managed Commits]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 4,
        upgradeToManagedCommitVersion = -1,
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
      // need an update() to get the latest snapshot and see if managed commit was enabled on the
      // table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 4).
      // The latest commit from filesystem listing is 4 -- same as unsafeVolatileSnapshot and this
      // unsafeVolatileSnapshot doesn't have managed commit enabled. So we should not need an
      // update().
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = false)
    }
  }

  testWithDefaultCommitOwnerUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [Managed Commits backfill size 1]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 4,
        upgradeToManagedCommitVersion = 2,
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
      // need an update() to get the latest snapshot and see if managed commit was enabled on the
      // table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(2),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)

      // We are asking for changes between 0 to 4 to a DeltaLog(unsafeVolatileSnapshot = 4).
      // Since the commits in filesystem are between 0 to 4, so we don't need to update() to get
      // the latest snapshot and see if managed commit was enabled on the table concurrently.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        endVersionOpt = Some(4),
        expectedLastBackfilledCommit = 4,
        updateExpected = false)

      // We are asking for changes between 0 to `end` to a DeltaLog(unsafeVolatileSnapshot = 4).
      // The latest commit from filesystem listing is 4 -- same as unsafeVolatileSnapshot and this
      // unsafeVolatileSnapshot has managed commit enabled. So we should need an update() to find
      // out latest commits from Commit Store.
      runGetChangeLogFiles(
        versionsToDeltaLogMapping(4),
        totalCommitsOnTable = 4,
        startVersion = 0,
        expectedLastBackfilledCommit = 4,
        updateExpected = true)
    }
  }

  testWithDefaultCommitOwnerUnset("DeltaLog.getChangeLogFile with and" +
    " without endVersion [Managed Commits backfill size 10]") {
    withTempDir { dir =>
      val versionsToDeltaLogMapping = generateDataForGetChangeLogFilesTest(
        dir,
        totalCommits = 8,
        upgradeToManagedCommitVersion = 2,
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
      // Since the unsafeVolatileSnapshot has managed-commits enabled, so we need to trigger an
      // update to find the latest commits from Commit Store.
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
  //           Test managed-commits with DeltaLog.getChangeLogFile API ENDS                  //
  /////////////////////////////////////////////////////////////////////////////////////////////
}
