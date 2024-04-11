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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaConfigs.{MANAGED_COMMIT_OWNER_CONF, MANAGED_COMMIT_OWNER_NAME}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.InitialSnapshot
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.{Action, Metadata}
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

  test("Optimistic Transaction errors out if 0th commit is not backfilled") {
    val commitStoreName = "nobackfilling-commit-store"
    object NoBackfillingCommitStoreBuilder extends CommitStoreBuilder {

      override def name: String = commitStoreName

      override def build(conf: Map[String, String]): CommitStore =
        new InMemoryCommitStore(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              logPath: Path,
              commitVersion: Long,
              actions: Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            val uuidFile =
              FileNames.unbackfilledDeltaFile(logPath, commitVersion)
            logStore.write(uuidFile, actions, overwrite = false, hadoopConf)
            val uuidFileStatus = uuidFile.getFileSystem(hadoopConf).getFileStatus(uuidFile)
            val commitTime = uuidFileStatus.getModificationTime
            commitImpl(logStore, hadoopConf, logPath, commitVersion, uuidFileStatus, commitTime)

            CommitResponse(Commit(commitVersion, uuidFileStatus, commitTime))
          }
        }
    }

    CommitStoreProvider.registerBuilder(NoBackfillingCommitStoreBuilder)
    withSQLConf(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> commitStoreName) {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val ex = intercept[IllegalStateException] {
          Seq(1).toDF.write.format("delta").save(tablePath)
        }
        assert(ex.getMessage.contains(s"Expected 0th commit to be written" +
          s" to file:$tablePath/_delta_log/00000000000000000000.json"))
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
      assert(unbackfilledCommitVersions === Array(0, 1, 2))
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
  testWithoutManagedCommits("snapshot is updated recursively when FS table is converted to commit" +
      " owner table on a concurrent cluster") {
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
      assert(snapshotV2.commitStoreOpt.isEmpty)
      DeltaLog.clearCache()

      // Add new commit to convert FS table to managed-commit table
      val deltaLog2 = DeltaLog.forTable(spark, tablePath)
      enableManagedCommit(deltaLog2, commitOwner = "tracking-in-memory")
      commitStore.registerTable(deltaLog2.logPath, 3)
      deltaLog2.startTransaction().commitManually(createTestAddFile("f2"))
      deltaLog2.startTransaction().commitManually()
      val snapshotV5 = deltaLog2.unsafeVolatileSnapshot
      assert(snapshotV5.version === 5)
      assert(snapshotV5.commitStoreOpt.nonEmpty)
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
          log.store.write(
            FileNames.unsafeDeltaFile(log.logPath, 2),
            Seq(newMetadata1.json).toIterator,
            overwrite = false,
            conf)
          log.store.write(
            FileNames.unsafeDeltaFile(log.logPath, 3),
            Seq(newMetadata2.json).toIterator,
            overwrite = false,
            conf)
          cs2.registerTable(log.logPath, maxCommitVersion = 3)

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
        assert(snapshotV3.commitStoreOpt === Some(cs2))
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
        startVersion: Long,
        endVersion: Option[Long]): GetCommitsResponse = {
        if (failAttempts.contains(numGetCommitsCalled + 1)) {
          numGetCommitsCalled += 1
          throw new IllegalStateException("Injected failure")
        }
        super.getCommits(logPath, startVersion, endVersion)
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
        assert(log.update().commitStoreOpt.get === cs1)
        log.startTransaction().commitManually(newMetadata1) // version 3
        (1 to 3).foreach { v =>
          // backfill commit 1 and 2 also as 3/4 are written directly to FS.
          val segment = log.unsafeVolatileSnapshot.logSegment
          log.store.write(
            path = FileNames.unsafeDeltaFile(log.logPath, v),
            actions = log.store.read(segment.deltas(v).getPath).toIterator,
            overwrite = true)
        }
        assert(log.update().commitStoreOpt === None)
        log.startTransaction().commitManually(newMetadata2) // version 4
        cs2.registerTable(log.logPath, maxCommitVersion = 4)
        assert(log.update().commitStoreOpt.get === cs2)

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
        val ex1 = intercept[IllegalStateException] { oldDeltaLog.update() }
        assert((cs1.numGetCommitsCalled, cs2.numGetCommitsCalled) === (1, 1))
        assert(ex1.getMessage.contains("Injected failure"))
        assert(oldDeltaLog.unsafeVolatileSnapshot.version == 1)
        assert(oldDeltaLog.getCapturedSnapshot().updateTimestamp != clock.getTimeMillis())

        // Attempt-2
        val ex2 = intercept[IllegalStateException] { oldDeltaLog.update() } // 2nd update also fails
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

  testWithoutManagedCommits("DeltaLog.getSnapshotAt") {
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
      commitStore.registerTable(deltaLog1.logPath, maxCommitVersion = 3)
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
}
