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

package org.apache.spark.sql.delta

import java.io.{File, RandomAccessFile}
import java.util.concurrent.CountDownLatch

import scala.collection.mutable

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME
import org.apache.spark.sql.delta.DeltaTestUtils.{verifyBackfilled, verifyUnbackfilled, BOOLEAN_DOMAIN}
import org.apache.spark.sql.delta.SnapshotManagementSuiteShims._
import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorBuilder, CommitCoordinatorProvider, CoordinatedCommitsBaseSuite, CoordinatedCommitsUsageLogs, InMemoryCommitCoordinator}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.storage.LogStore.logStoreClassConfKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}
import io.delta.storage.LogStore
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, GetCommitsResponse, TableDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class SnapshotManagementSuite extends QueryTest with DeltaSQLTestUtils with SharedSparkSession
  with DeltaSQLCommandTest with CoordinatedCommitsBaseSuite {


  /**
   * Truncate an existing checkpoint file to create a corrupt file.
   *
   * @param path the Delta table path
   * @param checkpointVersion the checkpoint version to be updated
   * @param shouldBeEmpty whether to create an empty checkpoint file
   */
  private def makeCorruptCheckpointFile(
      path: String,
      checkpointVersion: Long,
      shouldBeEmpty: Boolean,
      multipart: Option[(Int, Int)] = None): Unit = {
    if (multipart.isDefined) {
      val (part, totalParts) = multipart.get
      val checkpointFile = FileNames.checkpointFileWithParts(new Path(path, "_delta_log"),
        checkpointVersion, totalParts)(part - 1).toString
      assert(new File(checkpointFile).exists)
      val cp = new RandomAccessFile(checkpointFile, "rw")
      cp.setLength(if (shouldBeEmpty) 0 else 10)
      cp.close()
    } else {
      val checkpointFile =
        FileNames.checkpointFileSingular(new Path(path, "_delta_log"), checkpointVersion).toString
      assert(new File(checkpointFile).exists)
      val cp = new RandomAccessFile(checkpointFile, "rw")
      cp.setLength(if (shouldBeEmpty) 0 else 10)
      cp.close()
    }
  }

  private def deleteLogVersion(path: String, version: Long): Unit = {
    val deltaFile = new File(
      FileNames.unsafeDeltaFile(new Path(path, "_delta_log"), version).toString)
    assert(deltaFile.exists(), s"Could not find $deltaFile")
    assert(deltaFile.delete(), s"Failed to delete $deltaFile")
  }

  private def deleteCheckpointVersion(path: String, version: Long): Unit = {
    val deltaFile = new File(
      FileNames.checkpointFileSingular(new Path(path, "_delta_log"), version).toString)
    assert(deltaFile.exists(), s"Could not find $deltaFile")
    assert(deltaFile.delete(), s"Failed to delete $deltaFile")
  }

  private def testWithAndWithoutMultipartCheckpoint(name: String)(f: (Option[Int]) => Unit) = {
    testQuietly(name) {
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "1") {
        f(Some(1))
        f(Some(2))
      }
      f(None)
    }
  }

  testWithAndWithoutMultipartCheckpoint("recover from a corrupt checkpoint: previous checkpoint " +
      "doesn't exist") { partToCorrupt =>
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      var deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()

      DeltaLog.clearCache()
      deltaLog = DeltaLog.forTable(spark, path)
      val checkpointParts = deltaLog.snapshot.logSegment.checkpointProvider.topLevelFiles.size
      val multipart = partToCorrupt.map((_, checkpointParts))

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 0,
          shouldBeEmpty = testEmptyCheckpoint, multipart = multipart)
        DeltaLog.clearCache()
        // Checkpoint 0 is corrupted. Verify that we can still create the snapshot using
        // existing json files.
        DeltaLog.forTable(spark, path).snapshot
      }
    }
  }

  testWithAndWithoutMultipartCheckpoint("recover from a corrupt checkpoint: previous checkpoint " +
      "exists") { partToCorrupt =>
    withTempDir { tempDir =>
      // Create checkpoint 0 and 1
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      var deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(10).write.format("delta").mode("append").save(path)
      deltaLog.update()
      deltaLog.checkpoint()

      DeltaLog.clearCache()
      deltaLog = DeltaLog.forTable(spark, path)
      val checkpointParts = deltaLog.snapshot.logSegment.checkpointProvider.topLevelFiles.size
      val multipart = partToCorrupt.map((_, checkpointParts))

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1,
          shouldBeEmpty = testEmptyCheckpoint, multipart = multipart)
        // Checkpoint 1 is corrupted. Verify that we can still create the snapshot using
        // checkpoint 0.
        DeltaLog.clearCache()
        DeltaLog.forTable(spark, path).snapshot
      }
    }
  }

  testWithAndWithoutMultipartCheckpoint("should not recover when the current checkpoint is " +
      "broken but we don't have the entire history") { partToCorrupt =>
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)
      DeltaLog.forTable(spark, path).checkpoint()
      deleteLogVersion(path, version = 0)
      DeltaLog.clearCache()

      val deltaLog = DeltaLog.forTable(spark, path)
      val checkpointParts = deltaLog.snapshot.logSegment.checkpointProvider.topLevelFiles.size
      val multipart = partToCorrupt.map((_, checkpointParts))

      DeltaLog.clearCache()

      // We have different code paths for empty and non-empty checkpoints, and also different
      // code paths when listing with or without a checkpoint hint.
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1,
          shouldBeEmpty = testEmptyCheckpoint, multipart = multipart)
        // When finding a Delta log for the first time, we rely on _last_checkpoint hint
        val e = intercept[Exception] { DeltaLog.forTable(spark, path).snapshot }
        if (testEmptyCheckpoint) {
          // - checkpoint 1 is NOT in the list result
          // - try to get an alternative LogSegment in `getLogSegmentForVersion`
          // - fail to get an alternative LogSegment
          // - throw the below exception
          assert(e.isInstanceOf[IllegalStateException] && e.getMessage.contains(
            "Couldn't find all part files of the checkpoint version: 1"))
        } else {
          // - checkpoint 1 is in the list result
          // - Snapshot creation triggers state reconstruction
          // - fail to read protocol+metadata from checkpoint 1
          // - throw FileReadException
          // - fail to get an alternative LogSegment
          // - cannot find log file 0 so throw the above checkpoint 1 read failure
          // Guava cache wraps the root cause
          assert(e.isInstanceOf[SparkException] &&
            e.getMessage.contains("0001.checkpoint") &&
            e.getMessage.contains(SHOULD_NOT_RECOVER_CHECKPOINT_ERROR_MSG))
        }
      }
    }
  }

  testWithAndWithoutMultipartCheckpoint("should not recover when both the current and previous " +
      "checkpoints are broken") { partToCorrupt =>
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val staleLog = DeltaLog.forTable(spark, path)
      DeltaLog.clearCache()

      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      DeltaLog.clearCache()
      val checkpointParts0 =
        DeltaLog.forTable(spark, path).snapshot.logSegment.checkpointProvider.topLevelFiles.size

      spark.range(10).write.format("delta").mode("append").save(path)
      deltaLog.update()
      deltaLog.checkpoint()
      deleteLogVersion(path, version = 0)

      DeltaLog.clearCache()
      val checkpointParts1 =
        DeltaLog.forTable(spark, path).snapshot.logSegment.checkpointProvider.topLevelFiles.size

      makeCorruptCheckpointFile(path, checkpointVersion = 0, shouldBeEmpty = false,
        multipart = partToCorrupt.map((_, checkpointParts0)))

      val multipart = partToCorrupt.map((_, checkpointParts1))

      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 1,
          shouldBeEmpty = testEmptyCheckpoint, multipart = multipart)

        // The code paths are different, but the error and message end up being the same:
        //
        // testEmptyCheckpoint = true:
        // - checkpoint 1 is NOT in the list result.
        // - fallback to load version 0 using checkpoint 0
        // - fail to read checkpoint 0
        // - cannot find log file 0 so throw the above checkpoint 0 read failure
        //
        // testEmptyCheckpoint = false:
        // - checkpoint 1 is in the list result.
        // - Snapshot creation triggers state reconstruction
        // - fail to read protocol+metadata from checkpoint 1
        // - fallback to load version 0 using checkpoint 0
        // - fail to read checkpoint 0
        // - cannot find log file 0 so throw the original checkpoint 1 read failure
        val e = intercept[SparkException] { staleLog.update() }
        val version = if (testEmptyCheckpoint) 0 else 1
        assert(e.getMessage.contains(f"$version%020d.checkpoint") &&
          e.getMessage.contains(SHOULD_NOT_RECOVER_CHECKPOINT_ERROR_MSG))
      }
    }
  }

  test("should throw a clear exception when checkpoint exists but its corresponding delta file " +
    "doesn't exist") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val staleLog = DeltaLog.forTable(spark, path)
      DeltaLog.clearCache()

      spark.range(10).write.format("delta").save(path)
      DeltaLog.forTable(spark, path).checkpoint()
      // Delete delta files
      new File(tempDir, "_delta_log").listFiles().filter(_.getName.endsWith(".json"))
        .foreach(_.delete())
      val e = intercept[IllegalStateException] {
        staleLog.update()
      }
      assert(e.getMessage.contains("Could not find any delta files for version 0"))
    }
  }

  test("should throw an exception when trying to load a non-existent version") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val staleLog = DeltaLog.forTable(spark, path)
      DeltaLog.clearCache()

      spark.range(10).write.format("delta").save(path)
      DeltaLog.forTable(spark, path).checkpoint()
      val e = intercept[IllegalStateException] {
        staleLog.getSnapshotAt(2)
      }
      assert(e.getMessage.contains("Trying to load a non-existent version 2"))
    }
  }

  test("should throw a clear exception when the checkpoint is corrupt " +
    "but could not find any delta files") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val staleLog = DeltaLog.forTable(spark, path)
      DeltaLog.clearCache()

      spark.range(10).write.format("delta").save(path)
      DeltaLog.forTable(spark, path).checkpoint()
      // Delete delta files
      new File(tempDir, "_delta_log").listFiles().filter(_.getName.endsWith(".json"))
        .foreach(_.delete())
      if (coordinatedCommitsEnabledInTests) {
        new File(new File(tempDir, "_delta_log"), "_commits")
          .listFiles()
          .filter(_.getName.endsWith(".json"))
          .foreach(_.delete())
      }
      makeCorruptCheckpointFile(path, checkpointVersion = 0, shouldBeEmpty = false)
      val e = intercept[IllegalStateException] {
        staleLog.update()
      }
      assert(e.getMessage.contains("Could not find any delta files for version 0"))
    }
  }

  test("verifyDeltaVersions") {
    import SnapshotManagement.verifyDeltaVersions
    // empty array
    verifyDeltaVersions(
      spark,
      versions = Array.empty,
      expectedStartVersion = None,
      expectedEndVersion = None)
    // contiguous versions
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = None,
      expectedEndVersion = None)
    // contiguous versions with correct `expectedStartVersion` and `expectedStartVersion`
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = None,
      expectedEndVersion = Some(3))
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = Some(1),
      expectedEndVersion = None)
    verifyDeltaVersions(
      spark,
      versions = Array(1, 2, 3),
      expectedStartVersion = Some(1),
      expectedEndVersion = Some(3))
    // `expectedStartVersion` or `expectedEndVersion` doesn't match
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 2),
        expectedStartVersion = Some(0),
        expectedEndVersion = None)
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 2),
        expectedStartVersion = None,
        expectedEndVersion = Some(3))
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array.empty,
        expectedStartVersion = Some(0),
        expectedEndVersion = None)
    }
    intercept[IllegalArgumentException] {
      verifyDeltaVersions(
        spark,
        versions = Array.empty,
        expectedStartVersion = None,
        expectedEndVersion = Some(3))
    }
    // non contiguous versions
    intercept[IllegalStateException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 3),
        expectedStartVersion = None,
        expectedEndVersion = None)
    }
    // duplicates in versions
    intercept[IllegalStateException] {
      verifyDeltaVersions(
        spark,
        versions = Array(1, 2, 2, 3),
        expectedStartVersion = None,
        expectedEndVersion = None)
    }
    // unsorted versions
    intercept[IllegalStateException] {
      verifyDeltaVersions(
        spark,
        versions = Array(3, 2, 1),
        expectedStartVersion = None,
        expectedEndVersion = None)
    }
  }

  test("configurable snapshot cache storage level") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      DeltaLog.clearCache()
      // Corrupted snapshot tests leave a cached snapshot not tracked by the DeltaLog cache
      sparkContext.getPersistentRDDs.foreach(_._2.unpersist())
      assert(sparkContext.getPersistentRDDs.isEmpty)

      withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL.key -> "DISK_ONLY") {
        DeltaLog.forTable(spark, path).snapshot.stateDS.collect()
        val persistedRDDs = sparkContext.getPersistentRDDs
        assert(persistedRDDs.size == 1)
        assert(persistedRDDs.values.head.getStorageLevel == StorageLevel.DISK_ONLY)
      }

      DeltaLog.clearCache()
      assert(sparkContext.getPersistentRDDs.isEmpty)

      withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL.key -> "NONE") {
        DeltaLog.forTable(spark, path).snapshot.stateDS.collect()
        val persistedRDDs = sparkContext.getPersistentRDDs
        assert(persistedRDDs.size == 1)
        assert(persistedRDDs.values.head.getStorageLevel == StorageLevel.NONE)
      }

      DeltaLog.clearCache()
      assert(sparkContext.getPersistentRDDs.isEmpty)

      withSQLConf(DeltaSQLConf.DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL.key -> "invalid") {
        intercept[IllegalArgumentException] {
          spark.read.format("delta").load(path).collect()
        }
      }
    }
  }

  test("SerializableFileStatus json serialization/deserialization") {
    val testCases = Seq(
      SerializableFileStatus(path = "xyz", length = -1, isDir = true, modificationTime = 0)
        -> """{"path":"xyz","length":-1,"isDir":true,"modificationTime":0}""",
      SerializableFileStatus(
        path = "s3://a.b/pq", length = 123L, isDir = false, modificationTime = 246L)
        -> """{"path":"s3://a.b/pq","length":123,"isDir":false,"modificationTime":246}"""
    )
    for ((obj, json) <- testCases) {
      assert(JsonUtils.toJson(obj) == json)
      val status = JsonUtils.fromJson[SerializableFileStatus](json)
      assert(status.modificationTime === obj.modificationTime)
      assert(status.isDir === obj.isDir)
      assert(status.length === obj.length)
      assert(status.path === obj.path)
    }
  }

  test("getLogSegmentAfterCommit can find specified commit") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val oldLogSegment = log.snapshot.logSegment
      spark.range(10).write.format("delta").save(path)
      val newLogSegment = log.snapshot.logSegment
      assert(log.getLogSegmentAfterCommit(
        log.snapshot.tableCommitCoordinatorClientOpt,
        oldLogSegment.checkpointProvider) === newLogSegment)
      spark.range(10).write.format("delta").mode("append").save(path)
      val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
      val commitFileProvider = DeltaCommitFileProvider(log.snapshot)
      intercept[IllegalArgumentException] {
        val commitFile = fs.getFileStatus(commitFileProvider.deltaFile(1))
        val commit = new Commit(1, commitFile, 0)
        // Version exists, but not contiguous with old logSegment
        log.getLogSegmentAfterCommit(1, None, oldLogSegment, commit, None, EmptyCheckpointProvider)
      }
      intercept[IllegalArgumentException] {
        val commitFile = fs.getFileStatus(commitFileProvider.deltaFile(0))
        val commit = new Commit(0, commitFile, 0)

        // Version exists, but newLogSegment already contains it
        log.getLogSegmentAfterCommit(0, None, newLogSegment, commit, None, EmptyCheckpointProvider)
      }
      assert(log.getLogSegmentAfterCommit(
        log.snapshot.tableCommitCoordinatorClientOpt,
        oldLogSegment.checkpointProvider) === log.snapshot.logSegment)
    }
  }

  testQuietly("checkpoint/json not found when executor restart " +
    "after expired checkpoints in the snapshot cache are cleaned up") {
    withTempDir { tempDir =>
      // Create checkpoint 1 and 3
      val path = tempDir.getCanonicalPath
      spark.range(10).write.format("delta").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)
      deltaLog.checkpoint()
      // simulate checkpoint 1 expires and is cleaned up
      deleteCheckpointVersion(path, 1)
      // simulate executor hangs and restart, cache invalidation
      deltaLog.snapshot.uncache()

      spark.read.format("delta").load(path).collect()
    }
  }
}

class SnapshotManagementWithCoordinatedCommitsBatch1Suite extends SnapshotManagementSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

class SnapshotManagementWithCoordinatedCommitsBatch2Suite extends SnapshotManagementSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)
}

class SnapshotManagementWithCoordinatedCommitsBatch100Suite extends SnapshotManagementSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)
}

class CountDownLatchLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends LocalLogStore(sparkConf, hadoopConf) {
  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    val files = super.listFrom(path, hadoopConf).toSeq
    if (ConcurrentBackfillCommitCoordinatorClient.beginConcurrentBackfills) {
      CountDownLatchLogStore.listFromCalled.countDown()
    }
    files.iterator
  }
}
object CountDownLatchLogStore {
  val listFromCalled = new CountDownLatch(1)
}

case class ConcurrentBackfillCommitCoordinatorClient(
    synchronousBackfillThreshold: Long,
    override val batchSize: Long
) extends InMemoryCommitCoordinator(batchSize) {
  private val deferredBackfills: mutable.Map[Long, () => Unit] = mutable.Map.empty
  override def getCommits(
      tableDesc: TableDescriptor,
      startVersion: java.lang.Long,
      endVersion: java.lang.Long): GetCommitsResponse = {
    if (ConcurrentBackfillCommitCoordinatorClient.beginConcurrentBackfills) {
      CountDownLatchLogStore.listFromCalled.await()
      logInfo(s"Finishing pending backfills concurrently: ${deferredBackfills.keySet}")
      deferredBackfills.keys.toSeq.sorted.foreach((version: Long) => deferredBackfills(version)())
      deferredBackfills.clear()
    }
    super.getCommits(tableDesc, startVersion, endVersion)
  }
  override def backfill(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      version: Long,
      fileStatus: FileStatus): Unit = {
    if (version > synchronousBackfillThreshold &&
        ConcurrentBackfillCommitCoordinatorClient.deferBackfills) {
      deferredBackfills(version) = () =>
        super.backfill(logStore, hadoopConf, logPath, version, fileStatus)
    } else {
      super.backfill(logStore, hadoopConf, logPath, version, fileStatus)
    }
  }
}
object ConcurrentBackfillCommitCoordinatorClient {
  var deferBackfills = false
  var beginConcurrentBackfills = false
}

object ConcurrentBackfillCommitCoordinatorBuilder extends CommitCoordinatorBuilder {
  val batchSize = 5
  private lazy val concurrentBackfillCommitCoordinatorClient =
    ConcurrentBackfillCommitCoordinatorClient(synchronousBackfillThreshold = 2, batchSize)
  override def getName: String = "awaiting-commit-coordinator"
  override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    concurrentBackfillCommitCoordinatorClient
  }
}

/**
 * Setup (Assuming batch size = 5 & synchronousBackfillThreshold = 2):
 * - LogStore contains backfilled commits [0, 2]
 * - CommitCoordinatorClient contains unbackfilled commits [3, ...]
 * - Backfills are pending for versions [3, 5]
 *
 * Goal: Create a gap for versions [3, 5] in the LogStore and CommitCoordinatorClient listings.
 *
 * Step 1: LogStore retrieves delta files for versions [0, 2] from the file system.
 * Step 2: Wait on the latch to ensure step (1) is completed before step (3) begins.
 * Step 3: Backfill commits [3, 5] from CommitCoordinatorClient to LogStore using deferredBackfills
 * map.
 * Step 4: CommitCoordinatorClient returns commits [6, ...] (if valid).
 *
 * Test that the code correctly handles the gap in the LogStore and CommitCoordinatorClient listings
 * by making an additional call to LogStore to fetch versions [3, 5].
 */
class SnapshotManagementParallelListingSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(logStoreClassConfKey, classOf[CountDownLatchLogStore].getName)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    CommitCoordinatorProvider.registerBuilder(ConcurrentBackfillCommitCoordinatorBuilder)
    ConcurrentBackfillCommitCoordinatorClient.beginConcurrentBackfills = false
    ConcurrentBackfillCommitCoordinatorClient.deferBackfills = false
  }

  private def writeDeltaData(path: String, endVersion: Long): Unit = {
    spark.range(10).write.format("delta").save(path)
    (1L to endVersion).foreach(
      _ => spark.range(10).write.format("delta").mode("append").save(path))
  }

  private def captureUsageRecordsAndGetSnapshot(dataPath: Path): (Snapshot, Seq[UsageRecord]) = {
    var snapshot: Snapshot = null
    val records = Log4jUsageLogger.track {
      snapshot = DeltaLog.forTable(spark, dataPath).update()
    }
    (snapshot, records)
  }

  private def verifyUsageRecords(
      records: Seq[UsageRecord],
      expectedNeedAdditionalFsListingCount: Int): Unit = {
    val filteredLogs = DeltaTestUtils.filterUsageRecords(
      records, CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_ADDITIONAL_LISTING_REQUIRED)
    assert(filteredLogs.size === expectedNeedAdditionalFsListingCount)
  }

  private def verifySnapshotBackfills(snapshot: Snapshot, backfillUntilInclusive: Long): Unit = {
    snapshot.logSegment.deltas.zipWithIndex.foreach { case (delta, index) =>
      if (index <= backfillUntilInclusive) {
        verifyBackfilled(delta)
      } else {
        verifyUnbackfilled(delta)
      }
    }
  }

  /**
   * concurrentBackfills: Whether to defer backfills for versions > synchronousBackfillThreshold to
   *                      simulate concurrent backfills to test addition file-system listing.
   * tryIncludeGapAtTheEnd: Whether to include a gap in listing at end of the version range or
   *                        somewhere in the middle.
   */
  BOOLEAN_DOMAIN.foreach { concurrentBackfills =>
    BOOLEAN_DOMAIN.foreach { tryIncludeGapAtTheEnd =>
      test(
        s"Backfills are properly reconciled with concurrentBackfills: $concurrentBackfills, " +
          s"tryIncludeGapAtTheEnd: $tryIncludeGapAtTheEnd") {
        ConcurrentBackfillCommitCoordinatorClient.deferBackfills = concurrentBackfills
        val batchSize = ConcurrentBackfillCommitCoordinatorBuilder.batchSize
        val endVersion = if (tryIncludeGapAtTheEnd) { batchSize } else { batchSize + 3 }
        withSQLConf(
            COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
              ConcurrentBackfillCommitCoordinatorBuilder.getName,
            DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key -> "false") {
          withTempDir { tempDir =>
            val path = tempDir.getCanonicalPath
            val dataPath = new Path(path)

            writeDeltaData(path, endVersion)

            // Invalidate cache to ensure re-listing.
            DeltaLog.invalidateCache(spark, dataPath)
            ConcurrentBackfillCommitCoordinatorClient.beginConcurrentBackfills = true

            val (snapshot, records) = captureUsageRecordsAndGetSnapshot(dataPath)
            val expectedNeedAdditionalFsListingCount = if (concurrentBackfills) { 1 } else { 0 }
            verifyUsageRecords(records, expectedNeedAdditionalFsListingCount)
            verifySnapshotBackfills(snapshot, backfillUntilInclusive = batchSize)
          }
        }
      }
    }
  }

  test("throws exception when additional listing also can't reconcile") {
    val batchSize = ConcurrentBackfillCommitCoordinatorBuilder.batchSize
    val endVersion = batchSize + 3
    withSQLConf(
        COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
          ConcurrentBackfillCommitCoordinatorBuilder.getName,
        DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key -> "false") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        val dataPath = new Path(path)

        writeDeltaData(path, endVersion)

        // Delete 5.json to create a permanent gap between file-system i.e. [0, 4] and
        // commit-store [6, 8] which would even an additional listing won't be able to reconcile.
        val deltaLog = DeltaLog.forTable(spark, dataPath)
        deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf()).delete(
          FileNames.unsafeDeltaFile(deltaLog.logPath, batchSize), true)

        // Invalidate cache to ensure re-listing.
        DeltaLog.invalidateCache(spark, dataPath)

        val e = intercept[IllegalStateException] {
          DeltaLog.forTable(spark, dataPath).update()
        }
        assert(e.getMessage.contains("unexpectedly still requires additional file-system listing"))
      }
    }
  }
}
