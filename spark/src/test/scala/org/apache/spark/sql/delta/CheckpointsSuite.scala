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

import java.io.File
import java.net.URI
import java.util.UUID

import scala.concurrent.duration._

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions, UsageRecord}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.DeltaCommitFileProvider
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class CheckpointsSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaCheckpointTestUtils
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite {

  def testDifferentV2Checkpoints(testName: String)(f: => Unit): Unit = {
    for (checkpointFormat <- Seq(V2Checkpoint.Format.JSON.name, V2Checkpoint.Format.PARQUET.name)) {
      test(s"$testName [v2CheckpointFormat: $checkpointFormat]") {
        withSQLConf(
          DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
          DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> checkpointFormat
        ) {
          f
        }
      }
    }
  }

  /** Get V2 [[CheckpointProvider]] from the underlying deltalog snapshot */
  def getV2CheckpointProvider(
      deltaLog: DeltaLog,
      update: Boolean = true): V2CheckpointProvider = {
    val snapshot = if (update) deltaLog.update() else deltaLog.unsafeVolatileSnapshot
    snapshot.checkpointProvider match {
      case v2CheckpointProvider: V2CheckpointProvider =>
        v2CheckpointProvider
      case provider : LazyCompleteCheckpointProvider
          if provider.underlyingCheckpointProvider.isInstanceOf[V2CheckpointProvider] =>
        provider.underlyingCheckpointProvider.asInstanceOf[V2CheckpointProvider]
      case EmptyCheckpointProvider =>
        throw new IllegalStateException("underlying snapshot doesn't have a checkpoint")
      case other =>
        throw new IllegalStateException(s"The underlying checkpoint is not a v2 checkpoint. " +
          s"It is: ${other.getClass.getName}")
    }
  }

  protected override def sparkConf = {
    // Set the gs LogStore impl to `LocalLogStore` so that it will work with
    // `FakeGCSFileSystemValidatingCheckpoint`.
    // The default one is `HDFSLogStore` which requires a `FileContext` but we don't have one.
    super.sparkConf.set("spark.delta.logStore.gs.impl", classOf[LocalLogStore].getName)
  }

  private def verifyCheckpoint(
      checkpoint: Option[LastCheckpointInfo],
      version: Int,
      parts: Option[Int]): Unit = {
    assert(checkpoint.isDefined)
    checkpoint.foreach { lastCheckpointInfo =>
      assert(lastCheckpointInfo.version == version)
      assert(lastCheckpointInfo.parts == parts)
    }
  }

  test("checkpoint with DVs") {
    for (v2Checkpoint <- Seq(true, false))
    withTempDir { tempDir =>
      val source = new File(DeletionVectorsSuite.table1Path) // this table has DVs in two versions
      val targetName = s"insertTest_${UUID.randomUUID().toString.replace("-", "")}"
      val target = new File(tempDir, targetName)

      // Copy the source2 DV table to a temporary directory, so that we do updates to it
      FileUtils.copyDirectory(source, target)
      import testImplicits._

      if (v2Checkpoint) {
        spark.sql(s"ALTER TABLE delta.`${target.getAbsolutePath}` SET TBLPROPERTIES " +
          s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")
      }

      sql(s"ALTER TABLE delta.`${target.getAbsolutePath}` " +
        s"SET TBLPROPERTIES (${DeltaConfigs.CHECKPOINT_INTERVAL.key} = 10)")

      def listFiles(file: File): Seq[File] = {
        if (file.isDirectory) {
          file.listFiles().flatMap(listFiles)
        } else {
          Seq(file)
        }
      }
      // scalastyle:off println
      println("TODO: printing all files before insert")
      listFiles(target).foreach(println)
      def insertData(data: String): Unit = {
        spark.sql(s"INSERT INTO TABLE delta.`${target.getAbsolutePath}` $data")
      }
      val newData = Seq.range(3000, 3010)
      newData.foreach { i => insertData(s"VALUES($i)") }
      println("TODO: printing all files after insert")
      listFiles(target).foreach(println)

      // Check the target file has checkpoint generated
      val deltaLog = DeltaLog.forTable(spark, target.getAbsolutePath)
      verifyCheckpoint(deltaLog.readLastCheckpointFile(), version = 10, parts = None)

      // Delete the commit files 0-9, so that we are forced to read the checkpoint file
      val logPath = new Path(new File(target, "_delta_log").getAbsolutePath)
      for (i <- 0 to 9) {
        val file = new File(FileNames.unsafeDeltaFile(logPath, version = i).toString)
        file.delete()
      }

      // Make sure the contents are the same
      import testImplicits._
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`${target.getAbsolutePath}`"),
        (DeletionVectorsSuite.expectedTable1DataV4 ++ newData).toSeq.toDF())
    }
  }

  def checkIntermittentError(
      tempDir: File, lastCheckpointMissing: Boolean, crcMissing: Boolean): Unit = {
    // Create a table with commit version 0, 1 and a checkpoint.
    val tablePath = tempDir.getAbsolutePath
    spark.range(10).write.format("delta").save(tablePath)
    spark.sql(s"INSERT INTO delta.`$tablePath`" +
      s"SELECT * FROM delta.`$tablePath` WHERE id = 1").collect()

    val log = DeltaLog.forTable(spark, tablePath)
    val conf = log.newDeltaHadoopConf()
    log.checkpoint()

    // Delete _last_checkpoint based on test configuration.
    val fs = log.logPath.getFileSystem(conf)
    if (lastCheckpointMissing) {
      fs.delete(log.LAST_CHECKPOINT)
    }
    // Delete CRC file based on test configuration.
    if (crcMissing) {
      // Delete all CRC files
      (0L to log.update().version).foreach { version =>
        fs.delete(FileNames.checksumFile(log.logPath, version))
      }
    }

    // In order to trigger an intermittent failure while reading checkpoint, this test corrupts
    // the checkpoint temporarily so that json/parquet checkpoint reader fails. The corrupted
    // file is written with same length so that when the file is uncorrupted in future, then we
    // can test that delta is able to read that file and produce correct results. If the "bad" file
    // is not of same length, then the read with "good" file will also fail as parquet reader will
    // use the cache file status's getLen to find out where the footer is and will fail after not
    // finding the magic bytes.
    val checkpointFileStatus =
    log.listFrom(0).filter(FileNames.isCheckpointFile).toSeq.head
    // Rename the correct checkpoint to a temp path and create a checkpoint with character 'r'
    // repeated.
    val tempPath = checkpointFileStatus.getPath.suffix(".temp")
    fs.rename(checkpointFileStatus.getPath, tempPath)
    val randomContentToWrite = Seq("r" * (checkpointFileStatus.getLen.toInt - 1)) // + 1 (\n)
    log.store.write(
      checkpointFileStatus.getPath, randomContentToWrite.toIterator, overwrite = true, conf)
    assert(log.store.read(checkpointFileStatus.getPath, conf) === randomContentToWrite)
    assert(fs.getFileStatus(tempPath).getLen === checkpointFileStatus.getLen)

    DeltaLog.clearCache()
    if (!crcMissing) {
      // When CRC is present, then P&M will be taken from CRC and snapshot will be initialized
      // without needing a checkpoint. But the underlying checkpoint provider points to a
      // corrupted checkpoint and so any query/state reconstruction on this will fail.
      intercept[Exception] {
        sql(s"SELECT * FROM delta.`$tablePath`").collect()
        DeltaLog.forTable(spark, tablePath).unsafeVolatileSnapshot.validateChecksum()
      }
      val snapshot = DeltaLog.forTable(spark, tablePath).unsafeVolatileSnapshot
      intercept[Exception] {
        snapshot.allFiles.collect()
      }
      // Undo the corruption
      assert(fs.delete(checkpointFileStatus.getPath, true))
      assert(fs.rename(tempPath, checkpointFileStatus.getPath))

      // Once the corruption in undone, then the queries starts passing on top of same snapshot.
      // This tests that we have not caches the intermittent error in the underlying checkpoint
      // provider.
      sql(s"SELECT * FROM delta.`$tablePath`").collect()
      assert(DeltaLog.forTable(spark, tablePath).update() === snapshot)
      return
    }
    // When CRC is missing, then P&M will be taken from checkpoint which is temporarily
    // corrupted, so we will end up creating a new snapshot without using checkpoint and the
    // query will succeed.
    sql(s"SELECT * FROM delta.`$tablePath`").collect()
    val snapshot = DeltaLog.forTable(spark, tablePath).unsafeVolatileSnapshot
    snapshot.computeChecksum
    snapshot.validateChecksum()
    assert(snapshot.checkpointProvider.isEmpty)
  }


  /**
   * Writes all actions in the top-level file of a new V2 Checkpoint. No sidecar files are
   * written.
   */
  private def writeAllActionsInV2Manifest(
      snapshot: Snapshot,
      v2CheckpointFormat: V2Checkpoint.Format): Path = {
    snapshot.ensureCommitFilesBackfilled()
    val checkpointMetadata = CheckpointMetadata(version = snapshot.version)
    val actionsDS = snapshot.stateDS
      .where("checkpointMetadata is null and " +
        "commitInfo is null and cdc is null and sidecar is null")
      .union(spark.createDataset(Seq(checkpointMetadata.wrap)))
      .toDF()

    val actionsToWrite = Checkpoints
      .buildCheckpoint(actionsDS, snapshot)
      .as[SingleAction]
      .collect()
      .toSeq
      .map(_.unwrap)

    val deltaLog = snapshot.deltaLog
    val (v2CheckpointPath, _) =
      if (v2CheckpointFormat == V2Checkpoint.Format.JSON) {
        val v2CheckpointPath =
          FileNames.newV2CheckpointJsonFile(deltaLog.logPath, snapshot.version)
        deltaLog.store.write(
          v2CheckpointPath,
          actionsToWrite.map(_.json).toIterator,
          overwrite = true,
          hadoopConf = deltaLog.newDeltaHadoopConf())
        (v2CheckpointPath, None)
      } else if (v2CheckpointFormat == V2Checkpoint.Format.PARQUET) {
        val sparkSession = spark
        // scalastyle:off sparkimplicits
        import sparkSession.implicits._
        // scalastyle:on sparkimplicits
        val dfToWrite = actionsToWrite.map(_.wrap).toDF()
        val v2CheckpointPath =
          FileNames.newV2CheckpointParquetFile(deltaLog.logPath, snapshot.version)
        val schemaOfDfWritten =
          Checkpoints.createCheckpointV2ParquetFile(
            spark,
            dfToWrite,
            v2CheckpointPath,
            deltaLog.newDeltaHadoopConf(),
            false)
        (v2CheckpointPath, Some(schemaOfDfWritten))
      } else {
        throw DeltaErrors.assertionFailedError(
          s"Unrecognized checkpoint V2 format: $v2CheckpointFormat")
      }
    v2CheckpointPath
  }
}

class OverwriteTrackingLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends LocalLogStore(sparkConf, hadoopConf) {

  var fileToOverwriteCount: Map[Path, Long] = Map[Path, Long]()

  private var isPartialWriteVisibleBool: Boolean = false
  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): Boolean =
    isPartialWriteVisibleBool

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    val toAdd = if (overwrite) 1 else 0
    fileToOverwriteCount += path -> (fileToOverwriteCount.getOrElse(path, 0L) + toAdd)
    super.write(path, actions, overwrite, hadoopConf)
  }

  def clearCounts(): Unit = {
    fileToOverwriteCount = Map[Path, Long]()
  }

  def setPartialWriteVisible(isPartialWriteVisibleBool: Boolean): Unit = {
    this.isPartialWriteVisibleBool = isPartialWriteVisibleBool
  }
}

class V2CheckpointManifestOverwriteSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaCheckpointTestUtils
  with DeltaSQLCommandTest {
  protected override def sparkConf = {
    // Set the logStore to OverwriteTrackingLogStore.
    super.sparkConf
      .set("spark.delta.logStore.class", classOf[OverwriteTrackingLogStore].getName)
      .set(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key, V2Checkpoint.Format.JSON.name)
  }
  for (isPartialWriteVisible <- BOOLEAN_DOMAIN)
  test("v2 checkpoint manifest write should use the logstore.write(overwrite) API correctly " +
      s"isPartialWriteVisible = $isPartialWriteVisible") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      // Create a simple table with V2 checkpoints enabled and json manifest.
      spark.range(10).write.format("delta").save(tablePath)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      spark.sql(s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES " +
          s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = 'v2')")
      val store = deltaLog.store.asInstanceOf[OverwriteTrackingLogStore]

      store.clearCounts()
      store.setPartialWriteVisible(isPartialWriteVisible)
      deltaLog.checkpoint()

      val snapshot = deltaLog.update()
      assert(snapshot.checkpointProvider.version == 1)
      // Two writes will use logStore.write:
      // 1. Checkpoint Manifest
      // 2. LAST_CHECKPOINT.
      assert(store.fileToOverwriteCount.size == 2)
      val manifestWriteRecord = store.fileToOverwriteCount.find {
        case (path, _) => FileNames.isCheckpointFile(path)
      }.getOrElse(fail("expected checkpoint manifest write using logStore.write"))
      val numOverwritesExpected = if (isPartialWriteVisible) 0 else 1
      assert(manifestWriteRecord._2 == numOverwritesExpected)
    }
  }
}

/** A fake GCS file system to verify delta checkpoints are written in a separate gcs thread. */
class FakeGCSFileSystemValidatingCheckpoint extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs:/")

  protected def shouldValidateFilePattern(f: Path): Boolean = f.getName.contains(".checkpoint")

  protected def assertGCSThread(f: Path): Unit = {
    if (shouldValidateFilePattern(f)) {
      assert(
        Thread.currentThread().getName.contains("delta-gcs-"),
        s"writing $f was happening in non gcs thread: ${Thread.currentThread()}")
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}

/** A fake GCS file system to verify delta commits are written in a separate gcs thread. */
class FakeGCSFileSystemValidatingCommits extends FakeGCSFileSystemValidatingCheckpoint {
  override protected def shouldValidateFilePattern(f: Path): Boolean = f.getName.contains(".json")
}

class CheckpointsWithCoordinatedCommitsBatch1Suite extends CheckpointsSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

class CheckpointsWithCoordinatedCommitsBatch2Suite extends CheckpointsSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)
}

class CheckpointsWithCoordinatedCommitsBatch100Suite extends CheckpointsSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)
}

