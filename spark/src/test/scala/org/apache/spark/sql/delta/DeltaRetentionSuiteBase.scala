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
import java.util.{Calendar, TimeZone}

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{CheckpointMetadata, Metadata, SidecarFile}
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.{newV2CheckpointJsonFile, newV2CheckpointParquetFile}
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ManualClock

trait DeltaRetentionSuiteBase extends QueryTest
  with SharedSparkSession
  with CoordinatedCommitsBaseSuite {
  protected val testOp = Truncate()

  protected override def sparkConf: SparkConf = super.sparkConf
    // Disable the log cleanup because it runs asynchronously and causes test flakiness
    .set("spark.databricks.delta.properties.defaults.enableExpiredLogCleanup", "false")

  protected def intervalStringToMillis(str: String): Long = {
    DeltaConfigs.getMilliSeconds(
      IntervalUtils.safeStringToInterval(UTF8String.fromString(str)))
  }

  /**
   * Returns milliseconds since epoch at 1:00am UTC of current day.
   *
   * Context:
   * Most DeltaRetentionSuite tests rely on ManualClock to time travel and
   * trigger metadata cleanup. Cleanup boundaries are determined by
   * finding files that were modified before 00:00 of the day on which
   * currentTime-LOG_RETENTION_PERIOD falls. This means that for a long running
   * test started at 23:59, the number of expired files would jump suddenly
   * in 1 minute (the expiration boundary would move by a day as soon as
   * system clock hits 00:00 of the next day). By fixing the start time of the
   * test to 01:00, we avoid these scenarios.
   *
   * This would still break if the test runs for more than 23 hours.
   */
  protected def getStartTimeForRetentionTest: Long = {
    val currentTime = System.currentTimeMillis()
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(currentTime)
    val dayStartTimeStamp = DateUtils.truncate(date, Calendar.DAY_OF_MONTH)
    dayStartTimeStamp.add(Calendar.HOUR_OF_DAY, 1);
    dayStartTimeStamp.getTimeInMillis
  }

  protected def getDeltaFiles(dir: File): Seq[File] =
    dir.listFiles().filter(f => FileNames.isDeltaFile(new Path(f.getCanonicalPath)))

  protected def getCheckpointFiles(dir: File): Seq[File] =
    dir.listFiles().filter(f => FileNames.isCheckpointFile(new Path(f.getCanonicalPath)))

  protected def getLogFiles(dir: File): Seq[File]

  protected def getFileVersions(files: Seq[File]): Set[Long] = {
    files.map(f => f.getName()).map(s => s.substring(0, s.indexOf(".")).toLong).toSet
  }

  protected def getDeltaVersions(dir: File): Set[Long] = {
    val backfilledDeltaVersions = getFileVersions(getDeltaFiles(dir))
    val unbackfilledDeltaVersions = getUnbackfilledDeltaVersions(dir)
    if (coordinatedCommitsEnabledInTests) {
      // The unbackfilled commit files (except commit 0) should be a superset of the backfilled
      // commit files since they're always deleted together in this suite.
      assert(
        unbackfilledDeltaVersions.toArray.sorted.startsWith(
          backfilledDeltaVersions.filter(_ != 0).toArray.sorted))
    }
    backfilledDeltaVersions
  }

  protected def getUnbackfilledDeltaFiles(dir: File): Seq[File] = {
    val commitDirPath = FileNames.commitDirPath(new Path(dir.toURI))
    getDeltaFiles(new File(commitDirPath.toUri))
  }

  protected def getUnbackfilledDeltaVersions(dir: File): Set[Long] =
    getFileVersions(getUnbackfilledDeltaFiles(dir))

  protected def getSidecarFiles(log: DeltaLog): Set[String] = {
    new java.io.File(log.sidecarDirPath.toUri)
      .listFiles()
      .filter(_.getName.endsWith(".parquet"))
      .map(_.getName)
      .toSet
  }

  protected def getCheckpointVersions(dir: File): Set[Long] = {
    getFileVersions(getCheckpointFiles(dir))
  }

  /** Compares the given versions with expected and generates a nice error message. */
  protected def compareVersions(
      versions: Set[Long],
      logType: String,
      expected: Iterable[Int]): Unit = {
    val expectedSet = expected.map(_.toLong).toSet
    val deleted = expectedSet -- versions
    val notDeleted = versions -- expectedSet
    if (!(deleted.isEmpty && notDeleted.isEmpty)) {
      fail(s"""Mismatch in log clean up for ${logType}s:
           |Shouldn't be deleted but deleted: ${deleted.toArray.sorted.mkString("[", ", ", "]")}
           |Should be deleted but not: ${notDeleted.toArray.sorted.mkString("[", ", ", "]")}
         """.stripMargin)
    }
  }

  // Set modification time of the new files in _delta_log directory and mark them as visited.
  def setModificationTimeOfNewFiles(
      log: DeltaLog,
      clock: ManualClock,
      visitedFiled: mutable.Set[String]): Unit = {
    val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
    val allFiles = fs.listFiles(log.logPath, true)
    while (allFiles.hasNext) {
      val file = allFiles.next()
      if (!visitedFiled.contains(file.getPath.toString)) {
        visitedFiled += file.getPath.toString
        fs.setTimes(file.getPath, clock.getTimeMillis(), 0)
      }
    }
  }

  protected def setModificationTime(
      log: DeltaLog,
      startTime: Long,
      version: Int,
      dayNum: Int,
      fs: FileSystem,
      checkpointOnly: Boolean = false): Unit = {
    val logDir = log.logPath.toUri.toString
    val paths = log
      .listFrom(version)
      .collect { case FileNames.CheckpointFile(f, v) if v == version => f.getPath }
      .toSeq
    paths.foreach { cpPath =>
      // Add some second offset so that we don't have files with same timestamps
      fs.setTimes(cpPath, day(startTime, dayNum) + version * 1000, 0)
    }
    if (!checkpointOnly) {
      val deltaPath = new Path(logDir + f"/$version%020d.json")
      if (fs.exists(deltaPath)) {
        // Add some second offset so that we don't have files with same timestamps
        fs.setTimes(deltaPath, day(startTime, dayNum) + version * 1000, 0)
      }
      // Add the same timestamp for unbackfilled delta files as well
      fs.listStatus(FileNames.commitDirPath(log.logPath))
        .find(_.getPath.getName.startsWith(f"$version%020d"))
        .foreach(f => fs.setTimes(f.getPath, day(startTime, dayNum) + version * 1000, 0))
    }
  }

  protected def day(startTime: Long, day: Int): Long =
    startTime + intervalStringToMillis(s"interval $day days")

  // Create a sidecar file with given AddFiles inside it.
  protected def createSidecarFile(log: DeltaLog, files: Seq[Int]): String = {
    val sparkSession = spark
    // scalastyle:off sparkimplicits
    import sparkSession.implicits._
    // scalastyle:on sparkimplicits
    var sidecarFileName: String = ""
    withTempDir { dir =>
      val adds = files.map(i => createTestAddFile(i.toString))
      adds.map(_.wrap).toDF.repartition(1).write.mode("overwrite").parquet(dir.getAbsolutePath)
      val srcPath =
        new Path(dir.listFiles().filter(_.getName.endsWith("parquet")).head.getAbsolutePath)
      val dstPath = new Path(log.sidecarDirPath, srcPath.getName)
      val fs = srcPath.getFileSystem(log.newDeltaHadoopConf())
      fs.mkdirs(log.sidecarDirPath)
      fs.rename(srcPath, dstPath)
      sidecarFileName = fs.getFileStatus(dstPath).getPath.getName
    }
    sidecarFileName
  }

  // Create a V2 Checkpoint at given version with given sidecar files.
  protected def createV2CheckpointWithSidecarFile(
      log: DeltaLog,
      version: Long,
      sidecarFileNames: Seq[String]): Unit = {
    val hadoopConf = log.newDeltaHadoopConf()
    val fs = log.logPath.getFileSystem(hadoopConf)
    val sidecarFiles = sidecarFileNames.map { fileName =>
      val sidecarPath = new Path(log.sidecarDirPath, fileName)
      val fileStatus = SerializableFileStatus.fromStatus(fs.getFileStatus(sidecarPath))
      SidecarFile(fileStatus)
    }
    val snapshot = log.getSnapshotAt(version)
    val actionsForCheckpoint =
      snapshot.nonFileActions ++ sidecarFiles :+ CheckpointMetadata(version)
    val v2CheckpointFormat =
      spark.conf.getOption(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key)
    v2CheckpointFormat match {
      case Some(V2Checkpoint.Format.JSON.name) | None =>
        log.store.write(
          newV2CheckpointJsonFile(log.logPath, version),
          actionsForCheckpoint.map(_.json).toIterator,
          overwrite = true,
          hadoopConf = hadoopConf)
      case Some(V2Checkpoint.Format.PARQUET.name) =>
        val parquetFile = newV2CheckpointParquetFile(log.logPath, version)
        val sparkSession = spark
        // scalastyle:off sparkimplicits
        import sparkSession.implicits._
        // scalastyle:on sparkimplicits
        val dfToWrite = actionsForCheckpoint.map(_.wrap).toDF
        Checkpoints.createCheckpointV2ParquetFile(
          spark,
          dfToWrite,
          parquetFile,
          hadoopConf,
          useRename = false)
      case _ =>
        assert(false, "Invalid v2 checkpoint format")
    }
    log.writeLastCheckpointFile(
      log,
      LastCheckpointInfo(version, -1, None, None, None, None),
      false)
  }

  /**
   * Start a txn that disables automatic log cleanup. Some tests may need to manually clean up logs
   * to get deterministic behaviors.
   */
  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
    val txn = log.startTransaction()
    // This will pick up `spark.databricks.delta.properties.defaults.enableExpiredLogCleanup` to
    // disable log cleanup.
    txn.updateMetadata(Metadata())
    txn
  }

  test("startTxnWithManualLogCleanup") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      startTxnWithManualLogCleanup(log).commit(Nil, testOp)
      assert(!log.enableExpiredLogCleanup())
    }
  }
}
