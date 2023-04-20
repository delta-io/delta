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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.DeltaFileOperations.tryDeleteNonRecursive
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.util.{Clock, SerializableConfiguration, SystemClock}

/**
 * Vacuums the table by clearing all untracked files and folders within this table.
 * First lists all the files and directories in the table, and gets the relative paths with
 * respect to the base of the table. Then it gets the list of all tracked files for this table,
 * which may or may not be within the table base path, and gets the relative paths of
 * all the tracked files with respect to the base of the table. Files outside of the table path
 * will be ignored. Then we take a diff of the files and delete directories that were already empty,
 * and all files that are within the table that are no longer tracked.
 */
object VacuumCommand extends VacuumCommandImpl with Serializable {

  case class FileNameAndSize(path: String, length: Long)
  /**
   * Additional check on retention duration to prevent people from shooting themselves in the foot.
   */
  protected def checkRetentionPeriodSafety(
      spark: SparkSession,
      retentionMs: Option[Long],
      configuredRetention: Long): Unit = {
    require(retentionMs.forall(_ >= 0), "Retention for Vacuum can't be less than 0.")
    val checkEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED)
    val retentionSafe = retentionMs.forall(_ >= configuredRetention)
    var configuredRetentionHours = TimeUnit.MILLISECONDS.toHours(configuredRetention)
    if (TimeUnit.HOURS.toMillis(configuredRetentionHours) < configuredRetention) {
      configuredRetentionHours += 1
    }
    require(!checkEnabled || retentionSafe,
      s"""Are you sure you would like to vacuum files with such a low retention period? If you have
        |writers that are currently writing to this table, there is a risk that you may corrupt the
        |state of your Delta table.
        |
        |If you are certain that there are no operations being performed on this table, such as
        |insert/upsert/delete/optimize, then you may turn off this check by setting:
        |spark.databricks.delta.retentionDurationCheck.enabled = false
        |
        |If you are not sure, please use a value not less than "$configuredRetentionHours hours".
       """.stripMargin)
  }

  /**
   * Clears all untracked files and folders within this table. First lists all the files and
   * directories in the table, and gets the relative paths with respect to the base of the
   * table. Then it gets the list of all tracked files for this table, which may or may not
   * be within the table base path, and gets the relative paths of all the tracked files with
   * respect to the base of the table. Files outside of the table path will be ignored.
   * Then we take a diff of the files and delete directories that were already empty, and all files
   * that are within the table that are no longer tracked.
   *
   * @param dryRun If set to true, no files will be deleted. Instead, we will list all files and
   *               directories that will be cleared.
   * @param retentionHours An optional parameter to override the default Delta tombstone retention
   *                       period
   * @return A Dataset containing the paths of the files/folders to delete in dryRun mode. Otherwise
   *         returns the base path of the table.
   */
  def gc(
      spark: SparkSession,
      deltaLog: DeltaLog,
      dryRun: Boolean = true,
      retentionHours: Option[Double] = None,
      clock: Clock = new SystemClock): DataFrame = {
    recordDeltaOperation(deltaLog, "delta.gc") {

      val path = deltaLog.dataPath
      val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
      val fs = path.getFileSystem(deltaHadoopConf)

      import org.apache.spark.sql.delta.implicits._

      val snapshot = deltaLog.update()

      require(snapshot.version >= 0, "No state defined for this table. Is this really " +
        "a Delta table? Refusing to garbage collect.")

      DeletionVectorUtils.assertDeletionVectorsNotReadable(
        spark, snapshot.metadata, snapshot.protocol)

      val snapshotTombstoneRetentionMillis = DeltaLog.tombstoneRetentionMillis(snapshot.metadata)
      val retentionMillis = retentionHours.map(h => TimeUnit.HOURS.toMillis(math.round(h)))
      checkRetentionPeriodSafety(spark, retentionMillis, snapshotTombstoneRetentionMillis)

      val deleteBeforeTimestamp = retentionMillis.map { millis =>
        clock.getTimeMillis() - millis
      }.getOrElse(snapshot.minFileRetentionTimestamp)
      logInfo(s"Starting garbage collection (dryRun = $dryRun) of untracked files older than " +
        s"${new Date(deleteBeforeTimestamp).toGMTString} in $path")
      val hadoopConf = spark.sparkContext.broadcast(
        new SerializableConfiguration(deltaHadoopConf))
      val basePath = fs.makeQualified(path).toString
      var isBloomFiltered = false
      val parallelDeleteEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_ENABLED)
      val parallelDeletePartitions =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_PARALLELISM)
        .getOrElse(spark.sessionState.conf.numShufflePartitions)
      val relativizeIgnoreError =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR)
      val startTimeToIdentifyEligibleFiles = System.currentTimeMillis()
      val validFiles = snapshot.stateDS
        .mapPartitions { actions =>
          val reservoirBase = new Path(basePath)
          val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
          actions.flatMap {
            _.unwrap match {
              case tombstone: RemoveFile if tombstone.delTimestamp < deleteBeforeTimestamp =>
                Nil
              case fa: FileAction =>
                getValidRelativePathsAndSubdirs(
                  fa,
                  fs,
                  reservoirBase,
                  relativizeIgnoreError,
                  isBloomFiltered)
              case _ => Nil
            }
          }
        }.toDF("path")

      val partitionColumns = snapshot.metadata.partitionSchema.fieldNames
      val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism

      val allFilesAndDirs = DeltaFileOperations.recursiveListDirs(
          spark,
          Seq(basePath),
          hadoopConf,
          hiddenDirNameFilter = DeltaTableUtils.isHiddenDirectory(partitionColumns, _),
          hiddenFileNameFilter = DeltaTableUtils.isHiddenDirectory(partitionColumns, _),
          fileListingParallelism = Option(parallelism)
        )
        .groupByKey(_.path)
        .mapGroups { (k, v) =>
          val duplicates = v.toSeq
          // of all the duplicates we can return the newest file.
          duplicates.maxBy(_.modificationTime)
        }

      try {
        allFilesAndDirs.cache()

        implicit val fileNameAndSizeEncoder = org.apache.spark.sql.Encoders.product[FileNameAndSize]

        val dirCounts = allFilesAndDirs.where(col("isDir")).count() + 1 // +1 for the base path

        // The logic below is as follows:
        //   1. We take all the files and directories listed in our reservoir
        //   2. We filter all files older than our tombstone retention period and directories
        //   3. We get the subdirectories of all files so that we can find non-empty directories
        //   4. We groupBy each path, and count to get how many files are in each sub-directory
        //   5. We subtract all the valid files and tombstones in our state
        //   6. We filter all paths with a count of 1, which will correspond to files not in the
        //      state, and empty directories. We can safely delete all of these
        val diff = allFilesAndDirs
          .where(col("modificationTime") < deleteBeforeTimestamp || col("isDir"))
          .mapPartitions { fileStatusIterator =>
            val reservoirBase = new Path(basePath)
            val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
            fileStatusIterator.flatMap { fileStatus =>
              if (fileStatus.isDir) {
                Iterator.single(FileNameAndSize(
                  relativize(fileStatus.getHadoopPath, fs, reservoirBase, isDir = true), 0L))
              } else {
                val dirs = getAllSubdirs(basePath, fileStatus.path, fs)
                val dirsWithSlash = dirs.map { p =>
                  val relativizedPath = relativize(new Path(p), fs, reservoirBase, isDir = true)
                  FileNameAndSize(relativizedPath, 0L)
                }
                dirsWithSlash ++ Iterator(
                  FileNameAndSize(relativize(
                    fileStatus.getHadoopPath, fs, reservoirBase, isDir = false), fileStatus.length))
              }
            }
          }.groupBy(col("path")).agg(count(new Column("*")).as("count"), sum("length").as("length"))
          .join(validFiles, Seq("path"), "leftanti")
          .where(col("count") === 1)


        val sizeOfDataToDeleteRow = diff.agg(sum("length").cast("long")).first
        val sizeOfDataToDelete = if (sizeOfDataToDeleteRow.isNullAt(0)) {
          0L
        } else {
          sizeOfDataToDeleteRow.getLong(0)
        }

        val diffFiles = diff
          .select(col("path"))
          .as[String]
          .map { relativePath =>
            assert(!stringToPath(relativePath).isAbsolute,
              "Shouldn't have any absolute paths for deletion here.")
            pathToString(DeltaFileOperations.absolutePath(basePath, relativePath))
          }
        val timeTakenToIdentifyEligibleFiles =
          System.currentTimeMillis() - startTimeToIdentifyEligibleFiles

        val numFiles = diffFiles.count()
        if (dryRun) {
          val stats = DeltaVacuumStats(
            isDryRun = true,
            specifiedRetentionMillis = retentionMillis,
            defaultRetentionMillis = snapshotTombstoneRetentionMillis,
            minRetainedTimestamp = deleteBeforeTimestamp,
            dirsPresentBeforeDelete = dirCounts,
            objectsDeleted = numFiles,
            sizeOfDataToDelete = sizeOfDataToDelete,
            timeTakenToIdentifyEligibleFiles = timeTakenToIdentifyEligibleFiles,
            timeTakenForDelete = 0L)

          recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
          logConsole(s"Found $numFiles files ($sizeOfDataToDelete bytes) and directories in " +
            s"a total of $dirCounts directories that are safe to delete.")

          return diffFiles.map(f => stringToPath(f).toString).toDF("path")
        }
        logVacuumStart(
          spark,
          deltaLog,
          path,
          diffFiles,
          sizeOfDataToDelete,
          retentionMillis,
          snapshotTombstoneRetentionMillis)

        val deleteStartTime = System.currentTimeMillis()
        val filesDeleted = try {
          delete(diffFiles, spark, basePath,
            hadoopConf, parallelDeleteEnabled, parallelDeletePartitions)
        } catch {
          case t: Throwable =>
            logVacuumEnd(deltaLog, spark, path)
            throw t
        }
        val timeTakenForDelete = System.currentTimeMillis() - deleteStartTime
        val stats = DeltaVacuumStats(
          isDryRun = false,
          specifiedRetentionMillis = retentionMillis,
          defaultRetentionMillis = snapshotTombstoneRetentionMillis,
          minRetainedTimestamp = deleteBeforeTimestamp,
          dirsPresentBeforeDelete = dirCounts,
          objectsDeleted = filesDeleted,
          sizeOfDataToDelete = sizeOfDataToDelete,
          timeTakenToIdentifyEligibleFiles = timeTakenToIdentifyEligibleFiles,
          timeTakenForDelete = timeTakenForDelete)
        recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
        logVacuumEnd(deltaLog, spark, path, Some(filesDeleted), Some(dirCounts))


        spark.createDataset(Seq(basePath)).toDF("path")
      } finally {
        allFilesAndDirs.unpersist()
      }
    }
  }
}

trait VacuumCommandImpl extends DeltaCommand {

  private val supportedFsForLogging = Seq(
    "wasbs", "wasbss", "abfs", "abfss", "adl", "gs", "file", "hdfs"
  )

  /**
   * Returns whether we should record vacuum metrics in the delta log.
   */
  private def shouldLogVacuum(
      spark: SparkSession,
      deltaLog: DeltaLog,
      hadoopConf: Configuration,
      path: Path): Boolean = {
    val logVacuumConf = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_LOGGING_ENABLED)

    if (logVacuumConf.nonEmpty) {
      return logVacuumConf.get
    }

    val logStore = deltaLog.store

    try {
      val rawResolvedUri: URI = logStore.resolvePathOnPhysicalStorage(path, hadoopConf).toUri
      val scheme = rawResolvedUri.getScheme
      supportedFsForLogging.contains(scheme)
    } catch {
      case _: UnsupportedOperationException =>
        logWarning("Vacuum event logging" +
          " not enabled on this file system because we cannot detect your cloud storage type.")
        false
    }
  }

  /**
   * Record Vacuum specific metrics in the commit log at the START of vacuum.
   *
   * @param spark - spark session
   * @param deltaLog - DeltaLog of the table
   * @param path - the (data) path to the root of the table
   * @param diff - the list of paths (files, directories) that are safe to delete
   * @param sizeOfDataToDelete - the amount of data (bytes) to be deleted
   * @param specifiedRetentionMillis - the optional override retention period (millis) to keep
   *                                   logically removed files before deleting them
   * @param defaultRetentionMillis - the default retention period (millis)
   */
  protected def logVacuumStart(
      spark: SparkSession,
      deltaLog: DeltaLog,
      path: Path,
      diff: Dataset[String],
      sizeOfDataToDelete: Long,
      specifiedRetentionMillis: Option[Long],
      defaultRetentionMillis: Long): Unit = {
    logInfo(s"Deleting untracked files and empty directories in $path. The amount of data to be " +
      s"deleted is $sizeOfDataToDelete (in bytes)")

    // We perform an empty commit in order to record information about the Vacuum
    if (shouldLogVacuum(spark, deltaLog, deltaLog.newDeltaHadoopConf(), path)) {
      val checkEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED)
      val txn = deltaLog.startTransaction()
      val metrics = Map[String, SQLMetric](
        "numFilesToDelete" -> createMetric(spark.sparkContext, "number of files to deleted"),
        "sizeOfDataToDelete" -> createMetric(spark.sparkContext,
          "The total amount of data to be deleted in bytes")
      )
      metrics("numFilesToDelete").set(diff.count())
      metrics("sizeOfDataToDelete").set(sizeOfDataToDelete)
      txn.registerSQLMetrics(spark, metrics)
      txn.commit(actions = Seq(), DeltaOperations.VacuumStart(
        checkEnabled,
        specifiedRetentionMillis,
        defaultRetentionMillis
      ))
    }
  }

  /**
   * Record Vacuum specific metrics in the commit log at the END of vacuum.
   *
   * @param deltaLog - DeltaLog of the table
   * @param spark - spark session
   * @param path - the (data) path to the root of the table
   * @param filesDeleted - if the vacuum completed this will contain the number of files deleted.
   *                       if the vacuum failed, this will be None.
   * @param dirCounts - if the vacuum completed this will contain the number of directories
   *                    vacuumed. if the vacuum failed, this will be None.
   */
  protected def logVacuumEnd(
      deltaLog: DeltaLog,
      spark: SparkSession,
      path: Path,
      filesDeleted: Option[Long] = None,
      dirCounts: Option[Long] = None): Unit = {
    if (shouldLogVacuum(spark, deltaLog, deltaLog.newDeltaHadoopConf(), path)) {
      val txn = deltaLog.startTransaction()
      val status = if (filesDeleted.isEmpty && dirCounts.isEmpty) { "FAILED" } else { "COMPLETED" }
      if (filesDeleted.nonEmpty && dirCounts.nonEmpty) {
        val metrics = Map[String, SQLMetric](
          "numDeletedFiles" -> createMetric(spark.sparkContext, "number of files deleted."),
          "numVacuumedDirectories" ->
            createMetric(spark.sparkContext, "num of directories vacuumed."),
          "status" -> createMetric(spark.sparkContext, "status of vacuum")
        )
        metrics("numDeletedFiles").set(filesDeleted.get)
        metrics("numVacuumedDirectories").set(dirCounts.get)
        txn.registerSQLMetrics(spark, metrics)
      }
      txn.commit(actions = Seq(), DeltaOperations.VacuumEnd(
        status
      ))
    }

    if (filesDeleted.nonEmpty) {
      logConsole(s"Deleted ${filesDeleted.get} files and directories in a total " +
        s"of ${dirCounts.get} directories.")
    }
  }

  /**
   * Attempts to relativize the `path` with respect to the `reservoirBase` and converts the path to
   * a string.
   */
  protected def relativize(
      path: Path,
      fs: FileSystem,
      reservoirBase: Path,
      isDir: Boolean): String = {
    pathToString(DeltaFileOperations.tryRelativizePath(fs, reservoirBase, path))
  }

  /**
   * Wrapper function for DeltaFileOperations.getAllSubDirectories
   * returns all subdirectories that `file` has with respect to `base`.
   */
  protected def getAllSubdirs(base: String, file: String, fs: FileSystem): Iterator[String] = {
    DeltaFileOperations.getAllSubDirectories(base, file)._1
  }

  /**
   * Attempts to delete the list of candidate files. Returns the number of files deleted.
   */
  protected def delete(
      diff: Dataset[String],
      spark: SparkSession,
      basePath: String,
      hadoopConf: Broadcast[SerializableConfiguration],
      parallel: Boolean,
      parallelPartitions: Int): Long = {
    import org.apache.spark.sql.delta.implicits._

    if (parallel) {
      diff.repartition(parallelPartitions).mapPartitions { files =>
        val fs = new Path(basePath).getFileSystem(hadoopConf.value.value)
        val filesDeletedPerPartition =
          files.map(p => stringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
        Iterator(filesDeletedPerPartition)
      }.collect().sum
    } else {
      val fs = new Path(basePath).getFileSystem(hadoopConf.value.value)
      val fileResultSet = diff.toLocalIterator().asScala
      fileResultSet.map(p => stringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
    }
  }

  // scalastyle:off pathfromuri
  protected def stringToPath(path: String): Path = new Path(new URI(path))
  // scalastyle:on pathfromuri

  protected def pathToString(path: Path): String = path.toUri.toString

  /** Returns the relative path of a file action or None if the file lives outside of the table. */
  protected def getActionRelativePath(
      action: FileAction,
      fs: FileSystem,
      basePath: Path,
      relativizeIgnoreError: Boolean): Option[String] = {
    val filePath = stringToPath(action.path)
    if (filePath.isAbsolute) {
      val maybeRelative =
        DeltaFileOperations.tryRelativizePath(fs, basePath, filePath, relativizeIgnoreError)
      if (maybeRelative.isAbsolute) {
        // This file lives outside the directory of the table.
        None
      } else {
        Some(pathToString(maybeRelative))
      }
    } else {
      Some(pathToString(filePath))
    }
  }


  /**
   * Returns the relative paths of all files and subdirectories for this action that must be
   * retained during GC.
   */
  protected def getValidRelativePathsAndSubdirs(
      action: FileAction,
      fs: FileSystem,
      basePath: Path,
      relativizeIgnoreError: Boolean,
      isBloomFiltered: Boolean): Seq[String] = {
    val paths = getActionRelativePath(action, fs, basePath, relativizeIgnoreError)
      .map {
        relativePath =>
        Seq(relativePath) ++ getAllSubdirs("/", relativePath, fs)
      }.getOrElse(Seq.empty)

    val deletionVectorPath =
      getDeletionVectorRelativePath(action).map(pathToString)

    paths ++ deletionVectorPath.toSeq
  }

  /**
   * Returns the path of the on-disk deletion vector if it is stored relative to the
   * `basePath` otherwise `None`.
   */
  protected def getDeletionVectorRelativePath(action: FileAction): Option[Path] = {
    val dv = action match {
      case a: AddFile if a.deletionVector != null =>
        Some(a.deletionVector)
      case r: RemoveFile if r.deletionVector != null =>
        Some(r.deletionVector)
      case _ => None
    }

    dv match {
      case Some(dv) if dv.isOnDisk =>
        if (dv.isRelative) {
          // We actually want a relative path here.
          Some(dv.absolutePath(new Path(".")))
        } else {
          assert(dv.isAbsolute)
          // This is never going to be a path relative to `basePath` for DVs.
          None
        }
      case None => None
    }
  }
}

case class DeltaVacuumStats(
    isDryRun: Boolean,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    specifiedRetentionMillis: Option[Long],
    defaultRetentionMillis: Long,
    minRetainedTimestamp: Long,
    dirsPresentBeforeDelete: Long,
    objectsDeleted: Long,
    sizeOfDataToDelete: Long,
    timeTakenToIdentifyEligibleFiles: Long,
    timeTakenForDelete: Long)
