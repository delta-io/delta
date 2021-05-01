/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.VacuumCommand.logInfo
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.DeltaFileOperations.tryDeleteNonRecursive
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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
      val sessionHadoopConf = spark.sessionState.newHadoopConf()
      val fs = path.getFileSystem(sessionHadoopConf)

      import spark.implicits._

      val snapshot = deltaLog.update()

      require(snapshot.version >= 0, "No state defined for this table. Is this really " +
        "a Delta table? Refusing to garbage collect.")

      val retentionMillis = retentionHours.map(h => TimeUnit.HOURS.toMillis(math.round(h)))
      checkRetentionPeriodSafety(spark, retentionMillis, deltaLog.tombstoneRetentionMillis)

      val deleteBeforeTimestamp = retentionMillis.map { millis =>
        clock.getTimeMillis() - millis
      }.getOrElse(deltaLog.minFileRetentionTimestamp)
      logInfo(s"Starting garbage collection (dryRun = $dryRun) of untracked files older than " +
        s"${new Date(deleteBeforeTimestamp).toGMTString} in $path")
      val hadoopConf = spark.sparkContext.broadcast(
        new SerializableConfiguration(sessionHadoopConf))
      val basePath = fs.makeQualified(path).toString
      var isBloomFiltered = false
      val parallelDeleteEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_ENABLED)

      val validFiles = snapshot.state
        .mapPartitions { actions =>
          val reservoirBase = new Path(basePath)
          val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
          actions.flatMap {
            _.unwrap match {
              case tombstone: RemoveFile if tombstone.delTimestamp < deleteBeforeTimestamp =>
                Nil
              case fa: FileAction =>
                val filePath = stringToPath(fa.path)
                val validFileOpt = if (filePath.isAbsolute) {
                  val maybeRelative =
                    DeltaFileOperations.tryRelativizePath(fs, reservoirBase, filePath)
                  if (maybeRelative.isAbsolute) {
                    // This file lives outside the directory of the table
                    None
                  } else {
                    Option(pathToString(maybeRelative))
                  }
                } else {
                  Option(pathToString(filePath))
                }
                validFileOpt.toSeq.flatMap { f =>
                  // paths are relative so provide '/' as the basePath.
                  allValidFiles(f, isBloomFiltered).flatMap { file =>
                    val dirs = getAllSubdirs("/", file, fs)
                    dirs ++ Iterator(file)
                  }
                }
              case _ => Nil
            }
          }
        }.toDF("path")

      val partitionColumns = snapshot.metadata.partitionSchema.fieldNames
      val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism
      val allFilesAndDirs =
        DeltaFileOperations.recursiveListDirs(
          spark,
          Seq(basePath),
          hadoopConf,
          hiddenFileNameFilter = DeltaTableUtils.isHiddenDirectory(partitionColumns, _),
          fileListingParallelism = Option(parallelism))
      try {
        allFilesAndDirs.cache()

        val dirCounts = allFilesAndDirs.where('isDir).count() + 1 // +1 for the base path

        // The logic below is as follows:
        //   1. We take all the files and directories listed in our reservoir
        //   2. We filter all files older than our tombstone retention period and directories
        //   3. We get the subdirectories of all files so that we can find non-empty directories
        //   4. We groupBy each path, and count to get how many files are in each sub-directory
        //   5. We subtract all the valid files and tombstones in our state
        //   6. We filter all paths with a count of 1, which will correspond to files not in the
        //      state, and empty directories. We can safely delete all of these
        val diff = allFilesAndDirs
          .where('modificationTime < deleteBeforeTimestamp || 'isDir)
          .mapPartitions { fileStatusIterator =>
            val reservoirBase = new Path(basePath)
            val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
            fileStatusIterator.flatMap { fileStatus =>
              if (fileStatus.isDir) {
                Iterator.single(relativize(fileStatus.getPath, fs, reservoirBase, isDir = true))
              } else {
                val dirs = getAllSubdirs(basePath, fileStatus.path, fs)
                val dirsWithSlash = dirs.map { p =>
                  relativize(new Path(p), fs, reservoirBase, isDir = true)
                }
                dirsWithSlash ++ Iterator(
                  relativize(new Path(fileStatus.path), fs, reservoirBase, isDir = false))
              }
            }
          }.groupBy($"value" as 'path)
          .count()
          .join(validFiles, Seq("path"), "leftanti")
          .where('count === 1)
          .select('path)
          .as[String]
          .map { relativePath =>
            assert(!stringToPath(relativePath).isAbsolute,
              "Shouldn't have any absolute paths for deletion here.")
            pathToString(DeltaFileOperations.absolutePath(basePath, relativePath))
          }

        if (dryRun) {
          val numFiles = diff.count()
          val stats = DeltaVacuumStats(
            isDryRun = true,
            specifiedRetentionMillis = retentionMillis,
            defaultRetentionMillis = deltaLog.tombstoneRetentionMillis,
            minRetainedTimestamp = deleteBeforeTimestamp,
            dirsPresentBeforeDelete = dirCounts,
            objectsDeleted = numFiles)

          recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
          logConsole(s"Found $numFiles files and directories in a total of " +
            s"$dirCounts directories that are safe to delete.")

          return diff.map(f => stringToPath(f).toString).toDF("path")
        }
        logVacuumStart(
          spark,
          deltaLog,
          path,
          diff,
          retentionMillis,
          deltaLog.tombstoneRetentionMillis)

        val filesDeleted = try {
          delete(diff, spark, basePath, hadoopConf, parallelDeleteEnabled)
        } catch { case t: Throwable =>
          logVacuumEnd(deltaLog, spark, path)
          throw t
        }
        val stats = DeltaVacuumStats(
          isDryRun = false,
          specifiedRetentionMillis = retentionMillis,
          defaultRetentionMillis = deltaLog.tombstoneRetentionMillis,
          minRetainedTimestamp = deleteBeforeTimestamp,
          dirsPresentBeforeDelete = dirCounts,
          objectsDeleted = filesDeleted)
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

  protected def logVacuumStart(
      spark: SparkSession,
      deltaLog: DeltaLog,
      path: Path,
      diff: Dataset[String],
      specifiedRetentionMillis: Option[Long],
      defaultRetentionMillis: Long): Unit = {
    logInfo(s"Deleting untracked files and empty directories in $path")
  }

  protected def logVacuumEnd(
      deltaLog: DeltaLog,
      spark: SparkSession,
      path: Path,
      filesDeleted: Option[Long] = None,
      dirCounts: Option[Long] = None): Unit = {
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
      parallel: Boolean): Long = {
    import spark.implicits._
    if (parallel) {
      diff.mapPartitions { files =>
        val fs = new Path(basePath).getFileSystem(hadoopConf.value.value)
        val filesDeletedPerPartition =
          files.map(p => stringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
        Iterator(filesDeletedPerPartition)
      }.reduce(_ + _)
    } else {
      val fs = new Path(basePath).getFileSystem(hadoopConf.value.value)
      val fileResultSet = diff.toLocalIterator().asScala
      fileResultSet.map(p => stringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
    }
  }

  protected def stringToPath(path: String): Path = new Path(new URI(path))

  protected def pathToString(path: Path): String = path.toUri.toString

  /**
   * This is used to create the list of files we want to retain during GC.
   */
  protected def allValidFiles(file: String, isBloomFiltered: Boolean): Seq[String] = Seq(file)
}

case class DeltaVacuumStats(
    isDryRun: Boolean,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    specifiedRetentionMillis: Option[Long],
    defaultRetentionMillis: Long,
    minRetainedTimestamp: Long,
    dirsPresentBeforeDelete: Long,
    objectsDeleted: Long)
