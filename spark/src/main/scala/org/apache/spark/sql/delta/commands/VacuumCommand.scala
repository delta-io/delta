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
import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.math.min
import scala.util.control.NonFatal
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, DeltaFileOperations, FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations.tryDeleteNonRecursive
import org.apache.spark.sql.delta.util.FileNames._
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.MDC
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{col, count, lit, replace, startswith, substr, sum}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.util.{Clock, SerializableConfiguration, SystemClock, Utils}

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
   * path : fully qualified uri
   * length: size in bytes
   * isDir: boolean indicating if it is a directory
   * modificationTime: file update time in milliseconds
   */
  val INVENTORY_SCHEMA = StructType(
    Seq(
      StructField("path", StringType),
      StructField("length", LongType),
      StructField("isDir", BooleanType),
      StructField("modificationTime", LongType)
    ))

  object VacuumType extends Enumeration {
    type VacuumType = Value
    val LITE = Value("LITE")
    val FULL = Value("FULL")
  }

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
   * Helper to compute all valid files based on basePath and Snapshot provided.
   */
  private def getValidFilesFromSnapshot(
      spark: SparkSession,
      basePath: String,
      snapshot: Snapshot,
      retentionMillis: Option[Long],
      hadoopConf: Broadcast[SerializableConfiguration],
      clock: Clock,
      checkAbsolutePathOnly: Boolean): DataFrame = {
    import org.apache.spark.sql.delta.implicits._
    require(snapshot.version >= 0, "No state defined for this table. Is this really " +
      "a Delta table? Refusing to garbage collect.")

    val snapshotTombstoneRetentionMillis = DeltaLog.tombstoneRetentionMillis(snapshot.metadata)
    checkRetentionPeriodSafety(spark, retentionMillis, snapshotTombstoneRetentionMillis)
    val deleteBeforeTimestamp = retentionMillis match {
      case Some(millis) => clock.getTimeMillis() - millis
      case _ => snapshot.minFileRetentionTimestamp
    }
    val relativizeIgnoreError =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR)

    val canonicalizedBasePath = SparkPath.fromPathString(basePath).urlEncoded
    snapshot.stateDS.mapPartitions { actions =>
      val reservoirBase = new Path(basePath)
      val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
      actions.flatMap {
        _.unwrap match {
          // Existing tables may not store canonicalized paths, so we check both the canonicalized
          // and non-canonicalized paths to ensure we don't accidentally delete wrong files.
          case fa: FileAction if checkAbsolutePathOnly &&
            !fa.path.contains(basePath) && !fa.path.contains(canonicalizedBasePath) => Nil
          case tombstone: RemoveFile if tombstone.delTimestamp < deleteBeforeTimestamp => Nil
          case fa: FileAction =>
            getValidRelativePathsAndSubdirs(
              fa,
              fs,
              reservoirBase,
              relativizeIgnoreError
            )
          case _ => Nil
        }
      }
    }.toDF("path")
  }

  def getFilesFromInventory(
      basePath: String,
      partitionColumns: Seq[String],
      inventory: DataFrame,
      shouldIcebergMetadataDirBeHidden: Boolean): Dataset[SerializableFileStatus] = {
    implicit val fileNameAndSizeEncoder: Encoder[SerializableFileStatus] =
      org.apache.spark.sql.Encoders.product[SerializableFileStatus]

    // filter out required fields from provided inventory DF
    val inventorySchema = StructType(
        inventory.schema.fields.filter(f => INVENTORY_SCHEMA.fields.map(_.name).contains(f.name))
      )
    if (inventorySchema != INVENTORY_SCHEMA) {
      throw DeltaErrors.invalidInventorySchema(INVENTORY_SCHEMA.treeString)
    }

    inventory
      .filter(startswith(col("path"), lit(s"$basePath/")))
      .select(
        substr(col("path"), lit(basePath.length + 2)).as("path"),
        col("length"), col("isDir"), col("modificationTime")
      )
      .flatMap {
        row =>
          val path = row.getString(0)
          if(!DeltaTableUtils.isHiddenDirectory(
              partitionColumns, path, shouldIcebergMetadataDirBeHidden)
          ) {
            Seq(SerializableFileStatus(path,
              row.getLong(1), row.getBoolean(2), row.getLong(3)))
          } else {
            None
          }
      }
      .map { f =>
        // Below logic will make paths url-encoded
        SerializableFileStatus(pathStringtoUrlEncodedString(f.path), f.length, f.isDir,
          f.modificationTime)
      }
  }

  /**
   * Clears all untracked files and folders within this table. If the inventory is not provided
   * then the command first lists all the files and directories in the table, if inventory is
   * provided then it will be used for identifying files and directories within the table and
   * gets the relative paths with respect to the base of the table. Then the command gets the
   * list of all tracked files for this table, which may or may not be within the table base path,
   * and gets the relative paths of all the tracked files with respect to the base of the table.
   * Files outside of the table path will be ignored. Then we take a diff of the files and delete
   * directories that were already empty, and all files that are within the table that are no longer
   * tracked.
   *
   * @param dryRun If set to true, no files will be deleted. Instead, we will list all files and
   *               directories that will be cleared.
   * @param retentionHours An optional parameter to override the default Delta tombstone retention
   *                       period
   * @param inventory An optional dataframe of files and directories within the table generated
   *                  from sources like blob store inventory report
   * @return A Dataset containing the paths of the files/folders to delete in dryRun mode. Otherwise
   *         returns the base path of the table.
   */
  // scalastyle:off argcount
  def gc(
      spark: SparkSession,
      deltaLog: DeltaLog,
      dryRun: Boolean = true,
      retentionHours: Option[Double] = None,
      inventory: Option[DataFrame] = None,
      vacuumTypeOpt: Option[String] = None,
      commandMetrics: Map[String, SQLMetric] = Map.empty,
      clock: Clock = new SystemClock): DataFrame = {
    // scalastyle:on argcount
    recordDeltaOperation(deltaLog, "delta.gc") {

      val vacuumStartTime = System.currentTimeMillis()
      val path = deltaLog.dataPath
      val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
      val fs = path.getFileSystem(deltaHadoopConf)

      import org.apache.spark.sql.delta.implicits._

      val snapshot = deltaLog.update()
      deltaLog.protocolWrite(snapshot.protocol)

      val snapshotTombstoneRetentionMillis = DeltaLog.tombstoneRetentionMillis(snapshot.metadata)
      val retentionMillis = retentionHours.map(h => TimeUnit.HOURS.toMillis(math.round(h)))
      val deleteBeforeTimestamp = retentionMillis match {
        case Some(millis) => clock.getTimeMillis() - millis
        case _ => snapshot.minFileRetentionTimestamp
      }
      logInfo(log"Starting garbage collection (dryRun = " +
        log"${MDC(DeltaLogKeys.IS_DRY_RUN, dryRun)}) of untracked " +
        log"files older than ${MDC(DeltaLogKeys.DATE,
          new Date(deleteBeforeTimestamp).toGMTString)} in " +
        log"${MDC(DeltaLogKeys.PATH, path)}")
      val hadoopConf = spark.sparkContext.broadcast(
        new SerializableConfiguration(deltaHadoopConf))
      val basePath = fs.makeQualified(path).toString
      val parallelDeleteEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_ENABLED)
      val parallelDeletePartitions =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_PARALLELISM)
        .getOrElse(spark.sessionState.conf.numShufflePartitions)
      val startTimeToIdentifyEligibleFiles = System.currentTimeMillis()

      val validFiles =
        getValidFilesFromSnapshot(
          spark,
          basePath,
          snapshot,
          retentionMillis,
          hadoopConf,
          clock,
          checkAbsolutePathOnly = false)

      val partitionColumns = snapshot.metadata.partitionSchema.fieldNames
      val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism
      val shouldIcebergMetadataDirBeHidden = UniversalFormat.icebergEnabled(snapshot.metadata)
      // By default, we will do full vacuum unless LITE vacuum conf is set
      val isLiteVacuumEnabled = spark.sessionState.conf.getConf(DeltaSQLConf.LITE_VACUUM_ENABLED)
      val defaultType = if (isLiteVacuumEnabled) VacuumType.LITE else VacuumType.FULL
      val vacuumType = vacuumTypeOpt.map(VacuumType.withName).getOrElse(defaultType)
      val latestCommitVersionOutsideOfRetentionWindowOpt: Option[Long] =
        if (vacuumType == VacuumType.LITE) {
          try {
            val timestamp = new Timestamp(deleteBeforeTimestamp)
            val commit = new DeltaHistoryManager(deltaLog).getActiveCommitAtTime(
              timestamp, canReturnLastCommit = true, mustBeRecreatable = false)
            Some(commit.version)
          } catch {
            case ex: DeltaErrors.TimestampEarlierThanCommitRetentionException => None
          }
        } else {
          None
        }

      // eligibleStartCommitVersionOpt and eligibleEndCommitVersionOpt are valid in case of
      // lite vacuum. They represent the range of commit versions(inclusive) which give us the
      // eligible files to be deleted.
      val
      (allFilesAndDirsWithDuplicates, eligibleStartCommitVersionOpt, eligibleEndCommitVersionOpt) =
        inventory match {
        case Some(inventoryDF) =>
          val files = getFilesFromInventory(
            basePath, partitionColumns, inventoryDF, shouldIcebergMetadataDirBeHidden)
          (files, None, None)
        case _ if vacuumType == VacuumType.LITE =>
          getFilesFromDeltaLog(spark, snapshot, basePath, hadoopConf,
            latestCommitVersionOutsideOfRetentionWindowOpt)
        case _ =>
          val files = DeltaFileOperations.recursiveListDirs(
              spark,
              Seq(basePath),
              hadoopConf,
              hiddenDirNameFilter =
                DeltaTableUtils.isHiddenDirectory(
                  partitionColumns, _, shouldIcebergMetadataDirBeHidden
                ),
              hiddenFileNameFilter =
                DeltaTableUtils.isHiddenDirectory(
                  partitionColumns, _, shouldIcebergMetadataDirBeHidden
                ),
              fileListingParallelism = Option(parallelism)
            )
            .map { f =>
              // Below logic will make paths url-encoded
              val path = pathStringtoUrlEncodedString(f.path)
              SerializableFileStatus(path, f.length, f.isDir, f.modificationTime)
            }
          (files, None, None)
          }
      val allFilesAndDirs = allFilesAndDirsWithDuplicates.groupByKey(_.path)
        .mapGroups { (k, v) =>
          val duplicates = v.toSeq
          // of all the duplicates we can return the newest file.
          duplicates.maxBy(_.modificationTime)
        }

      recordFrameProfile("Delta", "VacuumCommand.gc") {
        try {
          allFilesAndDirs.cache()

          implicit val fileNameAndSizeEncoder =
            org.apache.spark.sql.Encoders.product[FileNameAndSize]

          val dirCounts = allFilesAndDirs.where(col("isDir")).count() + 1 // +1 for the base path
          val filesAndDirsPresentBeforeDelete = allFilesAndDirs.count()

          // The logic below is as follows:
          //   1. We take all the files and directories listed in our reservoir
          //   2. We filter all files older than our tombstone retention period and directories
          //   3. We get the subdirectories of all files so that we can find non-empty directories
          //   4. We groupBy each path, and count to get how many files are in each sub-directory
          //   5. We subtract all the valid files and tombstones in our state
          //   6. We filter all paths with a count of 1, which will correspond to files not in the
          //      state, and empty directories. We can safely delete all of these
          val canonicalizedBasePath = SparkPath.fromPathString(basePath).urlEncoded
          val diff = allFilesAndDirs
            .where(col("modificationTime") < deleteBeforeTimestamp || col("isDir"))
            .mapPartitions { fileStatusIterator =>
              val reservoirBase = new Path(basePath)
              val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
              fileStatusIterator.flatMap { fileStatus =>
                if (fileStatus.isDir) {
                  Iterator.single(FileNameAndSize(
                    relativize(urlEncodedStringToPath(fileStatus.path), fs,
                      reservoirBase, isDir = true), 0L))
                } else {
                  val dirs = getAllSubdirs(canonicalizedBasePath, fileStatus.path, fs)
                  val dirsWithSlash = dirs.map { p =>
                    val relativizedPath = relativize(urlEncodedStringToPath(p), fs,
                      reservoirBase, isDir = true)
                    FileNameAndSize(relativizedPath, 0L)
                  }
                  dirsWithSlash ++ Iterator(
                    FileNameAndSize(relativize(
                      urlEncodedStringToPath(fileStatus.path), fs, reservoirBase, isDir = false),
                      fileStatus.length))
                }
              }
            }.groupBy(col("path")).agg(count(new Column("*")).as("count"),
              sum("length").as("length"))
            .join(validFiles, Seq("path"), "leftanti")
            .where(col("count") === 1)


          val sizeOfDataToDeleteRow = diff.agg(sum("length").cast("long")).first()
          val sizeOfDataToDelete = if (sizeOfDataToDeleteRow.isNullAt(0)) {
            0L
          } else {
            sizeOfDataToDeleteRow.getLong(0)
          }

          val diffFiles = diff
            .select(col("path"))
            .as[String]
            .map { relativePath =>
              assert(!urlEncodedStringToPath(relativePath).isAbsolute,
                "Shouldn't have any absolute paths for deletion here.")
              pathToUrlEncodedString(DeltaFileOperations.absolutePath(basePath, relativePath))
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
              filesAndDirsPresentBeforeDelete = filesAndDirsPresentBeforeDelete,
              objectsDeleted = numFiles,
              sizeOfDataToDelete = sizeOfDataToDelete,
              timeTakenToIdentifyEligibleFiles = timeTakenToIdentifyEligibleFiles,
              timeTakenForDelete = 0L,
              vacuumStartTime = vacuumStartTime,
              vacuumEndTime = System.currentTimeMillis,
              numPartitionColumns = partitionColumns.size,
              latestCommitVersion = snapshot.version,
              eligibleStartCommitVersion = eligibleStartCommitVersionOpt,
              eligibleEndCommitVersion = eligibleEndCommitVersionOpt,
              typeOfVacuum = vacuumType.toString
            )

            recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
            logInfo(log"Found ${MDC(DeltaLogKeys.NUM_FILES, numFiles.toLong)} files " +
              log"(${MDC(DeltaLogKeys.NUM_BYTES, sizeOfDataToDelete)} bytes) and directories in " +
              log"a total of ${MDC(DeltaLogKeys.NUM_DIRS, dirCounts)} directories " +
              log"that are safe to delete. Vacuum stats: ${MDC(DeltaLogKeys.VACUUM_STATS, stats)}")

            return diffFiles.map(f => urlEncodedStringToPath(f).toString).toDF("path")
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
              logVacuumEnd(deltaLog, spark, path, commandMetrics = commandMetrics)
              throw t
          }
          val timeTakenForDelete = System.currentTimeMillis() - deleteStartTime
          val stats = DeltaVacuumStats(
            isDryRun = false,
            specifiedRetentionMillis = retentionMillis,
            defaultRetentionMillis = snapshotTombstoneRetentionMillis,
            minRetainedTimestamp = deleteBeforeTimestamp,
            dirsPresentBeforeDelete = dirCounts,
            filesAndDirsPresentBeforeDelete = filesAndDirsPresentBeforeDelete,
            objectsDeleted = filesDeleted,
            sizeOfDataToDelete = sizeOfDataToDelete,
            timeTakenToIdentifyEligibleFiles = timeTakenToIdentifyEligibleFiles,
            timeTakenForDelete = timeTakenForDelete,
            vacuumStartTime = vacuumStartTime,
            vacuumEndTime = System.currentTimeMillis,
            numPartitionColumns = partitionColumns.size,
            latestCommitVersion = snapshot.version,
            eligibleStartCommitVersion = eligibleStartCommitVersionOpt,
            eligibleEndCommitVersion = eligibleEndCommitVersionOpt,
            typeOfVacuum = vacuumType.toString)
          recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
          logVacuumEnd(
            deltaLog,
            spark,
            path,
            commandMetrics = commandMetrics,
            Some(filesDeleted),
            Some(dirCounts))

          LastVacuumInfo.persistLastVacuumInfo(
            LastVacuumInfo(latestCommitVersionOutsideOfRetentionWindowOpt), deltaLog)

          logInfo(log"Deleted ${MDC(DeltaLogKeys.NUM_FILES, filesDeleted)} files " +
            log"(${MDC(DeltaLogKeys.NUM_BYTES, sizeOfDataToDelete)} bytes) and directories in " +
            log"a total of ${MDC(DeltaLogKeys.NUM_DIRS, dirCounts)} directories. " +
            log"Vacuum stats: ${MDC(DeltaLogKeys.VACUUM_STATS, stats)}")


          spark.createDataset(Seq(basePath)).toDF("path")
        } finally {
          allFilesAndDirs.unpersist()
        }
      }
    }
  }

  /**
   * Returns eligible files to be deleted by looking at the delta log. Additionally, it returns
   * the start and the end commit versions(inclusive) which give us the eligible files to be
   * deleted.
   */
  protected def getFilesFromDeltaLog(
      spark: SparkSession,
      snapshot: Snapshot,
      basePath: String,
      hadoopConf: Broadcast[SerializableConfiguration],
      latestCommitVersionOutsideOfRetentionWindowOpt: Option[Long])
    : (Dataset[SerializableFileStatus], Option[Long], Option[Long]) = {
    import org.apache.spark.sql.delta.implicits._
    val deltaLog = snapshot.deltaLog
    val earliestCommitVersion = DeltaHistoryManager.getEarliestDeltaFile(deltaLog)
    val latestCommitVersionOutsideOfRetentionWindowAsOfLastVacuumOpt =
      LastVacuumInfo.getLastVacuumInfo(deltaLog)
        .flatMap(_.latestCommitVersionOutsideOfRetentionWindow)

    // If there are no commit versions outside of the retention window,
    // then there is nothing to Vacuum.
    val latestCommitVersionOutsideOfRetentionWindow =
      latestCommitVersionOutsideOfRetentionWindowOpt.getOrElse {
        return (spark.emptyDataset[SerializableFileStatus], None, None)
      }

    // In the following two conditions, we return error saying lite vacuum is not possible:
    // 1. We are not able to locate the last vacuum info and we don't have commit files starting
    //    from 0
    // 2. Last vacuum info is there but metadata cleanup has cleaned up commit files since
    // the last Vacuum's latest commit version outside of the retention window.
    if (earliestCommitVersion != 0 &&
      latestCommitVersionOutsideOfRetentionWindowAsOfLastVacuumOpt
        .forall(_ < earliestCommitVersion)) {
      throw DeltaErrors.deltaCannotVacuumLite()
    }

    // The start and the end commit versions give the range of commit files we want to look into
    // to get the list of eligible files for deletion.
    val eligibleStartCommitVersion = math.min(
      deltaLog.update().version,
      latestCommitVersionOutsideOfRetentionWindowAsOfLastVacuumOpt
        .map(_ + 1).getOrElse(earliestCommitVersion))
    val eligibleEndCommitVersion = latestCommitVersionOutsideOfRetentionWindow

    // If there are no additional commit files to look into, then
    // there is nothing to vacuum.
    if (eligibleStartCommitVersion > latestCommitVersionOutsideOfRetentionWindow) {
      return (spark.emptyDataset[SerializableFileStatus], None, None)
    }

    (getFilesFromDeltaLog(spark, deltaLog, basePath, hadoopConf,
        eligibleStartCommitVersion, eligibleEndCommitVersion),
      Some(eligibleStartCommitVersion),
      Some(eligibleEndCommitVersion)
    )
  }

/**
 * Returns eligible files to be deleted by looking at the delta log given the start and the end
 * commit versions.
 */
  protected def getFilesFromDeltaLog(
      spark: SparkSession,
      deltaLog: DeltaLog,
      basePath: String,
      hadoopConf: Broadcast[SerializableConfiguration],
      eligibleStartCommitVersion: Long,
      eligibleEndCommitVersion: Long): Dataset[SerializableFileStatus] = {
    import org.apache.spark.sql.delta.implicits._
    // When coordinated commits are enabled, commit files could be found in _delta_log directory
    // as well as in commit directory. We get the delta log files outside of the retention window
    // from both the places.
    val prefix = listingPrefix(deltaLog.logPath, eligibleStartCommitVersion)
    val eligibleDeltaLogFilesFromDeltaLogDirectory =
      deltaLog.store.listFrom(prefix, deltaLog.newDeltaHadoopConf)
        .collect { case DeltaFile(f, deltaFileVersion) => (f, deltaFileVersion) }
        .takeWhile(_._2 <= eligibleEndCommitVersion)
        .toSeq

    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val commitDirPath = FileNames.commitDirPath(deltaLog.logPath)
    val updatedStartCommitVersion =
      eligibleDeltaLogFilesFromDeltaLogDirectory.lastOption.map(_._2)
        .getOrElse(eligibleStartCommitVersion)
    val eligibleDeltaLogFilesFromCommitDirectory = if (fs.exists(commitDirPath)) {
      deltaLog.store
        .listFrom(listingPrefix(commitDirPath, updatedStartCommitVersion),
          deltaLog.newDeltaHadoopConf)
        .collect { case UnbackfilledDeltaFile(f, deltaFileVersion, _) => (f, deltaFileVersion) }
        .takeWhile(_._2 <= eligibleEndCommitVersion)
        .toSeq
    } else {
      Seq.empty
    }

    val allDeltaLogFilesOutsideTheRetentionWindow = eligibleDeltaLogFilesFromDeltaLogDirectory ++
      eligibleDeltaLogFilesFromCommitDirectory
    val deltaLogFileIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT,
      allDeltaLogFilesOutsideTheRetentionWindow.map(_._1)).get

    val allActions = deltaLog.loadIndex(deltaLogFileIndex).as[SingleAction]
    val nonCDFFiles = allActions
      .where("remove IS NOT NULL")
      .select(col("remove")
      .as[RemoveFile])
      .mapPartitions { iter =>
        iter.flatMap { r =>
          val modificationTime = r.deletionTimestamp.getOrElse(0L)
          val dv = getDeletionVectorRelativePathAndSize(r).map { case (path, length) =>
            SerializableFileStatus(path, length, isDir = false, modificationTime)
          }
          dv.iterator ++ Iterator.single(SerializableFileStatus(
            r.path, r.size.getOrElse(0L), isDir = false, modificationTime))
        }
      }
      .as[SerializableFileStatus]
    val cdfFiles = allActions
      .where("cdc IS NOT NULL")
      .select(col("cdc")
      .as[AddCDCFile])
      .map(cdc => SerializableFileStatus(cdc.path, cdc.size, isDir = false, modificationTime = 0L))

    val relativizeIgnoreError =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR)
    nonCDFFiles.union(cdfFiles).mapPartitions { iter =>
      val reservoirBase = new Path(basePath)
      val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
      iter.flatMap { f =>
        // if file path is outside of the table base path, those files are not considered as they
        // are not part of this table. Shallow clone is one example where this happens.
        getRelativePath(f.path, fs, reservoirBase, relativizeIgnoreError)
          .map(SerializableFileStatus(_, f.length, f.isDir, f.modificationTime))
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
        logWarning(log"Vacuum event logging" +
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
    logInfo(
      log"Deleting untracked files and empty directories in " +
      log"${MDC(DeltaLogKeys.PATH, path)}. The amount " +
      log"of data to be deleted is ${MDC(DeltaLogKeys.NUM_BYTES, sizeOfDataToDelete)} (in bytes)"
      )

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
      val version = txn.commit(actions = Seq(), DeltaOperations.VacuumStart(
        checkEnabled,
        specifiedRetentionMillis,
        defaultRetentionMillis
      ))
      setCommitClock(deltaLog, version)
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
      commandMetrics: Map[String, SQLMetric],
      filesDeleted: Option[Long] = None,
      dirCounts: Option[Long] = None): Unit = {
    if (shouldLogVacuum(spark, deltaLog, deltaLog.newDeltaHadoopConf(), path)) {
      val txn = deltaLog.startTransaction()
      val status = if (filesDeleted.isEmpty && dirCounts.isEmpty) { "FAILED" } else { "COMPLETED" }
      if (filesDeleted.nonEmpty && dirCounts.nonEmpty) {
        // Populate top level metrics.
        commandMetrics.get("numDeletedFiles").foreach(_.set(filesDeleted.get))
        commandMetrics.get("numVacuumedDirectories").foreach(_.set(dirCounts.get))
        // Additionally, create a separate metrics map in case the commandMetrics is empty.
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
      val version = txn.commit(actions = Seq(), DeltaOperations.VacuumEnd(
        status
      ))
      setCommitClock(deltaLog, version)
    }

    if (filesDeleted.nonEmpty) {
      logConsole(s"Deleted ${filesDeleted.get} files and directories in a total " +
        s"of ${dirCounts.get} directories.")
    }
  }
  protected def setCommitClock(deltaLog: DeltaLog, version: Long) = {
    // This is done to make sure that the commit timestamp reflects the one provided by the clock
    // object.
    if (Utils.isTesting) {
      val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
      val filePath = DeltaCommitFileProvider(deltaLog.update()).deltaFile(version)
      if (fs.exists(filePath)) {
        fs.setTimes(filePath, deltaLog.clock.getTimeMillis(), deltaLog.clock.getTimeMillis())
      }
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
    pathToUrlEncodedString(DeltaFileOperations.tryRelativizePath(fs, reservoirBase, path))
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
          files.map(p => urlEncodedStringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
        Iterator(filesDeletedPerPartition)
      }.collect().sum
    } else {
      val fs = new Path(basePath).getFileSystem(hadoopConf.value.value)
      val fileResultSet = diff.toLocalIterator().asScala
      fileResultSet.map(p => urlEncodedStringToPath(p)).count(f => tryDeleteNonRecursive(fs, f))
    }
  }

  protected def urlEncodedStringToPath(path: String): Path = SparkPath.fromUrlString(path).toPath

  protected def pathToUrlEncodedString(path: Path): String = SparkPath.fromPath(path).toString

  protected def pathStringtoUrlEncodedString(path: String) =
    SparkPath.fromPathString(path).toString

  protected def getActionRelativePath(
      action: FileAction,
      fs: FileSystem,
      basePath: Path,
      relativizeIgnoreError: Boolean): Option[String] = {
    getRelativePath(action.path, fs, basePath, relativizeIgnoreError)
  }
  /** Returns the relative path of a file or None if the file lives outside of the table. */
  protected def getRelativePath(
      path: String,
      fs: FileSystem,
      basePath: Path,
      relativizeIgnoreError: Boolean): Option[String] = {
    val filePath = urlEncodedStringToPath(path)
    if (filePath.isAbsolute) {
      val maybeRelative =
        DeltaFileOperations.tryRelativizePath(fs, basePath, filePath, relativizeIgnoreError)
      if (maybeRelative.isAbsolute) {
        // This file lives outside the directory of the table.
        None
      } else {
        Some(pathToUrlEncodedString(maybeRelative))
      }
    } else {
      Some(pathToUrlEncodedString(filePath))
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
      relativizeIgnoreError: Boolean
  ): Seq[String] = {
    val paths = getActionRelativePath(action, fs, basePath, relativizeIgnoreError)
      .map {
        relativePath =>
        Seq(relativePath) ++ getAllSubdirs("/", relativePath, fs)
      }.getOrElse(Seq.empty)

    val deletionVectorPath =
      getDeletionVectorRelativePathAndSize(action).map(_._1)

    paths ++ deletionVectorPath.toSeq
  }

  /**
   * Returns the path of the on-disk deletion vector if it is stored relative to the
   * `basePath` and it's size otherwise `None`.
   */
  protected def getDeletionVectorRelativePathAndSize(action: FileAction): Option[(String, Long)] = {
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
          Some((pathToUrlEncodedString(dv.absolutePath(new Path("."))), dv.sizeInBytes))
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
    filesAndDirsPresentBeforeDelete: Long,
    objectsDeleted: Long,
    sizeOfDataToDelete: Long,
    timeTakenToIdentifyEligibleFiles: Long,
    timeTakenForDelete: Long,
    vacuumStartTime: Long,
    vacuumEndTime: Long,
    numPartitionColumns: Long,
    latestCommitVersion: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    eligibleStartCommitVersion: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    eligibleEndCommitVersion: Option[Long],
    typeOfVacuum: String
)

case class LastVacuumInfo(
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  latestCommitVersionOutsideOfRetentionWindow: Option[Long] = None
)

object LastVacuumInfo extends DeltaCommand {
  private val LAST_VACUUM_INFO_FILE_NAME = "_last_vacuum_info"

  /** The path to the file that holds metadata about the most recent Vacuum. */
  private def getLastVacuumInfoPath(logPath: Path): Path =
    new Path(logPath, LAST_VACUUM_INFO_FILE_NAME)

  def getLastVacuumInfo(deltaLog: DeltaLog): Option[LastVacuumInfo] = {
    try {
      val path = getLastVacuumInfoPath(deltaLog.logPath)
      val json = deltaLog.store.read(path, deltaLog.newDeltaHadoopConf()).head
      Some(JsonUtils.mapper.readValue[LastVacuumInfo](json))
    } catch {
      case _: FileNotFoundException =>
        None
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog,
          "delta.lastVacuumInfo.read.corruptedJson",
          data = Map("exception" -> Utils.exceptionString(e))
        )
        None
    }
  }

  def persistLastVacuumInfo(lastVacuumInfo: LastVacuumInfo, deltaLog: DeltaLog): Unit = {
    try {
      val path = getLastVacuumInfoPath(deltaLog.logPath)
      val json = Iterator.single(JsonUtils.toJson(lastVacuumInfo))
      deltaLog.store.write(path, json, overwrite = true, deltaLog.newDeltaHadoopConf())
    } catch {
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog,
          "delta.lastVacuumInfo.write.failure",
          data = Map("exception" -> Utils.exceptionString(e))
        )
    }
  }
}
