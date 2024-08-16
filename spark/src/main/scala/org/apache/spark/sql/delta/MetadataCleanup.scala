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

import java.util.{Calendar, TimeZone}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaHistoryManager.BufferingLogDeletionIterator
import org.apache.spark.sql.delta.TruncationGranularity.{DAY, HOUR, MINUTE, TruncationGranularity}
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, listingPrefix, CheckpointFile, DeltaFile, UnbackfilledDeltaFile}
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.internal.MDC

private[delta] object TruncationGranularity extends Enumeration {
  type TruncationGranularity = Value
  val DAY, HOUR, MINUTE = Value
}

/** Cleans up expired Delta table metadata. */
trait MetadataCleanup extends DeltaLogging {
  self: DeltaLog =>

  /** Whether to clean up expired log files and checkpoints. */
  def enableExpiredLogCleanup(metadata: Metadata): Boolean =
    DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.fromMetaData(metadata)

  /**
   * Returns the duration in millis for how long to keep around obsolete logs. We may keep logs
   * beyond this duration until the next calendar day to avoid constantly creating checkpoints.
   */
  def deltaRetentionMillis(metadata: Metadata): Long = {
    val interval = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    DeltaConfigs.getMilliSeconds(interval)
  }

  override def doLogCleanup(snapshotToCleanup: Snapshot): Unit = {
    if (enableExpiredLogCleanup(snapshot.metadata)) {
      cleanUpExpiredLogs(snapshotToCleanup)
    }
  }

  /** Clean up expired delta and checkpoint logs. Exposed for testing. */
  private[delta] def cleanUpExpiredLogs(
      snapshotToCleanup: Snapshot,
      deltaRetentionMillisOpt: Option[Long] = None,
      cutoffTruncationGranularity: TruncationGranularity = DAY): Unit = {
    recordDeltaOperation(this, "delta.log.cleanup") {
      val retentionMillis =
        deltaRetentionMillisOpt.getOrElse(deltaRetentionMillis(snapshot.metadata))
      val fileCutOffTime =
        truncateDate(clock.getTimeMillis() - retentionMillis, cutoffTruncationGranularity).getTime
      val formattedDate = fileCutOffTime.toGMTString
      logInfo(log"Starting the deletion of log files older than " +
        log"${MDC(DeltaLogKeys.DATE, formattedDate)}")

      val fs = logPath.getFileSystem(newDeltaHadoopConf())
      var numDeleted = 0
      val expiredDeltaLogs = listExpiredDeltaLogs(fileCutOffTime.getTime)
      if (expiredDeltaLogs.hasNext) {
        // Trigger compatibility checkpoint creation logic only when this round of metadata cleanup
        // is going to delete any deltas/checkpoint files.
        // We need to create compat checkpoint before deleting delta/checkpoint files so that we
        // don't have a window in b/w where the old checkpoint is deleted and there is no
        // compat-checkpoint available.
        val v2CompatCheckpointMetrics = new V2CompatCheckpointMetrics
        createSinglePartCheckpointForBackwardCompat(snapshotToCleanup, v2CompatCheckpointMetrics)
        logInfo(log"Compatibility checkpoint creation metrics: " +
          log"${MDC(DeltaLogKeys.METRICS, v2CompatCheckpointMetrics)}")
      }
      var wasCheckpointDeleted = false
      var maxBackfilledVersionDeleted = -1L
      expiredDeltaLogs.map(_.getPath).foreach { path =>
        // recursive = false
        if (fs.delete(path, false)) {
          numDeleted += 1
          if (FileNames.isCheckpointFile(path)) {
            wasCheckpointDeleted = true
          }
          if (FileNames.isDeltaFile(path)) {
            maxBackfilledVersionDeleted =
              Math.max(maxBackfilledVersionDeleted, FileNames.deltaVersion(path))
          }
        }
      }
      val commitDirPath = FileNames.commitDirPath(logPath)
      // Commit Directory might not exist on tables created in older versions and
      // never updated since.
      val expiredUnbackfilledDeltaLogs: Iterator[FileStatus] =
        if (fs.exists(commitDirPath)) {
          store
            .listFrom(listingPrefix(commitDirPath, 0), newDeltaHadoopConf())
            .takeWhile { case UnbackfilledDeltaFile(_, fileVersion, _) =>
              fileVersion <= maxBackfilledVersionDeleted
            }
        } else {
          Iterator.empty
        }
      val numDeletedUnbackfilled = expiredUnbackfilledDeltaLogs.count(
        log => fs.delete(log.getPath, false))
      if (wasCheckpointDeleted) {
        // Trigger sidecar deletion only when some checkpoints have been deleted as part of this
        // round of Metadata cleanup.
        val sidecarDeletionMetrics = new SidecarDeletionMetrics
        identifyAndDeleteUnreferencedSidecarFiles(
          snapshotToCleanup,
          fileCutOffTime.getTime,
          sidecarDeletionMetrics)
        logInfo(log"Sidecar deletion metrics: ${MDC(DeltaLogKeys.METRICS, sidecarDeletionMetrics)}")
      }
      logInfo(log"Deleted ${MDC(DeltaLogKeys.NUM_FILES, numDeleted)} log files and " +
        log"${MDC(DeltaLogKeys.NUM_FILES2, numDeletedUnbackfilled)} unbackfilled commit " +
        log"files older than ${MDC(DeltaLogKeys.DATE, formattedDate)}")
    }
  }

  /**
   * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
   * considered as expired, it must:
   *  - have a checkpoint file after it
   *  - be older than `fileCutOffTime`
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Iterator[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val latestCheckpoint = readLastCheckpointFile()
    if (latestCheckpoint.isEmpty) return Iterator.empty
    val threshold = latestCheckpoint.get.version - 1L
    val files = store.listFrom(listingPrefix(logPath, 0), newDeltaHadoopConf())
      .filter(f => isCheckpointFile(f) || isDeltaFile(f))
    def getVersion(filePath: Path): Long = {
      if (isCheckpointFile(filePath)) {
        checkpointVersion(filePath)
      } else {
        deltaVersion(filePath)
      }
    }

    new BufferingLogDeletionIterator(files, fileCutOffTime, threshold, getVersion)
  }

  /**
   * Truncates a timestamp down to a given unit. The unit can be either DAY, HOUR or MINUTE.
   * - DAY: The timestamp it truncated to the previous midnight.
   * - HOUR: The timestamp it truncated to the last hour.
   * - MINUTE: The timestamp it truncated to the last minute.
   */
  private[delta] def truncateDate(timeMillis: Long, unit: TruncationGranularity): Calendar = {
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(timeMillis)

    val calendarUnit = unit match {
      case DAY => Calendar.DAY_OF_MONTH
      case HOUR => Calendar.HOUR_OF_DAY
      case MINUTE => Calendar.MINUTE
    }

    DateUtils.truncate(date, calendarUnit)
  }

  /** Truncates a timestamp down to the previous midnight and returns the time. */
  private[delta] def truncateDay(timeMillis: Long): Calendar = {
    truncateDate(timeMillis, TruncationGranularity.DAY)
  }

  /**
   * Helper method to create a compatibility classic single file checkpoint file for this table.
   * This is needed so that any legacy reader which do not understand [[V2CheckpointTableFeature]]
   * could read the legacy classic checkpoint file and fail gracefully with Protocol requirement
   * failure.
   */
  protected[delta] def createSinglePartCheckpointForBackwardCompat(
      snapshotToCleanup: Snapshot,
      metrics: V2CompatCheckpointMetrics): Unit = {
    // Do nothing if this table does not use V2 Checkpoints, or has no checkpoints at all.
    if (!CheckpointProvider.isV2CheckpointEnabled(snapshotToCleanup)) return
    if (snapshotToCleanup.checkpointProvider.isEmpty) return

    val startTimeMs = System.currentTimeMillis()
    val hadoopConf = newDeltaHadoopConf()
    val checkpointInstance =
      CheckpointInstance(snapshotToCleanup.checkpointProvider.topLevelFiles.head.getPath)
    // The current checkpoint provider is already using a checkpoint with the naming
    // scheme of classic checkpoints. There is no need to create a compatibility checkpoint
    // in this case.
    if (checkpointInstance.format != CheckpointInstance.Format.V2) return

    val checkpointVersion = snapshotToCleanup.checkpointProvider.version
    val checkpoints = listFrom(checkpointVersion)
      .takeWhile(file => FileNames.getFileVersionOpt(file.getPath).exists(_ <= checkpointVersion))
      .collect {
        case file if FileNames.isCheckpointFile(file) => CheckpointInstance(file.getPath)
      }
      .filter(_.format != CheckpointInstance.Format.V2)
      .toArray
    val availableNonV2Checkpoints =
      getLatestCompleteCheckpointFromList(checkpoints, Some(checkpointVersion))
    if (availableNonV2Checkpoints.nonEmpty) {
      metrics.v2CheckpointCompatLogicTimeTakenMs = System.currentTimeMillis() - startTimeMs
      return
    }

    // topLevelFileIndex must be non-empty when topLevelFiles are present
    val shallowCopyDf =
      loadIndex(snapshotToCleanup.checkpointProvider.topLevelFileIndex.get, Action.logSchema)
    val finalPath =
      FileNames.checkpointFileSingular(snapshotToCleanup.deltaLog.logPath, checkpointVersion)
    Checkpoints.createCheckpointV2ParquetFile(
      spark,
      shallowCopyDf,
      finalPath,
      hadoopConf,
      useRename = false)
    metrics.v2CheckpointCompatLogicTimeTakenMs = System.currentTimeMillis() - startTimeMs
    metrics.checkpointVersion = checkpointVersion
  }

  /** Deletes any unreferenced files from the sidecar directory `_delta_log/_sidecar` */
  protected def identifyAndDeleteUnreferencedSidecarFiles(
      snapshotToCleanup: Snapshot,
      checkpointRetention: Long,
      metrics: SidecarDeletionMetrics): Unit = {
    val startTimeMs = System.currentTimeMillis()
    // If v2 checkpoints are not enabled on the table, we don't need to attempt the sidecar cleanup.
    if (!CheckpointProvider.isV2CheckpointEnabled(snapshotToCleanup)) return

    val hadoopConf = newDeltaHadoopConf()
    val fs = sidecarDirPath.getFileSystem(hadoopConf)
    // This can happen when the V2 Checkpoint feature is present in the Protocol but
    // only Classic checkpoints have been created for the table.
    if (!fs.exists(sidecarDirPath)) return

    val (parquetCheckpointFiles, otherFiles) = store
      .listFrom(listingPrefix(logPath, 0), hadoopConf)
      .collect { case CheckpointFile(status, _) => (status, CheckpointInstance(status.getPath)) }
      .collect { case (fileStatus, ci) if ci.format.usesSidecars => fileStatus }
      .toSeq
      .partition(_.getPath.getName.endsWith("parquet"))
    val (jsonCheckpointFiles, unknownFormatCheckpointFiles) =
      otherFiles.partition(_.getPath.getName.endsWith("json"))
    if (unknownFormatCheckpointFiles.nonEmpty) {
      logWarning(
        "Found checkpoint files other than parquet and json: " +
          s"${unknownFormatCheckpointFiles.map(_.getPath.toString).mkString(",")}")
    }
    metrics.numActiveParquetCheckpointFiles = parquetCheckpointFiles.size
    metrics.numActiveJsonCheckpointFiles = jsonCheckpointFiles.size
    val parquetCheckpointsFileIndex =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, parquetCheckpointFiles)
    val jsonCheckpointsFileIndex =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_JSON, jsonCheckpointFiles)
    val identifyActiveSidecarsStartTimeMs = System.currentTimeMillis()
    metrics.activeCheckpointsListingTimeTakenMs = identifyActiveSidecarsStartTimeMs - startTimeMs
    import org.apache.spark.sql.delta.implicits._
    val df = (parquetCheckpointsFileIndex ++ jsonCheckpointsFileIndex)
      .map(loadIndex(_, Action.logSchema(Set("sidecar"))))
      .reduceOption(_ union _)
      .getOrElse { return }

    val activeSidecarFiles = df
      .select("sidecar.path")
      .where("path is not null")
      .as[String]
      .collect()
      .map(p => new Path(p).getName) // Get bare file names
      .toSet

    val identifyAndDeleteSidecarsStartTimeMs = System.currentTimeMillis()
    metrics.identifyActiveSidecarsTimeTakenMs =
      identifyAndDeleteSidecarsStartTimeMs - identifyActiveSidecarsStartTimeMs
    // Retain all files created in the checkpoint retention window - irrespective of whether they
    // are referenced in a checkpoint or not. This is to make sure that we don't end up deleting an
    // in-progress checkpoint.
    val retentionTimestamp: Long = checkpointRetention
    val sidecarFilesIterator = new Iterator[FileStatus] {
      // Hadoop's RemoteIterator is neither java nor scala Iterator, so have to wrap it
      val remoteIterator = fs.listStatusIterator(sidecarDirPath)
      override def hasNext: Boolean = remoteIterator.hasNext()
      override def next(): FileStatus = remoteIterator.next()
    }
    val sidecarFilesToDelete = sidecarFilesIterator
      .collect { case file if file.getModificationTime < retentionTimestamp => file.getPath }
      .filterNot(path => activeSidecarFiles.contains(path.getName))
    val sidecarDeletionStartTimeMs = System.currentTimeMillis()
    logInfo("Starting the deletion of unreferenced sidecar files")
    val count = deleteMultiple(fs, sidecarFilesToDelete)

    logInfo(log"Deleted ${MDC(DeltaLogKeys.COUNT, count)} sidecar files")
    metrics.numSidecarFilesDeleted = count
    val endTimeMs = System.currentTimeMillis()
    metrics.identifyAndDeleteSidecarsTimeTakenMs =
      sidecarDeletionStartTimeMs - identifyAndDeleteSidecarsStartTimeMs
    metrics.overallSidecarProcessingTimeTakenMs = endTimeMs - startTimeMs
  }

  private def deleteMultiple(fs: FileSystem, paths: Iterator[Path]): Long = {
      paths.map { path =>
        if (fs.delete(path, false)) 1L else 0L
      }.sum
  }

  /** Class to track metrics related to V2 Checkpoint Sidecars deletion. */
  protected class SidecarDeletionMetrics {
    // number of sidecar files deleted
    var numSidecarFilesDeleted: Long = -1
    // number of active parquet checkpoint files present in delta log directory
    var numActiveParquetCheckpointFiles: Long = -1
    // number of active json checkpoint files present in delta log directory
    var numActiveJsonCheckpointFiles: Long = -1
    // time taken (in ms) to list and identify active checkpoints
    var activeCheckpointsListingTimeTakenMs: Long = -1
    // time taken (in ms) to list the sidecar directory to get all sidecars and delete those which
    // aren't referenced by any checkpoint anymore
    var identifyAndDeleteSidecarsTimeTakenMs: Long = -1
    // time taken (in ms) to read the active checkpoint json / parquet files and identify active
    // sidecar files
    var identifyActiveSidecarsTimeTakenMs: Long = -1
    // time taken (in ms) for everything related to sidecar processing
    var overallSidecarProcessingTimeTakenMs: Long = -1
  }

  /** Class to track metrics related to V2 Compatibility checkpoint creation. */
  protected[delta] class V2CompatCheckpointMetrics {
    // time taken (in ms) to run the v2 checkpoint compat logic
    var v2CheckpointCompatLogicTimeTakenMs: Long = -1

    // the version at which we have created a v2 compat checkpoint, -1 if no compat checkpoint was
    // created.
    var checkpointVersion: Long = -1
  }

  /**
   * Finds a checkpoint such that we are able to construct table snapshot for all versions at or
   * greater than the checkpoint version returned.
   */
  def findEarliestReliableCheckpoint: Option[Long] = {
    val hadoopConf = newDeltaHadoopConf()
    var earliestCheckpointVersionOpt: Option[Long] = None
    // This is used to collect the checkpoint files from the current version that we are listing.
    // When we list a file that is not part of the checkpoint, then we must have seen the entire
    // checkpoint. We then verify if the checkpoint was complete, and if it is not, we clear the
    // collection and wait for the next checkpoint to appear in the file listing.
    // Whenever we see a complete checkpoint for the first time, we remember it as the earliest
    // checkpoint.
    val currentCheckpointFiles = ArrayBuffer.empty[Path]
    var prevCommitVersion = 0L

    def currentCheckpointVersionOpt: Option[Long] =
      currentCheckpointFiles.headOption.map(checkpointVersion(_))

    def isCurrentCheckpointComplete: Boolean = {
      val instances = currentCheckpointFiles.map(CheckpointInstance(_)).toArray
      getLatestCompleteCheckpointFromList(instances).isDefined
    }

    store.listFrom(listingPrefix(logPath, 0L), hadoopConf)
      .map(_.getPath)
      .foreach {
        case CheckpointFile(f, checkpointVersion) if earliestCheckpointVersionOpt.isEmpty =>
          if (!currentCheckpointVersionOpt.contains(checkpointVersion)) {
            // If it's a different checkpoint, clear the existing one.
            currentCheckpointFiles.clear()
          }
          currentCheckpointFiles += f
        case DeltaFile(_, deltaVersion) =>
          if (earliestCheckpointVersionOpt.isEmpty && isCurrentCheckpointComplete) {
            // We have found a complete checkpoint, but we should not stop here. If a future
            // commit version is missing, then this checkpoint will be discarded and we will need
            // to restart the search from that point.

            // Ensure that the commit json is there at the checkpoint version. If it's not there,
            // we don't consider such a checkpoint as a reliable checkpoint.
            if (currentCheckpointVersionOpt.contains(deltaVersion)) {
              earliestCheckpointVersionOpt = currentCheckpointVersionOpt
              prevCommitVersion = deltaVersion
            }
          }
          // Need to clear it so that if there is a gap in commit versions, we are forced to
          // look for a new complete checkpoint.
          currentCheckpointFiles.clear()
          if (deltaVersion > prevCommitVersion + 1) {
            // Missing commit versions. Restart the search.
            earliestCheckpointVersionOpt = None
          }
          prevCommitVersion = deltaVersion
        case _ =>
      }

    earliestCheckpointVersionOpt
  }
}
