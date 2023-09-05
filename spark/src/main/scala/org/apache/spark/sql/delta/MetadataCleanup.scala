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
import org.apache.spark.sql.delta.TruncationGranularity.{DAY, HOUR, TruncationGranularity}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, listingPrefix, CheckpointFile, DeltaFile}
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}

private[delta] object TruncationGranularity extends Enumeration {
  type TruncationGranularity = Value
  val DAY, HOUR = Value
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
      logInfo(s"Starting the deletion of log files older than $formattedDate")

      val fs = logPath.getFileSystem(newDeltaHadoopConf())
      var numDeleted = 0
      listExpiredDeltaLogs(fileCutOffTime.getTime).map(_.getPath).foreach { path =>
        // recursive = false
        if (fs.delete(path, false)) numDeleted += 1
      }
      logInfo(s"Deleted $numDeleted log files older than $formattedDate")
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
   * Truncates a timestamp down to a given unit. The unit can be either DAY or HOUR.
   * - DAY: The timestamp it truncated to the previous midnight.
   * - HOUR: The timestamp it truncated to the last hour.
   */
  private[delta] def truncateDate(timeMillis: Long, unit: TruncationGranularity): Calendar = {
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(timeMillis)

    val calendarUnit = unit match {
      case DAY => Calendar.DAY_OF_MONTH
      case HOUR => Calendar.HOUR_OF_DAY
    }

    DateUtils.truncate(date, calendarUnit)
  }

  /** Truncates a timestamp down to the previous midnight and returns the time. */
  private[delta] def truncateDay(timeMillis: Long): Calendar = {
    truncateDate(timeMillis, TruncationGranularity.DAY)
  }

  /**
   * Finds a checkpoint such that we are able to construct table snapshot for all versions at or
   * greater than the checkpoint version returned.
   */
  def findEarliestReliableCheckpoint(): Option[Long] = {
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
