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

package org.apache.spark.sql.delta

import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.delta.DeltaHistoryManager.BufferingLogDeletionIterator
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, deltaVersion, isCheckpointFile}

/** Cleans up expired Delta table metadata. */
trait MetadataCleanup extends DeltaLogging {
  self: DeltaLog =>

  /** Whether to clean up expired log files and checkpoints. */
  def enableExpiredLogCleanup: Boolean =
    DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.fromMetaData(metadata)

  /**
   * Returns the duration in millis for how long to keep around obsolete logs. We may keep logs
   * beyond this duration until the next calendar day to avoid constantly creating checkpoints.
   */
  def deltaRetentionMillis: Long = {
    val interval = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    DeltaConfigs.getMilliSeconds(interval)
  }

  override def doLogCleanup(): Unit = {
    if (enableExpiredLogCleanup) {
      cleanUpExpiredLogs()
    }
  }

  /** Clean up expired delta and checkpoint logs. Exposed for testing. */
  private[delta] def cleanUpExpiredLogs(): Unit = {
    recordDeltaOperation(this, "delta.log.cleanup") {
      val fileCutOffTime = truncateDay(clock.getTimeMillis() - deltaRetentionMillis).getTime
      val formattedDate = fileCutOffTime.toGMTString
      logInfo(s"Starting the deletion of log files older than $formattedDate")

      var numDeleted = 0
      listExpiredDeltaLogs(fileCutOffTime.getTime).map(_.getPath).foreach { path =>
        // recursive = false
        if (fs.delete(path, false)) numDeleted += 1
      }
      logInfo(s"Deleted $numDeleted log files older than $formattedDate")
    }
  }

  /** Retrieve the version from the delta log file. */
  private def getVersion(filePath: Path): Long = {
    if (isCheckpointFile(filePath)) {
      checkpointVersion(filePath)
    } else {
      deltaVersion(filePath)
    }
  }

  /**
   *  Returns the maximum expired version that can be deleted. An expired version
   *  that is mandatory to time travel to a non expired version should not be deleted.
   *  Let's go through an example. If we have a delta table with 15 commits and only the
   *  version 0 is expired, if we delete the version 0 then all versions from 1 to 9 are
   *  no longer available for time travel even when they are not expired. So in this situation the
   *  maximum version to delete will be -1 (0 - (0 mod 10) -1) and the version 0 will be kept in
   *  the transaction log. We use the checkpoint interval configuration to determine the maximum
   *  version to delete during the log cleanup.
   */
  private def getMaxVersionToDelete(maxExpiredVersion: Long): Long = {
    maxExpiredVersion - (maxExpiredVersion % self.checkpointInterval) - 1
  }

  /** Whether the expired file can be deleted based on the version. */
  private def shouldDeleteExpiredFiles
      (expiredFile : FileStatus, maxVersionToDelete: Long): Boolean = {
    getVersion(expiredFile.getPath) <= maxVersionToDelete
  }

  /**
   * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
   * considered as expired, it must:
   *  - be older than `fileCutOffTime`
   *  - not needed to time travel to a non expired version
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Seq[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val files = store.listFrom(checkpointPrefix(logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))

    val expiredFiles =
      new BufferingLogDeletionIterator(files, fileCutOffTime, getVersion).toSeq
    val lastExpiredVersion = expiredFiles.lastOption
      .map(file => getVersion(file.getPath)).getOrElse(-1L)

    val maxVersionToDelete = getMaxVersionToDelete(lastExpiredVersion)
    if(maxVersionToDelete <= 0) {
      Seq()
    }
    else {
      expiredFiles.filter(expiredFile => shouldDeleteExpiredFiles(expiredFile, maxVersionToDelete))
    }
  }

  /** Truncates a timestamp down to the previous midnight and returns the time and a log string */
  private[delta] def truncateDay(timeMillis: Long): Calendar = {
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(timeMillis)
    DateUtils.truncate(
      date,
      Calendar.DAY_OF_MONTH)
  }
}
