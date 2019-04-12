/*
 * Copyright 2019 Databricks, Inc.
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

import java.util.Date

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.fs.FileStatus

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
  def deltaRetentionMillis: Long =
    DeltaConfigs.LOG_RETENTION.fromMetaData(metadata).milliseconds()

  override def doLogCleanup(): Unit = {
    if (enableExpiredLogCleanup) {
      cleanUpExpiredLogs()
    }
  }

  /** Clean up expired delta and checkpoint logs. Exposed for testing. */
  private[delta] def cleanUpExpiredLogs(): Unit = {
    recordDeltaOperation(this, "delta.log.cleanup") {
      val fileCutOffTime = clock.getTimeMillis() - deltaRetentionMillis
      val formattedDate = new Date(fileCutOffTime).toGMTString
      logInfo(s"Starting the deletion of log files older than $formattedDate")

      var numDeleted = 0
      listExpiredDeltaLogs(fileCutOffTime).map(_.getPath).foreach { path =>
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
   *  - be older than `deltaRetentionMillis`
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Iterator[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val latestCheckpoint = lastCheckpoint
    if (latestCheckpoint.isEmpty) return Iterator.empty
    val threshold = latestCheckpoint.get.version - 1L
    store.listFrom(deltaFile(logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))
      .takeWhile { fileStatus =>
        val filePath = fileStatus.getPath
        if (fileStatus.getModificationTime <= fileCutOffTime) {
          val fileVersion = if (isCheckpointFile(filePath)) {
            checkpointVersion(filePath)
          } else {
            deltaVersion(filePath)
          }
          fileVersion <= threshold
        } else {
          false
        }
      }
  }
}
