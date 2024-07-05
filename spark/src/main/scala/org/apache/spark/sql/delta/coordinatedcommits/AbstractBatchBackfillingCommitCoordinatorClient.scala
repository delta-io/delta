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

package org.apache.spark.sql.delta.coordinatedcommits

import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.TransactionExecutionObserver
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.{LoggingShims, MDC}

/**
 * An abstract [[CommitCoordinatorClient]] which triggers backfills every n commits.
 * - every commit version which satisfies `commitVersion % batchSize == 0` will trigger a backfill.
 */
trait AbstractBatchBackfillingCommitCoordinatorClient
  extends CommitCoordinatorClient
    with LoggingShims {

  /**
   * Size of batch that should be backfilled. So every commit version which satisfies
   * `commitVersion % batchSize == 0` will trigger a backfill.
   */
  val batchSize: Long

  /**
   * Commit a given `commitFile` to the table represented by given `logPath` at the
   * given `commitVersion`
   */
  private[delta] def commitImpl(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
    val executionObserver = TransactionExecutionObserver.getObserver
    val tablePath = CoordinatedCommitsUtils.getTablePath(logPath)
    if (commitVersion == 0) {
      throw CommitFailedException(
        retryable = false, conflict = false, message = "Commit version 0 must go via filesystem.")
    }
    logInfo(log"Attempting to commit version " +
      log"${MDC(DeltaLogKeys.VERSION, commitVersion)} on table " +
      log"${MDC(DeltaLogKeys.PATH, tablePath)}")
    val fs = logPath.getFileSystem(hadoopConf)
    if (batchSize <= 1) {
      // Backfill until `commitVersion - 1`
      logInfo(log"Making sure commits are backfilled until " +
        log"${MDC(DeltaLogKeys.VERSION, commitVersion - 1)} version for" +
        log" table ${MDC(DeltaLogKeys.PATH, tablePath.toString)}")
      backfillToVersion(
        logStore,
        hadoopConf,
        logPath,
        coordinatedCommitsTableConf,
        commitVersion - 1)
    }

    // Write new commit file in _commits directory
    val fileStatus = CoordinatedCommitsUtils.writeCommitFile(
      logStore, hadoopConf, logPath, commitVersion, actions, generateUUID())

    // Do the actual commit
    val commitTimestamp = updatedActions.getCommitInfo.getCommitTimestamp
    var commitResponse =
      commitImpl(
        logStore,
        hadoopConf,
        logPath,
        coordinatedCommitsTableConf,
        commitVersion,
        fileStatus,
        commitTimestamp)

    val mcToFsConversion = isCoordinatedCommitsToFSConversion(commitVersion, updatedActions)
    // Backfill if needed
    executionObserver.beginBackfill()
    if (batchSize <= 1) {
      // Always backfill when batch size is configured as 1
      backfill(logStore, hadoopConf, logPath, commitVersion, fileStatus)
      val targetFile = FileNames.unsafeDeltaFile(logPath, commitVersion)
      val targetFileStatus = fs.getFileStatus(targetFile)
      val newCommit = commitResponse.getCommit.copy(fileStatus = targetFileStatus)
      commitResponse = commitResponse.copy(commit = newCommit)
    } else if (commitVersion % batchSize == 0 || mcToFsConversion) {
      logInfo(log"Making sure commits are backfilled till " +
        log"${MDC(DeltaLogKeys.VERSION, commitVersion)} " +
        log"version for table ${MDC(DeltaLogKeys.PATH, tablePath.toString)}")
      backfillToVersion(
        logStore,
        hadoopConf,
        logPath,
        coordinatedCommitsTableConf,
        commitVersion)
    }
    logInfo(log"Commit ${MDC(DeltaLogKeys.VERSION, commitVersion)} done successfully on table " +
      log"${MDC(DeltaLogKeys.PATH, tablePath)}")
    commitResponse
  }

  private def isCoordinatedCommitsToFSConversion(
      commitVersion: Long,
      updatedActions: UpdatedActions): Boolean = {
    val oldMetadataHasCoordinatedCommits =
      CoordinatedCommitsUtils.getCommitCoordinatorName(updatedActions.getOldMetadata).nonEmpty
    val newMetadataHasCoordinatedCommits =
      CoordinatedCommitsUtils.getCommitCoordinatorName(updatedActions.getNewMetadata).nonEmpty
    oldMetadataHasCoordinatedCommits && !newMetadataHasCoordinatedCommits && commitVersion > 0
  }

  protected def generateUUID(): String = UUID.randomUUID().toString

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      version: Long,
      lastKnownBackfilledVersionOpt: Option[Long] = None): Unit = {
    // Confirm the last backfilled version by checking the backfilled delta file's existence.
    val validLastKnownBackfilledVersionOpt = lastKnownBackfilledVersionOpt.filter { version =>
      val fs = logPath.getFileSystem(hadoopConf)
      fs.exists(FileNames.unsafeDeltaFile(logPath, version))
    }
    val startVersionOpt = validLastKnownBackfilledVersionOpt.map(_ + 1)
    getCommits(logPath, coordinatedCommitsTableConf, startVersionOpt, Some(version))
      .getCommits
      .foreach { commit =>
        backfill(logStore, hadoopConf, logPath, commit.getVersion, commit.getFileStatus)
    }
  }

  /** Backfills a given `fileStatus` to `version`.json */
  protected def backfill(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      version: Long,
      fileStatus: FileStatus): Unit = {
    val targetFile = FileNames.unsafeDeltaFile(logPath, version)
    logInfo(log"Backfilling commit ${MDC(DeltaLogKeys.PATH, fileStatus.getPath)} to " +
      log"${MDC(DeltaLogKeys.PATH2, targetFile.toString)}")
    val commitContentIterator = logStore.readAsIterator(fileStatus, hadoopConf)
    try {
      logStore.write(
        targetFile,
        commitContentIterator,
        overwrite = false,
        hadoopConf)
      registerBackfill(logPath, version)
    } catch {
      case _: FileAlreadyExistsException =>
        logInfo(log"The backfilled file ${MDC(DeltaLogKeys.FILE_NAME, targetFile)} already exists.")
    } finally {
      commitContentIterator.close()
    }
  }

  /**
   * Callback to tell the CommitCoordinator that all commits <= `backfilledVersion` are backfilled.
   */
  protected[delta] def registerBackfill(
      logPath: Path,
      backfilledVersion: Long): Unit

}
