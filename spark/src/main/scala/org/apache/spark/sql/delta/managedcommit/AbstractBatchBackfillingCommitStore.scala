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

package org.apache.spark.sql.delta.managedcommit

import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging

/**
 * An abstract [[CommitStore]] which triggers backfills every n commits.
 * - every commit version which satisfies `commitVersion % batchSize == 0` will trigger a backfill.
 */
trait AbstractBatchBackfillingCommitStore extends CommitStore with Logging {

  /**
   * Size of batch that should be backfilled. So every commit version which satisfies
   * `commitVersion % batchSize == 0` will trigger a backfill.
   */
  val batchSize: Long

  /**
   * Commit a given `commitFile` to the table represented by given `logPath` at the
   * given `commitVersion`
   */
  protected def commitImpl(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
    val tablePath = getTablePath(logPath)
    logInfo(s"Attempting to commit version $commitVersion on table $tablePath")
    val fs = logPath.getFileSystem(hadoopConf)
    if (batchSize <= 1) {
      // Backfill until `commitVersion - 1`
      logInfo(s"Making sure commits are backfilled until $commitVersion version for" +
        s" table ${tablePath.toString}")
      backfillToVersion(logStore, hadoopConf, logPath)
    }

    // Write new commit file in _commits directory
    val fileStatus = writeCommitFile(logStore, hadoopConf, logPath, commitVersion, actions)

    // Do the actual commit
    val commitTimestamp = updatedActions.commitInfo.getTimestamp
    var commitResponse =
      commitImpl(logStore, hadoopConf, logPath, commitVersion, fileStatus, commitTimestamp)

    // Backfill if needed
    if (commitVersion == 0 || batchSize <= 1) {
      // Always backfill zeroth commit or when batch size is configured as 1
      backfill(logStore, hadoopConf, logPath, commitVersion, fileStatus)
      val targetFile = FileNames.deltaFile(logPath, commitVersion)
      val targetFileStatus = fs.getFileStatus(targetFile)
      val newCommit = commitResponse.commit.copy(fileStatus = targetFileStatus)
      commitResponse = commitResponse.copy(commit = newCommit)
    } else if (commitVersion % batchSize == 0) {
      logInfo(s"Making sure commits are backfilled till $commitVersion version for" +
        s"table ${tablePath.toString}")
      backfillToVersion(logStore, hadoopConf, logPath)
    }
    logInfo(s"Commit $commitVersion done successfully on table $tablePath")
    commitResponse
  }

  protected def writeCommitFile(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      commitVersion: Long,
      actions: Iterator[String]): FileStatus = {
    val uuidStr = generateUUID()
    val commitPath = FileNames.uuidDeltaFile(logPath, commitVersion, Some(uuidStr))
    logStore.write(commitPath, actions, overwrite = false, hadoopConf)
    commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath)
  }

  protected def generateUUID(): String = UUID.randomUUID().toString

  /** Backfills all un-backfilled commits */
  protected def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path): Unit = {
    getCommits(logPath, startVersion = 0).foreach { commit =>
      backfill(logStore, hadoopConf, logPath, commit.version, commit.fileStatus)
    }
  }

  /** Backfills a given `fileStatus` to `version`.json */
  protected def backfill(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      version: Long,
      fileStatus: FileStatus): Unit = {
    val targetFile = FileNames.deltaFile(logPath, version)
    logInfo(s"Backfilling commit ${fileStatus.getPath} to ${targetFile.toString}")
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
        logInfo(s"The backfilled file $targetFile already exists.")
    } finally {
      commitContentIterator.close()
    }
  }

  /** Callback to tell the CommitStore that all commits <= `backfilledVersion` are backfilled. */
  protected[delta] def registerBackfill(
      logPath: Path,
      backfilledVersion: Long): Unit

  protected def getTablePath(logPath: Path): Path = logPath.getParent
}
