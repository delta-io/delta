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
   * Commit a given `commitFile` to the table represented by given `tablePath` at the
   * given `commitVersion`
   */
  protected def commitImpl(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path,
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {

    logInfo(s"Attempting to commit version $commitVersion on table $tablePath")
    val fs = tablePath.getFileSystem(hadoopConf)
    if (batchSize <= 1) {
      // Backfill until `commitVersion - 1`
      logInfo(s"Making sure commits are backfilled until $commitVersion version for" +
        s" table ${tablePath.toString}")
      backfillToVersion(logStore, hadoopConf, tablePath)
    }

    // Write new commit file in _commits directory
    val fileStatus = writeCommitFile(logStore, hadoopConf, tablePath, commitVersion, actions)

    // Do the actual commit
    val commitTimestamp = updatedActions.commitInfo.getTimestamp
    var commitResponse =
      commitImpl(logStore, hadoopConf, tablePath, commitVersion, fileStatus, commitTimestamp)

    // Backfill if needed
    if (commitVersion == 0 || batchSize <= 1) {
      // Always backfill zeroth commit or when batch size is configured as 1
      backfill(logStore, hadoopConf, tablePath, commitVersion, fileStatus)
      val targetFile = FileNames.deltaFile(logPath(tablePath), commitVersion)
      val targetFileStatus = fs.getFileStatus(targetFile)
      val newCommit = commitResponse.commit.copy(fileStatus = targetFileStatus)
      commitResponse = commitResponse.copy(commit = newCommit)
    } else if (commitVersion % batchSize == 0) {
      logInfo(s"Making sure commits are backfilled till $commitVersion version for" +
        s"table ${tablePath.toString}")
      backfillToVersion(logStore, hadoopConf, tablePath)
    }
    logInfo(s"Commit $commitVersion done successfully on table $tablePath")
    commitResponse
  }

  protected def writeCommitFile(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path,
      commitVersion: Long,
      actions: Iterator[String]): FileStatus = {
    val uuidStr = generateUUID()
    val commitPath = FileNames.uuidDeltaFile(logPath(tablePath), commitVersion, Some(uuidStr))
    logStore.write(commitPath, actions, overwrite = false, hadoopConf)
    commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath)
  }

  protected def generateUUID(): String = UUID.randomUUID().toString

  /** Backfills all un-backfilled commits */
  protected def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path): Unit = {
    getCommits(tablePath, startVersion = 0).foreach { case commit =>
      val fileStatus = commit.fileStatus
      backfill(logStore, hadoopConf, tablePath, commit.version, fileStatus)
    }
  }

  /** Backfills a given `fileStatus` to `version`.json */
  protected def backfill(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path,
      version: Long,
      fileStatus: FileStatus): Unit = {
    val targetFile = FileNames.deltaFile(logPath(tablePath), version)
    logInfo(s"Backfilling commit ${fileStatus.getPath} to ${targetFile.toString}")
    val commitContentIterator = logStore.readAsIterator(fileStatus, hadoopConf)
    try {
      logStore.write(
        targetFile,
        commitContentIterator,
        overwrite = false,
        hadoopConf)
      registerBackfill(tablePath, version)
    } catch {
      case _: FileAlreadyExistsException =>
        logInfo(s"The backfilled file $targetFile already exists.")
    } finally {
      commitContentIterator.close()
    }
  }

  /** Callback to tell the CommitStore that all commits <= `backfilledVersion` are backfilled. */
  protected[delta] def registerBackfill(
      tablePath: Path,
      backfilledVersion: Long): Unit

  protected def logPath(tablePath: Path): Path = new Path(tablePath, DeltaLog.LOG_DIR_NAME)
}
