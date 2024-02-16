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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

import org.apache.spark.sql.delta.SerializableFileStatus
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

class InMemoryCommitStore(val batchSize: Long) extends AbstractBatchBackfillingCommitStore {

  private[managedcommit] class PerTableData {
    // Map from version to Commit data
    val commitsMap: mutable.SortedMap[Long, Commit] = mutable.SortedMap.empty
    // We maintain maxCommitVersion explicitly since commitsMap might be empty
    // if all commits for a table have been backfilled.
    var maxCommitVersion: Long = -1
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  }

  private[managedcommit] val perTableMap = new ConcurrentHashMap[Path, PerTableData]()

  private[managedcommit] def withWriteLock[T](tablePath: Path)(operation: => T): T = {
    val lock = perTableMap
      .computeIfAbsent(tablePath, _ => new PerTableData()) // computeIfAbsent is atomic
      .lock
      .writeLock()
    lock.lock()
    try {
      operation
    } finally {
      lock.unlock()
    }
  }

  private[managedcommit] def withReadLock[T](tablePath: Path)(operation: => T): T = {
    val lock = perTableMap
      .computeIfAbsent(tablePath, _ => new PerTableData()) // computeIfAbsent is atomic
      .lock
      .readLock()
    lock.lock()
    try {
      operation
    } finally {
      lock.unlock()
    }
  }

  /**
   * This method acquires a write lock, validates the commit version is next in line,
   * updates commit maps, and releases the lock.
   *
   * @throws CommitFailedException if the commit version is not the expected next version,
   *                               indicating a version conflict.
   */
  protected def commitImpl(
      logStore: LogStore,
      hadoopConf: Configuration,
      tablePath: Path,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse = {
    withWriteLock[CommitResponse](tablePath) {
      val tableData = perTableMap.get(tablePath)
      val expectedVersion = tableData.maxCommitVersion + 1
      if (commitVersion != expectedVersion) {
        throw new CommitFailedException(
          retryable = commitVersion < expectedVersion,
          conflict = commitVersion < expectedVersion,
          s"Commit version $commitVersion is not valid. Expected version: $expectedVersion.")
      }

      val commit =
        Commit(commitVersion, SerializableFileStatus.fromStatus(commitFile), commitTimestamp)
      tableData.commitsMap(commitVersion) = commit
      tableData.maxCommitVersion = commitVersion

      logInfo(s"Added commit file ${commitFile.getPath} to commit-store.")
      CommitResponse(commit)
    }
  }

  override def getCommits(
      tablePath: Path,
      startVersion: Long,
      endVersion: Option[Long]): Seq[Commit] = {
    withReadLock[Seq[Commit]](tablePath) {
      val tableData = perTableMap.get(tablePath)
      // Calculate the end version for the range, or use the last key if endVersion is not provided
      val effectiveEndVersion =
        endVersion.getOrElse(tableData.commitsMap.lastOption.map(_._1).getOrElse(startVersion))
      val commitsInRange = tableData.commitsMap.range(startVersion, effectiveEndVersion + 1)
      commitsInRange.values.toSeq
    }
  }

  override protected[managedcommit] def registerBackfill(
      tablePath: Path,
      untilVersion: Long,
      deltaFile: Path): Unit = {
    withWriteLock(tablePath) {
      val tableData = perTableMap.get(tablePath)
      if (untilVersion > tableData.maxCommitVersion) {
        throw new IllegalArgumentException(
          s"Unexpected backfill version: $untilVersion. " +
            s"Max backfill version: ${tableData.maxCommitVersion}")
      }
      // Remove keys with versions less than or equal to 'untilVersion'
      val versionsToRemove = tableData.commitsMap.keys.takeWhile(_ <= untilVersion).toList
      versionsToRemove.foreach(tableData.commitsMap.remove)
    }
  }
}
