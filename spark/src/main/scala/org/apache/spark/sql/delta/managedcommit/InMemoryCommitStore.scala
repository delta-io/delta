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

import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

class InMemoryCommitStore(val batchSize: Long) extends AbstractBatchBackfillingCommitStore {

  private[managedcommit] class PerTableData(var maxCommitVersion: Long = -1) {
    // Map from version to Commit data
    val commitsMap: mutable.SortedMap[Long, Commit] = mutable.SortedMap.empty
    // We maintain maxCommitVersion explicitly since commitsMap might be empty
    // if all commits for a table have been backfilled.
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  }

  private[managedcommit] val perTableMap = new ConcurrentHashMap[Path, PerTableData]()

  private[managedcommit] def withWriteLock[T](logPath: Path)(operation: => T): T = {
    val lock = perTableMap
      .computeIfAbsent(logPath, _ => new PerTableData()) // computeIfAbsent is atomic
      .lock
      .writeLock()
    lock.lock()
    try {
      operation
    } finally {
      lock.unlock()
    }
  }

  private[managedcommit] def withReadLock[T](logPath: Path)(operation: => T): T = {
    val lock = perTableMap
      .computeIfAbsent(logPath, _ => new PerTableData()) // computeIfAbsent is atomic
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
  private[delta] def commitImpl(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse = {
    withWriteLock[CommitResponse](logPath) {
      val tableData = perTableMap.get(logPath)
      val expectedVersion = tableData.maxCommitVersion + 1
      if (commitVersion != expectedVersion) {
        throw CommitFailedException(
          retryable = commitVersion < expectedVersion,
          conflict = commitVersion < expectedVersion,
          s"Commit version $commitVersion is not valid. Expected version: $expectedVersion.")
      }

      val commit = Commit(commitVersion, commitFile, commitTimestamp)
      tableData.commitsMap(commitVersion) = commit
      tableData.maxCommitVersion = commitVersion

      logInfo(s"Added commit file ${commitFile.getPath} to commit-store.")
      CommitResponse(commit)
    }
  }

  override def getCommits(
      logPath: Path,
      startVersion: Long,
      endVersion: Option[Long]): GetCommitsResponse = {
    withReadLock[GetCommitsResponse](logPath) {
      val tableData = perTableMap.get(logPath)
      // Calculate the end version for the range, or use the last key if endVersion is not provided
      val effectiveEndVersion =
        endVersion.getOrElse(tableData.commitsMap.lastOption.map(_._1).getOrElse(startVersion))
      val commitsInRange = tableData.commitsMap.range(startVersion, effectiveEndVersion + 1)
      GetCommitsResponse(commitsInRange.values.toSeq, tableData.maxCommitVersion)
    }
  }

  override protected[delta] def registerBackfill(
      logPath: Path,
      backfilledVersion: Long): Unit = {
    withWriteLock(logPath) {
      val tableData = perTableMap.get(logPath)
      if (backfilledVersion > tableData.maxCommitVersion) {
        throw new IllegalArgumentException(
          s"Unexpected backfill version: $backfilledVersion. " +
            s"Max backfill version: ${tableData.maxCommitVersion}")
      }
      // Remove keys with versions less than or equal to 'untilVersion'
      val versionsToRemove = tableData.commitsMap.keys.takeWhile(_ <= backfilledVersion).toList
      versionsToRemove.foreach(tableData.commitsMap.remove)
    }
  }

  def registerTable(logPath: Path, maxCommitVersion: Long): Unit = {
    val newPerTableData = new PerTableData(maxCommitVersion)
    if (perTableMap.putIfAbsent(logPath, newPerTableData) != null) {
      throw new IllegalStateException(s"Table $logPath already exists in the commit store.")
    }
  }
}

/**
 * The InMemoryCommitStoreBuilder class is responsible for creating singleton instances of
 * InMemoryCommitStore with the specified batchSize.
 */
case class InMemoryCommitStoreBuilder(batchSize: Long) extends CommitStoreBuilder {
  private lazy val inMemoryStore = new InMemoryCommitStore(batchSize)

  /** Name of the commit-store */
  def name: String = "in-memory"

  /** Returns a commit store based on the given conf */
  def build(conf: Map[String, String]): CommitStore = {
    inMemoryStore
  }
}
