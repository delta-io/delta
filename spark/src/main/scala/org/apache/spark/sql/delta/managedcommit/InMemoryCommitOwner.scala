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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

class InMemoryCommitOwner(val batchSize: Long)
  extends AbstractBatchBackfillingCommitOwnerClient {

  /**
   * @param maxCommitVersion represents the max commit version known for the table. This is
   *                         initialized at the time of pre-registration and updated whenever a
   *                         commit is successfully added to the commit-owner.
   * @param active represents whether this commit-owner has ratified any commit or not.
   * |----------------------------|------------------|---------------------------|
   * |        State               | maxCommitVersion |          active           |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    | currentVersion+1 |          false            |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    |       X          |          true             |
   * | and more commits are done  |                  |                           |
   * |----------------------------|------------------|---------------------------|
   */
  private[managedcommit] class PerTableData(
    var maxCommitVersion: Long = -1,
    var active: Boolean = false
  ) {
    def updateLastRatifiedCommit(commitVersion: Long): Unit = {
      active = true
      maxCommitVersion = commitVersion
    }

    /**
     * Returns the last ratified commit version for the table. If no commits have been done from
     * commit-owner yet, returns -1.
     */
    def lastRatifiedCommitVersion: Long = if (!active) -1 else maxCommitVersion

    // Map from version to Commit data
    val commitsMap: mutable.SortedMap[Long, Commit] = mutable.SortedMap.empty
    // We maintain maxCommitVersion explicitly since commitsMap might be empty
    // if all commits for a table have been backfilled.
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  }

  private[managedcommit] val perTableMap = new ConcurrentHashMap[Path, PerTableData]()

  private[managedcommit] def withWriteLock[T](logPath: Path)(operation: => T): T = {
    val tableData = Option(perTableMap.get(logPath)).getOrElse {
      throw new IllegalArgumentException(s"Unknown table $logPath.")
    }
    val lock = tableData.lock.writeLock()
    lock.lock()
    try {
      operation
    } finally {
      lock.unlock()
    }
  }

  private[managedcommit] def withReadLock[T](logPath: Path)(operation: => T): T = {
    val tableData = perTableMap.get(logPath)
    if (tableData == null) {
      throw new IllegalArgumentException(s"Unknown table $logPath.")
    }
    val lock = tableData.lock.readLock()
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
      managedCommitTableConf: Map[String, String],
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse = {
    addToMap(logPath, commitVersion, commitFile, commitTimestamp)
  }

  private[sql] def addToMap(
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
      tableData.updateLastRatifiedCommit(commitVersion)

      logInfo(s"Added commit file ${commitFile.getPath} to commit-owner.")
      CommitResponse(commit)
    }
  }

  override def getCommits(
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Long,
      endVersion: Option[Long]): GetCommitsResponse = {
    withReadLock[GetCommitsResponse](logPath) {
      val tableData = perTableMap.get(logPath)
      // Calculate the end version for the range, or use the last key if endVersion is not provided
      val effectiveEndVersion =
        endVersion.getOrElse(tableData.commitsMap.lastOption.map(_._1).getOrElse(startVersion))
      val commitsInRange = tableData.commitsMap.range(startVersion, effectiveEndVersion + 1)
      GetCommitsResponse(commitsInRange.values.toSeq, tableData.lastRatifiedCommitVersion)
    }
  }

  override protected[delta] def registerBackfill(
      logPath: Path,
      backfilledVersion: Long): Unit = {
    withWriteLock(logPath) {
      val tableData = perTableMap.get(logPath)
      if (backfilledVersion > tableData.lastRatifiedCommitVersion) {
        throw new IllegalArgumentException(
          s"Unexpected backfill version: $backfilledVersion. " +
            s"Max backfill version: ${tableData.maxCommitVersion}")
      }
      // Remove keys with versions less than or equal to 'untilVersion'
      val versionsToRemove = tableData.commitsMap.keys.takeWhile(_ <= backfilledVersion).toList
      versionsToRemove.foreach(tableData.commitsMap.remove)
    }
  }

  override def registerTable(
      logPath: Path,
      currentVersion: Long,
      currentMetadata: Metadata,
      currentProtocol: Protocol): Map[String, String] = {
    val newPerTableData = new PerTableData(currentVersion + 1)
    perTableMap.compute(logPath, (_, existingData) => {
      if (existingData != null) {
        if (existingData.lastRatifiedCommitVersion != -1) {
          throw new IllegalStateException(s"Table $logPath already exists in the commit-owner.")
        }
        // If lastRatifiedCommitVersion is -1 i.e. the commit-owner has never attempted any commit
        // for this table => this table was just pre-registered. If there is another
        // pre-registration request for an older version, we reject it and table can't go backward.
        if (currentVersion < existingData.maxCommitVersion) {
          throw new IllegalStateException(s"Table $logPath already registered with commit-owner")
        }
      }
      newPerTableData
    })
    Map.empty
  }

  override def semanticEquals(other: CommitOwnerClient): Boolean = this == other
}

/**
 * The [[InMemoryCommitOwnerBuilder]] class is responsible for creating singleton instances of
 * [[InMemoryCommitOwner]] with the specified batchSize.
 */
case class InMemoryCommitOwnerBuilder(batchSize: Long) extends CommitOwnerBuilder {
  private lazy val inMemoryStore = new InMemoryCommitOwner(batchSize)

  /** Name of the commit-owner */
  def name: String = "in-memory"

  /** Returns a commit-owner based on the given conf */
  def build(conf: Map[String, String]): CommitOwnerClient = {
    inMemoryStore
  }
}
