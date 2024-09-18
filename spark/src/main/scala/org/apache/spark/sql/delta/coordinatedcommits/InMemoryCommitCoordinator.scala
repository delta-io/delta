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

import java.util.{Map => JMap, Optional}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.logging.DeltaLogKeys
import io.delta.storage.LogStore
import io.delta.storage.commit.{
  Commit => JCommit,
  CommitCoordinatorClient,
  CommitFailedException => JCommitFailedException,
  CommitResponse,
  GetCommitsResponse => JGetCommitsResponse,
  TableDescriptor,
  TableIdentifier
}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession

class InMemoryCommitCoordinator(val batchSize: Long)
  extends AbstractBatchBackfillingCommitCoordinatorClient {

  /**
   * @param maxCommitVersion represents the max commit version known for the table. This is
   *                         initialized at the time of pre-registration and updated whenever a
   *                         commit is successfully added to the commit-coordinator.
   * @param active represents whether this commit-coordinator has ratified any commit or not.
   * |----------------------------|------------------|---------------------------|
   * |        State               | maxCommitVersion |          active           |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    | currentVersion+1 |          false            |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    |       X          |          true             |
   * | and more commits are done  |                  |                           |
   * |----------------------------|------------------|---------------------------|
   */
  private[coordinatedcommits] class PerTableData(
    var maxCommitVersion: Long = -1,
    var active: Boolean = false
  ) {
    def updateLastRatifiedCommit(commitVersion: Long): Unit = {
      active = true
      maxCommitVersion = commitVersion
    }

    /**
     * Returns the last ratified commit version for the table. If no commits have been done from
     * commit-coordinator yet, returns -1.
     */
    def lastRatifiedCommitVersion: Long = if (!active) -1 else maxCommitVersion

    // Map from version to Commit data
    val commitsMap: mutable.SortedMap[Long, JCommit] = mutable.SortedMap.empty
    // We maintain maxCommitVersion explicitly since commitsMap might be empty
    // if all commits for a table have been backfilled.
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  }

  private[coordinatedcommits] val perTableMap = new ConcurrentHashMap[Path, PerTableData]()

  private[coordinatedcommits] def withWriteLock[T](logPath: Path)(operation: => T): T = {
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

  private[coordinatedcommits] def withReadLock[T](logPath: Path)(operation: => T): T = {
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
      coordinatedCommitsTableConf: Map[String, String],
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
        throw new JCommitFailedException(
          commitVersion < expectedVersion,
          commitVersion < expectedVersion,
          s"Commit version $commitVersion is not valid. Expected version: $expectedVersion.")
      }

      val commit = new JCommit(commitVersion, commitFile, commitTimestamp)
      tableData.commitsMap(commitVersion) = commit
      tableData.updateLastRatifiedCommit(commitVersion)

      logInfo(log"Added commit file ${MDC(DeltaLogKeys.PATH, commitFile.getPath)} " +
        log"to commit-coordinator.")
      new CommitResponse(commit)
    }
  }

  override def getCommits(
      tableDesc: TableDescriptor,
      startVersion: java.lang.Long,
      endVersion: java.lang.Long): JGetCommitsResponse = {
    withReadLock[JGetCommitsResponse](tableDesc.getLogPath) {
      val startVersionOpt: Option[Long] = Option(startVersion).map(_.toLong)
      val endVersionOpt: Option[Long] = Option(endVersion).map(_.toLong)
      val tableData = perTableMap.get(tableDesc.getLogPath)
      val effectiveStartVersion = startVersionOpt.getOrElse(0L)
      // Calculate the end version for the range, or use the last key if endVersion is not provided
      val effectiveEndVersion = endVersionOpt.getOrElse(
        tableData.commitsMap.lastOption.map(_._1).getOrElse(effectiveStartVersion))
      val commitsInRange = tableData.commitsMap.range(
        effectiveStartVersion, effectiveEndVersion + 1)
      new JGetCommitsResponse(
        commitsInRange.values.toSeq.asJava, tableData.lastRatifiedCommitVersion)
    }
  }

  override protected[sql] def registerBackfill(
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
      tableIdentifier: Optional[TableIdentifier],
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): JMap[String, String] = {
    val newPerTableData = new PerTableData(currentVersion + 1)
    perTableMap.compute(logPath, (_, existingData) => {
      if (existingData != null) {
        if (existingData.lastRatifiedCommitVersion != -1) {
          throw new IllegalStateException(
            s"Table $logPath already exists in the commit-coordinator.")
        }
        // If lastRatifiedCommitVersion is -1 i.e. the commit-coordinator has never attempted any
        // commit for this table => this table was just pre-registered. If there is another
        // pre-registration request for an older version, we reject it and table can't go backward.
        if (currentVersion < existingData.maxCommitVersion) {
          throw new IllegalStateException(
            s"Table $logPath already registered with commit-coordinator")
        }
      }
      newPerTableData
    })
    Map.empty[String, String].asJava
  }

  def dropTable(logPath: Path): Unit = {
    withWriteLock(logPath) {
      perTableMap.remove(logPath)
    }
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other

  private[delta] def removeCommitTestOnly(
      logPath: Path,
      commitVersion: Long
  ): Unit = {
    val tableData = perTableMap.get(logPath)
    tableData.commitsMap.remove(commitVersion)
    if (commitVersion == tableData.maxCommitVersion) {
      tableData.maxCommitVersion -= 1
    }
  }
}

/**
 * The [[InMemoryCommitCoordinatorBuilder]] class is responsible for creating singleton instances of
 * [[InMemoryCommitCoordinator]] with the specified batchSize.
 */
case class InMemoryCommitCoordinatorBuilder(batchSize: Long) extends CommitCoordinatorBuilder {
  private lazy val inMemoryStore = new InMemoryCommitCoordinator(batchSize)

  /** Name of the commit-coordinator */
  def getName: String = "in-memory"

  /** Returns a commit-coordinator based on the given conf */
  def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    inMemoryStore
  }
}
