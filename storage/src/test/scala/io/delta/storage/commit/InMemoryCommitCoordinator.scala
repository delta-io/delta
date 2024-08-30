/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.storage.commit

import java.lang.{Long => JLong}
import java.nio.file.FileAlreadyExistsException
import java.util.{ArrayList, Collections, Iterator => JIterator, Map => JMap, Optional, TreeMap, UUID}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import io.delta.storage.LogStore
import io.delta.storage.commit.actions.AbstractMetadata
import io.delta.storage.commit.actions.AbstractProtocol
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class InMemoryCommitCoordinator(val batchSize: Long) extends CommitCoordinatorClient {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[InMemoryCommitCoordinator])

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
  private[commit] class PerTableData(
    var maxCommitVersion: Long = -1,
    var active: Boolean = false
  ) {
    def updateLastRatifiedCommit(commitVersion: Long): Unit = {
      this.active = true
      this.maxCommitVersion = commitVersion
    }

    /**
     * Returns the last ratified commit version for the table. If no commits have been done from
     * commit-coordinator yet, returns -1.
     */
    def lastRatifiedCommitVersion: Long = if (!active) -1 else maxCommitVersion

    // Map from version to Commit data
    val commitsMap: TreeMap[Long, Commit] = new TreeMap[Long, Commit]
    // We maintain maxCommitVersion explicitly since commitsMap might be empty
    // if all commits for a table have been backfilled.
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  }

  private[commit] val perTableMap = new ConcurrentHashMap[String, PerTableData]()

  override def registerTable(
      logPath: Path,
      tableIdentifier: Optional[TableIdentifier],
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): JMap[String, String] = {
    val newPerTableData = new PerTableData(currentVersion + 1)
    perTableMap.compute(logPath.toString, (_, existingData) => {
      if (existingData != null) {
        if (existingData.lastRatifiedCommitVersion != -1) {
          throw new IllegalStateException(
            s"Table $logPath already exists in the commit-coordinator.")
        }
        // If lastRatifiedCommitVersion is -1 i.e. the commit-coordinator has never
        // attempted any commit for this table => this table was just pre-registered. If
        // there is another pre-registration request for an older version, we reject it and
        // table can't go backward.
        if (currentVersion < existingData.maxCommitVersion) {
          throw new IllegalStateException(
            s"Table $logPath already registered with commit-coordinator")
        }
      }
      newPerTableData
    })
    Collections.emptyMap[String, String]()
  }

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      tableDesc: TableDescriptor,
      commitVersion: Long,
      actions: JIterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
    val logPath = tableDesc.getLogPath
    val tablePath = CoordinatedCommitsUtils.getTablePath(logPath)
    if (commitVersion == 0) {
      throw new CommitFailedException(false, false, "Commit version 0 must go via filesystem.")
    }
    logger.info("Attempting to commit version {} on table {}", commitVersion, tablePath)
    val fs = logPath.getFileSystem(hadoopConf)
    if (batchSize <= 1) {
      // Backfill until `commitVersion - 1`
      logger.info(
        "Making sure commits are backfilled until {}" + " version for table {}",
        commitVersion - 1,
        tablePath)
      backfillToVersion(logStore, hadoopConf, tableDesc, commitVersion - 1, null)
    }
    // Write new commit file in _commits directory
    val fileStatus = CoordinatedCommitsUtils.writeCommitFile(
      logStore, hadoopConf, logPath.toString, commitVersion, actions, generateUUID())
    // Do the actual commit
    val commitTimestamp = updatedActions.getCommitInfo.getCommitTimestamp
    val commitResponse = addToMap(logPath, commitVersion, fileStatus, commitTimestamp)

    val mcToFsConversion = CoordinatedCommitsUtils.isCoordinatedCommitsToFSConversion(
      commitVersion, updatedActions)
    // Backfill if needed
    if (batchSize <= 1) {
      // Always backfill when batch size is configured as 1
      backfill(logStore, hadoopConf, logPath, commitVersion, fileStatus)
      val targetFile = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, commitVersion)
      val targetFileStatus = fs.getFileStatus(targetFile)
      val newCommit = commitResponse.getCommit.withFileStatus(targetFileStatus)
      return new CommitResponse(newCommit)
    }
    else if (commitVersion % batchSize == 0 || mcToFsConversion) {
      logger.info(
        "Making sure commits are backfilled till {} version for table {}",
        commitVersion,
        tablePath)
      backfillToVersion(logStore, hadoopConf, tableDesc, commitVersion, null)
    }
    logger.info("Commit {} done successfully on table {}", commitVersion, tablePath)
    commitResponse
  }

  override def getCommits(
      tableDesc: TableDescriptor,
      startVersion: JLong,
      endVersion: JLong)
      : GetCommitsResponse = withReadLock[GetCommitsResponse](tableDesc.getLogPath) {
    val tableData = perTableMap.get(tableDesc.getLogPath.toString)
    val startVersionOpt = Optional.ofNullable(startVersion)
    val endVersionOpt = Optional.ofNullable(endVersion)
    val effectiveStartVersion = startVersionOpt.orElse(0L)
    // Calculate the end version for the range, or use the last key if endVersion is not
    // provided
    val effectiveEndVersion = endVersionOpt.orElseGet(
      () => if (tableData.commitsMap.isEmpty) effectiveStartVersion
      else tableData.commitsMap.lastKey)
    val commitsInRange = tableData.commitsMap.subMap(effectiveStartVersion, effectiveEndVersion + 1)
    new GetCommitsResponse(
      new ArrayList[Commit](commitsInRange.values), tableData.lastRatifiedCommitVersion)
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      tableDesc: TableDescriptor,
      version: Long,
      lastKnownBackfilledVersion: JLong): Unit = {
    val logPath = tableDesc.getLogPath
    // Confirm the last backfilled version by checking the backfilled delta file's existence.
    var validLastKnownBackfilledVersion = lastKnownBackfilledVersion
    if (lastKnownBackfilledVersion != null) {
      val fs = logPath.getFileSystem(hadoopConf)
      if (!fs.exists(CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, version))) {
        validLastKnownBackfilledVersion = null
      }
    }
    var startVersion: JLong = null
    if (validLastKnownBackfilledVersion != null) startVersion = validLastKnownBackfilledVersion + 1
    val commitsResponse = getCommits(tableDesc, startVersion, version)
    commitsResponse.getCommits.forEach((commit: Commit) => {
      backfill(logStore, hadoopConf, logPath, commit.getVersion, commit.getFileStatus)
    })
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other

  /** Backfills a given `fileStatus` to `version`.json */
  protected def backfill(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      version: Long,
      fileStatus: FileStatus): Unit = {
    val targetFile = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, version)
    logger.info("Backfilling commit " + fileStatus.getPath + " to " + targetFile)
    val commitContentIterator = logStore.read(fileStatus.getPath, hadoopConf)
    try {
      logStore.write(targetFile, commitContentIterator, false, hadoopConf)
      registerBackfill(logPath, version)
    } catch {
      case _: FileAlreadyExistsException =>
        logger.info("The backfilled file " + targetFile + " already exists.")
    } finally commitContentIterator.close()
  }

  protected def generateUUID(): String = UUID.randomUUID().toString

  private def addToMap(
      logPath: Path,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse = withWriteLock[CommitResponse](logPath) {
    val tableData = perTableMap.get(logPath.toString)
    val expectedVersion = tableData.maxCommitVersion + 1
    if (commitVersion != expectedVersion) {
      throw new CommitFailedException(
        commitVersion < expectedVersion,
        commitVersion < expectedVersion,
        s"Commit version $commitVersion is not valid. Expected version: $expectedVersion.")
    }
    val commit = new Commit(commitVersion, commitFile, commitTimestamp)
    tableData.commitsMap.put(commitVersion, commit)
    tableData.updateLastRatifiedCommit(commitVersion)

    logger.info("Added commit file " + commitFile.getPath + " to commit-coordinator.")
    new CommitResponse(commit)
  }

  /**
   * Callback to tell the CommitCoordinator that all commits <= `backfilledVersion` are
   * backfilled.
   */
  protected[delta] def registerBackfill(logPath: Path, backfilledVersion: Long): Unit = {
    withWriteLock(logPath) {
      val tableData = perTableMap.get(logPath.toString)
      if (backfilledVersion > tableData.lastRatifiedCommitVersion) {
        throw new IllegalArgumentException(
          "Unexpected backfill version: " + backfilledVersion + ". " +
            "Max backfill version: " + tableData.maxCommitVersion)
      }
      // Remove keys with versions less than or equal to 'untilVersion'
      val iterator = tableData.commitsMap.keySet.iterator
      while (iterator.hasNext) {
        val version = iterator.next
        if (version <= backfilledVersion) {
          iterator.remove()
        } else {
          return
        }
      }
    }
  }

  private[commit] def withReadLock[T](logPath: Path)(operation: => T): T = {
    val tableData = perTableMap.get(logPath.toString)
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

  private[commit] def withWriteLock[T](logPath: Path)(operation: => T): T = {
    val tableData = Option(perTableMap.get(logPath.toString)).getOrElse {
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
}
