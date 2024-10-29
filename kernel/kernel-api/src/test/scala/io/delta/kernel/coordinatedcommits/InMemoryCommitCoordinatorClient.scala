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

package io.delta.kernel.coordinatedcommits

import io.delta.kernel.TableIdentifier
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.engine.coordinatedcommits.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.kernel.engine.coordinatedcommits.{Commit, CommitFailedException, CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{ArrayList, Collections, Optional, TreeMap, UUID, Map => JMap}

class InMemoryCommitCoordinatorClient(val batchSize: Long) extends CommitCoordinatorClient {
  private val logger: Logger = LoggerFactory.getLogger(classOf[InMemoryCommitCoordinatorClient])
  private val perTableMap = new ConcurrentHashMap[String, PerTableData]()

  /**
   * @param maxCommitVersion represents the max commit version known for the table. This is
   *                         initialized at the time of pre-registration and updated whenever a
   *                         commit is successfully added to the commit-coordinator.
   * @param active           represents whether this commit-coordinator has ratified any commit or
   *                         not.
   * |----------------------------|------------------|---------------------------|
   * |        State               | maxCommitVersion |          active           |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    | currentVersion+1 |          false            |
   * |----------------------------|------------------|---------------------------|
   * | Table is pre-registered    |       X          |          true             |
   * | and more commits are done  |                  |                           |
   * |----------------------------|------------------|---------------------------|
   */
  private class PerTableData(
      var maxCommitVersion: Long = -1,
      var active: Boolean = false) {
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

  /////////////////
  // Public APIs //
  /////////////////

  override def registerTable(
      engine: Engine,
      logPath: String,
      tableIdentifier: TableIdentifier,
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
      engine: Engine,
      tableDescriptor: TableDescriptor,
      commitVersion: Long,
      actions: CloseableIterator[Row],
      updatedActions: UpdatedActions): CommitResponse = {
    val logPath = tableDescriptor.getLogPath
    val tablePath = new Path(logPath).getParent.toString
    if (commitVersion == 0) {
      throw new CommitFailedException(false, false, "Commit version 0 must go via filesystem.")
    }
    logger.info("Attempting to commit version {} on table {}", commitVersion, tablePath)
    if (batchSize <= 1) {
      // Backfill until `commitVersion - 1`
      logger.info(
        "Making sure commits are backfilled until {}" + " version for table {}",
        commitVersion - 1,
        tablePath)
      backfillToVersion(engine, tableDescriptor, commitVersion - 1, null)
    }
    // Write new commit file in _commits directory
    val fileStatus = CommitCoordinatorUtils.writeUnbackfilledCommitFile(
      engine, logPath, commitVersion, actions, generateUUID())
    // Do the actual commit
    val commitTimestamp = updatedActions.getCommitInfo.getCommitTimestamp
    val commitResponse = addToMap(logPath, commitVersion, fileStatus, commitTimestamp)

    val mcToFsConversion = CommitCoordinatorUtils.isCoordinatedCommitsToFSConversion(
      commitVersion, updatedActions)
    // Backfill if needed
    if (batchSize <= 1) {
      // Always backfill when batch size is configured as 1
      val targetFile = backfill(engine, logPath, commitVersion, fileStatus)
      val targetFileStatus = engine.getFileSystemClient.getFileStatus(targetFile)
      val oldCommit = commitResponse.getCommit
      val newCommit = new Commit(oldCommit.getVersion, targetFileStatus, oldCommit.getTimestamp)
      return new CommitResponse(newCommit)
    }
    else if (commitVersion % batchSize == 0 || mcToFsConversion) {
      logger.info(
        "Making sure commits are backfilled till {} version for table {}",
        commitVersion,
        tablePath)
      backfillToVersion(engine, tableDescriptor, commitVersion, null)
    }
    logger.info("Commit {} done successfully on table {}", commitVersion, tablePath)
    commitResponse
  }

  override def getCommits(
      engine: Engine,
      tableDescriptor: TableDescriptor,
      startVersion: java.lang.Long,
      endVersion: java.lang.Long): GetCommitsResponse =
    withReadLock[GetCommitsResponse](tableDescriptor.getLogPath) {
      val tableData = perTableMap.get(tableDescriptor.getLogPath)
      val startVersionOpt = Optional.ofNullable(startVersion)
      val endVersionOpt = Optional.ofNullable(endVersion)
      val effectiveStartVersion = startVersionOpt.orElse(0L)
      // Calculate the end version for the range, or use the last key if endVersion is not
      // provided
      val effectiveEndVersion = endVersionOpt.orElseGet(
        () => if (tableData.commitsMap.isEmpty) effectiveStartVersion
        else tableData.commitsMap.lastKey)
      val commitsInRange =
        tableData.commitsMap.subMap(effectiveStartVersion, effectiveEndVersion + 1)
      new GetCommitsResponse(
        new ArrayList[Commit](commitsInRange.values), tableData.lastRatifiedCommitVersion)
    }

  override def backfillToVersion(
      engine: Engine,
      tableDescriptor: TableDescriptor,
      version: Long,
      lastKnownBackfilledVersion: java.lang.Long): Unit = {
    val logPath = tableDescriptor.getLogPath
    // Confirm the last backfilled version by checking the backfilled delta file's existence.
    var validLastKnownBackfilledVersion = lastKnownBackfilledVersion
    if (lastKnownBackfilledVersion != null) {
      val backfilledFileExists = engine
        .getFileSystemClient
        .exists(CommitCoordinatorUtils.getBackfilledDeltaFilePath(logPath, version))
      if (!backfilledFileExists) {
        validLastKnownBackfilledVersion = null
      }
    }
    var startVersion: java.lang.Long = null
    if (validLastKnownBackfilledVersion != null) startVersion = validLastKnownBackfilledVersion + 1
    val commitsResponse = getCommits(engine, tableDescriptor, startVersion, version)
    commitsResponse.getCommits.forEach((commit: Commit) => {
      backfill(engine, logPath, commit.getVersion, commit.getFileStatus)
    })
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other

  ///////////////////////
  // Protected Methods //
  ///////////////////////

  /** Visible for testing. */
  protected def generateUUID(): String = UUID.randomUUID().toString

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  /**
   * Backfills a given `fileStatus` to `version`.json.
   *
   * Returns the backfilled path.
   */
  private def backfill(
      engine: Engine,
      logPath: String,
      version: Long,
      fileStatus: FileStatus): String = {
    val targetFile = CommitCoordinatorUtils.getBackfilledDeltaFilePath(logPath, version)
    logger.info(s"Backfilling commit ${fileStatus.getPath} to $targetFile")

    try {
      engine.getFileSystemClient.copy(fileStatus.getPath, targetFile, false /* overwrite */)

      registerBackfill(logPath, version)
    } catch {
      case _: FileAlreadyExistsException =>
        logger.info(s"The backfilled file $targetFile already exists.")
    }

    targetFile
  }

  private def addToMap(
      logPath: String,
      commitVersion: Long,
      commitFile: FileStatus,
      commitTimestamp: Long): CommitResponse = withWriteLock[CommitResponse](logPath) {
    val tableData = perTableMap.get(logPath)
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
   * Callback to tell the CommitCoordinator that all commits <= `backfilledVersion` are backfilled.
   */
  private def registerBackfill(logPath: String, backfilledVersion: Long): Unit = {
    withWriteLock(logPath) {
      val tableData = perTableMap.get(logPath)
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

  private def withReadLock[T](logPath: String)(operation: => T): T = {
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

  private def withWriteLock[T](logPath: String)(operation: => T): T = {
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
}
