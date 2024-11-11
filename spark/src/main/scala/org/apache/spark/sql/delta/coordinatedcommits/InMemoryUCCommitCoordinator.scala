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

import java.io.IOException
import java.net.URI
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.util.FileNames
import io.delta.storage.commit.{
  Commit => JCommit,
  CommitFailedException => JCommitFailedException,
  GetCommitsResponse => JGetCommitsResponse
}
import io.delta.storage.commit.uccommitcoordinator.{CommitLimitReachedException => JCommitLimitReachedException, InvalidTargetTableException => JInvalidTargetTableException}
import org.apache.hadoop.fs.{FileStatus, Path}


final object UCCoordinatedCommitsRequestType extends Enumeration {
  type UCCoordinatedCommitsRequestType = Value
  val COMMIT = Value
  val GET_COMMITS = Value
}

/**
 * A mock UC commit coordinator for testing purposes.
 */
class InMemoryUCCommitCoordinator {

  /**
   * Represents the data associated with a table.
   * `ucCommits` mimics the underlying list for the commit files.
   */
  private class PerTableData(val path: URI) {

    /**
     * Represents a UC commit record.
     * @param commit represents the commit itself.
     * @param isBackfilled represents whether the commit is backfilled or not.
     */
    private case class UCCommit(
        commit: JCommit,
        isBackfilled: Boolean = false) {
      /** Version of the underlying commit file */
      val version: Long = commit.getVersion
    }

    /** Underlying storage of UC commit records */
    private val ucCommits: mutable.ArrayBuffer[UCCommit] = mutable.ArrayBuffer.empty

    /** RWLock to protect the commitsMap */
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()

    /**
     * Returns the last ratified commit version for the table.
     * If no commits have been done from commit-coordinator yet, returns -1.
     */
    def lastRatifiedCommitVersion: Long = ucCommits.lastOption.map(_.version).getOrElse(-1L)

    /**
     * Returns true if:
     * - the table has ratified any commit, and
     */
    def isActive: Boolean = {
      ucCommits.nonEmpty
    }

    /** Appends a commit to the table's commit history */
    def appendCommit(
        commit: JCommit
    ): Unit = {
      ucCommits += UCCommit(commit)
    }

    /** Removes all commits until the given version (inclusive) */
    def removeCommitsUntilVersion(version: Long): Unit = {
      val toRemove = ucCommits.takeWhile(_.version <= version)
      ucCommits --= toRemove
    }

    /** Marks the last commit as backfilled */
    def markLastCommitBackfilled(): Unit = {
      ucCommits.lastOption.foreach { lastUCCommit =>
        ucCommits.update(ucCommits.size - 1, lastUCCommit.copy(isBackfilled = true))
      }
    }

    /**
     * Returns the unbackfilled commits in the given range.
     * If `startVersion` is not provided, the first commit is used.
     * If `endVersion` is not provided, the last commit is used.
     */
    def getCommits(startVersion: Option[Long], endVersion: Option[Long]): Seq[JCommit] = {
      val effectiveStartVersion = startVersion.getOrElse(0L)
      val effectiveEndVersion = endVersion.getOrElse(
        ucCommits.lastOption.map(_.version).getOrElse(return Seq.empty))
      // Collect unbackfilled `Commit`s from the `UCCommit`s in the range.
      ucCommits.filter(c =>
        effectiveStartVersion <= c.version && c.version <= effectiveEndVersion && !c.isBackfilled
      ).map(_.commit).toSeq
    }
  }

  /**
   * Variable to allow to control the behavior of the InMemoryUCCommitCoordinator
   * externally. If set to true, the coordinator will throw an IOException after
   * a successful commit. This will be reset to false once the exception has been
   * thrown.
   */
  var throwIOExceptionAfterCommit: Boolean = false

  /**
   * Variable to allow to control the behavior of the InMemoryUCCommitCoordinator
   * externally. If set to true, the coordinator will throw an IOException before
   * persisting a commit to the in memory map. This will be reset to false once the
   * exception has been thrown.
   */
  var throwIOExceptionBeforeCommit: Boolean = false

  /** The maximum number of unbackfilled commits this commit coordinator can store at a time */
  private val MAX_NUM_COMMITS = 10

  /**
   * Map from table UUID to the data associated with the table.
   * Mimics the underlying storage for the commit files of different tables.
   */
  private val perTableMap = new ConcurrentHashMap[UUID, PerTableData]()

  /** Performs the given operation with lock acquired on the table entry */
  private def withLock[T](tableUUID: UUID, writeLock: Boolean = false)(operation: => T): T = {
    val tableData = Option(perTableMap.get(tableUUID)).getOrElse {
      throw new IllegalArgumentException(s"Unknown table $tableUUID.")
    }
    val lock = if (writeLock) tableData.lock.writeLock() else tableData.lock.readLock()
    lock.lock()
    try {
      operation
    } finally {
      lock.unlock()
    }
  }

  private def validateTableURI(
      srcTable: URI,
      targetTable: URI,
      request: UCCoordinatedCommitsRequestType.UCCoordinatedCommitsRequestType): Unit = {
    if (srcTable != targetTable) {
      val errorMsg = s"Source table $srcTable and targetTable $targetTable do not match for " +
        s"$request"
      throw new JInvalidTargetTableException(errorMsg)
    }
  }

  // scalastyle:off argcount
  def commitToCoordinator(
       tableId: String,
       tableUri: URI,
       commitFileName: Option[String] = None,
       commitVersion: Option[Long] = None,
       commitFileSize: Option[Long] = None,
       commitFileModTime: Option[Long] = None,
       commitTimestamp: Option[Long] = None,
       lastKnownBackfilledVersion: Option[Long] = None,
       isDisownCommit: Boolean = false,
       protocolOpt: Option[Protocol] = None,
       metadataOpt: Option[Metadata] = None): Unit = {
    // either commitFileName or backfilledUntil (or both) need to be set
    require(commitFileName.nonEmpty || lastKnownBackfilledVersion.nonEmpty)
    val tableUUID = UUID.fromString(tableId)
    // if this is the first commit, we just accept it
    val path = Option(perTableMap.get(tableUUID)).map(_.path).getOrElse {
      // The first commit has to be an actual commit and not just a backfill-only
      // request so commitVersion.get is accessible.
      require(commitVersion.nonEmpty)
      // Register the table with the commit coordinator.
      perTableMap.putIfAbsent(tableUUID, new PerTableData(tableUri))
      tableUri
    }

    commitFileName.foreach { fileName =>
      // ensure that all other necessary parameters are provided
      require(commitVersion.nonEmpty)
      require(commitFileSize.nonEmpty)
      require(commitFileModTime.nonEmpty)
      require(commitTimestamp.nonEmpty)
      validateTableURI(path, tableUri, UCCoordinatedCommitsRequestType.COMMIT)

      // Check that there is still space in the commit coordinator.
      val currentNumCommits = getCommitsFromCoordinator(
        tableId, tableUri, startVersion = None, endVersion = None).getCommits.size
      if (currentNumCommits == MAX_NUM_COMMITS) {
        val errorMsg = s"Too many unbackfilled commits for $tableId. Cannot " +
          s"store more than $MAX_NUM_COMMITS commits"
        throw new JCommitLimitReachedException(errorMsg)
      }

      if (throwIOExceptionBeforeCommit) {
        throwIOExceptionBeforeCommit = false
        throw new IOException("Problem before comitting")
      }
      // Store the commit. For the InMemoryUCCommit coordinator, we concatenate the full commit path
      // here already so that we don't have to do it during getCommits.
      val basePath = FileNames.commitDirPath(
        DeltaTableUtils.safeConcatPaths(new Path(tableUri), "_delta_log"))
      val commitFilePath = new Path(basePath, fileName)
      val fileStatus = new FileStatus(
        commitFileSize.get, false, 0, 0, commitFileModTime.get, commitFilePath)
      withLock(tableUUID, writeLock = true) {
        val tableData = perTableMap.get(tableUUID)
        // We only check the expected version matches the commit version if the table is active.
        // If the table was just registered, the check is not necessary.
        if (tableData.isActive) {
          val expectedVersion = tableData.lastRatifiedCommitVersion + 1
          if (commitVersion.get != expectedVersion) {
            throw new JCommitFailedException(
              commitVersion.get < expectedVersion,
              commitVersion.get < expectedVersion,
              s"Commit version ${commitVersion.get} is not valid. " +
                s"Expected version: $expectedVersion.")
          }
        }
        tableData.appendCommit(
          new JCommit(commitVersion.get, fileStatus, commitTimestamp.get)
        )
      }
    }
    if (throwIOExceptionAfterCommit) {
      throwIOExceptionAfterCommit = false
      throw new IOException("Problem after comitting")
    }

    // Register any backfills.
    lastKnownBackfilledVersion.foreach { backfilledUntil =>
      withLock(tableUUID, writeLock = true) {
        val tableData = perTableMap.get(tableUUID)
        val maxVersionToRemove = if (backfilledUntil == tableData.lastRatifiedCommitVersion) {
          // If the backfill version is the last ratified commit version, we remove all but the
          // last commit, and mark the last commit as backfilled. This is to ensure that every
          // active table keeps track of at least one commit record.
          tableData.markLastCommitBackfilled()
          backfilledUntil - 1
        } else if (backfilledUntil < tableData.lastRatifiedCommitVersion) {
          backfilledUntil
        } else {
          throw new IllegalArgumentException(
            s"Unexpected backfill version: $backfilledUntil. " +
              s"Max backfill version: ${tableData.lastRatifiedCommitVersion}")
        }
        tableData.removeCommitsUntilVersion(maxVersionToRemove)
      }
    }
  }

  def getCommitsFromCoordinator(
      tableId: String,
      tableUri: URI,
      startVersion: Option[Long],
      endVersion: Option[Long]): JGetCommitsResponse = {
    val tableUUID = UUID.fromString(tableId)
    val path = Option(perTableMap.get(tableUUID)).map(_.path).getOrElse {
      return new JGetCommitsResponse(Seq.empty.asJava, -1)
    }
    validateTableURI(path, tableUri, UCCoordinatedCommitsRequestType.GET_COMMITS)
    withLock[JGetCommitsResponse](tableUUID) {
      val tableData = perTableMap.get(tableUUID)
      val commits = tableData.getCommits(startVersion, endVersion)
      new JGetCommitsResponse(commits.asJava, tableData.lastRatifiedCommitVersion)
    }
  }
}
