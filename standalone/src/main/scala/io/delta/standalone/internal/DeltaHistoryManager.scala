/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.sql.Timestamp

import org.apache.hadoop.fs.Path
import io.delta.standalone.internal.actions.{Action, CommitInfo, CommitMarker}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.FileNames
import io.delta.standalone.internal.storage.ReadOnlyLogStore

/**
 * This class keeps tracks of the version of commits and their timestamps for a Delta table to
 * help with operations like describing the history of a table.
 *
 * @param deltaLog the transaction log of this table
 */
private[internal] case class DeltaHistoryManager(deltaLog: DeltaLogImpl) {

  /** Get the persisted commit info for the given delta file. */
  def getCommitInfo(version: Long): CommitInfo = {
    val info = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, version))
      .iterator
      .map(Action.fromJson)
      .collectFirst { case c: CommitInfo => c }
    if (info.isEmpty) {
      CommitInfo.empty(Some(version))
    } else {
      info.head.copy(version = Some(version))
    }
  }

  /**
   * Check whether the given version can be recreated by replaying the DeltaLog.
   *
   * @throws IllegalArgumentException if version is outside range of available versions
   */
  def checkVersionExists(version: Long): Unit = {
    val earliestVersion = getEarliestReproducibleCommitVersion
    val latestVersion = deltaLog.update().version
    if (version < earliestVersion || version > latestVersion) {
      throw DeltaErrors.versionNotExistException(version, earliestVersion, latestVersion)
    }
  }

  /**
   * Returns the latest commit that happened at or before `time`.
   *
   * @param timestamp the timestamp to search for
   * @throws RuntimeException if the state at the given commit in not recreatable
   * @throws IllegalArgumentException if the provided timestamp is before the earliest commit or
   *                                  after the latest commit
   */
  def getActiveCommitAtTime(timestamp: Timestamp): Commit = {
    val time = timestamp.getTime
    val earliestVersion = getEarliestReproducibleCommitVersion
    val latestVersion = deltaLog.update().version

    // Search for the commit
    val commits = getCommits(deltaLog.store, deltaLog.logPath, earliestVersion, latestVersion + 1)

    // If it returns empty, we will fail below with `timestampEarlierThanTableFirstCommit`
    val commit = lastCommitBeforeTimestamp(commits, time).getOrElse(commits.head)

    // Error handling
    val commitTs = new Timestamp(commit.timestamp)
    if (commit.timestamp > time) {
      throw DeltaErrors.timestampEarlierThanTableFirstCommit(timestamp, commitTs)
    } else if (commit.timestamp < time && commit.version == latestVersion) {
      throw DeltaErrors.timestampLaterThanTableLastCommit(timestamp, commitTs)
    }

    commit
  }

  /**
   * Get the earliest commit, which we can recreate. Note that this version isn't guaranteed to
   * exist when performing an action as a concurrent operation can delete the file during cleanup.
   * This value must be used as a lower bound.
   *
   * We search for the earliest checkpoint we have, or whether we have the 0th delta file, because
   * that way we can reconstruct the entire history of the table. This method assumes that the
   * commits are contiguous.
   */
  private def getEarliestReproducibleCommitVersion: Long = {
    val files = deltaLog.store.listFrom(FileNames.deltaFile(deltaLog.logPath, 0))
      .filter(f => FileNames.isDeltaFile(f.getPath) || FileNames.isCheckpointFile(f.getPath))

    // A map of checkpoint version and number of parts, to number of parts observed
    val checkpointMap = new scala.collection.mutable.HashMap[(Long, Int), Int]()
    var smallestDeltaVersion = Long.MaxValue
    var lastCompleteCheckpoint: Option[Long] = None

    // Iterate through the log files - this will be in order starting from the lowest version.
    // Checkpoint files come before deltas, so when we see a checkpoint, we remember it and
    // return it once we detect that we've seen a smaller or equal delta version.
    while (files.hasNext) {
      val nextFilePath = files.next().getPath
      if (FileNames.isDeltaFile(nextFilePath)) {
        val version = FileNames.deltaVersion(nextFilePath)
        if (version == 0L) return version
        smallestDeltaVersion = math.min(version, smallestDeltaVersion)

        // Note that we also check this condition at the end of the function - we check it
        // here too to to try and avoid more file listing when it's unnecessary.
        if (lastCompleteCheckpoint.exists(_ >= smallestDeltaVersion)) {
          return lastCompleteCheckpoint.get
        }
      } else if (FileNames.isCheckpointFile(nextFilePath)) {
        val checkpointVersion = FileNames.checkpointVersion(nextFilePath)
        val parts = FileNames.numCheckpointParts(nextFilePath)
        if (parts.isEmpty) {
          lastCompleteCheckpoint = Some(checkpointVersion)
        } else {
          // if we have a multi-part checkpoint, we need to check that all parts exist
          val numParts = parts.getOrElse(1)
          val preCount = checkpointMap.getOrElse(checkpointVersion -> numParts, 0)
          if (numParts == preCount + 1) {
            lastCompleteCheckpoint = Some(checkpointVersion)
          }
          checkpointMap.put(checkpointVersion -> numParts, preCount + 1)
        }
      }
    }

    if (lastCompleteCheckpoint.exists(_ >= smallestDeltaVersion)) {
      lastCompleteCheckpoint.get
    } else if (smallestDeltaVersion < Long.MaxValue) {
      throw DeltaErrors.noReproducibleHistoryFound(deltaLog.logPath)
    } else {
      throw DeltaErrors.noHistoryFound(deltaLog.logPath)
    }
  }

  /**
   * Returns the commit version and timestamps of all commits in `[start, end)`. If `end` is not
   * specified, will return all commits that exist after `start`. Will guarantee that the commits
   * returned will have both monotonically increasing versions as well as timestamps.
   * Exposed for tests.
   */
  private def getCommits(
      logStore: ReadOnlyLogStore,
      logPath: Path,
      start: Long,
      end: Long): Array[Commit] = {
    val commits = logStore.listFrom(FileNames.deltaFile(logPath, start))
      .filter(f => FileNames.isDeltaFile(f.getPath))
      .map { fileStatus =>
        Commit(FileNames.deltaVersion(fileStatus.getPath), fileStatus.getModificationTime)
      }
      .takeWhile(_.version < end)

    monotonizeCommitTimestamps(commits.toArray)
  }

  /**
   * Makes sure that the commit timestamps are monotonically increasing with respect to commit
   * versions. Requires the input commits to be sorted by the commit version.
   */
  private def monotonizeCommitTimestamps[T <: CommitMarker](commits: Array[T]): Array[T] = {
    var i = 0
    val length = commits.length
    while (i < length - 1) {
      val prevTimestamp = commits(i).getTimestamp
      assert(commits(i).getVersion < commits(i + 1).getVersion, "Unordered commits provided.")
      if (prevTimestamp >= commits(i + 1).getTimestamp) {
        commits(i + 1) = commits(i + 1).withTimestamp(prevTimestamp + 1).asInstanceOf[T]
      }
      i += 1
    }
    commits
  }

  /** Returns the latest commit that happened at or before `time`. */
  private def lastCommitBeforeTimestamp(commits: Seq[Commit], time: Long): Option[Commit] = {
    val i = commits.lastIndexWhere(_.timestamp <= time)
    if (i < 0) None else Some(commits(i))
  }

  /** A helper class to represent the timestamp and version of a commit. */
  case class Commit(version: Long, timestamp: Long) extends CommitMarker {
    override def withTimestamp(timestamp: Long): Commit = this.copy(timestamp = timestamp)

    override def getTimestamp: Long = timestamp

    override def getVersion: Long = version
  }
}
