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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.sql.Timestamp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.actions.{Action, CommitInfo, CommitMarker, JobInfo, NotebookInfo}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{DateTimeUtils, DeltaCommitFileProvider, FileNames, TimestampFormatter}
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

/**
 * This class keeps tracks of the version of commits and their timestamps for a Delta table to
 * help with operations like describing the history of a table.
 *
 * @param deltaLog The transaction log of this table
 * @param maxKeysPerList How many commits to list when performing a parallel search. Exposed for
 *                       tests. Currently set to `1000`, which is the maximum keys returned by S3
 *                       per list call. Azure can return `5000`, therefore we choose 1000.
 */
class DeltaHistoryManager(
    deltaLog: DeltaLog,
    maxKeysPerList: Int = 1000) extends DeltaLogging {

  private def spark: SparkSession = SparkSession.active

  private def getSerializableHadoopConf: SerializableConfiguration = {
    new SerializableConfiguration(deltaLog.newDeltaHadoopConf())
  }

  import DeltaHistoryManager._

  /**
   * Returns the information of the latest `limit` commits made to this table in reverse
   * chronological order.
   */
  def getHistory(limitOpt: Option[Int]): Seq[DeltaHistory] = {
    val listStart = limitOpt.map { limit =>
      math.max(deltaLog.update().version - limit + 1, 0)
    }.getOrElse(getEarliestDeltaFile(deltaLog))
    getHistory(listStart)
  }

  /**
   * Get the commit information of the Delta table from commit `[start, end)`. If `end` is `None`,
   * we return all commits from start to now.
   */
  def getHistory(
      start: Long,
      endOpt: Option[Long] = None): Seq[DeltaHistory] = {
    import org.apache.spark.sql.delta.implicits._
    val conf = getSerializableHadoopConf
    val logPath = deltaLog.logPath.toString
    val snapshot = endOpt.map(end => deltaLog.getSnapshotAt(end - 1)).getOrElse(deltaLog.update())
    val commitFileProvider = DeltaCommitFileProvider(snapshot)
    // We assume that commits are contiguous, therefore we try to load all of them in order
    val info = spark.range(start, snapshot.version + 1)
      .mapPartitions { versions =>
        val logStore = LogStore(SparkEnv.get.conf, conf.value)
        val basePath = new Path(logPath)
        val fs = basePath.getFileSystem(conf.value)
        versions.flatMap { commit =>
          try {
            val deltaFile = commitFileProvider.deltaFile(commit)
            val ci = DeltaHistoryManager.getCommitInfo(logStore, deltaFile, conf.value)
            val metadata = fs.getFileStatus(deltaFile)
            Some(ci.withTimestamp(metadata.getModificationTime))
          } catch {
            case _: FileNotFoundException =>
              // We have a race-condition where files can be deleted while reading. It's fine to
              // skip those files
              None
          }
        }.map(DeltaHistory.fromCommitInfo)
      }
    // Spark should return the commits in increasing order as well
    monotonizeCommitTimestamps(info.collect()).reverse
  }

  /**
   * Returns the latest commit that happened at or before `time`.
   * @param timestamp The timestamp to search for
   * @param canReturnLastCommit Whether we can return the latest version of the table if the
   *                            provided timestamp is after the latest commit
   * @param mustBeRecreatable Whether the state at the given commit should be recreatable
   * @param canReturnEarliestCommit Whether we can return the earliest commit if no such commit
   *                                exists.
   */
  def getActiveCommitAtTime(
      timestamp: Timestamp,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean = true,
      canReturnEarliestCommit: Boolean = false): Commit = {
    val time = timestamp.getTime
    val earliest = if (mustBeRecreatable) {
      getEarliestRecreatableCommit
    } else {
      getEarliestDeltaFile(deltaLog)
    }
    val latestVersion = deltaLog.update().version

    // Search for the commit
    val commit = if (latestVersion - earliest > 2 * maxKeysPerList) {
      parallelSearch(time, earliest, latestVersion + 1)
    } else {
      val commits = getCommits(
        deltaLog.store,
        deltaLog.logPath,
        earliest,
        Some(latestVersion + 1),
        deltaLog.newDeltaHadoopConf())
      // If it returns empty, we will fail below with `timestampEarlierThanCommitRetention`.
      lastCommitBeforeTimestamp(commits, time).getOrElse(commits.head)
    }

    // Error handling
    val commitTs = new Timestamp(commit.timestamp)
    val timestampFormatter = TimestampFormatter(
      DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone))
    val tsString = DateTimeUtils.timestampToString(
      timestampFormatter, DateTimeUtils.fromJavaTimestamp(commitTs))
    if (commit.timestamp > time && !canReturnEarliestCommit) {
      throw DeltaErrors.TimestampEarlierThanCommitRetentionException(timestamp, commitTs, tsString)
    } else if (commit.version == latestVersion && !canReturnLastCommit) {
      if (commit.timestamp < time) {
        throw DeltaErrors.TemporallyUnstableInputException(
          timestamp, commitTs, tsString, commit.version)
      }
    }
    commit
  }

  /**
   * Check whether the given version exists.
   * @param mustBeRecreatable whether the snapshot of this version needs to be recreated.
   * @param allowOutOfRange whether to allow the version is exceeding the latest snapshot version.
   */
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean = true,
      allowOutOfRange: Boolean = false): Unit = {
    val earliest = if (mustBeRecreatable) {
      getEarliestRecreatableCommit
    } else {
      getEarliestDeltaFile(deltaLog)
    }
    val latest = deltaLog.update().version
    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw VersionNotFoundException(version, earliest, latest)
    }
  }

  /**
   * Searches for the latest commit with the timestamp, which has happened at or before `time` in
   * the range `[start, end)`.
   */
  private def parallelSearch(
      time: Long,
      start: Long,
      end: Long): Commit = {
    parallelSearch0(
      spark,
      getSerializableHadoopConf,
      deltaLog.logPath.toString,
      time,
      start,
      end,
      maxKeysPerList)
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
  private[delta] def getEarliestRecreatableCommit: Long = {
    val files = deltaLog.store.listFrom(
        FileNames.listingPrefix(deltaLog.logPath, 0),
        deltaLog.newDeltaHadoopConf())
      .filter(f => FileNames.isDeltaFile(f) || FileNames.isCheckpointFile(f))

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
        // here too to try and avoid more file listing when it's unnecessary.
        if (lastCompleteCheckpoint.exists(_ >= smallestDeltaVersion - 1)) {
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
      return lastCompleteCheckpoint.get
    } else if (smallestDeltaVersion < Long.MaxValue) {
      throw DeltaErrors.noRecreatableHistoryFound(deltaLog.logPath)
    } else {
      throw DeltaErrors.noHistoryFound(deltaLog.logPath)
    }
  }
}

/** Contains many utility methods that can also be executed on Spark executors. */
object DeltaHistoryManager extends DeltaLogging {
  /** Get the persisted commit info (if available) for the given delta file. */
  def getCommitInfoOpt(
      logStore: LogStore,
      deltaFile: Path,
      hadoopConf: Configuration): Option[CommitInfo] = {
    val logs = logStore.readAsIterator(deltaFile, hadoopConf)
    try {
      logs
        .map(Action.fromJson)
        .collectFirst { case c: CommitInfo => c.copy(version = Some(deltaVersion(deltaFile))) }
    } finally {
      logs.close()
    }
  }

  /**
   * Get the persisted commit info for the given delta file. If commit info
   * is not found in the commit, a mostly empty [[CommitInfo]] object with only
   * the version populated will be returned.
   */
  private def getCommitInfo(
      logStore: LogStore,
      deltaFile: Path,
      hadoopConf: Configuration): CommitInfo = {
    getCommitInfoOpt(logStore, deltaFile, hadoopConf).getOrElse {
      CommitInfo.empty(Some(deltaVersion(deltaFile)))
    }
  }

  /**
   * Get the earliest commit available for this table. Note that this version isn't guaranteed to
   * exist when performing an action as a concurrent operation can delete the file during cleanup.
   * This value must be used as a lower bound.
   */
  def getEarliestDeltaFile(deltaLog: DeltaLog): Long = {
    deltaLog.store
      .listFrom(
        path = FileNames.listingPrefix(deltaLog.logPath, 0),
        hadoopConf = deltaLog.newDeltaHadoopConf())
      .collectFirst { case DeltaFile(_, version) => version }
      .getOrElse {
        throw DeltaErrors.noHistoryFound(deltaLog.logPath)
      }
  }

  /**
   * When calling getCommits, the initial few timestamp values may be wrong because they are not
   * properly monotonized. Callers should pass a start value at least
   * this far behind the first timestamp they care about if they need correct values.
   */
  private[delta] val POTENTIALLY_UNMONOTONIZED_TIMESTAMPS = 100

  /**
   * Returns the commit version and timestamps of all commits in `[start, end)`. If `end` is not
   * specified, will return all commits that exist after `start`. Will guarantee that the commits
   * returned will have both monotonically increasing versions as well as timestamps.
   * Exposed for tests.
   */
  private[delta] def getCommits(
      logStore: LogStore,
      logPath: Path,
      start: Long,
      end: Option[Long],
      hadoopConf: Configuration): Array[Commit] = {
    val until = end.getOrElse(Long.MaxValue)
    val commits =
      logStore
        .listFrom(listingPrefix(logPath, start), hadoopConf)
        .collect { case DeltaFile(file, version) => Commit(version, file.getModificationTime) }
        .takeWhile(_.version < until)

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
        logWarning(s"Found Delta commit ${commits(i).getVersion} with a timestamp $prevTimestamp " +
          s"which is greater than the next commit timestamp ${commits(i + 1).getTimestamp}.")
        commits(i + 1) = commits(i + 1).withTimestamp(prevTimestamp + 1).asInstanceOf[T]
      }
      i += 1
    }
    commits
  }

  /**
   * Searches for the latest commit with the timestamp, which has happened at or before `time` in
   * the range `[start, end)`. The algorithm works as follows:
   *  1. We use Spark to list our commit history in parallel `maxKeysPerList` at a time.
   *  2. We then perform our search in each fragment of commits containing at most `maxKeysPerList`
   *     elements.
   *  3. All fragments that are before `time` will return the last commit in the fragment.
   *  4. All fragments that are after `time` will exit early and return the first commit in the
   *     fragment.
   *  5. The fragment that contains the version we are looking for will return the version we are
   *     looking for.
   *  6. Once all the results are returned from Spark, we make sure that the commit timestamps are
   *     monotonically increasing across the fragments, because we couldn't adjust for the
   *     boundaries when working in parallel.
   *  7. We then return the version we are looking for in this smaller list on the Driver.
   * We will return the first available commit if the condition cannot be met. This method works
   * even for boundary commits, and can be best demonstrated through an example:
   * Imagine we have commits 999, 1000, 1001, 1002. t_999 < t_1000 but t_1000 > t_1001 and
   * t_1001 < t_1002. So at the the boundary, we will need to eventually adjust t_1001. Assume the
   * result needs to be t_1001 after the adjustment as t_search < t_1002 and t_search > t_1000.
   * What will happen is that the first fragment will return t_1000, and the second fragment will
   * return t_1001. On the Driver, we will adjust t_1001 = t_1000 + 1 milliseconds, and our linear
   * search will return t_1001.
   *
   * Placed in the static object to avoid serializability issues.
   *
   * @param spark The active SparkSession
   * @param conf The session specific Hadoop Configuration
   * @param logPath The path of the DeltaLog
   * @param time The timestamp to search for in milliseconds
   * @param start Earliest available commit version (approximate is acceptable)
   * @param end Latest available commit version (approximate is acceptable)
   * @param step The number with which to chunk each linear search across commits. Provide the
   *             max number of keys returned by the underlying FileSystem for in a single RPC for
   *             best results.
   */
  private def parallelSearch0(
      spark: SparkSession,
      conf: SerializableConfiguration,
      logPath: String,
      time: Long,
      start: Long,
      end: Long,
      step: Long): Commit = {
    import org.apache.spark.sql.delta.implicits._
    val possibleCommits = spark.range(start, end, step).mapPartitions { startVersions =>
      val logStore = LogStore(SparkEnv.get.conf, conf.value)
      val basePath = new Path(logPath)
      startVersions.map { startVersion =>
        val commits = getCommits(
          logStore,
          basePath,
          startVersion,
          Some(math.min(startVersion + step, end)),
          conf.value)
        lastCommitBeforeTimestamp(commits, time).getOrElse(commits.head)
      }
    }.collect()

    // Spark should return the commits in increasing order as well
    val commitList = monotonizeCommitTimestamps(possibleCommits)
    lastCommitBeforeTimestamp(commitList, time).getOrElse(commitList.head)
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

  /**
   * Abstract iterator for breaking a sequence of items into groups based on custom logic. It wraps
   * an underlying iterator, transforming and grouping its items according to specific rules defined
   * in subclasses. This iterator excels in scenarios requiring the ordered processing of data
   * streams, where specific items mark the beginning of new groups. It ensures items within a group
   * are emitted together.
   */
  private abstract class GroupBreakingIterator[T](
      underlying: Iterator[T]) extends Iterator[ArrayBuffer[T]] {
    private var bufferedOutput: Option[mutable.ArrayBuffer[T]] = None
    private val bufferedUnderlying = underlying.buffered

    protected def isNewGroupStart(item: T): Boolean

    protected def transformItem(item: T): T

    private def queueItemsIfNeeded(): Unit = {
      if (bufferedOutput.isEmpty && bufferedUnderlying.hasNext) {
        val group = new mutable.ArrayBuffer[T]()
        while (bufferedUnderlying.hasNext &&
          (!isNewGroupStart(bufferedUnderlying.head) || group.isEmpty)) {
          group += transformItem(bufferedUnderlying.next())
        }
        bufferedOutput = Some(group)
      }
    }

    override def hasNext: Boolean = {
      queueItemsIfNeeded()
      bufferedOutput.nonEmpty
    }

    override def next(): ArrayBuffer[T] = {
      if (!hasNext) throw new NoSuchElementException("No more items to return")
      val output = bufferedOutput.get
      bufferedOutput = None
      output
    }
  }

  /**
   * An iterator that helps select old log files for deletion. It takes the input iterator of
   * eligible log file groups to delete, grouped by version and timestamp-adjusting dependency, and
   * returns all groups up to but not including the last group containing a checkpoint.
   * This ensures time-travel can be supported anywhere within the retention window.
   * Example Scenario:
   * - 25 commits made.
   * - Checkpoint files at the 10th and 20th commits.
   * - Retention window starts from the 15th commit.
   * We keep everything from the 10th commit checkpoint onwards.
   * Note 1: 10th commit is retained in the above scenario to get a valid timestamp for version 10.
   * Note 2: This code assumes that underlying iterator list groups in a sorted-by-version order.
   */
  class LastCheckpointPreservingLogDeletionIterator(
      underlying: Iterator[ArrayBuffer[ArrayBuffer[FileStatus]]])
    extends Iterator[ArrayBuffer[ArrayBuffer[ArrayBuffer[FileStatus]]]]{

    private class CheckpointGroupingIterator(
        underlying: Iterator[ArrayBuffer[ArrayBuffer[FileStatus]]])
      extends GroupBreakingIterator[ArrayBuffer[ArrayBuffer[FileStatus]]](underlying) {

      override protected def isNewGroupStart(
          files: ArrayBuffer[ArrayBuffer[FileStatus]]): Boolean = {
        val checkpointPaths = files.flatten.toArray.collect {
          case f if isCheckpointFile(f) => CheckpointInstance(f.getPath)
        }
        Checkpoints.getLatestCompleteCheckpointFromList(checkpointPaths).isDefined
      }

      override protected def transformItem(
          files: ArrayBuffer[ArrayBuffer[FileStatus]]): ArrayBuffer[ArrayBuffer[FileStatus]] = files
    }

    // 1. Group by checkpoints dependency.
    // 2. Drop everything but the last checkpoint-dependency group.
    // .sliding(2) can return a single entry if the underlying iterators' size is 1
    // https://www.scala-lang.org/old/node/7939
    private val lastCheckpointPreservingIterator:
        Iterator[ArrayBuffer[ArrayBuffer[ArrayBuffer[FileStatus]]]] =
      new CheckpointGroupingIterator(underlying).sliding(2).filter(_.length == 2).map(_.head)

    override def hasNext: Boolean = lastCheckpointPreservingIterator.hasNext

    override def next: ArrayBuffer[ArrayBuffer[ArrayBuffer[FileStatus]]] =
      lastCheckpointPreservingIterator.next()
  }

  /**
   * An iterator that helps select old log files for deletion. It takes the input iterator of log
   * files from the earliest file, and returns should-be-deleted files until the given maxTimestamp
   * is reached. Note that this iterator may stop deleting files earlier than maxTimestamp if it
   * finds that files that need to be preserved for adjusting the timestamps of subsequent files.
   * Let's go through an example. Assume the following commit history:
   *
   * +---------+-----------+--------------------+
   * | Version | Timestamp | Adjusted Timestamp |
   * +---------+-----------+--------------------+
   * |       0 |         0 |                  0 |
   * |       1 |         5 |                  5 |
   * |       2 |        10 |                 10 |
   * |       3 |         7 |                 11 |
   * |       4 |         8 |                 12 |
   * |       5 |        14 |                 14 |
   * +---------+-----------+--------------------+
   *
   * As you can see from the example, we require timestamps to be monotonically increasing with
   * respect to the version of the commit, and each commit to have a unique timestamp. If we have
   * a commit which doesn't obey one of these two requirements, we adjust the timestamp of that
   * commit to be one millisecond greater than the previous commit. We don't adjust or look at
   * timestamps of checkpoints since they could be far in the future.
   *
   * Given the above commit history, the behavior of this iterator will be as follows:
   *  - For maxTimestamp = 9, we can delete versions 0 and 1
   *  - Until we receive maxTimestamp >= 12, we can't delete versions 2 and 3.
   *    This is because version 2 is used to adjust the timestamps of commits up to version 4.
   *  - For maxTimestamp >= 14 we can delete everything
   * The semantics of time travel guarantee that for a given timestamp, the user will ALWAYS get the
   * same version. Consider a user asks to get the version at timestamp 11. If all files are there,
   * we would return version 3 (timestamp 11) for this query. If we delete versions 0-2, the
   * original timestamp of version 3 (7) will not have an anchor to adjust on, and if the time
   * travel query is re-executed we would return version 4. This is the motivation behind this
   * iterator implementation.
   *
   * The implementation maintains an internal "maybeDelete" buffer of files that we are unsure of
   * deleting because they may be necessary to adjust time of future files. For each file we get
   * from the underlying iterator, we check whether it needs time adjustment or not. If it does need
   * time adjustment, then we cannot immediately decide whether it is safe to delete that file or
   * not and therefore we put it in each the buffer. Then we iteratively peek ahead at the future
   * files and accordingly decide whether to delete all the buffered files or retain them.
   *
   * @param underlying The iterator which gives the list of files in ascending version order
   * @param maxTimestamp The timestamp until which we can delete (inclusive).
   */
  class TimestampAdjustingLogDeletionIterator(
      underlying: Iterator[ArrayBuffer[FileStatus]],
      maxTimestamp: Long) extends Iterator[ArrayBuffer[ArrayBuffer[FileStatus]]] {

    private class TimestampAdjustingGroupedIterator(underlying: Iterator[ArrayBuffer[FileStatus]])
      extends GroupBreakingIterator[ArrayBuffer[FileStatus]](underlying) {

      private var lastCommitFileOpt: Option[FileStatus] = None

      private def versionGetter(filePath: Path): Long = {
        if (isCheckpointFile(filePath)) {
          checkpointVersion(filePath)
        } else {
          deltaVersion(filePath)
        }
      }

      /**
       * Files need a time adjustment if their timestamp isn't later than the lastCommitFileOpt.
       */
      private def needsTimestampAdjustment(commitFile: FileStatus): Boolean = {
        lastCommitFileOpt.exists { lastFile =>
          versionGetter(lastFile.getPath) < versionGetter(commitFile.getPath) &&
            lastFile.getModificationTime >= commitFile.getModificationTime
        }
      }

      /**
       * A new group can start if we have a delta file that doesn't need its timestamps changed.
       * Non-delta files must continue to be in the same group because delta files coming later
       * might need to adjust their timestamps based on the lastCommitFileInfoOpt.
       */
      override protected def isNewGroupStart(files: ArrayBuffer[FileStatus]): Boolean = {
        files.find(isDeltaFile).exists(!needsTimestampAdjustment(_))
      }

      private def createAdjustedFileStatus(
          commitFile: FileStatus,
          adjustedModificationTime: Long): FileStatus = {
        new FileStatus(
          commitFile.getLen,
          commitFile.isDirectory,
          commitFile.getReplication,
          commitFile.getBlockSize,
          adjustedModificationTime,
          commitFile.getPath)
      }

      override protected def transformItem(
          files: ArrayBuffer[FileStatus]): ArrayBuffer[FileStatus] = {
        val commitFileIndex = files.indexWhere(isDeltaFile)
        if (commitFileIndex >= 0) {
          val commitFile = files(commitFileIndex)
          if (needsTimestampAdjustment(commitFile)) {
            files(commitFileIndex) =
              createAdjustedFileStatus(commitFile, lastCommitFileOpt.get.getModificationTime + 1)
          }
          lastCommitFileOpt = Some(files(commitFileIndex))
        }
        files
      }
    }

    /** Whether the given file can be deleted based on the version and retention timestamp input. */
    private def shouldDeleteFile(file: FileStatus): Boolean = {
      file.getModificationTime <= maxTimestamp
    }

    /**
     * A timestamp-adjusting group can be deleted if the last delta file in it can be deleted.
     */
    private def shouldDeleteGroup(group: ArrayBuffer[ArrayBuffer[FileStatus]]): Boolean = {
      group.flatten.reverse.find((file: FileStatus) => isDeltaFile(file))
        .forall(file => shouldDeleteFile(file))
    }

    private def transformLastGroup(
        group: ArrayBuffer[ArrayBuffer[FileStatus]]
    ): ArrayBuffer[ArrayBuffer[FileStatus]] = {
      val deltaFileIdx = group.lastIndexWhere { files =>
        files.exists((file: FileStatus) => isDeltaFile(file)) }
      group.take(deltaFileIdx + 1) ++
        group.drop(deltaFileIdx + 1).takeWhile(_.forall(shouldDeleteFile))
    }

    val filteredIterator: Iterator[ArrayBuffer[ArrayBuffer[FileStatus]]] =
      new TimestampAdjustingGroupedIterator(underlying).takeWhile(shouldDeleteGroup)

    override def hasNext: Boolean = filteredIterator.hasNext

    /**
     * We potentially drop trailing checkpoints from the last group by explicitly checking if they
     * are within the retention window.
     */
    override def next(): ArrayBuffer[ArrayBuffer[FileStatus]] = {
      val group = filteredIterator.next()
      if (hasNext) {
        group
      } else {
        transformLastGroup(group)
      }
    }
  }

  /**
   * An iterator that groups same types of files by version. For example for an input iterator:
   *   - 11.checkpoint.0.1.parquet
   *   - 11.checkpoint.1.1.parquet
   *   - 11.json
   *   - 12.checkpoint.parquet
   *   - 12.json
   *   - 13.json
   *   - 14.json
   *   - 15.checkpoint.0.1.parquet
   *   - 15.checkpoint.1.1.parquet
   *   - 15.checkpoint.<uuid>.parquet
   *   - 15.json This will return:
   *     - (11, Seq(11.checkpoint.0.1.parquet, 11.checkpoint.1.1.parquet, 11.json))
   *     - (12, Seq(12.checkpoint.parquet, 12.json))
   *     - (13, Seq(13.json))
   *     - (14, Seq(14.json))
   *     - (15, Seq(15.checkpoint.0.1.parquet, 15.checkpoint.1.1.parquet,
   *       15.checkpoint.<uuid>.parquet, 15.json))
   */
  class DeltaLogGroupingIterator(checkpointAndDeltas: Iterator[FileStatus])
    extends Iterator[(Long, ArrayBuffer[FileStatus])] {

    private val bufferedIterator = checkpointAndDeltas.buffered

    private def getFileVersion(file: FileStatus): Long = {
      if (isCheckpointFile(file.getPath)) {
        checkpointVersion(file.getPath)
      } else {
        deltaVersion(file.getPath)
      }
    }

    override def hasNext: Boolean = bufferedIterator.hasNext

    override def next(): (Long, ArrayBuffer[FileStatus]) = {
      val first = bufferedIterator.next()
      val buffer = scala.collection.mutable.ArrayBuffer(first)
      val firstFileVersion = getFileVersion(first)
      while (bufferedIterator.headOption.exists(getFileVersion(_) == firstFileVersion)) {
        buffer += bufferedIterator.next()
      }
      firstFileVersion -> buffer
    }
  }
}

/**
 * class describing the output schema of
 * [[org.apache.spark.sql.delta.commands.DescribeDeltaHistoryCommand]]
 */
case class DeltaHistory(
    version: Option[Long],
    timestamp: Timestamp,
    userId: Option[String],
    userName: Option[String],
    operation: String,
    operationParameters: Map[String, String],
    job: Option[JobInfo],
    notebook: Option[NotebookInfo],
    clusterId: Option[String],
    readVersion: Option[Long],
    isolationLevel: Option[String],
    isBlindAppend: Option[Boolean],
    operationMetrics: Option[Map[String, String]],
    userMetadata: Option[String],
    engineInfo: Option[String]) extends CommitMarker {

  override def withTimestamp(timestamp: Long): DeltaHistory = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime

  override def getVersion: Long = version.get
}

object DeltaHistory {
  /** Create an instance of [[DeltaHistory]] from [[CommitInfo]] */
  def fromCommitInfo(ci: CommitInfo): DeltaHistory = {
    DeltaHistory(
      version = ci.version,
      timestamp = ci.timestamp,
      userId = ci.userId,
      userName = ci.userName,
      operation = ci.operation,
      operationParameters = ci.operationParameters,
      job = ci.job,
      notebook = ci.notebook,
      clusterId = ci.clusterId,
      readVersion = ci.readVersion,
      isolationLevel = ci.isolationLevel,
      isBlindAppend = ci.isBlindAppend,
      operationMetrics = ci.operationMetrics,
      userMetadata = ci.userMetadata,
      engineInfo = ci.engineInfo)
  }
}
