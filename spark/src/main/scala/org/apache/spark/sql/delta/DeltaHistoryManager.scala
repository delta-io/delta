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
import java.util.concurrent.CompletableFuture

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.actions.{Action, CommitInfo, CommitMarker, JobInfo, NotebookInfo}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{DateTimeUtils, DeltaCommitFileProvider, FileNames, TimestampFormatter}
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

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
    val snapshot = deltaLog.update()
    val listStart = limitOpt.map { limit =>
      math.max(snapshot.version - limit + 1, 0)
    }.getOrElse(getEarliestDeltaFile(deltaLog))
    getHistory(listStart, end = Some(snapshot.version))
  }

  /**
   * Get the commit information of the Delta table from commit `[start, end]` in
   * reverse chronological order. An empty Seq is returned when `start > end`.
   *
   * @param useInCommitTimestamps Whether ICT should be used as the commit-timestamp for
   *                              the commits.
   *                              If `true`, all commits in the range must have ICTs and
   *                              the timestamp returned for each commit will be the ICT.
   *                              If `false`, the file modification time will be used as the
   *                              timestamp.
   */
  private[delta] def getHistoryImpl(
      start: Long,
      end: Long,
      useInCommitTimestamps: Boolean,
      commitFileProvider: DeltaCommitFileProvider): Seq[DeltaHistory] = {
    import org.apache.spark.sql.delta.implicits._
    val conf = getSerializableHadoopConf
    val logPath = deltaLog.logPath.toString
    // We assume that commits are contiguous, therefore we try to load all of them in order
    val info = spark.range(start, end + 1)
      .mapPartitions { versions =>
        val logStore = LogStore(SparkEnv.get.conf, conf.value)
        val basePath = new Path(logPath)
        val fs = basePath.getFileSystem(conf.value)
        versions.flatMap { commit =>
          try {
            val deltaFile = commitFileProvider.deltaFile(commit)
            val commitInfoOpt = DeltaHistoryManager
              .getCommitInfoOpt(logStore, deltaFile, conf.value)
            val timestamp = if (useInCommitTimestamps) {
              CommitInfo.getRequiredInCommitTimestamp(commitInfoOpt, commit.toString)
            } else {
              fs.getFileStatus(deltaFile).getModificationTime
            }
            val ci = commitInfoOpt.getOrElse(CommitInfo.empty(Some(commit)))
            Some(ci.withTimestamp(timestamp))
          } catch {
            case _: FileNotFoundException =>
              // We have a race-condition where files can be deleted while reading. It's fine to
              // skip those files
              None
          }
        }.map(DeltaHistory.fromCommitInfo)
      }
    val monotonizedCommits = if (useInCommitTimestamps) {
      // ICT timestamps are guaranteed to be monotonically increasing.
      info.collect()
    } else {
      monotonizeCommitTimestamps(info.collect())
    }
    // Spark should return the commits in increasing order as well
    monotonizedCommits.reverse
  }

  /**
   * Get the commit information of the Delta table from commit `[start, end]` in reverse
   * chronological order. If `end` is `None`, we return all commits from start to now.
   * @param start The start of the commit range, inclusive.
   * @param end The end of the commit range, inclusive.
   */
  def getHistory(
      start: Long,
      end: Option[Long] = None): Seq[DeltaHistory] = {
    val currentSnapshot = deltaLog.unsafeVolatileSnapshot
    val (snapshotNewerThanResolvedEnd, resolvedEnd) = end match {
        case Some(endInclusive) if currentSnapshot.version >= endInclusive =>
          // Use the cache snapshot if it's fresh enough for the [start, endInclusive] query.
          (currentSnapshot, math.min(currentSnapshot.version, endInclusive))
        case _ =>
          // Either end doesn't exist or the currently cached snapshot isn't new enough to
          // satisfy it.
          val snapshot = deltaLog.update()
          val endInclusive = end.getOrElse(snapshot.version).min(snapshot.version)
          (snapshot, endInclusive)
      }

    val commitFileProvider = DeltaCommitFileProvider(snapshotNewerThanResolvedEnd)
    if (!DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(
        snapshotNewerThanResolvedEnd.metadata)) {
      getHistoryImpl(start, resolvedEnd, useInCommitTimestamps = false, commitFileProvider)
    } else {
      val ictEnablementCommit =
        InCommitTimestampUtils.getValidatedICTEnablementInfo(snapshotNewerThanResolvedEnd.metadata)
      ictEnablementCommit match {
        case Some(Commit(ictEarliest, _)) =>
          // getHistoryImpl will return an empty Seq if start > end.
          val nonICTCommits = getHistoryImpl(
              start,
              math.min(resolvedEnd, ictEarliest - 1),
              useInCommitTimestamps = false,
              commitFileProvider)
          val ictCommits = getHistoryImpl(
              math.max(ictEarliest, start),
              resolvedEnd,
              useInCommitTimestamps = true,
              commitFileProvider)
          // Merge the two sequences, ensuring ICT commits are listed first as they are more recent,
          // followed by non-ICT commits, maintaining the reverse chronological order.
          ictCommits ++ nonICTCommits
        case _ => // Enablement info not found, ICT is enabled for all available commits.
          getHistoryImpl(start, resolvedEnd, useInCommitTimestamps = true, commitFileProvider)
      }
    }
  }

  /**
   * Returns the latest commit that happened at or before `time` in the range `[start, end)`.
   * All the commits in the range `[start, end)` are assumed to not have inCommitTimestamps.
   * If no such commit exists, the earliest commit is returned.
   */
  def getCommitFromNonICTRange(start: Long, end: Long, time: Long): Commit = {
    if (end - start > 2 * maxKeysPerList) {
      parallelSearch(time, start, end)
    } else {
      val commits = getCommits(
        deltaLog.store,
        deltaLog.logPath,
        start,
        Some(end),
        deltaLog.newDeltaHadoopConf())
      lastCommitBeforeTimestamp(commits, time).getOrElse(commits.head)
    }
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
    val earliestVersion = if (mustBeRecreatable) {
      getEarliestRecreatableCommit
    } else {
      getEarliestDeltaFile(deltaLog)
    }
    val snapshot = deltaLog.update()
    val commitFileProvider = DeltaCommitFileProvider(snapshot)
    val latestVersion = snapshot.version

    // In most cases, the earliest commit should not be the result of this search.
    // When ICT is enabled, use -1L as the placeholder timestamp for the earliest commit
    // for the search and only fetch the real timestamp if the earliest commit is
    // the result of the search. We can potentially avoid one unnecessary IO this way.
    val placeholderEarliestCommit = Commit(earliestVersion, -1L)
    val ictEnablementCommit =
      if (DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata)) {
        InCommitTimestampUtils.getValidatedICTEnablementInfo(snapshot.metadata)
          // If missing, ICT is enabled for all available versions
          .getOrElse(placeholderEarliestCommit)
      } else {
        // Pretend ICT will be enabled after the latest version and requested timestamp.
        // This will force us to use the non-ICT search path below.
        Commit(latestVersion + 1, time + 1)
      }

    var commitOpt = if (ictEnablementCommit.timestamp <= time) {
      // ICT was enabled as-of the requested time
      if (snapshot.timestamp <= time) {
        // We just proved we should use the latest snapshot
        Some(Commit(snapshot.version, snapshot.timestamp))
      } else {
        // start ICT search over [earliest available ICT version, latestVersion)
        val ictEnabledForEntireWindow = (ictEnablementCommit.version <= earliestVersion)
        val searchWindowLowerBoundCommit =
          if (ictEnabledForEntireWindow) placeholderEarliestCommit else ictEnablementCommit

        // Note that this search can return `placeholderEarliestCommit`.
        // The real timestamp of the earliest commit will be fetched later.
        getActiveCommitAtTimeFromICTRange(
          time,
          searchWindowLowerBoundCommit,
          latestVersion + 1,
          deltaLog.newDeltaHadoopConf(),
          deltaLog.logPath,
          deltaLog.store,
          numChunks = 10,
          spark,
          commitFileProvider)
      }
    } else {
      // ICT was NOT enabled as-of the requested time
      if (ictEnablementCommit.version <= earliestVersion) {
        // We're searching for a non-ICT time but the non-ICT commits are all missing.
        // If `canReturnEarliestCommit` is `false`, we need the details of the
        // earliest commit to populate the TimestampEarlierThanCommitRetentionException
        // error correctly.
        // Else, when `canReturnEarliestCommit` is `true`, the earliest commit
        // is the desired result.
        // The real timestamp of the earliest commit will be fetched later.
        Some(placeholderEarliestCommit)
      } else {
        // start non-ICT search over [earliestVersion, ictEnablementVersion)
        Some(getCommitFromNonICTRange(earliestVersion, end = ictEnablementCommit.version, time))
      }
    }

    // We need to fetch the correct timestamp for the earliest commit if it was the result of the
    // search.
    // If commitOpt == ictEnablementCommit, we also need to validate the existence of the ICT
    // enablement commit.
    if (commitOpt.contains(placeholderEarliestCommit) || commitOpt.contains(ictEnablementCommit)) {
      commitOpt = getFirstCommitAndICTAfter(
        commitOpt.get.version,
        latestVersion,
        deltaLog.logPath,
        deltaLog.store,
        deltaLog.newDeltaHadoopConf(),
        commitFileProvider)
    }

    // Error handling
    val commit = commitOpt.getOrElse {
      throw DeltaErrors.noHistoryFound(deltaLog.logPath)
    }
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

  /**
   * This thread pool is used by `getActiveCommitAtTime` to parallelize the search for
   * relevant commits when the feature inCommitTimestamps is enabled.
   */
  private[delta] lazy val threadPool: DeltaThreadPool = DeltaThreadPool(
      "delta-history-manager",
      SparkEnv.get.conf.get(DeltaSQLConf.DELTA_HISTORY_MANAGER_THREAD_POOL_SIZE)
    )

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

  private def getCommitWithInCommitTimestamp(
      version: Long,
      commitFileStatus: FileStatus,
      logStore: LogStore,
      conf: Configuration): Option[Commit] = {
    val logs = logStore.readAsIterator(commitFileStatus, conf)
    try {
      val ci = logs
        .map(Action.fromJson)
        .collectFirst { case c: CommitInfo => c }
      Some(Commit(version, CommitInfo.getRequiredInCommitTimestamp(ci, version.toString)))
    } catch {
      case _: FileNotFoundException =>
        None
    } finally {
      logs.close()
    }
  }

  /**
   * Returns the first available commit in the range [version, upperBoundExclusive).
   * The timestamp of the returned commit will be the ICT. If no ICT is found for the commit,
   * an exception will be thrown.
   * The function optimistically tries to read the commit info for `version` first.
   * For commits that have been backfilled as per `commitFileProvider`: if the
   * no commit is found at that version, it falls back to a listing.
   * For unbackfilled commits, an IllegalStateException is thrown if the commit is not found.
   */
  private[delta] def getFirstCommitAndICTAfter(
      version: Long,
      upperBoundExclusive: Long,
      basePath: Path,
      logStore: LogStore,
      conf: Configuration,
      commitFileProvider: DeltaCommitFileProvider): Option[Commit] = {
    val deltaFile = commitFileProvider.deltaFile(version)
    val commitInfoOpt = try {
      getCommitInfoOpt(logStore, deltaFile, conf)
    } catch {
      case _: FileNotFoundException => None
    }
    if (commitInfoOpt.isDefined) {
      val timestamp = CommitInfo.getRequiredInCommitTimestamp(commitInfoOpt, version.toString)
      Some(Commit(version, timestamp))
    } else if (version >= commitFileProvider.minUnbackfilledVersion) {
      // Unbackfilled commits should never disappear during the lifetime of time travel
      // query.
      throw new IllegalStateException(
        s"Could not find commit $version which was expected to be at path ${deltaFile.toString}.")
    } else {
      logStore
        .listFrom(FileNames.listingPrefix(basePath, version), conf)
        .takeWhile {
          fs => FileNames.getFileVersionOpt(fs.getPath).forall(_ < upperBoundExclusive)
        }
        .collectFirst { case DeltaFile(f, v) =>
          getCommitWithInCommitTimestamp(v, f, logStore, conf)
        }
        .flatten
    }
  }

  /**
   * Returns the latest commit (with its inCommitTimestamp) that happened at or before
   * `searchTimestamp` in the range `[startCommit.version, end)`.
   * If no such commit exists, None is returned.
   *
   * The algorithm divides the range into `numChunks` chunks. It then finds the last
   * chunk where the ICT of its first available commit is less than or equal to `searchTimestamp`.
   * This chunk is then further divided into `numChunks` chunks and the process is repeated.
   */
  private[delta] def getActiveCommitAtTimeFromICTRange(
      searchTimestamp: Long,
      startCommit: Commit,
      end: Long,
      conf: Configuration,
      basePath: Path,
      logStore: LogStore,
      numChunks: Long,
      spark: SparkSession,
      commitFileProvider: DeltaCommitFileProvider): Option[Commit] = {
    require(startCommit.version < end, "start must be less than end")
    var curStartCommit = startCommit
    var curEnd = end
    while (curStartCommit.version < curEnd) {
      val numVersionsInRange = curEnd - curStartCommit.version
      val chunkSize = math.max(numVersionsInRange / numChunks, 1)

      // min(chunkSize) = 1 and curStartCommit.version < end
      // therefore, getChunkEnd(chunkStart) will always be > chunkStart
      def getChunkEnd(chunkStart: Long): Long = math.min(chunkStart + chunkSize, curEnd)

      val chunkStartICTFutures =
        (curStartCommit.version until curEnd by chunkSize).map { chunkStart =>
          if (chunkStart == curStartCommit.version) {
            CompletableFuture.completedFuture(Option(curStartCommit))
          } else {
            threadPool.submit(spark) {
              getFirstCommitAndICTAfter(
                chunkStart,
                upperBoundExclusive = getChunkEnd(chunkStart),
                basePath,
                logStore,
                conf,
                commitFileProvider
              )
            }
          }
        }
      val knownTightestLowerBoundCommit = chunkStartICTFutures
        .map(ThreadUtils.awaitResult(_, Duration.Inf))
        .takeWhile(_.forall(_.timestamp <= searchTimestamp))
        .flatten
        .lastOption
        .getOrElse {
          return None
        }
      val nextStartCommit = knownTightestLowerBoundCommit
      val nextEnd = getChunkEnd(nextStartCommit.version)
      if (nextStartCommit.version + 2 > nextEnd ||
          knownTightestLowerBoundCommit.timestamp == searchTimestamp) {
        return Some(knownTightestLowerBoundCommit)
      }
      curStartCommit = nextStartCommit
      curEnd = nextEnd
    }
    None
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
   * An iterator that helps select old log files for deletion. It takes the input iterator of log
   * files from the earliest file, and returns should-be-deleted files until the given maxTimestamp
   * or maxVersion to delete is reached. Note that this iterator may stop deleting files earlier
   * than maxTimestamp or maxVersion if it finds that files that need to be preserved for adjusting
   * the timestamps of subsequent files. Let's go through an example. Assume the following commit
   * history:
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
   * commit to be one millisecond greater than the previous commit.
   *
   * Given the above commit history, the behavior of this iterator will be as follows:
   *  - For maxVersion = 1 and maxTimestamp = 9, we can delete versions 0 and 1
   *  - Until we receive maxVersion >= 4 and maxTimestamp >= 12, we can't delete versions 2 and 3.
   *    This is because version 2 is used to adjust the timestamps of commits up to version 4.
   *  - For maxVersion >= 5 and maxTimestamp >= 14 we can delete everything
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
   * @param maxVersion The version until which we can delete (inclusive).
   * @param versionGetter A method to get the commit version from the file path.
   */
  class BufferingLogDeletionIterator(
      underlying: Iterator[FileStatus],
      maxTimestamp: Long,
      maxVersion: Long,
      versionGetter: Path => Long) extends Iterator[FileStatus] {
    /**
     * Our output iterator
     */
    private val filesToDelete = new mutable.Queue[FileStatus]()
    /**
     * Our intermediate buffer which will buffer files as long as the last file requires a timestamp
     * adjustment.
     */
    private val maybeDeleteFiles = new mutable.ArrayBuffer[FileStatus]()
    private var lastFile: FileStatus = _
    private var hasNextCalled: Boolean = false

    private def init(): Unit = {
      if (underlying.hasNext) {
        lastFile = underlying.next()
        maybeDeleteFiles.append(lastFile)
      }
    }

    init()

    /** Whether the given file can be deleted based on the version and retention timestamp input. */
    private def shouldDeleteFile(file: FileStatus): Boolean = {
      file.getModificationTime <= maxTimestamp && versionGetter(file.getPath) <= maxVersion
    }

    /**
     * Files need a time adjustment if their timestamp isn't later than the lastFile.
     */
    private def needsTimeAdjustment(file: FileStatus): Boolean = {
      versionGetter(lastFile.getPath) < versionGetter(file.getPath) &&
        lastFile.getModificationTime >= file.getModificationTime
    }

    /**
     * Enqueue the files in the buffer if the last file is safe to delete. Clears the buffer.
     */
    private def flushBuffer(): Unit = {
      if (maybeDeleteFiles.lastOption.exists(shouldDeleteFile)) {
        filesToDelete ++= maybeDeleteFiles
      }
      maybeDeleteFiles.clear()
    }

    /**
     * Peeks at the next file in the iterator. Based on the next file we can have three
     * possible outcomes:
     * - The underlying iterator returned a file, which doesn't require timestamp adjustment. If
     *   the file in the buffer has expired, flush the buffer to our output queue.
     * - The underlying iterator returned a file, which requires timestamp adjustment. In this case,
     *   we add this file to the buffer and fetch the next file
     * - The underlying iterator is empty. In this case, we check the last file in the buffer. If
     *   it has expired, then flush the buffer to the output queue.
     * Once this method returns, the buffer is expected to have 1 file (last file of the
     * underlying iterator) unless the underlying iterator is fully consumed.
     */
    private def queueFilesInBuffer(): Unit = {
      var continueBuffering = true
      while (continueBuffering) {
        if (!underlying.hasNext) {
          flushBuffer()
          return
        }

        var currentFile = underlying.next()
        require(currentFile != null, "FileStatus iterator returned null")
        if (needsTimeAdjustment(currentFile)) {
          currentFile = new FileStatus(
            currentFile.getLen, currentFile.isDirectory, currentFile.getReplication,
            currentFile.getBlockSize, lastFile.getModificationTime + 1, currentFile.getPath)
          maybeDeleteFiles.append(currentFile)
        } else {
          flushBuffer()
          maybeDeleteFiles.append(currentFile)
          continueBuffering = false
        }
        lastFile = currentFile
      }
    }

    override def hasNext: Boolean = {
      hasNextCalled = true
      if (filesToDelete.isEmpty) queueFilesInBuffer()
      filesToDelete.nonEmpty
    }

    override def next(): FileStatus = {
      if (!hasNextCalled) throw new NoSuchElementException()
      hasNextCalled = false
      filesToDelete.dequeue()
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

