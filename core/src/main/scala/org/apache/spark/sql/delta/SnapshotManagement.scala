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

import java.io.FileNotFoundException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine

import com.databricks.spark.util.TagDefinitions.TAG_ASYNC
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.ThreadUtils

/**
 * Wraps the most recently updated snapshot along with the timestamp the update was started.
 * Defined outside the class since it's used in tests.
 */
case class CapturedSnapshot(snapshot: Snapshot, updateTimestamp: Long)


/**
 * Manages the creation, computation, and access of Snapshot's for Delta tables. Responsibilities
 * include:
 *  - Figuring out the set of files that are required to compute a specific version of a table
 *  - Updating and exposing the latest snapshot of the Delta table in a thread-safe manner
 */
trait SnapshotManagement { self: DeltaLog =>
  import SnapshotManagement.verifyDeltaVersions

  @volatile private[delta] var asyncUpdateTask: Future[Unit] = _

  @volatile protected var currentSnapshot: CapturedSnapshot = getSnapshotAtInit(lastCheckpoint)

  /**
   * Get the LogSegment that will help in computing the Snapshot of the table at DeltaLog
   * initialization, or None if the directory was empty/missing.
   *
   * @param startingCheckpoint A checkpoint that we can start our listing from
   */
  protected def getLogSegmentFrom(
      startingCheckpoint: Option[CheckpointMetaData]): Option[LogSegment] = {
    getLogSegmentForVersion(startingCheckpoint.map(_.version))
  }

  /** Get an iterator of files in the _delta_log directory starting with the startVersion. */
  protected def listFrom(startVersion: Long): Iterator[FileStatus] = {
    store.listFrom(checkpointPrefix(logPath, startVersion), newDeltaHadoopConf())
  }

  /** Returns true if the path is delta log files. Delta log files can be delta commit file
   * (e.g., 000000000.json), or checkpoint file. (e.g., 000000001.checkpoint.00001.00003.parquet)
   * @param path Path of a file
   * @return Boolean Whether the file is delta log files
   */
  protected def isDeltaCommitOrCheckpointFile(path: Path): Boolean = {
    isCheckpointFile(path) || isDeltaFile(path)
  }

  /** Returns an iterator containing a list of files found from the provided path */
  protected def listFromOrNone(startVersion: Long): Option[Iterator[FileStatus]] = {
    // LIST the directory, starting from the provided lower bound (treat missing dir as empty).
    // NOTE: "empty/missing" is _NOT_ equivalent to "contains no useful commit files."
    try {
      Some(listFrom(startVersion)).filterNot(_.isEmpty)
    } catch {
      case _: FileNotFoundException => None
    }
  }

  /**
   * Returns the delta files and checkpoint files starting from the given `startVersion`.
   * `versionToLoad` is an optional parameter to set the max bound. It's usually used to load a
   * table snapshot for a specific version.
   *
   * @param startVersion the version to start. Inclusive.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   * @return Some array of files found (possibly empty, if no usable commit files are present), or
   *         None if the listing returned no files at all.
   */
  private final def listDeltaAndCheckpointFiles(
      startVersion: Long,
      versionToLoad: Option[Long]): Option[Array[FileStatus]] =
    recordFrameProfile("Delta", "SnapshotManagement.listDeltaAndCheckpointFiles") {
      listFromOrNone(startVersion).map { _
        // Pick up all checkpoint and delta files
        .filter { file => isDeltaCommitOrCheckpointFile(file.getPath) }
        // Checkpoint files of 0 size are invalid but Spark will ignore them silently when reading
        // such files, hence we drop them so that we never pick up such checkpoints.
        .filterNot { file => isCheckpointFile(file.getPath) && file.getLen == 0 }
        // take files until the version we want to load
        .takeWhile(f => versionToLoad.forall(v => getFileVersion(f.getPath) <= v))
        .toArray
      }
    }

  /**
   * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
   * `versionToLoad` is not provided, will generate the list of files that are needed to load the
   * latest version of the Delta table. This method also performs checks to ensure that the delta
   * files are contiguous.
   *
   * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
   *                        typically that of a known checkpoint. If this version's not provided,
   *                        we will start listing from version 0.
   * @param versionToLoad A specific version to load. Typically used with time travel and the
   *                      Delta streaming source. If not provided, we will try to load the latest
   *                      version of the table.
   * @return Some LogSegment to build a Snapshot if files do exist after the given
   *         startCheckpoint. None, if the directory was missing or empty.
   */
  protected def getLogSegmentForVersion(
      startCheckpoint: Option[Long],
      versionToLoad: Option[Long] = None): Option[LogSegment] = {
    // List from the starting checkpoint. If a checkpoint doesn't exist, this will still return
    // deltaVersion=0.
    val newFiles = listDeltaAndCheckpointFiles(startCheckpoint.getOrElse(0L), versionToLoad)
    getLogSegmentForVersion(startCheckpoint, versionToLoad, newFiles)
  }

  /**
   * Helper function for the getLogSegmentForVersion above. Called with a provided files list,
   * and will then try to construct a new LogSegment using that.
   */
  protected def getLogSegmentForVersion(
      startCheckpoint: Option[Long],
      versionToLoad: Option[Long],
      files: Option[Array[FileStatus]]): Option[LogSegment] = {
    recordFrameProfile("Delta", "SnapshotManagement.getLogSegmentForVersion") {
      val newFiles = files.filterNot(_.isEmpty)
        .getOrElse {
          // No files found even when listing from 0 => empty directory => table does not exist yet.
          if (startCheckpoint.isEmpty) return None
          // [SC-95011] FIXME(ryan.johnson): We always write the commit and checkpoint files
          // before updating _last_checkpoint. If the listing came up empty, then we either
          // encountered a list-after-put inconsistency in the underlying log store, or somebody
          // corrupted the table by deleting files. Either way, we can't safely continue.
          //
          // For now, we preserve existing behavior by returning Array.empty, which will trigger a
          // recursive call to [[getLogSegmentForVersion]] below (same as before the refactor).
          Array.empty[FileStatus]
        }

      if (newFiles.isEmpty && startCheckpoint.isEmpty) {
        // We can't construct a snapshot because the directory contained no usable commit
        // files... but we can't return None either, because it was not truly empty.
        throw DeltaErrors.emptyDirectoryException(logPath.toString)
      } else if (newFiles.isEmpty) {
        // The directory may be deleted and recreated and we may have stale state in our DeltaLog
        // singleton, so try listing from the first version
        return getLogSegmentForVersion(None, versionToLoad)
      }
      val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
      // Find the latest checkpoint in the listing that is not older than the versionToLoad
      val lastChkpoint = versionToLoad.map(CheckpointInstance(_, None))
        .getOrElse(CheckpointInstance.MaxValue)
      val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
      val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
      val newCheckpointVersion = newCheckpoint.map(_.version).getOrElse {
        // If we do not have any checkpoint, pass new checkpoint version as -1 so that first
        // delta version can be 0.
        startCheckpoint.foreach { startCheckpoint =>
          // `startCheckpoint` was given but no checkpoint found on delta log. This means that the
          // last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists.
          // Try to look up another valid checkpoint and create `LogSegment` from it.
          //
          // [SC-95011] FIXME(ryan.johnson): Something has gone very wrong if the checkpoint doesn't
          // exist at all. This code should only handle rejected incomplete checkpoints.
          recordDeltaEvent(this, "delta.checkpoint.error.partial")
          val snapshotVersion = versionToLoad.getOrElse(deltaVersion(deltas.last.getPath))
          getLogSegmentWithMaxExclusiveCheckpointVersion(snapshotVersion, startCheckpoint)
            .foreach { alternativeLogSegment => return Some(alternativeLogSegment) }

          // No alternative found, but the directory contains files so we cannot return None.
          throw DeltaErrors.missingPartFilesException(
            startCheckpoint, new FileNotFoundException(
              s"Checkpoint file to load version: $startCheckpoint is missing."))
        }
        -1L
      }

      // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
      // it will list all existing delta files.
      val deltasAfterCheckpoint = deltas.filter { file =>
        deltaVersion(file.getPath) > newCheckpointVersion
      }

      val deltaVersions = deltasAfterCheckpoint.map(f => deltaVersion(f.getPath))
      // We may just be getting a checkpoint file after the filtering
      if (deltaVersions.nonEmpty) {
        if (deltaVersions.head != newCheckpointVersion + 1) {
          throw DeltaErrors.logFileNotFoundException(
            deltaFile(logPath, newCheckpointVersion + 1), deltaVersions.last, metadata)
        }
        verifyDeltaVersions(spark, deltaVersions, Some(newCheckpointVersion + 1), versionToLoad)
      }

      val newVersion = deltaVersions.lastOption.getOrElse(newCheckpoint.get.version)
      val newCheckpointFiles: Seq[FileStatus] = newCheckpoint.map { newCheckpoint =>
        val newCheckpointPaths = newCheckpoint.getCorrespondingFiles(logPath).toSet
        val newCheckpointFileArray = checkpoints.filter(f => newCheckpointPaths.contains(f.getPath))
        assert(newCheckpointFileArray.length == newCheckpointPaths.size,
          "Failed in getting the file information for:\n" +
            newCheckpointPaths.mkString(" -", "\n -", "") + "\n" +
            "among\n" + checkpoints.map(_.getPath).mkString(" -", "\n -", ""))
        newCheckpointFileArray.toSeq
      }.getOrElse(Nil)

      // In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
      // they may just be before the checkpoint version unless we have a bug in log cleanup.
      if (deltas.isEmpty) {
        throw new IllegalStateException(s"Could not find any delta files for version $newVersion")
      }
      if (versionToLoad.exists(_ != newVersion)) {
        throw new IllegalStateException(
          s"Trying to load a non-existent version ${versionToLoad.get}")
      }
      val lastCommitTimestamp = deltas.last.getModificationTime

      Some(LogSegment(
        logPath,
        newVersion,
        deltasAfterCheckpoint,
        newCheckpointFiles,
        newCheckpoint.map(_.version),
        lastCommitTimestamp))
    }
  }

  /**
   * Load the Snapshot for this Delta table at initialization. This method uses the `lastCheckpoint`
   * file as a hint on where to start listing the transaction log directory. If the _delta_log
   * directory doesn't exist, this method will return an `InitialSnapshot`.
   *
   * @param lastCheckpointOpt: checkpoint hint used to load checkpoint
   */
  protected def getSnapshotAtInit(
    lastCheckpointOpt: Option[CheckpointMetaData]): CapturedSnapshot = {
    recordFrameProfile("Delta", "SnapshotManagement.getSnapshotAtInit") {
      val currentTimestamp = clock.getTimeMillis()
      getLogSegmentFrom(lastCheckpointOpt).map { segment =>
        val startCheckpoint = segment.checkpointVersionOpt
          .map(v => s" starting from checkpoint $v.").getOrElse(".")
        logInfo(s"Loading version ${segment.version}$startCheckpoint")
        val snapshot = createSnapshot(
          initSegment = segment,
          checkpointMetadataOptHint = lastCheckpointOpt)

        logInfo(s"Returning initial snapshot $snapshot")
        CapturedSnapshot(snapshot, currentTimestamp)
      }.getOrElse {
        logInfo(s"Creating initial snapshot without metadata, because the directory is empty")
        CapturedSnapshot(new InitialSnapshot(logPath, this), currentTimestamp)
      }
    }
  }

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = Option(currentSnapshot).map(_.snapshot).orNull

  protected def createSnapshot(
      initSegment: LogSegment,
      checkpointMetadataOptHint: Option[CheckpointMetaData]): Snapshot = {
    val checksumOpt = readChecksum(initSegment.version)
    createSnapshotFromGivenOrEquivalentLogSegment(initSegment) { segment =>
      new Snapshot(
        path = logPath,
        version = segment.version,
        logSegment = segment,
        deltaLog = this,
        timestamp = segment.lastCommitTimestamp,
        checksumOpt = checksumOpt,
        checkpointMetadataOpt = getCheckpointMetadataForSegment(segment, checkpointMetadataOptHint))
    }
  }

  /**
   * Returns the [[CheckpointMetaData]] for the given [[LogSegment]].
   * If the passed `checkpointMetadataOptHint` matches the `segment`, then it is returned
   * directly.
   */
  protected def getCheckpointMetadataForSegment(
      segment: LogSegment,
      checkpointMetadataOptHint: Option[CheckpointMetaData]): Option[CheckpointMetaData] = {
    // validate that `checkpointMetadataOptHint` and `segment` has same info regarding the
    // checkpoint version and parts.
    val checkpointMatches =
      (segment.checkpointVersionOpt == checkpointMetadataOptHint.map(_.version)) &&
        (segment.checkpoint.size == checkpointMetadataOptHint.flatMap(_.parts).getOrElse(1))
    if (checkpointMatches) checkpointMetadataOptHint else CheckpointMetaData.fromLogSegment(segment)
  }

  /**
   * Returns a [[LogSegment]] for reading `snapshotVersion` such that the segment's checkpoint
   * version (if checkpoint present) is LESS THAN `maxExclusiveCheckpointVersion`.
   * This is useful when trying to skip a bad checkpoint. Returns `None` when we are not able to
   * construct such [[LogSegment]], for example, no checkpoint can be used but we don't have the
   * entire history from version 0 to version `snapshotVersion`.
   */
  private def getLogSegmentWithMaxExclusiveCheckpointVersion(
      snapshotVersion: Long,
      maxExclusiveCheckpointVersion: Long): Option[LogSegment] = {
    assert(
      snapshotVersion >= maxExclusiveCheckpointVersion,
      s"snapshotVersion($snapshotVersion) is less than " +
        s"maxExclusiveCheckpointVersion($maxExclusiveCheckpointVersion)")
    val largestCheckpointVersionToSearch = snapshotVersion.min(maxExclusiveCheckpointVersion - 1)
    val previousCp = if (largestCheckpointVersionToSearch < 0) {
      None
    } else {
      findLastCompleteCheckpoint(
        // The largest possible `CheckpointInstance` at version `largestCheckpointVersionToSearch`
        CheckpointInstance(largestCheckpointVersionToSearch, numParts = Some(Int.MaxValue)))
    }
    previousCp match {
      case Some(cp) =>
        val filesSinceCheckpointVersion = listDeltaAndCheckpointFiles(
          startVersion = cp.version,
          versionToLoad = Some(snapshotVersion))
          .getOrElse(Array.empty)
        val (checkpoints, deltas) =
          filesSinceCheckpointVersion.partition(f => isCheckpointFile(f.getPath))
        if (deltas.isEmpty) {
          // We cannot find any delta files. Returns None as we cannot construct a `LogSegment` only
          // from checkpoint files. This is because in order to create a `LogSegment`, we need to
          // set `LogSegment.lastCommitTimestamp`, and it must be read from the file modification
          // time of the delta file for `snapshotVersion`. It cannot be the file modification time
          // of a checkpoint file because it should be deterministic regardless how we construct the
          // Snapshot, and only delta json log files can ensure that.
          return None
        }
        // `checkpoints` may contain multiple checkpoints for different part sizes, we need to
        // search `FileStatus`s of the checkpoint files for `cp`.
        val checkpointFileNames = cp.getCorrespondingFiles(logPath).map(_.getName).toSet
        val newCheckpointFiles =
          checkpoints.filter(f => checkpointFileNames.contains(f.getPath.getName))
        assert(newCheckpointFiles.length == checkpointFileNames.size,
          "Failed in getting the file information for:\n" +
            checkpointFileNames.mkString(" -", "\n -", "") + "\n" +
            "among\n" + checkpoints.map(_.getPath).mkString(" -", "\n -", ""))
        // Create the list of `FileStatus`s for delta files after `cp.version`.
        val deltasAfterCheckpoint = deltas.filter { file =>
          deltaVersion(file.getPath) > cp.version
        }
        val deltaVersions = deltasAfterCheckpoint.map(f => deltaVersion(f.getPath))
        // `deltaVersions` should not be empty and `verifyDeltaVersions` will verify it
        try {
          verifyDeltaVersions(spark, deltaVersions, Some(cp.version + 1), Some(snapshotVersion))
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to find a valid LogSegment for $snapshotVersion", e)
            return None
        }
        Some(LogSegment(
          logPath,
          snapshotVersion,
          deltas,
          newCheckpointFiles,
          Some(cp.version),
          deltas.last.getModificationTime))
      case None =>
        val deltas =
          listDeltaAndCheckpointFiles(startVersion = 0, versionToLoad = Some(snapshotVersion))
            .getOrElse(Array.empty)
            .filter(file => isDeltaFile(file.getPath))
        val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
        try {
          verifyDeltaVersions(spark, deltaVersions, Some(0), Some(snapshotVersion))
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to find a valid LogSegment for $snapshotVersion", e)
            return None
        }
        Some(LogSegment(
          logPath = logPath,
          version = snapshotVersion,
          deltas = deltas,
          checkpoint = Nil,
          checkpointVersionOpt = None,
          lastCommitTimestamp = deltas.last.getModificationTime))
    }
  }

  /**
   * Create a [[Snapshot]] from the given [[LogSegment]]. If failing to create the snapshot, we will
   * search an equivalent [[LogSegment]] using a different checkpoint and retry up to
   * [[DeltaSQLConf.DELTA_SNAPSHOT_LOADING_MAX_RETRIES]] times.
   */
  protected def createSnapshotFromGivenOrEquivalentLogSegment(
      initSegment: LogSegment)(snapshotCreator: LogSegment => Snapshot): Snapshot = {
    val numRetries =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_LOADING_MAX_RETRIES)
    var attempt = 0
    var segment = initSegment
    // Remember the first error we hit. If all retries fail, we will throw the first error to
    // provide the root cause. We catch `SparkException` because corrupt checkpoint files are
    // detected in the executor side when a task is trying to read them.
    var firstError: SparkException = null
    while (true) {
      try {
        return snapshotCreator(segment)
      } catch {
        case e: SparkException if attempt < numRetries && segment.checkpointVersionOpt.nonEmpty =>
          if (firstError == null) {
            firstError = e
          }
          logWarning(s"Failed to create a snapshot from log segment: $segment. " +
            s"Trying a different checkpoint.", e)
          segment = getLogSegmentWithMaxExclusiveCheckpointVersion(
            segment.version,
            segment.checkpointVersionOpt.get).getOrElse {
              // Throw the first error if we cannot find an equivalent `LogSegment`.
              throw firstError
            }
          attempt += 1
        case e: SparkException if firstError != null =>
          logWarning(s"Failed to create a snapshot from log segment: $segment", e)
          throw firstError
      }
    }
    throw new IllegalStateException("should not happen")
  }

  /** Checks if the snapshot of the table has surpassed our allowed staleness. */
  private def isSnapshotStale(lastUpdateTimestamp: Long): Boolean = {
    val stalenessLimit = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      clock.getTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }


  /**
   * Checks if the snapshot has already been updated since the specified timestamp.
   *
   * Note that this should be used differently from isSnapshotStale. Staleness is
   * used to allow async updates if the table has been updated within the staleness
   * window, which allows for better perf in exchange for possibly using a slightly older
   * view of the table. For eg, if a table is queried multiple times in quick succession.
   *
   * On the other hand, isSnapshotFresh is used to identify duplicate updates within a
   * single transaction. For eg, if a table isn't cached and the snapshot was fetched from the
   * logstore, then updating the snapshot again in the same transaction is superfluous. We can
   * use this function to detect and skip such an update.
   */
  private def isSnapshotFresh(
      capturedSnapshot: CapturedSnapshot,
      checkIfUpdatedSinceTs: Option[Long]): Boolean = {
    checkIfUpdatedSinceTs.exists(_ < capturedSnapshot.updateTimestamp)
  }

  /**
   * Update ActionLog by applying the new delta files if any.
   *
   * @param stalenessAcceptable Whether we can accept working with a stale version of the table. If
   *                            the table has surpassed our staleness tolerance, we will update to
   *                            the latest state of the table synchronously. If staleness is
   *                            acceptable, and the table hasn't passed the staleness tolerance, we
   *                            will kick off a job in the background to update the table state,
   *                            and can return a stale snapshot in the meantime.
   * @param checkIfUpdatedSinceTs Skip the update if we've already updated the snapshot since the
   *                              specified timestamp.
   */
  def update(
      stalenessAcceptable: Boolean = false,
      checkIfUpdatedSinceTs: Option[Long] = None): Snapshot = {
    // currentSnapshot is volatile. Make a local copy of it at the start of the update call, so
    // that there's no chance of a race condition changing the snapshot partway through the update.
    val capturedSnapshot = currentSnapshot
    // Eagerly exit if the snapshot is already new enough to satisfy the caller
    if (isSnapshotFresh(capturedSnapshot, checkIfUpdatedSinceTs)) {
      return capturedSnapshot.snapshot
    }
    val doAsync = stalenessAcceptable && !isSnapshotStale(capturedSnapshot.updateTimestamp)
    if (!doAsync) {
      recordFrameProfile("Delta", "SnapshotManagement.update") {
        lockInterruptibly {
          updateInternal(isAsync = false)
        }
      }
    } else {
      if (asyncUpdateTask == null || asyncUpdateTask.isCompleted) {
        val jobGroup = spark.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
        asyncUpdateTask = Future[Unit] {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
          spark.sparkContext.setJobGroup(
            jobGroup,
            s"Updating state of Delta table at ${capturedSnapshot.snapshot.path}",
            interruptOnCancel = true)
          tryUpdate(isAsync = true)
        }(SnapshotManagement.deltaLogAsyncUpdateThreadPool)
      }
      currentSnapshot.snapshot
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  private def tryUpdate(isAsync: Boolean): Snapshot = {
    if (deltaLogLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        deltaLogLock.unlock()
      }
    } else {
      currentSnapshot.snapshot
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  protected def updateInternal(isAsync: Boolean): Snapshot =
    recordDeltaOperation(this, "delta.log.update", Map(TAG_ASYNC -> isAsync.toString)) {
      val updateTimestamp = clock.getTimeMillis()
      val previousSnapshot = currentSnapshot.snapshot
      val segmentOpt =
        getLogSegmentForVersion(previousSnapshot.logSegment.checkpointVersionOpt)
      installSnapshotInternal(previousSnapshot, segmentOpt, updateTimestamp, isAsync)
    }

  /** Install the provided segmentOpt as the currentSnapshot on the cluster */
  def installSnapshotInternal(
      previousSnapshot: Snapshot,
      segmentOpt: Option[LogSegment],
      updateTimestamp: Long,
      isAsync: Boolean): Snapshot = {
    segmentOpt.map { segment =>
      if (segment == previousSnapshot.logSegment) {
        // If no changes were detected, just refresh the timestamp
        val timestampToUse = math.max(updateTimestamp, currentSnapshot.updateTimestamp)
        currentSnapshot = currentSnapshot.copy(updateTimestamp = timestampToUse)
      } else {
        val startingFrom = segment.checkpointVersionOpt
          .map(v => s" starting from checkpoint version $v.").getOrElse(".")
        logInfo(s"Loading version ${segment.version}$startingFrom")

        val newSnapshot = createSnapshot(
          initSegment = segment,
          checkpointMetadataOptHint = snapshot.getCheckpointMetadataOpt)

        if (previousSnapshot.version > -1 &&
          previousSnapshot.metadata.id != newSnapshot.metadata.id) {
          val msg = s"Change in the table id detected while updating snapshot. " +
            s"\nPrevious snapshot = $previousSnapshot\nNew snapshot = $newSnapshot."
          logError(msg)
          recordDeltaEvent(self, "delta.metadataCheck.update", data = Map(
            "prevSnapshotVersion" -> previousSnapshot.version,
            "prevSnapshotMetadata" -> previousSnapshot.metadata,
            "nextSnapshotVersion" -> newSnapshot.version,
            "nextSnapshotMetadata" -> newSnapshot.metadata))
        }
        logInfo(s"Updated snapshot to $newSnapshot")
        replaceSnapshot(newSnapshot, updateTimestamp)
      }
    }.getOrElse {
      logInfo(s"No delta log found for the Delta table at $logPath")
      replaceSnapshot(new InitialSnapshot(logPath, this), updateTimestamp)
    }
    currentSnapshot.snapshot
  }

  /** Replace the given snapshot with the provided one. */
  protected def replaceSnapshot(newSnapshot: Snapshot, updateTimestamp: Long): Unit = {
    if (!deltaLogLock.isHeldByCurrentThread) {
      recordDeltaEvent(this, "delta.update.unsafeReplace")
    }
    val oldSnapshot = currentSnapshot.snapshot
    currentSnapshot = CapturedSnapshot(newSnapshot, updateTimestamp)
    oldSnapshot.uncache()
  }

  /** Get the snapshot at `version`. */
  def getSnapshotAt(
      version: Long,
      commitTimestamp: Option[Long] = None,
      lastCheckpointHint: Option[CheckpointInstance] = None): Snapshot = {
    val current = snapshot
    if (current.version == version) {
      return current
    }

    // Do not use the hint if the version we're asking for is smaller than the last checkpoint hint
    val startingCheckpoint = lastCheckpointHint.collect { case ci if ci.version <= version => ci }
      .orElse(findLastCompleteCheckpoint(CheckpointInstance(version, None)))
    getLogSegmentForVersion(startingCheckpoint.map(_.version), Some(version)).map { segment =>
      createSnapshot(
        initSegment = segment,
        checkpointMetadataOptHint = None)
    }.getOrElse {
      // We can't return InitialSnapshot because our caller asked for a specific snapshot version.
      throw DeltaErrors.emptyDirectoryException(logPath.toString)
    }
  }
}

object SnapshotManagement {
  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }

  /**
   * - Verify the versions are contiguous.
   * - Verify the versions start with `expectedStartVersion` if it's specified.
   * - Verify the versions end with `expectedEndVersion` if it's specified.
   */
  def verifyDeltaVersions(
      spark: SparkSession,
      versions: Array[Long],
      expectedStartVersion: Option[Long],
      expectedEndVersion: Option[Long]): Unit = {
    if (versions.nonEmpty) {
      // Turn this to a vector so that we can compare it with a range.
      val deltaVersions = versions.toVector
      if ((deltaVersions.head to deltaVersions.last) != deltaVersions) {
        throw DeltaErrors.deltaVersionsNotContiguousException(spark, deltaVersions)
      }
    }
    expectedStartVersion.foreach { v =>
      require(versions.nonEmpty && versions.head == v, "Did not get the first delta " +
        s"file version: $v to compute Snapshot")
    }
    expectedEndVersion.foreach { v =>
      require(versions.nonEmpty && versions.last == v, "Did not get the first delta " +
        s"file version: $v to compute Snapshot")
    }
  }
}

/**
 * Provides information around which files in the transaction log need to be read to create
 * the given version of the log.
 * @param logPath The path to the _delta_log directory
 * @param version The Snapshot version to generate
 * @param deltas The delta commit files (.json) to read
 * @param checkpoint The checkpoint file to read
 * @param checkpointVersionOpt The checkpoint version used to start replay
 * @param lastCommitTimestamp The "unadjusted" timestamp of the last commit within this segment. By
 *                            unadjusted, we mean that the commit timestamps may not necessarily be
 *                            monotonically increasing for the commits within this segment.
 */
case class LogSegment(
    logPath: Path,
    version: Long,
    deltas: Seq[FileStatus],
    checkpoint: Seq[FileStatus],
    checkpointVersionOpt: Option[Long],
    lastCommitTimestamp: Long) {

  override def hashCode(): Int = logPath.hashCode() * 31 + (lastCommitTimestamp % 10000).toInt

  /**
   * An efficient way to check if a cached Snapshot's contents actually correspond to a new
   * segment returned through file listing.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: LogSegment =>
        version == other.version && lastCommitTimestamp == other.lastCommitTimestamp &&
          logPath == other.logPath
      case _ => false
    }
  }
}

object LogSegment {
  /** The LogSegment for an empty transaction log directory. */
  def empty(path: Path): LogSegment = LogSegment(
    logPath = path,
    version = -1L,
    deltas = Nil,
    checkpoint = Nil,
    checkpointVersionOpt = None,
    lastCommitTimestamp = -1L)
}
