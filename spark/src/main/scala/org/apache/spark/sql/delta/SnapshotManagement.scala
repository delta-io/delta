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
import java.util.Objects
import java.util.concurrent.{CompletableFuture, Future}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.TagDefinitions.TAG_ASYNC
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.coordinatedcommits.{CoordinatedCommitsUsageLogs, CoordinatedCommitsUtils, TableCommitCoordinatorClient}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import com.fasterxml.jackson.annotation.JsonIgnore
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{ThreadUtils, Utils}

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

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  protected val snapshotLock = new ReentrantLock()

  /**
   * Cached fileStatus for the latest CRC file seen in the deltaLog.
   */
  @volatile protected var lastSeenChecksumFileStatusOpt: Option[FileStatus] = None
  @volatile protected var currentSnapshot: CapturedSnapshot = getSnapshotAtInit

  /**
   * Run `body` inside `snapshotLock` lock using `lockInterruptibly` so that the thread
   * can be interrupted when waiting for the lock.
   */
  def withSnapshotLockInterruptibly[T](body: => T): T = {
    snapshotLock.lockInterruptibly()
    try {
      body
    } finally {
      snapshotLock.unlock()
    }
  }

  /** Get an iterator of files in the _delta_log directory starting with the startVersion. */
  private[delta] def listFrom(startVersion: Long): Iterator[FileStatus] = {
    store.listFrom(listingPrefix(logPath, startVersion), newDeltaHadoopConf())
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
   * @return A tuple where the first element is an array of log files (possibly empty, if no
   *         usable log files are found), and the second element is the latest checksum file found
   *         which has a version less than or equal to `versionToLoad`.
   */
  private def listFromFileSystemInternal(
      startVersion: Long,
      versionToLoad: Option[Long],
      includeMinorCompactions: Boolean
  ): (Option[Array[(FileStatus, FileType.Value, Long)]], Option[FileStatus]) = {
    var latestAvailableChecksumFileStatus = Option.empty[FileStatus]
    val files = listFromOrNone(startVersion).map {
      _.flatMap {
        case DeltaFile(f, fileVersion) =>
          Some((f, FileType.DELTA, fileVersion))
        case CompactedDeltaFile(f, startVersion, endVersion)
            if includeMinorCompactions && versionToLoad.forall(endVersion <= _) =>
          Some((f, FileType.COMPACTED_DELTA, startVersion))
        case CheckpointFile(f, fileVersion) if f.getLen > 0 =>
          Some((f, FileType.CHECKPOINT, fileVersion))
        case ChecksumFile(f, version) if versionToLoad.forall(version <= _) =>
          latestAvailableChecksumFileStatus = Some(f)
          None
        case _ =>
          None
      }
      // take files up to the version we want to load
      .takeWhile { case (_, _, fileVersion) => versionToLoad.forall(fileVersion <= _) }
      .toArray
    }
    (files, latestAvailableChecksumFileStatus)
  }

  /**
   * This method is designed to efficiently and reliably list delta, compacted delta, and
   * checkpoint files associated with a Delta Lake table. It makes parallel calls to both the
   * file-system and a commit-coordinator (if available), reconciles the results to account for
   * asynchronous backfill operations, and ensures a comprehensive list of file statuses without
   * missing any concurrently backfilled files.
   * *Note*: If table is a coordinated-commits table, the commit coordinator MUST be passed to
   * correctly list the commits.
   * The function also collects the latest checksum file found in the listings and returns it.
   *
   * @param startVersion the version to start. Inclusive.
   * @param tableCommitCoordinatorClientOpt the optional commit coordinator to use for fetching
   *        un-backfilled commits.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   * @param includeMinorCompactions Whether to include minor compaction files in the result
   * @return A tuple where the first element is an array of log files (possibly empty, if no
   *         usable log files are found), and the second element is the latest checksum file found
   *         which has a version less than or equal to `versionToLoad`.
   */
  protected def listDeltaCompactedDeltaCheckpointFilesAndLatestChecksumFile(
      startVersion: Long,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      versionToLoad: Option[Long],
      includeMinorCompactions: Boolean): (Option[Array[FileStatus]], Option[FileStatus]) = {
    val tableCommitCoordinatorClient = tableCommitCoordinatorClientOpt.getOrElse {
      val (filesOpt, checksumOpt) =
        listFromFileSystemInternal(startVersion, versionToLoad, includeMinorCompactions)
      return (filesOpt.map(_.map(_._1)), checksumOpt)
    }

    // Submit a potential async call to get commits from commit coordinator if available
    val threadPool = SnapshotManagement.commitCoordinatorGetCommitsThreadPool
    // TODO(table-identifier-plumbing): Plumb the right tableIdentifier from the deltaLog.update and
    // Cold deltaLog initialization codepath.
    val tableIdentifierOpt = None
    def getCommitsTask(isAsyncRequest: Boolean): GetCommitsResponse = {
      CoordinatedCommitsUtils.getCommitsFromCommitCoordinatorWithUsageLogs(
        this, tableCommitCoordinatorClient, tableIdentifierOpt,
        startVersion, versionToLoad, isAsyncRequest)
    }
    val unbackfilledCommitsResponseFuture =
      if (threadPool.getActiveCount < threadPool.getMaximumPoolSize) {
        threadPool.submit[GetCommitsResponse](spark) { getCommitsTask(isAsyncRequest = true) }
      } else {
        // If the thread pool is full, we should not submit more tasks to it. Instead, we should
        // run the task in the current thread.
        logInfo(log"Getting un-backfilled commits from commit coordinator in the same " +
          log"thread for table ${MDC(DeltaLogKeys.PATH, dataPath)}")
        recordDeltaEvent(
          this, CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_LISTING_THREADPOOL_FULL)
        CompletableFuture.completedFuture(getCommitsTask(isAsyncRequest = false))
      }

    var maxDeltaVersionSeen = startVersion - 1
    val (initialLogTuplesFromFsListingOpt, initialChecksumOpt) =
      listFromFileSystemInternal(startVersion, versionToLoad, includeMinorCompactions)
    // Ideally listFromFileSystemInternal should return lexicographically sorted files and so
    // maxDeltaVersionSeen should be equal to the last delta version. But we are being
    // defensive here and taking max of all the delta fileVersions seen.
    initialLogTuplesFromFsListingOpt.foreach { logTuples =>
      logTuples.filter(_._2 == FileType.DELTA).map(_._3).foreach { deltaVersion =>
        maxDeltaVersionSeen = Math.max(maxDeltaVersionSeen, deltaVersion)
      }
    }
    val unbackfilledCommitsResponse = try {
      unbackfilledCommitsResponseFuture.get()
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw new CommitCoordinatorGetCommitsFailedException(e.getCause)
    }

    def requiresAdditionalListing(): Boolean = {
      // A gap in delta versions may occur if some delta files are backfilled "after" the
      // file-system listing but before the commit-coordinator listing. To handle this scenario, we
      // perform an additional listing from the file system because those missing files would be
      // backfilled by now and show up in the file-system.
      // Note: We only care about missing delta files with version <= versionToLoad
      val areDeltaFilesMissing = unbackfilledCommitsResponse.getCommits.asScala.headOption match {
        case Some(commit) =>
          // Missing Delta files: [maxDeltaVersionSeen + 1, commit.head.version - 1]
          maxDeltaVersionSeen + 1 < commit.getVersion
        case None =>
          // Missing Delta files: [maxDeltaVersionSeen + 1, latestTableVersion]
          // When there are no commits, we should consider the latestTableVersion from the commit
          // store to detect if ALL trailing commits were concurrently backfilled.
          unbackfilledCommitsResponse.getLatestTableVersion >= 0 &&
            maxDeltaVersionSeen < unbackfilledCommitsResponse.getLatestTableVersion
      }
      versionToLoad.forall(maxDeltaVersionSeen < _) && areDeltaFilesMissing
    }

    val initialMaxDeltaVersionSeen = maxDeltaVersionSeen
    val (additionalLogTuplesFromFsListingOpt, additionalChecksumOpt) =
      if (requiresAdditionalListing()) {
        recordDeltaEvent(
          this, CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_ADDITIONAL_LISTING_REQUIRED)
        listFromFileSystemInternal(
          startVersion = initialMaxDeltaVersionSeen + 1, versionToLoad, includeMinorCompactions)
      } else {
        (None, initialChecksumOpt)
      }
    additionalLogTuplesFromFsListingOpt.foreach { logTuples =>
      logTuples.filter(_._2 == FileType.DELTA).map(_._3).foreach { deltaVersion =>
        maxDeltaVersionSeen = Math.max(maxDeltaVersionSeen, deltaVersion)
      }
    }
    if (requiresAdditionalListing()) {
      // We should not have any gaps in File-System versions and CommitCoordinator versions after
      // the additional listing.
      val eventData = Map(
        "initialCommitVersionsFromFsListingOpt" ->
          initialLogTuplesFromFsListingOpt.map(_.map(_._3)),
        "initialMaxDeltaVersionSeen" -> initialMaxDeltaVersionSeen,
        "additionalCommitVersionsFromFsListingOpt" ->
          additionalLogTuplesFromFsListingOpt.map(_.map(_._3)),
        "maxDeltaVersionSeen" -> maxDeltaVersionSeen,
        "unbackfilledCommitVersions" ->
          unbackfilledCommitsResponse.getCommits.asScala.map(commit => commit.getVersion),
        "latestCommitVersion" -> unbackfilledCommitsResponse.getLatestTableVersion)
      recordDeltaEvent(
        deltaLog = this,
        opType = CoordinatedCommitsUsageLogs.FS_COMMIT_COORDINATOR_LISTING_UNEXPECTED_GAPS,
        data = eventData)
      if (Utils.isTesting) {
        throw new IllegalStateException(
          s"Delta table at $dataPath unexpectedly still requires additional file-system listing " +
            s"after an additional file-system listing was already performed to reconcile the gap " +
            s"between concurrent file-system and commit-owner calls. Details: $eventData")
        }
    }

    val finalLogTuplesFromFsListingOpt: Option[Array[(FileStatus, FileType.Value, Long)]] =
      (initialLogTuplesFromFsListingOpt, additionalLogTuplesFromFsListingOpt) match {
        case (Some(initial), Some(additional)) =>
          // Filter initial list to exclude files with versions beyond
          // `initialListingMaxDeltaVersionSeen` to prevent duplicating non-delta files with
          // higher versions in the combined list. Ideally we shouldn't need this, but we are
          // being defensive here if the log has missing files.
          // E.g. initial = [0.json, 1.json, 2.checkpoint], initialListingMaxDeltaVersionSeen = 1,
          // additional = [2.checkpoint], final = [0.json, 1.json, 2.checkpoint]
          Some(initial.takeWhile(_._3 <= initialMaxDeltaVersionSeen) ++ additional)
        case (Some(initial), None) => Some(initial)
        case (None, Some(additional)) => Some(additional)
        case _ => None
      }

    val unbackfilledCommitsFiltered = unbackfilledCommitsResponse.getCommits.asScala
      .dropWhile(_.getVersion <= maxDeltaVersionSeen)
      .takeWhile(commit => versionToLoad.forall(commit.getVersion <= _))
      .map(_.getFileStatus)

    // If result from fs listing is None and result from commit-coordinator is empty, return none.
    // This is used by caller to distinguish whether table doesn't exist.
    val logTuplesToReturn = finalLogTuplesFromFsListingOpt.map { logTuplesFromFsListing =>
      logTuplesFromFsListing.map(_._1) ++ unbackfilledCommitsFiltered
    }
    val latestChecksumOpt = additionalChecksumOpt.orElse(initialChecksumOpt)
    (logTuplesToReturn, latestChecksumOpt)
  }

  /**
   * This method is designed to efficiently and reliably list delta, compacted delta, and
   * checkpoint files associated with a Delta Lake table. It makes parallel calls to both the
   * file-system and a commit-coordinator (if available), reconciles the results to account for
   * asynchronous backfill operations, and ensures a comprehensive list of file statuses without
   * missing any concurrently backfilled files.
   * *Note*: If table is a coordinated-commits table, the commit-coordinator client MUST be passed
   * to correctly list the commits.
   *
   * @param startVersion the version to start. Inclusive.
   * @param tableCommitCoordinatorClientOpt the optional commit-coordinator client to use for
   *                                  fetching un-backfilled commits.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   * @param includeMinorCompactions Whether to include minor compaction files in the result
   * @return Some array of files found (possibly empty, if no usable commit files are present), or
   *         None if the listing returned no files at all.
   */
  protected final def listDeltaCompactedDeltaAndCheckpointFiles(
      startVersion: Long,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      versionToLoad: Option[Long],
      includeMinorCompactions: Boolean): Option[Array[FileStatus]] = {
    recordDeltaOperation(self, "delta.deltaLog.listDeltaAndCheckpointFiles") {
      val (logTuplesOpt, latestChecksumOpt) =
        listDeltaCompactedDeltaCheckpointFilesAndLatestChecksumFile(
          startVersion, tableCommitCoordinatorClientOpt, versionToLoad, includeMinorCompactions)
      lastSeenChecksumFileStatusOpt = latestChecksumOpt
      logTuplesOpt
    }
  }

  /**
   * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
   * `versionToLoad` is not provided, will generate the list of files that are needed to load the
   * latest version of the Delta table. This method also performs checks to ensure that the delta
   * files are contiguous.
   *
   * @param versionToLoad A specific version to load. Typically used with time travel and the
   *                      Delta streaming source. If not provided, we will try to load the latest
   *                      version of the table.
   * @param oldCheckpointProviderOpt The [[CheckpointProvider]] from the previous snapshot. This is
   *                              used as a start version for the listing when `startCheckpoint` is
   *                              unavailable. This is also used to initialize the [[LogSegment]].
   * @param lastCheckpointInfo [[LastCheckpointInfo]] from the _last_checkpoint. This could be
   *                           used to initialize the Snapshot's [[LogSegment]].
   * @return Some LogSegment to build a Snapshot if files do exist after the given
   *         startCheckpoint. None, if the directory was missing or empty.
   */
  protected def createLogSegment(
      versionToLoad: Option[Long] = None,
      oldCheckpointProviderOpt: Option[UninitializedCheckpointProvider] = None,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient] = None,
      lastCheckpointInfo: Option[LastCheckpointInfo] = None): Option[LogSegment] = {
    // List based on the last known checkpoint version.
    // if that is -1, list from version 0L
    val lastCheckpointVersion = getCheckpointVersion(lastCheckpointInfo, oldCheckpointProviderOpt)
    val listingStartVersion = Math.max(0L, lastCheckpointVersion)
    val includeMinorCompactions =
      spark.conf.get(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS)
    val newFiles = listDeltaCompactedDeltaAndCheckpointFiles(
      listingStartVersion, tableCommitCoordinatorClientOpt, versionToLoad, includeMinorCompactions)
    getLogSegmentForVersion(
      versionToLoad,
      newFiles,
      validateLogSegmentWithoutCompactedDeltas = true,
      oldCheckpointProviderOpt = oldCheckpointProviderOpt,
      tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
      lastCheckpointInfo = lastCheckpointInfo
    )
  }

  private def createLogSegment(previousSnapshot: Snapshot): Option[LogSegment] = {
    createLogSegment(
      oldCheckpointProviderOpt = Some(previousSnapshot.checkpointProvider),
      tableCommitCoordinatorClientOpt = previousSnapshot.tableCommitCoordinatorClientOpt)
  }

  /**
   * Returns the last known checkpoint version based on [[LastCheckpointInfo]] or
   * [[CheckpointProvider]].
   * Returns -1 if both the info is not available.
   */
  protected def getCheckpointVersion(
      lastCheckpointInfoOpt: Option[LastCheckpointInfo],
      oldCheckpointProviderOpt: Option[UninitializedCheckpointProvider]): Long = {
    lastCheckpointInfoOpt.map(_.version)
      .orElse(oldCheckpointProviderOpt.map(_.version))
      .getOrElse(-1)
  }

  /**
   * Helper method to validate that selected deltas are contiguous from checkpoint version till
   * the required `versionToLoad`.
   * @param selectedDeltas - deltas selected for snapshot creation.
   * @param checkpointVersion - checkpoint version selected for snapshot creation. Should be `-1` if
   *                            no checkpoint is selected.
   * @param versionToLoad - version for which we want to create the Snapshot.
   */
  private def validateDeltaVersions(
      selectedDeltas: Array[FileStatus],
      checkpointVersion: Long,
      versionToLoad: Option[Long]): Unit = {
    // checkpointVersion should be passed as -1 if no checkpoint is needed for the LogSegment.

    // We may just be getting a checkpoint file.
    selectedDeltas.headOption.foreach { headDelta =>
      val headDeltaVersion = deltaVersion(headDelta)
      val lastDeltaVersion = selectedDeltas.last match {
        case CompactedDeltaFile(_, _, endV) => endV
        case DeltaFile(_, v) => v
      }

      if (headDeltaVersion != checkpointVersion + 1) {
        throw DeltaErrors.logFileNotFoundException(
          unsafeDeltaFile(logPath, checkpointVersion + 1),
          lastDeltaVersion,
          unsafeVolatileMetadata) // metadata is best-effort only
      }
      val deltaVersions = selectedDeltas.flatMap {
        case CompactedDeltaFile(_, startV, endV) => (startV to endV)
        case DeltaFile(_, v) => Seq(v)
      }
      verifyDeltaVersions(spark, deltaVersions, Some(checkpointVersion + 1), versionToLoad)
    }
  }

  /**
   * Helper function for the getLogSegmentForVersion above. Called with a provided files list,
   * and will then try to construct a new LogSegment using that.
   * *Note*: If table is a coordinated-commits table, the commit-coordinator MUST be passed to
   * correctly list the commits.
   */
  protected def getLogSegmentForVersion(
      versionToLoad: Option[Long],
      files: Option[Array[FileStatus]],
      validateLogSegmentWithoutCompactedDeltas: Boolean,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      oldCheckpointProviderOpt: Option[UninitializedCheckpointProvider],
      lastCheckpointInfo: Option[LastCheckpointInfo]): Option[LogSegment] = {
    recordFrameProfile("Delta", "SnapshotManagement.getLogSegmentForVersion") {
      val lastCheckpointVersion = getCheckpointVersion(lastCheckpointInfo, oldCheckpointProviderOpt)
      val newFiles = files.filterNot(_.isEmpty)
        .getOrElse {
          // No files found even when listing from 0 => empty directory => table does not exist yet.
          if (lastCheckpointVersion < 0) return None
          // We always write the commit and checkpoint files before updating _last_checkpoint.
          // If the listing came up empty, then we either encountered a list-after-put
          // inconsistency in the underlying log store, or somebody corrupted the table by
          // deleting files. Either way, we can't safely continue.
          //
          // For now, we preserve existing behavior by returning Array.empty, which will trigger a
          // recursive call to [[createLogSegment]] below.
          Array.empty[FileStatus]
        }

      if (newFiles.isEmpty && lastCheckpointVersion < 0) {
        // We can't construct a snapshot because the directory contained no usable commit
        // files... but we can't return None either, because it was not truly empty.
        throw DeltaErrors.emptyDirectoryException(logPath.toString)
      } else if (newFiles.isEmpty) {
        // The directory may be deleted and recreated and we may have stale state in our DeltaLog
        // singleton, so try listing from the first version
        return createLogSegment(versionToLoad = versionToLoad)
      }
      val (checkpoints, deltasAndCompactedDeltas) = newFiles.partition(isCheckpointFile)
      val (deltas, compactedDeltas) = deltasAndCompactedDeltas.partition(isDeltaFile)
      // Find the latest checkpoint in the listing that is not older than the versionToLoad
      val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
      val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, versionToLoad)
      val newCheckpointVersion = newCheckpoint.map(_.version).getOrElse {
        // If we do not have any checkpoint, pass new checkpoint version as -1 so that first
        // delta version can be 0.
        if (lastCheckpointVersion >= 0) {
          // `startCheckpoint` was given but no checkpoint found on delta log. This means that the
          // last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists.
          // Try to look up another valid checkpoint and create `LogSegment` from it.
          // This case can arise if the user deleted the table (all commits and checkpoints) but
          // left the _last_checkpoint intact.
          recordDeltaEvent(this, "delta.checkpoint.error.partial")
          val snapshotVersion = versionToLoad.getOrElse(deltaVersion(deltas.last))
          getLogSegmentWithMaxExclusiveCheckpointVersion(
              snapshotVersion, lastCheckpointVersion, tableCommitCoordinatorClientOpt)
            .foreach { alternativeLogSegment => return Some(alternativeLogSegment) }

          // No alternative found, but the directory contains files so we cannot return None.
          throw DeltaErrors.missingPartFilesException(
            lastCheckpointVersion, new FileNotFoundException(
              s"Checkpoint file to load version: $lastCheckpointVersion is missing."))
        }
        -1L
      }

      // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
      // it will list all existing delta files.
      val deltasAfterCheckpoint = deltas.filter { file =>
        deltaVersion(file) > newCheckpointVersion
      }

      // Here we validate that we are able to create a valid LogSegment by just using commit deltas
      // and without considering minor-compacted deltas. We want to fail early if log is messed up
      // i.e. some commit deltas are missing (although compacted-deltas are present).
      // We should not do this validation when we want to update the logSegment after a conflict
      // via the [[SnapshotManagement.getUpdatedLogSegment]] method. In that specific flow, we just
      // list from the committed version and reuse existing pre-commit logsegment together with
      // listing result to create the new pre-commit logsegment. Because of this, we don't have info
      // about all the delta files (e.g. when minor compactions are used in existing preCommit log
      // segment) and hence the validation if attempted will fail. So we need to set
      // `validateLogSegmentWithoutCompactedDeltas` to false in that case.
      if (validateLogSegmentWithoutCompactedDeltas) {
        validateDeltaVersions(deltasAfterCheckpoint, newCheckpointVersion, versionToLoad)
      }

      val newVersion =
        deltasAfterCheckpoint.lastOption.map(deltaVersion).getOrElse(newCheckpoint.get.version)
      // reuse the oldCheckpointProvider if it is same as what we are looking for.
      val checkpointProviderOpt = newCheckpoint.map { ci =>
        oldCheckpointProviderOpt
          .collect { case cp if cp.version == ci.version => cp }
          .getOrElse(ci.getCheckpointProvider(this, checkpoints, lastCheckpointInfo))
      }
      // In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
      // they may just be before the checkpoint version unless we have a bug in log cleanup.
      if (deltas.isEmpty) {
        throw new IllegalStateException(s"Could not find any delta files for version $newVersion")
      }
      if (versionToLoad.exists(_ != newVersion)) {
        throwNonExistentVersionError(versionToLoad.get)
      }
      val lastCommitTimestamp = deltas.last.getModificationTime

      val deltasAndCompactedDeltasForLogSegment = useCompactedDeltasForLogSegment(
        deltasAndCompactedDeltas,
        deltasAfterCheckpoint,
        latestCommitVersion = newVersion,
        checkpointVersionToUse = newCheckpointVersion)

      validateDeltaVersions(
        deltasAndCompactedDeltasForLogSegment, newCheckpointVersion, versionToLoad)

      Some(LogSegment(
        logPath,
        newVersion,
        deltasAndCompactedDeltasForLogSegment,
        checkpointProviderOpt,
        lastCommitTimestamp))
    }
  }

  /**
   * @param deltasAndCompactedDeltas - all deltas or compacted deltas which could be used
   * @param deltasAfterCheckpoint - deltas after the last checkpoint file
   * @param latestCommitVersion - commit version for which we are trying to create Snapshot for
   * @param checkpointVersionToUse - underlying checkpoint version to use in Snapshot, -1 if no
   *                               checkpoint is used.
   * @return Returns a list of deltas/compacted-deltas which can be used to construct the
   *         [[LogSegment]] instead of `deltasAfterCheckpoint`.
   */
  protected def useCompactedDeltasForLogSegment(
      deltasAndCompactedDeltas: Seq[FileStatus],
      deltasAfterCheckpoint: Array[FileStatus],
      latestCommitVersion: Long,
      checkpointVersionToUse: Long): Array[FileStatus] = {

    val selectedDeltas = mutable.ArrayBuffer.empty[FileStatus]
    var highestVersionSeen = checkpointVersionToUse
    val commitRangeCovered = mutable.ArrayBuffer.empty[Long]
    // track if there is at least 1 compacted delta in `deltasAndCompactedDeltas`
    var hasCompactedDeltas = false
    for (file <- deltasAndCompactedDeltas) {
      val (startVersion, endVersion) = file match {
        case CompactedDeltaFile(_, startVersion, endVersion) =>
          hasCompactedDeltas = true
          (startVersion, endVersion)
        case DeltaFile(_, version) =>
          (version, version)
      }

      // select the compacted delta if the startVersion doesn't straddle `highestVersionSeen` and
      // the endVersion doesn't cross the latestCommitVersion.
      if (highestVersionSeen < startVersion && endVersion <= latestCommitVersion) {
        commitRangeCovered.appendAll(startVersion to endVersion)
        selectedDeltas += file
        highestVersionSeen = endVersion
      }
    }
    // If there are no compacted deltas in the `deltasAndCompactedDeltas` list, return from this
    // method.
    if (!hasCompactedDeltas) return deltasAfterCheckpoint
    // Validation-1: Commits represented by `compactedDeltasToUse` should be unique and there must
    // not be any duplicates.
    val coveredCommits = commitRangeCovered.toSet
    val hasDuplicates = (commitRangeCovered.size != coveredCommits.size)

    // Validation-2: All commits from (CheckpointVersion + 1) to latestCommitVersion should be
    // either represented by compacted delta or by the delta.
    val requiredCommits = (checkpointVersionToUse + 1) to latestCommitVersion
    val missingCommits = requiredCommits.toSet -- coveredCommits
    if (!hasDuplicates && missingCommits.isEmpty) return selectedDeltas.toArray

    // If the above check failed, that means the compacted delta validation failed.
    // Just record that event and return just the deltas (deltasAfterCheckpoint).
    val eventData = Map(
      "deltasAndCompactedDeltas" -> deltasAndCompactedDeltas.map(_.getPath.getName),
      "deltasAfterCheckpoint" -> deltasAfterCheckpoint.map(_.getPath.getName),
      "latestCommitVersion" -> latestCommitVersion,
      "checkpointVersionToUse" -> checkpointVersionToUse,
      "hasDuplicates" -> hasDuplicates,
      "missingCommits" -> missingCommits
    )
    recordDeltaEvent(
      deltaLog = this,
      opType = "delta.getLogSegmentForVersion.compactedDeltaValidationFailed",
      data = eventData)
    if (Utils.isTesting) {
      assert(false, s"Validation around Compacted deltas failed while creating Snapshot. " +
        s"[${JsonUtils.toJson(eventData)}]")
    }
    deltasAfterCheckpoint
  }

  def throwNonExistentVersionError(versionToLoad: Long): Unit = {
    throw new IllegalStateException(
      s"Trying to load a non-existent version $versionToLoad")
  }

  /**
   * Load the Snapshot for this Delta table at initialization. This method uses the `lastCheckpoint`
   * file as a hint on where to start listing the transaction log directory. If the _delta_log
   * directory doesn't exist, this method will return an `InitialSnapshot`.
   */
  protected def getSnapshotAtInit: CapturedSnapshot = withSnapshotLockInterruptibly {
    recordFrameProfile("Delta", "SnapshotManagement.getSnapshotAtInit") {
      val snapshotInitWallclockTime = clock.getTimeMillis()
      val lastCheckpointOpt = readLastCheckpointFile()
      val initialSegmentForNewSnapshot = createLogSegment(
        versionToLoad = None,
        lastCheckpointInfo = lastCheckpointOpt)
      val snapshot = getUpdatedSnapshot(
        oldSnapshotOpt = None,
        initialSegmentForNewSnapshot = initialSegmentForNewSnapshot,
        initialTableCommitCoordinatorClient = None,
        isAsync = false)
      CapturedSnapshot(snapshot, snapshotInitWallclockTime)
    }
  }

  /**
   * Returns the current snapshot. This does not automatically `update()`.
   *
   * WARNING: This is not guaranteed to give you the latest snapshot of the log, nor stay
   * consistent across multiple accesses. If you need the latest snapshot, it is recommended
   * to fetch it using `deltaLog.update()`; and save the returned snapshot so it does not
   * unexpectedly change from under you. See how [[OptimisticTransaction]] and [[DeltaScan]]
   * use the snapshot as examples for write/read paths respectively.
   * This API should only be used in scenarios where any recent snapshot will suffice and an
   * update is undesired, or by internal code that holds the DeltaLog lock to prevent races.
   */
  def unsafeVolatileSnapshot: Snapshot = Option(currentSnapshot).map(_.snapshot).orNull

  /**
   * WARNING: This API is unsafe and deprecated. It will be removed in future versions.
   * Use the above unsafeVolatileSnapshot to get the most recently cached snapshot on
   * the cluster.
   */
  @deprecated("This method is deprecated and will be removed in future versions. " +
    "Use unsafeVolatileSnapshot instead", "12.0")
  def snapshot: Snapshot = unsafeVolatileSnapshot

  /**
   * Unsafe due to thread races that can change it at any time without notice, even between two
   * calls in the same method. Like [[unsafeVolatileSnapshot]] it depends on, this method should be
   * used only with extreme care in production code (or by unit tests where no races are possible).
   */
  private[delta] def unsafeVolatileMetadata =
    Option(unsafeVolatileSnapshot).map(_.metadata).getOrElse(Metadata())

  protected def createSnapshot(
      initSegment: LogSegment,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      checksumOpt: Option[VersionChecksum]): Snapshot = {
    val startingFrom = if (!initSegment.checkpointProvider.isEmpty) {
      log" starting from checkpoint version " +
      log"${MDC(DeltaLogKeys.START_VERSION, initSegment.checkpointProvider.version)}."
    } else log"."
    logInfo(log"Loading version ${MDC(DeltaLogKeys.VERSION, initSegment.version)}" + startingFrom)
    createSnapshotFromGivenOrEquivalentLogSegment(
        initSegment, tableCommitCoordinatorClientOpt) { segment =>
      new Snapshot(
        path = logPath,
        version = segment.version,
        logSegment = segment,
        deltaLog = this,
        checksumOpt = checksumOpt.orElse(
          readChecksum(segment.version, lastSeenChecksumFileStatusOpt))
      )
    }
  }

  /**
   * Returns a [[LogSegment]] for reading `snapshotVersion` such that the segment's checkpoint
   * version (if checkpoint present) is LESS THAN `maxExclusiveCheckpointVersion`.
   * This is useful when trying to skip a bad checkpoint. Returns `None` when we are not able to
   * construct such [[LogSegment]], for example, no checkpoint can be used but we don't have the
   * entire history from version 0 to version `snapshotVersion`.
   * *Note*: If table is a coordinated-commits table, the commit-coordinator MUST be passed to
   * correctly list the commits.
   */
  private def getLogSegmentWithMaxExclusiveCheckpointVersion(
      snapshotVersion: Long,
      maxExclusiveCheckpointVersion: Long,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient]): Option[LogSegment] = {
    assert(
      snapshotVersion >= maxExclusiveCheckpointVersion,
      s"snapshotVersion($snapshotVersion) is less than " +
        s"maxExclusiveCheckpointVersion($maxExclusiveCheckpointVersion)")
    val upperBoundVersion = math.min(snapshotVersion + 1, maxExclusiveCheckpointVersion)
    val previousCp =
      if (upperBoundVersion > 0) findLastCompleteCheckpointBefore(upperBoundVersion) else None
    previousCp match {
      case Some(cp) =>
        val filesSinceCheckpointVersion = listDeltaCompactedDeltaAndCheckpointFiles(
          startVersion = cp.version,
          tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
          versionToLoad = Some(snapshotVersion),
          includeMinorCompactions = false
        ).getOrElse(Array.empty)
        val (checkpoints, deltas) = filesSinceCheckpointVersion.partition(isCheckpointFile)
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
        val checkpointProvider =
          cp.getCheckpointProvider(this, checkpoints, lastCheckpointInfoHint = None)
        // Create the list of `FileStatus`s for delta files after `cp.version`.
        val deltasAfterCheckpoint = deltas.filter { file =>
          deltaVersion(file) > cp.version
        }
        val deltaVersions = deltasAfterCheckpoint.map(deltaVersion)
        // `deltaVersions` should not be empty and `verifyDeltaVersions` will verify it
        try {
          verifyDeltaVersions(spark, deltaVersions, Some(cp.version + 1), Some(snapshotVersion))
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to find a valid LogSegment for " +
              log"${MDC(DeltaLogKeys.VERSION, snapshotVersion)}", e)
            return None
        }
        Some(LogSegment(
          logPath,
          snapshotVersion,
          deltas,
          Some(checkpointProvider),
          deltas.last.getModificationTime))
      case None =>
        val listFromResult =
          listDeltaCompactedDeltaAndCheckpointFiles(
            startVersion = 0,
            tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
            versionToLoad = Some(snapshotVersion),
            includeMinorCompactions = false)
        val (deltas, deltaVersions) =
          listFromResult
            .getOrElse(Array.empty)
            .flatMap(DeltaFile.unapply(_))
            .unzip
        try {
          verifyDeltaVersions(spark, deltaVersions, Some(0), Some(snapshotVersion))
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to find a valid LogSegment for " +
              log"${MDC(DeltaLogKeys.VERSION, snapshotVersion)}", e)
            return None
        }
        Some(LogSegment(
          logPath = logPath,
          version = snapshotVersion,
          deltas = deltas,
          checkpointProviderOpt = None,
          lastCommitTimestamp = deltas.last.getModificationTime))
    }
  }

  /**
   * Used to compute the LogSegment after a commit, by adding the delta file with the specified
   * version to the preCommitLogSegment (which must match the immediately preceding version).
   */
  protected[delta] def getLogSegmentAfterCommit(
      committedVersion: Long,
      newChecksumOpt: Option[VersionChecksum],
      preCommitLogSegment: LogSegment,
      commit: Commit,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      oldCheckpointProvider: CheckpointProvider): LogSegment = recordFrameProfile(
    "Delta", "SnapshotManagement.getLogSegmentAfterCommit") {
    // If the table doesn't have any competing updates, then go ahead and use the optimized
    // incremental logSegment computation to fetch the LogSegment for the committedVersion.
    // See the comment in the getLogSegmentAfterCommit overload for why we can't always safely
    // return the committedVersion's snapshot when there is contention.
    val useFastSnapshotConstruction = !snapshotLock.hasQueuedThreads
    if (useFastSnapshotConstruction) {
      SnapshotManagement.appendCommitToLogSegment(
        preCommitLogSegment, commit.getFileStatus, committedVersion)
    } else {
      val latestCheckpointProvider =
        Seq(preCommitLogSegment.checkpointProvider, oldCheckpointProvider).maxBy(_.version)
      getLogSegmentAfterCommit(tableCommitCoordinatorClientOpt, latestCheckpointProvider)
    }
  }

  protected[delta] def getLogSegmentAfterCommit(
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      oldCheckpointProvider: UninitializedCheckpointProvider): LogSegment = {
    /**
     * We can't specify `versionToLoad = committedVersion` for the call below.
     * If there are a lot of concurrent commits to the table on the same cluster, each
     * would generate a different snapshot, and thus each would trigger a new state
     * reconstruction. The last commit would get stuck waiting for each of the previous
     * jobs to finish to grab the update lock.
     * Instead, just do a general update to the latest available version. The racing commits
     * can then use the version check short-circuit to avoid constructing a new snapshot.
     */
    createLogSegment(
      oldCheckpointProviderOpt = Some(oldCheckpointProvider),
      tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt
    ).getOrElse {
      // This shouldn't be possible right after a commit
      logError(log"No delta log found for the Delta table at ${MDC(DeltaLogKeys.PATH, logPath)}")
      throw DeltaErrors.emptyDirectoryException(logPath.toString)
    }
  }

  /**
   * Create a [[Snapshot]] from the given [[LogSegment]]. If failing to create the snapshot, we will
   * search an equivalent [[LogSegment]] using a different checkpoint and retry up to
   * [[DeltaSQLConf.DELTA_SNAPSHOT_LOADING_MAX_RETRIES]] times.
   */
  protected def createSnapshotFromGivenOrEquivalentLogSegment(
      initSegment: LogSegment,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient])
      (snapshotCreator: LogSegment => Snapshot): Snapshot = {
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
        case e: SparkException if attempt < numRetries && !segment.checkpointProvider.isEmpty =>
          if (firstError == null) {
            firstError = e
          }
          logWarning(log"Failed to create a snapshot from log segment " +
            log"${MDC(DeltaLogKeys.SEGMENT, segment)}. Trying a different checkpoint.", e)
          segment = getLogSegmentWithMaxExclusiveCheckpointVersion(
            segment.version,
            segment.checkpointProvider.version,
            tableCommitCoordinatorClientOpt).getOrElse {
              // Throw the first error if we cannot find an equivalent `LogSegment`.
              throw firstError
            }
          attempt += 1
        case e: SparkException if firstError != null =>
          logWarning(log"Failed to create a snapshot from log segment " +
            log"${MDC(DeltaLogKeys.SEGMENT, segment)}", e)
          throw firstError
      }
    }
    throw new IllegalStateException("should not happen")
  }

  /** Checks if the given timestamp is outside the current staleness window */
  protected def isCurrentlyStale: Long => Boolean = {
    val limit = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val cutoffOpt = if (limit > 0) Some(math.max(0, clock.getTimeMillis() - limit)) else None
    timestamp => cutoffOpt.forall(timestamp < _)
  }

  /**
   * Get the newest logSegment, using the previous logSegment as a hint. This is faster than
   * doing a full update, but it won't work if the table's log directory was replaced.
   */
  def getUpdatedLogSegment(
      oldLogSegment: LogSegment,
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient]
  ): (LogSegment, Seq[FileStatus]) = {
    val includeCompactions = spark.conf.get(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS)
    val newFilesOpt = listDeltaCompactedDeltaAndCheckpointFiles(
        startVersion = oldLogSegment.version + 1,
        tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
        versionToLoad = None,
        includeMinorCompactions = includeCompactions)
    val newFiles = newFilesOpt.getOrElse {
      // An empty listing likely implies a list-after-write inconsistency or that somebody clobbered
      // the Delta log.
      return (oldLogSegment, Nil)
    }
    val allFiles = (
      oldLogSegment.checkpointProvider.topLevelFiles ++
        oldLogSegment.deltas ++
        newFiles
      ).toArray
    val lastCheckpointInfo = Option.empty[LastCheckpointInfo]
    val newLogSegment = getLogSegmentForVersion(
      versionToLoad = None,
      files = Some(allFiles),
      validateLogSegmentWithoutCompactedDeltas = false,
      tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
      lastCheckpointInfo = lastCheckpointInfo,
      oldCheckpointProviderOpt = Some(oldLogSegment.checkpointProvider)
    ).getOrElse(oldLogSegment)
    val fileStatusesOfConflictingCommits = newFiles.collect {
      case DeltaFile(f, v) if v <= newLogSegment.version => f
    }
    (newLogSegment, fileStatusesOfConflictingCommits)
  }

  /**
   * Returns the snapshot, if it has been updated since the specified timestamp.
   *
   * Note that this should be used differently from isSnapshotStale. Staleness is
   * used to allow async updates if the table has been updated within the staleness
   * window, which allows for better perf in exchange for possibly using a slightly older
   * view of the table. For eg, if a table is queried multiple times in quick succession.
   *
   * On the other hand, getSnapshotIfFresh is used to identify duplicate updates within a
   * single transaction. For eg, if a table isn't cached and the snapshot was fetched from the
   * logstore, then updating the snapshot again in the same transaction is superfluous. We can
   * use this function to detect and skip such an update.
   */
  private def getSnapshotIfFresh(
      capturedSnapshot: CapturedSnapshot,
      checkIfUpdatedSinceTs: Option[Long]): Option[Snapshot] = {
    checkIfUpdatedSinceTs.collect {
      case ts if ts <= capturedSnapshot.updateTimestamp => capturedSnapshot.snapshot
    }
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
    val startTimeMs = System.currentTimeMillis()
    // currentSnapshot is volatile. Make a local copy of it at the start of the update call, so
    // that there's no chance of a race condition changing the snapshot partway through the update.
    val capturedSnapshot = currentSnapshot
    val oldVersion = capturedSnapshot.snapshot.version
    def sendEvent(
      newSnapshot: Snapshot,
      snapshotAlreadyUpdatedAfterRequiredTimestamp: Boolean = false
    ): Unit = {
      recordDeltaEvent(
        this,
        opType = "deltaLog.update",
        data = Map(
          "snapshotAlreadyUpdatedAfterRequiredTimestamp" ->
            snapshotAlreadyUpdatedAfterRequiredTimestamp,
          "newVersion" -> newSnapshot.version,
          "oldVersion" -> oldVersion,
          "timeTakenMs" -> (System.currentTimeMillis() - startTimeMs)
        )
      )
    }
    // Eagerly exit if the snapshot is already new enough to satisfy the caller
    getSnapshotIfFresh(capturedSnapshot, checkIfUpdatedSinceTs).foreach { snapshot =>
      sendEvent(snapshot, snapshotAlreadyUpdatedAfterRequiredTimestamp = true)
      return snapshot
    }
    val doAsync = stalenessAcceptable && !isCurrentlyStale(capturedSnapshot.updateTimestamp)
    if (!doAsync) {
      recordFrameProfile("Delta", "SnapshotManagement.update") {
        withSnapshotLockInterruptibly {
          val newSnapshot = updateInternal(isAsync = false)
          sendEvent(newSnapshot = capturedSnapshot.snapshot)
          newSnapshot
        }
      }
    } else {
      // Kick off an async update, if one is not already obviously running. Intentionally racy.
      if (Option(asyncUpdateTask).forall(_.isDone)) {
        try {
          val jobGroup = spark.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
          asyncUpdateTask = SnapshotManagement.deltaLogAsyncUpdateThreadPool.submit(spark) {
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
            spark.sparkContext.setJobGroup(
              jobGroup,
              s"Updating state of Delta table at ${capturedSnapshot.snapshot.path}",
              interruptOnCancel = true)
            tryUpdate(isAsync = true)
          }
        } catch {
          case NonFatal(e) if !Utils.isTesting =>
            // Failed to schedule the future -- fail in testing, but just log it in prod.
            recordDeltaEvent(this, "delta.snapshot.asyncUpdateFailed", data = Map("exception" -> e))
        }
      }
      currentSnapshot.snapshot
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  private def tryUpdate(isAsync: Boolean): Snapshot = {
    if (snapshotLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        snapshotLock.unlock()
      }
    } else {
      currentSnapshot.snapshot
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `snapshotLock` before calling this method.
   */
  protected def updateInternal(isAsync: Boolean): Snapshot =
    recordDeltaOperation(this, "delta.log.update", Map(TAG_ASYNC -> isAsync.toString)) {
      val updateStartTimeMs = clock.getTimeMillis()
      val previousSnapshot = currentSnapshot.snapshot
      val segmentOpt = createLogSegment(previousSnapshot)
      val newSnapshot = getUpdatedSnapshot(
        oldSnapshotOpt = Some(previousSnapshot),
        initialSegmentForNewSnapshot = segmentOpt,
        initialTableCommitCoordinatorClient = previousSnapshot.tableCommitCoordinatorClientOpt,
        isAsync = isAsync)
      installSnapshot(newSnapshot, updateStartTimeMs)
    }

  /**
   * Updates and installs a new snapshot in the `currentSnapshot`.
   * This method takes care of recursively creating new snapshots if the commit-coordinator has
   * changed.
   * @param oldSnapshotOpt The previous snapshot, if any.
   * @param initialSegmentForNewSnapshot the log segment constructed for the new snapshot
   * @param initialTableCommitCoordinatorClient the commit-coordinator used for constructing the
   *                           `initialSegmentForNewSnapshot`
   * @param isAsync Whether the update is async.
   * @return The new snapshot.
   */
  protected def getUpdatedSnapshot(
      oldSnapshotOpt: Option[Snapshot],
      initialSegmentForNewSnapshot: Option[LogSegment],
      initialTableCommitCoordinatorClient: Option[TableCommitCoordinatorClient],
      isAsync: Boolean): Snapshot = {
    var newSnapshot = getSnapshotForLogSegmentInternal(
      oldSnapshotOpt,
      initialSegmentForNewSnapshot,
      initialTableCommitCoordinatorClient,
      isAsync
    )
    // Identify whether the snapshot was created using a "stale" commit-coordinator. If yes, we need
    // to again invoke [[updateSnapshot]] so that we could get the latest commits from the updated
    // commit-coordinator client. We need to do it only once as the delta spec mandates the commit
    // which changes the commit-coordinator to be backfilled.
    val usedStaleCommitCoordinator =
      newSnapshot.tableCommitCoordinatorClientOpt.exists { newStore =>
        initialTableCommitCoordinatorClient.forall(!_.semanticsEquals(newStore))
      }
    if (usedStaleCommitCoordinator) {
      val segmentOpt = createLogSegment(newSnapshot)
      newSnapshot =
        getSnapshotForLogSegmentInternal(
          Some(newSnapshot), segmentOpt, newSnapshot.tableCommitCoordinatorClientOpt, isAsync)
    }
    newSnapshot
  }

  /** Creates a Snapshot for the given `segmentOpt` */
  protected def getSnapshotForLogSegmentInternal(
      previousSnapshotOpt: Option[Snapshot],
      segmentOpt: Option[LogSegment],
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      isAsync: Boolean): Snapshot = {
    segmentOpt.map { segment =>
      if (previousSnapshotOpt.exists(_.logSegment == segment)) {
        val previousSnapshot = previousSnapshotOpt.get
        previousSnapshot.updateLastKnownBackfilledVersion(segment.lastBackfilledVersionInSegment)
        previousSnapshot
      } else {
        val newSnapshot = createSnapshot(
          initSegment = segment,
          tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
          checksumOpt = None)
        previousSnapshotOpt.foreach(logMetadataTableIdChange(_, newSnapshot))
        logInfo(log"Updated snapshot to ${MDC(DeltaLogKeys.SNAPSHOT, newSnapshot)}")
        newSnapshot
      }
    }.getOrElse {
      logInfo("Creating initial snapshot without metadata, because the directory is empty")
      new InitialSnapshot(logPath, this)
    }
  }

  /** Installs the given `newSnapshot` as the `currentSnapshot` */
  protected def installSnapshot(newSnapshot: Snapshot, updateTimestamp: Long): Snapshot = {
    if (!snapshotLock.isHeldByCurrentThread) {
      if (Utils.isTesting) {
        throw new RuntimeException("DeltaLog snapshot replaced without taking lock")
      }
      recordDeltaEvent(this, "delta.update.unsafeReplace")
    }
    if (currentSnapshot == null) {
      // cold snapshot initialization
      currentSnapshot = CapturedSnapshot(newSnapshot, updateTimestamp)
      return newSnapshot
    }
    val CapturedSnapshot(oldSnapshot, oldTimestamp) = currentSnapshot
    if (oldSnapshot eq newSnapshot) {
      // Same snapshot as before, so just refresh the timestamp
      val timestampToUse = math.max(updateTimestamp, oldTimestamp)
      currentSnapshot = CapturedSnapshot(newSnapshot, timestampToUse)
    } else {
      // Install the new snapshot and uncache the old one
      currentSnapshot = CapturedSnapshot(newSnapshot, updateTimestamp)
      oldSnapshot.uncache()
    }
    newSnapshot
  }

  /** Log a change in the metadata's table id whenever we install a newer version of a snapshot */
  private def logMetadataTableIdChange(previousSnapshot: Snapshot, newSnapshot: Snapshot): Unit = {
    if (previousSnapshot.version > -1 &&
      previousSnapshot.metadata.id != newSnapshot.metadata.id) {
      val msg = s"Change in the table id detected while updating snapshot. " +
        s"\nPrevious snapshot = $previousSnapshot\nNew snapshot = $newSnapshot."
      logWarning(msg)
      recordDeltaEvent(self, "delta.metadataCheck.update", data = Map(
        "prevSnapshotVersion" -> previousSnapshot.version,
        "prevSnapshotMetadata" -> previousSnapshot.metadata,
        "nextSnapshotVersion" -> newSnapshot.version,
        "nextSnapshotMetadata" -> newSnapshot.metadata))
    }
  }

  /**
   * Creates a snapshot for a new delta commit.
   */
  protected def createSnapshotAfterCommit(
      initSegment: LogSegment,
      newChecksumOpt: Option[VersionChecksum],
      tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient],
      committedVersion: Long): Snapshot = {
    logInfo(
      log"Creating a new snapshot v${MDC(DeltaLogKeys.VERSION, initSegment.version)} " +
      log"for commit version ${MDC(DeltaLogKeys.VERSION2, committedVersion)}")
    createSnapshot(
      initSegment,
      tableCommitCoordinatorClientOpt = tableCommitCoordinatorClientOpt,
      checksumOpt = newChecksumOpt
    )
  }

  /**
   * Called after committing a transaction and updating the state of the table.
   *
   * @param committedVersion the version that was committed
   * @param commit information about the commit file.
   * @param newChecksumOpt the checksum for the new commit, if available.
   *                       Usually None, since the commit would have just finished.
   * @param preCommitLogSegment the log segment of the table prior to commit
   */
  def updateAfterCommit(
      committedVersion: Long,
      commit: Commit,
      newChecksumOpt: Option[VersionChecksum],
      preCommitLogSegment: LogSegment): Snapshot = withSnapshotLockInterruptibly {
    recordDeltaOperation(this, "delta.log.updateAfterCommit") {
      val updateTimestamp = clock.getTimeMillis()
      val previousSnapshot = currentSnapshot.snapshot
      // Somebody else could have already updated the snapshot while we waited for the lock
      if (committedVersion <= previousSnapshot.version) return previousSnapshot
      val segment = getLogSegmentAfterCommit(
        committedVersion,
        newChecksumOpt,
        preCommitLogSegment,
        commit,
        previousSnapshot.tableCommitCoordinatorClientOpt,
        previousSnapshot.checkpointProvider)

      // This likely implies a list-after-write inconsistency
      if (segment.version < committedVersion) {
        recordDeltaEvent(this, "delta.commit.inconsistentList", data = Map(
          "committedVersion" -> committedVersion,
          "currentVersion" -> segment.version
        ))
        throw DeltaErrors.invalidCommittedVersion(committedVersion, segment.version)
      }

      val newSnapshot = createSnapshotAfterCommit(
        segment,
        newChecksumOpt,
        previousSnapshot.tableCommitCoordinatorClientOpt,
        committedVersion)
      logMetadataTableIdChange(previousSnapshot, newSnapshot)
      logInfo(log"Updated snapshot to ${MDC(DeltaLogKeys.SNAPSHOT, newSnapshot)}")
      installSnapshot(newSnapshot, updateTimestamp)
    }
  }

  /** Get the snapshot at `version`. */
  def getSnapshotAt(
      version: Long,
      lastCheckpointHint: Option[CheckpointInstance] = None): Snapshot = {
    getSnapshotAt(version, lastCheckpointHint, lastCheckpointProvider = None)
  }

  /**
   * Get the snapshot at `version` using the given `lastCheckpointProvider` or `lastCheckpointHint`
   * as the listing hint.
   */
  private[delta] def getSnapshotAt(
      version: Long,
      lastCheckpointHint: Option[CheckpointInstance],
      lastCheckpointProvider: Option[CheckpointProvider]): Snapshot = {

    // See if the version currently cached on the cluster satisfies the requirement
    val currentSnapshot = unsafeVolatileSnapshot
    val upperBoundSnapshot = if (currentSnapshot.version >= version) {
      // current snapshot is already newer than what we are looking for. so it could be used as
      // upper bound.
      currentSnapshot
    } else {
      val latestSnapshot = update()
      if (latestSnapshot.version < version) {
        throwNonExistentVersionError(version)
      }
      latestSnapshot
    }
    if (upperBoundSnapshot.version == version) {
      return upperBoundSnapshot
    }

    val (lastCheckpointInfoOpt, lastCheckpointProviderOpt) = lastCheckpointProvider match {
      // NOTE: We must ignore any hint whose version is higher than the requested version.
      case Some(checkpointProvider) if checkpointProvider.version <= version =>
        // Prefer the last checkpoint provider hint, because it doesn't require any I/O to use.
        None -> Some(checkpointProvider)
      case _ =>
        val lastCheckpointInfoForListing = lastCheckpointHint
            .filter(_.version <= version)
            .orElse(findLastCompleteCheckpointBefore(version))
            .map(manuallyLoadCheckpoint)
        lastCheckpointInfoForListing -> None
    }
    val logSegmentOpt = createLogSegment(
      versionToLoad = Some(version),
      oldCheckpointProviderOpt = lastCheckpointProviderOpt,
      tableCommitCoordinatorClientOpt = upperBoundSnapshot.tableCommitCoordinatorClientOpt,
      lastCheckpointInfo = lastCheckpointInfoOpt)
    val logSegment = logSegmentOpt.getOrElse {
      // We can't return InitialSnapshot because our caller asked for a specific snapshot version.
      throw DeltaErrors.emptyDirectoryException(logPath.toString)
    }
    createSnapshot(
      initSegment = logSegment,
      tableCommitCoordinatorClientOpt = upperBoundSnapshot.tableCommitCoordinatorClientOpt,
      checksumOpt = None)
  }

  // Visible for testing
  private[delta] def getCapturedSnapshot(): CapturedSnapshot = currentSnapshot
}

object SnapshotManagement {
  // A thread pool for reading checkpoint files and collecting checkpoint v2 actions like
  // checkpointMetadata, sidecarFiles.
  private[delta] lazy val checkpointV2ThreadPool = {
    val numThreads = SparkSession.active.sessionState.conf.getConf(
      DeltaSQLConf.CHECKPOINT_V2_DRIVER_THREADPOOL_PARALLELISM)
    DeltaThreadPool("checkpointV2-threadpool", numThreads)
  }

  protected[delta] lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    new DeltaThreadPool(tpe)
  }

  private lazy val commitCoordinatorGetCommitsThreadPool = {
    val numThreads = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.COORDINATED_COMMITS_GET_COMMITS_THREAD_POOL_SIZE)
    val tpe = ThreadUtils.newDaemonCachedThreadPool("commit-coordinator-get-commits", numThreads)
    new DeltaThreadPool(tpe)
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
      require(versions.nonEmpty && versions.last == v, "Did not get the last delta " +
        s"file version: $v to compute Snapshot")
    }
  }

  def appendCommitToLogSegment(
      oldLogSegment: LogSegment,
      commitFileStatus: FileStatus,
      committedVersion: Long): LogSegment = {
    require(oldLogSegment.version + 1 == committedVersion)
    oldLogSegment.copy(
      version = committedVersion,
      deltas = oldLogSegment.deltas :+ commitFileStatus,
      lastCommitFileModificationTimestamp = commitFileStatus.getModificationTime)
  }
}

/** A serializable variant of HDFS's FileStatus. */
case class SerializableFileStatus(
    path: String,
    length: Long,
    isDir: Boolean,
    modificationTime: Long) {

  // Important note! This is very expensive to compute, but we don't want to cache it
  // as a `val` because Paths internally contain URIs and therefore consume lots of memory.
  @JsonIgnore
  def getHadoopPath: Path = new Path(path)

  def toFileStatus: FileStatus = {
    new FileStatus(length, isDir, 0, 0, modificationTime, new Path(path))
  }

  override def equals(obj: Any): Boolean = obj match {
    // We only compare the paths to stay consistent with FileStatus.equals.
    case other: SerializableFileStatus => Objects.equals(path, other.path)
    case _ => false
  }

  // We only use the path to stay consistent with FileStatus.hashCode.
  override def hashCode(): Int = Objects.hashCode(path)
}

object SerializableFileStatus {
  def fromStatus(status: FileStatus): SerializableFileStatus = {
    SerializableFileStatus(
      Option(status.getPath).map(_.toString).orNull,
      status.getLen,
      status.isDirectory,
      status.getModificationTime)
  }

  val EMPTY: SerializableFileStatus = fromStatus(new FileStatus())
}

/**
 * Provides information around which files in the transaction log need to be read to create
 * the given version of the log.
 *
 * @param logPath The path to the _delta_log directory
 * @param version The Snapshot version to generate
 * @param deltas The delta commit files (.json) to read
 * @param checkpointProvider provider to give information about Checkpoint files.
 * @param lastCommitFileModificationTimestamp The "unadjusted" file modification timestamp of the
 *          last commit within this segment. By unadjusted, we mean that the commit timestamps may
 *          not necessarily be monotonically increasing for the commits within this segment.
 */
case class LogSegment(
    logPath: Path,
    version: Long,
    deltas: Seq[FileStatus],
    checkpointProvider: UninitializedCheckpointProvider,
    lastCommitFileModificationTimestamp: Long) {

  override def hashCode(): Int =
    logPath.hashCode() * 31 + (lastCommitFileModificationTimestamp % 10000).toInt

  /**
   * An efficient way to check if a cached Snapshot's contents actually correspond to a new
   * segment returned through file listing.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: LogSegment =>
        version == other.version &&
          logPath == other.logPath &&
          checkpointProvider.version == other.checkpointProvider.version &&
          lastMatchingBackfilledCommitIsEqual(other)
      case _ => false
    }
  }

  private def lastMatchingBackfilledCommitIsEqual(other: LogSegment): Boolean = {
    def fileStatusEquals(fileStatus1: FileStatus, fileStatus2: FileStatus): Boolean = {
      fileStatus1.getPath == fileStatus2.getPath &&
        fileStatus1.getLen == fileStatus2.getLen &&
        fileStatus1.getModificationTime == fileStatus2.getModificationTime
    }

    val backfilledPrefixThis = deltas.takeWhile(isBackfilledDeltaFile)
    val backfilledPrefixOther = other.deltas.takeWhile(isBackfilledDeltaFile)
    val sizeToAnalyze = math.min(backfilledPrefixThis.size, backfilledPrefixOther.size)
    val backfilledPrefixThisStripped = backfilledPrefixThis.take(sizeToAnalyze)
    val backfilledPrefixOtherStripped = backfilledPrefixOther.take(sizeToAnalyze)
    backfilledPrefixThisStripped.zip(backfilledPrefixOtherStripped)
      .forall { case (delta1, delta2) => fileStatusEquals(delta1, delta2) } &&
      checkpointProvider.topLevelFiles.size == other.checkpointProvider.topLevelFiles.size &&
      checkpointProvider.topLevelFiles.zip(other.checkpointProvider.topLevelFiles).forall {
        case (cp1, cp2) => fileStatusEquals(cp1, cp2)
      }
  }

  private[delta] lazy val lastBackfilledVersionInSegment =
    // This works if the last backfilled file is a minor-compaction, because
    // FileNames.getFileVersion returns the minor-compaction end version,
    // which correctly initializes the lastBackfilledVersionInSegment.
    CoordinatedCommitsUtils.getLastBackfilledFile(deltas).map(getFileVersion)
      .getOrElse(checkpointProvider.version)
}

/** Exception thrown When [[TableCommitCoordinatorClient.getCommits]] fails due to any reason. */
class CommitCoordinatorGetCommitsFailedException(cause: Throwable) extends Exception(cause)

object LogSegment {

  def apply(
      logPath: Path,
      version: Long,
      deltas: Seq[FileStatus],
      checkpointProviderOpt: Option[UninitializedCheckpointProvider],
      lastCommitTimestamp: Long): LogSegment = {
    val checkpointProvider = checkpointProviderOpt.getOrElse(EmptyCheckpointProvider)
    LogSegment(logPath, version, deltas, checkpointProvider, lastCommitTimestamp)
  }

  /** The LogSegment for an empty transaction log directory. */
  def empty(path: Path): LogSegment = LogSegment(
    logPath = path,
    version = -1L,
    deltas = Nil,
    checkpointProviderOpt = None,
    lastCommitTimestamp = -1L)
}
