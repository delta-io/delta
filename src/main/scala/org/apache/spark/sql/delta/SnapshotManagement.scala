/*
 * Copyright 2019 Databricks, Inc.
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
import scala.util.{Failure, Success, Try}

import com.databricks.spark.util.TagDefinitions.TAG_ASYNC
import org.apache.spark.sql.delta.actions.{Metadata, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.ThreadUtils

trait SnapshotManagement { self: DeltaLog =>

  /** The timestamp when the last successful update action is finished. */
  @volatile protected var lastUpdateTimestamp: Long = -1L

  @volatile protected var currentSnapshot: Snapshot = getInitialSnapshot

  protected def getFileIndices(
      startingCheckpoint: Option[CheckpointMetaData]): Option[LogReplayInfo] = {
    getFileIndices(startingCheckpoint.map(c => CheckpointInstance(c.version, c.parts)))
  }

  /**
   * Get the `Seq[DeltaLogFileIndex]` that will help in computing the Snapshot of the table.
   * This will require listing the transaction log directory.
   */
  protected def getFileIndices(
      startingCheckpoint: Option[CheckpointInstance],
      versionToLoad: Option[Long] = None): Option[LogReplayInfo] = {
    if (startingCheckpoint.isEmpty) {
      val deltas = store.listFrom(deltaFile(logPath, 0))
        .filter(f => isDeltaFile(f.getPath))
        .takeWhile(f => versionToLoad.forall(v => deltaVersion(f.getPath) <= v))
        .toArray

      if (deltas.isEmpty) {
        return None
      }
      val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
      verifyDeltaVersions(deltaVersions)
      require(deltaVersions.head == 0L,
        s"Did not get the first delta file version: 0 to compute Snapshot")
      versionToLoad.foreach { version =>
        require(deltaVersions.last == version,
          s"Did not get the last delta file version: $version to compute Snapshot")
      }
      val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
      val lastCommitTimestamp = deltas.lastOption.map(_.getModificationTime)

      Some(LogReplayInfo(deltaVersions.last, deltaIndex, None, None, lastCommitTimestamp))
    } else {
      val checkpointFiles = startingCheckpoint.map(_.getCorrespondingFiles(logPath)).get
      val chkVersion = startingCheckpoint.get.version
      val deltas = store.listFrom(deltaFile(logPath, chkVersion + 1))
        .filter(f => isDeltaFile(f.getPath))
        .takeWhile(f => versionToLoad.forall(v => deltaVersion(f.getPath) <= v))
        .toArray
      val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
      verifyDeltaVersions(deltaVersions)
      val newVersion = deltaVersions.lastOption.getOrElse(chkVersion)
      if (deltaVersions.nonEmpty) {
        require(deltaVersions.head == chkVersion + 1,
          s"Did not get the first delta file version: ${chkVersion + 1} to compute Snapshot")
        versionToLoad.foreach { version =>
          require(deltaVersions.last == version,
            s"Did not get the last delta file version: $version to compute Snapshot")
        }
      }

      val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
      val chkIndex = try {
        DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, fs, checkpointFiles)
      } catch {
        case e: FileNotFoundException
            if Option(e.getMessage).exists(_.contains("parquet does not exist")) =>
          recordDeltaEvent(this, "delta.checkpoint.error.partial")
          throw DeltaErrors.missingPartFilesException(chkVersion, e)
        case e: AnalysisException
            if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
          recordDeltaEvent(this, "delta.checkpoint.error.partial")
          throw DeltaErrors.missingPartFilesException(chkVersion, e)
      }

      val lastCommitTimestamp = deltas.lastOption.map(_.getModificationTime)
      Some(LogReplayInfo(
        newVersion, deltaIndex, Some(chkIndex), Some(chkVersion), lastCommitTimestamp))
    }
  }

  protected def getInitialSnapshot: Snapshot = Try(getFileIndices(lastCheckpoint)) match {
    case Success(Some(info)) =>
      val startCheckpoint = info.checkpointVersion.map(v => s" starting from checkpoint $v")
      logInfo(s"Loading version ${info.version}$startCheckpoint")
      val snapshot = createSnapshot(
        info.version,
        None,
        info.checkpoint.toSeq :+ info.deltas,
        minFileRetentionTimestamp,
        // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
        // empty. The next "update" call will take care of that if there are delta files.
        info.lastCommitTimestamp.getOrElse(-1L),
        1)

      lastUpdateTimestamp = clock.getTimeMillis()
      snapshot
    case Success(None) =>
      new InitialSnapshot(logPath, this, Metadata())
    case Failure(_: FileNotFoundException) =>
      // The log directory may not exist
      new InitialSnapshot(logPath, this, Metadata())
    case Failure(t) => throw t
  }

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = currentSnapshot

  protected def createSnapshot(
      version: Long,
      previousSnapshot: Option[Dataset[SingleAction]],
      files: Seq[DeltaLogFileIndex],
      minFileRetentionTimestamp: Long,
      timestamp: Long,
      lineageLength: Int): Snapshot = {
    val checksumOpt = readChecksum(version)
    new Snapshot(
      logPath,
      version,
      previousSnapshot,
      files,
      minFileRetentionTimestamp,
      this,
      timestamp,
      checksumOpt,
      lineageLength)
  }

  /** Checks if the snapshot of the table has surpassed our allowed staleness. */
  private def isSnapshotStale: Boolean = {
    val stalenessLimit = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      clock.getTimeMillis() - lastUpdateTimestamp >= stalenessLimit
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
   */
  def update(stalenessAcceptable: Boolean = false): Snapshot = {
    val doAsync = stalenessAcceptable && !isSnapshotStale
    if (!doAsync) {
      lockInterruptibly {
        updateInternal(isAsync = false)
      }
    } else {
      if (asyncUpdateTask == null || asyncUpdateTask.isCompleted) {
        val jobGroup = spark.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
        asyncUpdateTask = Future[Unit] {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
          spark.sparkContext.setJobGroup(
            jobGroup,
            s"Updating state of Delta table at ${currentSnapshot.path}",
            interruptOnCancel = true)
          tryUpdate(isAsync = true)
        }(SnapshotManagement.deltaLogAsyncUpdateThreadPool)
      }
      currentSnapshot
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  private def tryUpdate(isAsync: Boolean = false): Snapshot = {
    if (deltaLogLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        deltaLogLock.unlock()
      }
    } else {
      currentSnapshot
    }
  }

  /**
   * Gets the log replay information required to create a snapshot in order to update the Delta
   * table. This needs to be called while holding the `deltaLogLock`.
   */
  protected def getFilesForUpdate: Option[LogReplayInfo] = {
    val newFiles = store
      // List from the current version since we want to get the checkpoint file for the
      // current version
      .listFrom(checkpointPrefix(logPath, math.max(currentSnapshot.version, 0L)))
      // Pick up checkpoint files not older than the current version and delta files newer
      // than the current version
      .filter { file =>
      isCheckpointFile(file.getPath) ||
        (isDeltaFile(file.getPath) && deltaVersion(file.getPath) > currentSnapshot.version)
    }.toArray

    val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
    if (deltas.isEmpty) {
      return None
    }

    val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
    verifyDeltaVersions(deltaVersions)
    val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
      .getOrElse(CheckpointInstance.MaxValue)
    val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
    val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
    if (newCheckpoint.isDefined) {
      // If there is a new checkpoint, start new lineage there.
      val newCheckpointVersion = newCheckpoint.get.version
      assert(
        newCheckpointVersion >= currentSnapshot.version,
        s"Attempting to load a checkpoint($newCheckpointVersion) " +
          s"older than current version (${currentSnapshot.version})")
      val newCheckpointPaths = newCheckpoint.get.getCorrespondingFiles(logPath).toSet

      val newVersion = deltaVersions.last

      val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
      val newCheckpointFiles = checkpoints.filter(f => newCheckpointPaths.contains(f.getPath))
      val checkpointIndex =
        DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, newCheckpointFiles)

      Some(LogReplayInfo(
        newVersion,
        deltaIndex,
        Some(checkpointIndex),
        newCheckpoint.map(_.version),
        deltas.lastOption.map(_.getModificationTime)))
    } else {
      assert(currentSnapshot.version + 1 == deltaVersions.head,
        s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
      val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
      val latestCommit = deltas.last
      Some(LogReplayInfo(
        deltaVersion(latestCommit.getPath), // deltas is not empty, so can call .last
        deltaIndex,
        None,
        None,
        Some(latestCommit.getModificationTime)))
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  protected def updateInternal(isAsync: Boolean): Snapshot =
    recordDeltaOperation(this, "delta.log.update", Map(TAG_ASYNC -> isAsync.toString)) {
      withStatusCode("DELTA", "Updating the Delta table's state") {
        try {
          val fileInfoOpt = getFilesForUpdate
          if (fileInfoOpt.isEmpty) {
            lastUpdateTimestamp = clock.getTimeMillis()
            return currentSnapshot
          }
          val fileInfo = fileInfoOpt.get

          val newSnapshot = if (fileInfo.checkpoint.isDefined) {
            logInfo(s"Loading version ${fileInfo.version} starting from " +
              s"checkpoint ${fileInfo.checkpointVersion.get}")

            createSnapshot(
              fileInfo.version,
              None,
              fileInfo.checkpoint.toSeq :+ fileInfo.deltas,
              minFileRetentionTimestamp,
              fileInfo.lastCommitTimestamp.get, // guaranteed to be non-empty
              1)
          } else {
            // If there is no new checkpoint, just apply the deltas to the existing state.
            if (currentSnapshot.lineageLength >= maxSnapshotLineageLength) {
              // Load Snapshot from scratch to avoid StackOverflowError
              getSnapshotAt(fileInfo.version, fileInfo.lastCommitTimestamp)
            } else {
              createSnapshot(
                fileInfo.version,
                Some(currentSnapshot.state),
                fileInfo.deltas :: Nil,
                minFileRetentionTimestamp,
                fileInfo.lastCommitTimestamp.get, // guaranteed to be non-empty
                lineageLength = currentSnapshot.lineageLength + 1)
            }
          }
          currentSnapshot.uncache()
          currentSnapshot = newSnapshot
        } catch {
          case f: FileNotFoundException =>
            val message = s"No delta log found for the Delta table at $logPath"
            logInfo(message)
            // When the state is empty, this is expected. The log will be lazily created when
            // needed. When the state is not empty, it's a real issue and we can't continue
            // to execution.
            if (currentSnapshot.version != -1) {
              val e = new FileNotFoundException(message)
              e.setStackTrace(f.getStackTrace)
              throw e
            }
        }
        lastUpdateTimestamp = clock.getTimeMillis()
        currentSnapshot
      }
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
    val lastCheckpoint = lastCheckpointHint.collect { case ci if ci.version <= version => ci }
      .orElse(findLastCompleteCheckpoint(CheckpointInstance(version, None)))
    val infoOpt = getFileIndices(lastCheckpoint, Some(version))
    if (infoOpt.isEmpty) {
      throw DeltaErrors.logFileNotFoundException(deltaFile(logPath, 0L), 0L, metadata)
    }
    val info = infoOpt.get

    createSnapshot(
      version,
      None,
      info.checkpoint.toSeq :+ info.deltas,
      minFileRetentionTimestamp,
      info.lastCommitTimestamp.getOrElse(-1L),
      1)
  }

  /**
   * Verify the versions are contiguous.
   */
  protected def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty &&
      (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw new IllegalStateException(s"versions ($deltaVersions) are not contiguous")
    }
  }

  /**
   * Provides information around which files in the transaction log need to be read to create
   * the given version of the log.
   * @param version The Snapshot version to generate
   * @param deltas The delta files to read
   * @param checkpoint The checkpoint file to read
   * @param checkpointVersion The checkpoint version used to start replay
   */
  protected case class LogReplayInfo(
      version: Long,
      deltas: DeltaLogFileIndex,
      checkpoint: Option[DeltaLogFileIndex],
      checkpointVersion: Option[Long],
      lastCommitTimestamp: Option[Long])
}

object SnapshotManagement {
  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }
}
