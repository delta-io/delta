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

import com.databricks.spark.util.TagDefinitions.TAG_ASYNC
import org.apache.spark.sql.delta.actions.{Metadata, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames.{checkpointFileSingular, checkpointFileWithParts, checkpointPrefix, deltaFile, deltaVersion, isCheckpointFile, isDeltaFile}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.ThreadUtils

trait SnapshotManagement { self: DeltaLog =>

  def createSnapshot(
      path: Path,
      version: Long,
      previousSnapshot: Option[Dataset[SingleAction]],
      files: Seq[DeltaLogFileIndex],
      minFileRetentionTimestamp: Long,
      deltaLog: DeltaLog,
      timestamp: Long,
      lineageLength: Int = 1): Snapshot =
    new Snapshot(
      path,
      version,
      previousSnapshot,
      files,
      minFileRetentionTimestamp,
      deltaLog,
      timestamp,
      lineageLength)

  /** The timestamp when the last successful update action is finished. */
  @volatile private var lastUpdateTimestamp = -1L

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = currentSnapshot

  @volatile protected var currentSnapshot: Snapshot = lastCheckpoint.map { c =>
    val checkpointFiles = c.parts
      .map(p => checkpointFileWithParts(logPath, c.version, p))
      .getOrElse(Seq(checkpointFileSingular(logPath, c.version)))
    val deltas = store.listFrom(deltaFile(logPath, c.version + 1))
      .filter(f => isDeltaFile(f.getPath))
      .toArray
    val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
    verifyDeltaVersions(deltaVersions)
    val newVersion = deltaVersions.lastOption.getOrElse(c.version)
    logInfo(s"Loading version $newVersion starting from checkpoint ${c.version}")
    try {
      val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
      val checkpointIndex = DeltaLogFileIndex(
        DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, fs, checkpointFiles)
      val snapshot = createSnapshot(
        logPath,
        newVersion,
        None,
        checkpointIndex :: deltaIndex :: Nil,
        minFileRetentionTimestamp,
        this,
        // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
        // empty. The next "update" call will take care of that if there are delta files.
        deltas.lastOption.map(_.getModificationTime).getOrElse(-1L))

      validateChecksum(snapshot)
      lastUpdateTimestamp = clock.getTimeMillis()
      snapshot
    } catch {
      case e: FileNotFoundException
        if Option(e.getMessage).exists(_.contains("parquet does not exist")) =>
        recordDeltaEvent(this, "delta.checkpoint.error.partial")
        throw DeltaErrors.missingPartFilesException(c, e)
      case e: AnalysisException if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
        recordDeltaEvent(this, "delta.checkpoint.error.partial")
        throw DeltaErrors.missingPartFilesException(c, e)
    }
  }.getOrElse {
    new InitialSnapshot(logPath, this, Metadata())
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  protected def updateInternal(isAsync: Boolean): Snapshot =
    recordDeltaOperation(this, "delta.log.update", Map(TAG_ASYNC -> isAsync.toString)) {
      withStatusCode("DELTA", "Updating the Delta table's state") {
        try {
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
            lastUpdateTimestamp = clock.getTimeMillis()
            return currentSnapshot
          }

          val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
          verifyDeltaVersions(deltaVersions)
          val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
            .getOrElse(CheckpointInstance.MaxValue)
          val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
          val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
          val newSnapshot = if (newCheckpoint.isDefined) {
            // If there is a new checkpoint, start new lineage there.
            val newCheckpointVersion = newCheckpoint.get.version
            assert(
              newCheckpointVersion >= currentSnapshot.version,
              s"Attempting to load a checkpoint($newCheckpointVersion) " +
                s"older than current version (${currentSnapshot.version})")
            val newCheckpointFiles = newCheckpoint.get.getCorrespondingFiles(logPath)

            val newVersion = deltaVersions.last

            val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
            val checkpointIndex =
              DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, fs, newCheckpointFiles)

            logInfo(s"Loading version $newVersion starting from checkpoint $newCheckpointVersion")

            createSnapshot(
              logPath,
              newVersion,
              None,
              checkpointIndex :: deltaIndex :: Nil,
              minFileRetentionTimestamp,
              this,
              deltas.last.getModificationTime)
          } else {
            // If there is no new checkpoint, just apply the deltas to the existing state.
            assert(currentSnapshot.version + 1 == deltaVersions.head,
              s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
            if (currentSnapshot.lineageLength >= maxSnapshotLineageLength) {
              // Load Snapshot from scratch to avoid StackOverflowError
              getSnapshotAt(deltaVersions.last, Some(deltas.last.getModificationTime))
            } else {
              val deltaIndex = DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, deltas)
              createSnapshot(
                logPath,
                deltaVersions.last,
                Some(currentSnapshot.state),
                deltaIndex :: Nil,
                minFileRetentionTimestamp,
                this,
                deltas.last.getModificationTime,
                lineageLength = currentSnapshot.lineageLength + 1)
            }
          }
          validateChecksum(newSnapshot)
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
}

object SnapshotManagement {
  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }
}
