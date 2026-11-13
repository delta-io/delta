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

package org.apache.spark.sql.delta.commands.backfill

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.amt.AMTUtils
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions.col

trait BackfillExecutor extends DeltaLogging {
  def spark: SparkSession
  def deltaLog: DeltaLog
  def catalogTableOpt: Option[CatalogTable]
  def backfillTxnId: String
  def backfillStats: BackfillCommandStats

  def backFillBatchOpType: String
  def filesToBackfill(snapshot: Snapshot): Dataset[AddFile]
  def constructBatch(files: Seq[AddFile]): BackfillBatch

  /**
   * Whether the batches can be planned from a single scan of the table, see
   * [[executeBatchesFromSingleScan]]. This is only correct if no transaction that commits after
   * the scan can add a file that [[filesToBackfill]] would return.
   */
  protected def canPlanBatchesFromSingleScan: Boolean = false

  /**
   * Execute the command by consuming a sequence of [[BackfillBatch]].
   * Returns an option with the last commit version when available. Otherwise, it returns None.
   */
  def run(maxNumFilesPerCommit: Int): Option[Long] = {
    executeBackfillBatches(maxNumFilesPerCommit)
  }

  /** Counters shared by all batches of one run. */
  private class BackfillProgress {
    val observer: BackfillExecutionObserver = BackfillExecutionObserver.getObserver
    val numSuccessfulBatch = new AtomicInteger(0)
    val numFailedBatch = new AtomicInteger(0)
    var batchId = 0
    var lastCommitOpt: Option[Long] = None
  }

  /**
   * Execute all available [[BackfillBatch]].
   * Returns an option with the last commit version when available. Otherwise, it returns None.
   *
   * Note, in the case of competing concurrent transactions, this method will exit after processing
   * a maximum of `backfill.maxNumFilesFactor` times the total number of files in the table.
   */
  private def executeBackfillBatches(maxNumFilesPerCommit: Int): Option[Long] = {
    val progress = new BackfillProgress
    try {
      if (canPlanBatchesFromSingleScan &&
          spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_SINGLE_SCAN_ENABLED)) {
        executeBatchesFromSingleScan(maxNumFilesPerCommit, progress)
      }
      // Without a single scan, this backfills the whole table. After a single scan, this finds
      // nothing to do unless a file that needs a backfill was added after the scan.
      executeBatchesScanningBeforeEachBatch(maxNumFilesPerCommit, progress)
    } finally {
      backfillStats.numSuccessfulBatches = progress.numSuccessfulBatch.get()
      backfillStats.numFailedBatches = progress.numFailedBatch.get()
    }
  }

  /**
   * Scans the latest snapshot for files to backfill before each batch, and commits up to
   * `maxNumFilesPerCommit` of them. Stops when a scan returns fewer files than that.
   */
  private def executeBatchesScanningBeforeEachBatch(
      maxNumFilesPerCommit: Int,
      progress: BackfillProgress): Option[Long] = {
    val totalFileCount = deltaLog.update(catalogTableOpt = catalogTableOpt).numOfFiles
    val factor = spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_FACTOR)
    val maxFilesToProcess = totalFileCount * factor

    var totalFilesProcessed = 0L
    var filesInBatch = Seq.empty[AddFile]

    // If the last batch contained fewer files than the maxNumFilesPerCommit we exit.
    // This protects against live-locking with fast concurrent txns that only commit
    // a few files.
    // Having excluded this option the backfill can only live-lock with an equally fast
    // concurrent txn, i.e a competing un-backfill that only commits logs files.
    // To protect against this we set a maximum backfill limit equal to a factor of the
    // total table file count.
    def moreFilesToProcess(): Boolean = {
      filesInBatch.length == maxNumFilesPerCommit && totalFilesProcessed < maxFilesToProcess
    }

    do {
      val snapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
      filesInBatch = filesToBackfill(snapshot).limit(maxNumFilesPerCommit).collect()
      if (filesInBatch.isEmpty) {
        return progress.lastCommitOpt
      }
      commitBatch(snapshot, filesInBatch, progress, skipPostCommitCheckpoint = false)
      totalFilesProcessed += filesInBatch.length
    } while (moreFilesToProcess())

    if (totalFilesProcessed >= maxFilesToProcess) {
      recordDeltaEvent(
        deltaLog,
        opType = "delta.backfillExceededMaxFilesToProcess",
        data = Map("maxFilesProcessed" -> maxFilesToProcess))
    }
    progress.lastCommitOpt
  }

  /**
   * Finds the files to backfill with a single scan of the table, and commits them in batches of
   * up to `maxNumFilesPerCommit` files. Does nothing if the scan finds no more files than fit in
   * one batch: [[executeBatchesScanningBeforeEachBatch]] commits them without another scan.
   *
   * Scanning before each batch costs a state reconstruction of the whole table per batch, which
   * makes backfilling a table with many files quadratic in its number of files. Instead, this
   * scans once and keeps the result up to date by itself:
   *  - Each batch reads the snapshot of the previous backfill commit, so its conflict checking
   *    only covers the transactions that committed since then, never earlier backfill batches.
   *    The conflict resolution drops the files those transactions removed or re-added from the
   *    batch (see `ConflictChecker.resolveRowTrackingBackfillConflicts`).
   *  - The paths those transactions touched are also dropped from all later batches. Without
   *    this, a later batch could re-add a file that has been removed, or overwrite a newer
   *    version of the file (for example one with a deletion vector).
   *  - No commit after the scan adds a file that needs a backfill (see
   *    [[canPlanBatchesFromSingleScan]]), so the scan finds every file.
   *
   * The commits of the backfill do not write checkpoints themselves. Instead, the backfill writes
   * one after every `backfill.singleScan.checkpointInterval` commits and when it finishes, so that
   * a checkpoint does not also cost a state reconstruction for most batches.
   */
  private def executeBatchesFromSingleScan(
      maxNumFilesPerCommit: Int,
      progress: BackfillProgress): Unit = {
    val initialSnapshot = deltaLog.update(catalogTableOpt = catalogTableOpt)
    val candidates = filesToBackfill(initialSnapshot)
    val numCandidates = candidates.count()
    if (numCandidates <= maxNumFilesPerCommit) {
      return
    }

    // Range partition by path so that the driver only holds about one batch of files at a time,
    // and the batches are in path order like the ones planned by scanning before each batch.
    val numBatches = math.ceil(numCandidates.toDouble / maxNumFilesPerCommit).toInt
    val plannedBatches = candidates
      .repartitionByRange(numBatches, col("path"))
      .sortWithinPartitions("path")
      .toLocalIterator()
      .asScala
      .grouped(maxNumFilesPerCommit)

    // AMT tables keep their own checkpoint cadence.
    val writeOwnCheckpoints = !AMTUtils.amtEnabled(initialSnapshot)
    val checkpointInterval =
      spark.conf.get(DeltaSQLConf.DELTA_BACKFILL_SINGLE_SCAN_CHECKPOINT_INTERVAL)
        .getOrElse(deltaLog.checkpointInterval(initialSnapshot.metadata))
    var numCommitsSinceCheckpoint = 0

    // Paths of the files that concurrent transactions removed or re-added after the scan, in the
    // same canonical form as the paths in the snapshot state.
    val canonicalizePath = deltaLog.getCanonicalPathFunction(runsOnExecutors = false)
    val pathsTouchedConcurrently = mutable.HashSet.empty[String]
    // All concurrent commits up to this version are in `pathsTouchedConcurrently`.
    var concurrentCommitsCheckedUpTo = initialSnapshot.version

    def recordConcurrentCommits(upToVersion: Long): Unit = {
      if (upToVersion > concurrentCommitsCheckedUpTo) {
        deltaLog
          .getChanges(
            concurrentCommitsCheckedUpTo + 1,
            upToVersion,
            catalogTableOpt,
            failOnDataLoss = true)
          .foreach { case (_, actions) =>
            actions.foreach {
              case f: FileAction => pathsTouchedConcurrently += canonicalizePath(f.path)
              case _ =>
            }
          }
        concurrentCommitsCheckedUpTo = upToVersion
      }
    }

    var readSnapshot = initialSnapshot
    plannedBatches.foreach { plannedFiles =>
      // Commits between the previous backfill commit and `readSnapshot` are not conflict
      // checked against the next batch.
      recordConcurrentCommits(readSnapshot.version)
      val filesInBatch = plannedFiles.filterNot(f => pathsTouchedConcurrently.contains(f.path))
      if (filesInBatch.nonEmpty) {
        val (commitVersion, postCommitSnapshot) = commitBatch(
          readSnapshot, filesInBatch, progress, skipPostCommitCheckpoint = writeOwnCheckpoints)
        // The commits between `readSnapshot` and this batch were conflict checked against this
        // batch, but they also touch the files of later batches.
        recordConcurrentCommits(commitVersion - 1)
        concurrentCommitsCheckedUpTo = commitVersion
        readSnapshot = postCommitSnapshot
          .filter(_.version >= commitVersion)
          .getOrElse(deltaLog.getSnapshotAt(commitVersion, catalogTableOpt = catalogTableOpt))

        numCommitsSinceCheckpoint += 1
        if (writeOwnCheckpoints && checkpointInterval > 0 &&
            numCommitsSinceCheckpoint >= checkpointInterval) {
          checkpointBackfillCommit(commitVersion)
          numCommitsSinceCheckpoint = 0
        }
      }
    }

    if (writeOwnCheckpoints && numCommitsSinceCheckpoint > 0) {
      progress.lastCommitOpt.foreach(checkpointBackfillCommit)
    }
  }

  /** Writes a checkpoint at `version`, which must be a commit of this backfill. */
  private def checkpointBackfillCommit(version: Long): Unit = {
    // Like the post-commit checkpoint, only checkpoint the version this backfill committed.
    val snapshotToCheckpoint = deltaLog.getSnapshotAt(version, catalogTableOpt = catalogTableOpt)
    deltaLog.checkpoint(snapshotToCheckpoint, catalogTableOpt)
  }

  /**
   * Commits one batch of `filesInBatch`, reading `snapshot`. Returns the version of the commit
   * and the snapshot after it, when available.
   */
  private def commitBatch(
      snapshot: Snapshot,
      filesInBatch: Seq[AddFile],
      progress: BackfillProgress,
      skipPostCommitCheckpoint: Boolean): (Long, Option[Snapshot]) = {
    val batch = constructBatch(filesInBatch)
    progress.observer.executeBatch {
      val txn = deltaLog.startTransaction(catalogTableOpt, Some(snapshot))
      txn.skipPostCommitCheckpoint = skipPostCommitCheckpoint
      txn.trackFilesRead(filesInBatch)
      val commitVersion = recordDeltaOperation(deltaLog, backFillBatchOpType) {
        batch.execute(spark, backfillTxnId, progress.batchId, txn,
          progress.numSuccessfulBatch, progress.numFailedBatch)
      }
      progress.lastCommitOpt = Some(commitVersion)
      progress.batchId += 1
      (commitVersion, txn.getCommitted.map(_.postCommitSnapshot))
    }
  }
}
