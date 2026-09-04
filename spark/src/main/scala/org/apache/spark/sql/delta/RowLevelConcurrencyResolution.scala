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

import java.util.UUID

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.util.ThreadUtils

/**
 * Row-level concurrency resolution for the [[ConflictChecker]]: instead of aborting a concurrent
 * DV-based DELETE/UPDATE that touches the same physical files as the winning transaction, MERGE the
 * two transactions' deletion vectors when they deleted disjoint rows.
 *
 * Mixed into [[ConflictChecker]] as a self-typed trait: it reads and rewrites the checker's
 * transaction state (`currentTransactionInfo`, `winningCommitSummary`, `deltaLog`, `spark`)
 * directly, and lives in its own file so ConflictChecker stays focused on file-level conflict
 * detection. The two deletion-vector helpers are `protected` so the OPTIMIZE-vs-DML reconciliation
 * (which mixes into the same checker) can reuse them.
 */
trait RowLevelConcurrencyResolution extends DeltaLogging { self: ConflictChecker =>

  /** Whether row-level concurrency resolution is enabled and applicable to this table. */
  protected lazy val rowLevelConcurrencyEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED) &&
      DeletionVectorUtils.deletionVectorsWritable(
        currentTransactionInfo.protocol, currentTransactionInfo.metadata)

  /** The operation name of the winning commit, if available. */
  protected lazy val winningOperationName: Option[String] =
    winningCommitSummary.commitInfo.map(_.operation)

  /**
   * Resolves "same physical file" conflicts with the winning transaction at the row level.
   *
   * A DV-based DELETE/UPDATE emits, for each touched file `P`, a `RemoveFile(P)` (tombstone of the
   * pre-image) and an `AddFile(P)` carrying a larger deletion vector. When both the winning and the
   * current transaction touch the same file `P` this way, the two operations are logically
   * independent as long as they mark *different* rows deleted. For every such shared file we:
   *   1. decode the winning DV, the current DV and their common base DV (from the pre-image
   *      `RemoveFile`) as [[RoaringBitmapArray]]s;
   *   2. check whether the newly-deleted rows are disjoint, i.e. `(dv_win INTERSECT dv_cur) MINUS
   *      dv_base` is empty. If they overlap, this is a genuine row-level conflict and we leave the
   *      file for the standard checks to abort;
   *   3. on disjoint sets, merge the DVs (`dv_win UNION dv_cur`), persist a new DV file,
   *      and rebase the current transaction onto the winner's post-image: the current `AddFile(P)`
   *      now carries the merged DV and the current `RemoveFile(P)` now tombstones the winner's
   *      `AddFile(P)`.
   *
   * Worked example: file `P` holds rows 0..99, undeleted at the current txn's read time
   * (`dv_base = {}`). The winner commits `DELETE WHERE id IN (5, 10)` so `dv_win = {5, 10}`; the
   * current txn holds `DELETE WHERE id IN (20, 21)` so `dv_cur = {20, 21}`. The overlap is
   * `({5,10} INTERSECT {20,21}) MINUS {} = {}` -> disjoint, so we merge to `dv = {5, 10, 20, 21}`,
   * point the current `AddFile(P)` at it, and tombstone the winner's `AddFile(P)`; both deletes
   * survive. Had the current txn instead deleted row 5 (`dv_cur = {5, 21}`), the overlap would be
   * `{5}` -> the same row was deleted twice concurrently, a genuine conflict left for the standard
   * checks to abort. (For a 3+ way chain `dv_base` is the previous winner's DV rather than the
   * original pre-image, which keeps the overlap test conservative and the merge correct.)
   *
   * Resolved paths are recorded in [[rowLevelResolvedPaths]] and skipped by the file-level delete
   * and append checks. Row identity is preserved for free: the merged file is the same physical
   * file, so its base row ID is unchanged and [[reassignOverlappingRowIds]] (already run) leaves it
   * alone. Deletion vectors index physical row positions within one immutable Parquet file, so the
   * merge needs no row tracking.
   */
  protected def resolveRowLevelConflicts(): Unit = {
    if (!rowLevelConcurrencyEnabled) return

    // Winning transaction's DV updates: path present in both an AddFile (with a DV) and a
    // RemoveFile of the winning commit.
    val winningRemovedPaths = winningCommitSummary.removedFiles.map(_.path).toSet
    val winningDvUpdates: Map[String, AddFile] = winningCommitSummary.addedFiles.iterator
      .filter(a => a.deletionVector != null && winningRemovedPaths.contains(a.path))
      .map(a => a.path -> a)
      .toMap
    if (winningDvUpdates.isEmpty) return

    // Current transaction's DV updates and file removes, indexed by path. Built in a single pass
    // over the actions (last-writer-wins per path, matching the prior `.collect{}.toMap`).
    val currentAddByPath = mutable.Map.empty[String, AddFile]
    val currentRemoveByPath = mutable.Map.empty[String, RemoveFile]
    currentTransactionInfo.actions.foreach {
      case a: AddFile if a.deletionVector != null => currentAddByPath(a.path) = a
      case r: RemoveFile => currentRemoveByPath(r.path) = r
      case _ =>
    }

    val sharedPaths = winningDvUpdates.keySet
      .intersect(currentAddByPath.keySet)
      .intersect(currentRemoveByPath.keySet)
    if (sharedPaths.isEmpty) return

    recordTime("resolved-row-level-conflicts") {
      val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
      val tablePath = deltaLog.dataPath

      // Reconcile each shared file's DVs independently; the work is driver-side object-store DV
      // I/O (read + merge + write). When many files conflict, parallelize across a bounded pool
      // to shorten the conflict window; a single file stays on the caller thread.
      def reconcileOnePath(path: String): Option[(String, (AddFile, RemoveFile))] =
        try {
          reconcileFileDeletionVectors(
              dvStore, tablePath,
              winningDvUpdates(path), currentAddByPath(path), currentRemoveByPath(path))
            .map(path -> _)
        } catch {
          case NonFatal(e) =>
            // Fail safe: DV decode/merge/write is a pure optimization over the conservative
            // default. If it fails for this file (unreadable/corrupt DV, transient I/O), skip
            // row-level resolution for it so the standard file-level checks abort cleanly with a
            // retryable Concurrent* exception instead of surfacing an unexpected error out of
            // conflict detection. Other shared files are still resolved independently.
            logWarning(log"Row-level concurrency resolution failed for file " +
              log"${MDC(DeltaLogKeys.PATH, path)}; leaving it for the standard conflict checks", e)
            None
        }

      // Bounded driver parallelism (cf. DeltaFileOperations footer reads, which use 8).
      val pathList = sharedPaths.toSeq
      val parallelism = math.min(pathList.size, 8)
      val reconciled =
        if (parallelism <= 1) pathList.flatMap(reconcileOnePath)
        else ThreadUtils.parmap(pathList, "rowLevelConflictResolution", parallelism)(
          reconcileOnePath).flatten

      // Apply per-file results on the caller thread (no shared-state races across the pool).
      val replacements = reconciled.toMap
      if (replacements.nonEmpty) {
        val resolvedPaths = replacements.keySet

        // Rewrite BOTH sides of the conflict so the residual is a plain no-conflict state that the
        // file-level checks (unchanged from upstream) handle. Current txn: rebase each AddFile(P)
        // onto the merged DV and each RemoveFile(P) onto the winner's post-image, and drop P from
        // readFiles (it is no longer "read" for the delete-read check).
        val newActions = currentTransactionInfo.actions.map {
          case a: AddFile if replacements.contains(a.path) => replacements(a.path)._1
          case r: RemoveFile if replacements.contains(r.path) => replacements(r.path)._2
          case other => other
        }
        val newReadFiles =
          currentTransactionInfo.readFiles.filterNot(f => resolvedPaths.contains(f.path))
        currentTransactionInfo =
          currentTransactionInfo.copy(actions = newActions, readFiles = newReadFiles)
        // Winning side: drop the reconciled AddFile(P)/RemoveFile(P) pair from the summary, so no
        // check sees P as a winner-side add or remove.
        winningCommitSummary = pruneReconciledFiles(winningCommitSummary, resolvedPaths)

        recordDeltaEvent(
          deltaLog,
          opType = "delta.rowLevelConcurrency.deletionVectorsMerged",
          data = Map(
            "winningCommitVersion" -> winningCommitVersion,
            "resolvedPaths" -> resolvedPaths.size,
            "winningOperation" -> winningOperationName.getOrElse("UNKNOWN")))
      }
    }
  }

  /**
   * Returns a copy of `summary` with the reconciled files removed: for each resolved path we drop
   * the winner's `AddFile(P)` (its deletion vector was folded into the current transaction's merged
   * DV) and its `RemoveFile(P)` (tombstone of the shared pre-image). Rebuilding from the filtered
   * action list recomputes the derived views (`addedFiles`, `removedFiles`,
   * `changedDataAddedFiles`, ...), so the file-level conflict checks see a winner that never
   * touched these files.
   *
   * Only the same-path reconciled pair is pruned. A rewrite-only DML winner's *new image* files (an
   * UPDATE writes updated row values to a fresh path) are left in the summary and arbitrated by the
   * standard added-files check: an UPDATE can move a row *into* the loser's predicate (winner
   * `SET x = 15`, loser `DELETE WHERE x > 10`, row was `x = 5`), a genuine write-skew the DV union
   * cannot detect. All non-file actions (protocol, metadata, domain metadata) are preserved.
   */
  private def pruneReconciledFiles(
      summary: WinningCommitSummary,
      resolvedPaths: Set[String]): WinningCommitSummary = {
    val prunedActions = summary.actions.filterNot {
      case a: AddFile => resolvedPaths.contains(a.path)
      case r: RemoveFile => resolvedPaths.contains(r.path)
      case _ => false
    }
    new WinningCommitSummary(prunedActions, summary.fileStatus, summary.readTimeMs)
  }

  /**
   * Reconciles the winning and current transactions' deletion vectors for a single shared file `P`.
   * Returns the rebased `(AddFile, RemoveFile)` to substitute into the current transaction when the
   * two transactions deleted disjoint rows, or `None` when they overlap (a genuine row-level
   * conflict, left for the standard checks). All deletion-vector I/O for `P` happens here, so its
   * caller can wrap it in a fail-safe boundary.
   */
  private def reconcileFileDeletionVectors(
      dvStore: DeletionVectorStore,
      tablePath: Path,
      winningAdd: AddFile,
      currentAdd: AddFile,
      currentRemove: RemoveFile): Option[(AddFile, RemoveFile)] = {
    def bitmapOf(dv: DeletionVectorDescriptor): RoaringBitmapArray =
      readDeletionVectorOrEmpty(dvStore, dv, tablePath)
    val baseBitmap = bitmapOf(currentRemove.deletionVector)
    val winningBitmap = bitmapOf(winningAdd.deletionVector)
    val currentBitmap = bitmapOf(currentAdd.deletionVector)

    // `baseBitmap` is the DV of `P` at the current txn's read time (carried on its RemoveFile).
    // Both `dv_win` and `dv_cur` are supersets of it (a DV only grows), so each side's *newly*
    // deleted rows are `dv \ base`, and their overlap is `(dv_win INTERSECT dv_cur) MINUS base`.
    // Empty overlap => the two txns deleted disjoint rows, and `current ; winner` is a valid
    // serialization under both WriteSerializable and Serializable (each deleted only rows the
    // other did not touch), so the DV union is safe. Non-empty => the same row was deleted
    // concurrently: a genuine conflict, left for the standard checks to abort.
    //
    // N-way: the current txn reconciles against a chain of winners, once per winning commit.
    // Induction on the number of prior winners `k` already merged into the current txn:
    //   k = 0 (first winner): `base` is P's read-time DV, so `dv_cur \ base` and `dv_win \ base`
    //     are exactly the two txns' own new deletes -- the test is precise.
    //   k -> k+1: merging winner k rebased the current RemoveFile onto winner k's AddFile (so now
    //     `base = dv_win_k`) and the current AddFile onto the merged DV. Winner k+1 committed on
    //     top of winner k, so `dv_win_k` is a subset of `dv_win_{k+1}`. The test
    //     `(dv_win_{k+1} INTERSECT dv_cur) MINUS dv_win_k` then reduces to winner k+1's *own* new
    //     deletes intersected with the current txn's own new deletes -- prior winners cancel out.
    // Every step compares only the two operations' genuinely new rows, and the merged DV is a
    // union of disjoint contributions, so the result is independent of winner order.
    val newlyDeletedOverlap = winningBitmap.copy()
    newlyDeletedOverlap.and(currentBitmap)
    newlyDeletedOverlap.andNot(baseBitmap)

    if (!newlyDeletedOverlap.isEmpty) {
      // Overlapping row-level modification -> genuine conflict, leave for the standard checks.
      None
    } else {
      // Disjoint: merge the deletion vectors and rebase onto the winner's post-image.
      val mergedBitmap = winningBitmap.copy()
      mergedBitmap.merge(currentBitmap)
      val mergedDescriptor = writeMergedDeletionVector(dvStore, tablePath, mergedBitmap)
      // Keep the current AddFile's identity (base row ID / default row commit version already
      // reconciled by the row-ID phases) but point it at the merged DV.
      val rebasedAdd = currentAdd
        .copy(deletionVector = mergedDescriptor, dataChange = true)
        .withoutTightBoundStats
      // Tombstone the winner's now-live AddFile (carries the winning DV) instead of the stale
      // pre-image.
      val rebasedRemove = winningAdd.removeWithTimestamp()
      Some((rebasedAdd, rebasedRemove))
    }
  }

  /** Reads a deletion vector into a [[RoaringBitmapArray]], returning an empty bitmap for none. */
  protected def readDeletionVectorOrEmpty(
      dvStore: DeletionVectorStore,
      dv: DeletionVectorDescriptor,
      tablePath: Path): RoaringBitmapArray = {
    if (dv == null || dv.isEmpty) new RoaringBitmapArray() else dvStore.read(dv, tablePath)
  }

  /**
   * Persists a merged bitmap to a new deletion vector file and returns its descriptor.
   *
   * NOTE: this writes a DV file as a side effect of conflict resolution. If the commit ultimately
   * fails or is retried against another winning version, the file is orphaned and later reclaimed
   * by VACUUM (same lifecycle as any DV written by DML). This mirrors how the DML write path
   * persists DVs (see `DeletionVectorWriter.storeSerializedBitmap`).
   */
  protected def writeMergedDeletionVector(
      dvStore: DeletionVectorStore,
      tablePath: Path,
      bitmap: RoaringBitmapArray): DeletionVectorDescriptor = {
    // An empty DV has no on-disk representation (matches DeletionVectorWriter).
    if (bitmap.isEmpty) return DeletionVectorDescriptor.EMPTY
    val tablePathWithFs = dvStore.pathWithFileSystem(tablePath)
    val fileId = UUID.randomUUID()
    val writer = dvStore.createWriter(dvStore.generateFileNameInTable(tablePathWithFs, fileId))
    try {
      val serialized = DeletionVectorUtils.serialize(
        bitmap, RoaringBitmapArrayFormat.Portable, Some(tablePath))
      val range = writer.write(serialized)
      DeletionVectorDescriptor.onDiskWithRelativePath(
        id = fileId,
        sizeInBytes = serialized.length,
        cardinality = bitmap.cardinality,
        offset = Some(range.offset))
    } finally {
      writer.close()
    }
  }
}
