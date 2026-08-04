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

import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.fs.Path

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

  /**
   * Paths of files whose "same physical file" conflict with the winning transaction was resolved at
   * the row level by [[resolveRowLevelConflicts]] (deletion vectors merged). The file-level delete
   * and append checks skip these paths, since they have already been reconciled.
   */
  protected val rowLevelResolvedPaths = mutable.Set.empty[String]

  /** Whether row-level concurrency resolution is enabled and applicable to this table. */
  protected lazy val rowLevelConcurrencyEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED) &&
      DeletionVectorUtils.deletionVectorsWritable(
        currentTransactionInfo.protocol, currentTransactionInfo.metadata)

  /** The operation name of the winning commit, if available. */
  protected lazy val winningOperationName: Option[String] =
    winningCommitSummary.commitInfo.map(_.operation)

  /**
   * Whether a file added by the winning transaction can be skipped in the added-files (append)
   * conflict check thanks to row-level concurrency resolution.
   *
   * This is true only when the file's "same physical file" conflict was already reconciled by
   * merging deletion vectors ([[resolveRowLevelConflicts]] recorded the path in
   * [[rowLevelResolvedPaths]]). In that case the winner's re-added `AddFile(P)` carries the winning
   * DV that we already folded into the current transaction's merged DV, so re-checking it would be
   * a false conflict.
   *
   * We deliberately do NOT skip a rewrite-only DML winner's *new image* files here (an UPDATE
   * writes updated row values to a fresh path). Those are ordinary non-blind changed-data files
   * and can legitimately conflict: e.g. an UPDATE can move a row *into* the loser's predicate
   * (winner `SET x = 15`, loser `DELETE WHERE x > 10`, row was `x = 5`), a genuine write-skew that
   * the DV union cannot detect. They are arbitrated by the standard added-files check (and, when
   * enabled, by conflict-time data skipping over their stats).
   */
  protected def canSkipAddedFileForRowLevelConcurrency(addFile: AddFile): Boolean = {
    if (!rowLevelConcurrencyEnabled) return false
    rowLevelResolvedPaths.contains(addFile.path)
  }

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

    // Current transaction's DV updates, indexed by path.
    val currentAddByPath = currentTransactionInfo.actions.collect {
      case a: AddFile if a.deletionVector != null => a.path -> a
    }.toMap
    val currentRemoveByPath = currentTransactionInfo.actions.collect {
      case r: RemoveFile => r.path -> r
    }.toMap

    val sharedPaths = winningDvUpdates.keySet
      .intersect(currentAddByPath.keySet)
      .intersect(currentRemoveByPath.keySet)
    if (sharedPaths.isEmpty) return

    recordTime("resolved-row-level-conflicts") {
      val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
      val tablePath = deltaLog.dataPath

      // path -> (rebased AddFile, rebased RemoveFile)
      val replacements = mutable.Map.empty[String, (AddFile, RemoveFile)]
      for (path <- sharedPaths) {
        val winningAdd = winningDvUpdates(path)
        val currentAdd = currentAddByPath(path)
        val currentRemove = currentRemoveByPath(path)

        def bitmapOf(dv: DeletionVectorDescriptor): RoaringBitmapArray =
          readDeletionVectorOrEmpty(dvStore, dv, tablePath)
        val baseBitmap = bitmapOf(currentRemove.deletionVector)
        val winningBitmap = bitmapOf(winningAdd.deletionVector)
        val currentBitmap = bitmapOf(currentAdd.deletionVector)

        // `baseBitmap` is the DV of `P` at the current txn's read time (carried on its RemoveFile);
        // for a 2-way conflict it equals the winner's pre-image too. Both `dv_win` and `dv_cur` are
        // supersets of it (a DV only grows), so the newly-deleted rows are `dv \ base` on each side
        // and their overlap is `(dv_win INTERSECT dv_cur) MINUS base`. If empty, the two txns
        // touched disjoint rows and the schedule `current ; winner` is a valid serialization under
        // both WriteSerializable and Serializable (the winner's rewrites/deletes are of rows the
        // current txn did not touch), so merging is safe. If non-empty, the same row was touched by
        // both -> genuine conflict, left for the standard checks. (For 3+ way chains `base` becomes
        // previous winner's DV rather than the original pre-image; the merge stays correct and the
        // overlap test stays conservative.)
        val newlyDeletedOverlap = winningBitmap.copy()
        newlyDeletedOverlap.and(currentBitmap)
        newlyDeletedOverlap.andNot(baseBitmap)

        if (newlyDeletedOverlap.isEmpty) {
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
          replacements(path) = (rebasedAdd, rebasedRemove)
          rowLevelResolvedPaths += path
        }
        // else: overlapping row-level modification -> genuine conflict, leave for standard checks.
      }

      if (replacements.nonEmpty) {
        val newActions = currentTransactionInfo.actions.map {
          case a: AddFile if replacements.contains(a.path) => replacements(a.path)._1
          case r: RemoveFile if replacements.contains(r.path) => replacements(r.path)._2
          case other => other
        }
        // Resolved files are no longer "read" for the purposes of the delete-read check.
        val newReadFiles = currentTransactionInfo.readFiles
          .filterNot(f => rowLevelResolvedPaths.contains(f.path))
        currentTransactionInfo =
          currentTransactionInfo.copy(actions = newActions, readFiles = newReadFiles)

        recordDeltaEvent(
          deltaLog,
          opType = "delta.rowLevelConcurrency.deletionVectorsMerged",
          data = Map(
            "winningCommitVersion" -> winningCommitVersion,
            "resolvedPaths" -> rowLevelResolvedPaths.size,
            "winningOperation" -> winningOperationName.getOrElse("UNKNOWN")))
      }
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
