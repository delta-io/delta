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

import java.io.{Closeable, InterruptedIOException, UncheckedIOException}
import java.net.SocketTimeoutException
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.commands.{DeletionVectorUtils, DeletionVectorWriter}
import org.apache.spark.sql.delta.commands.DeletionVectorWriter.DeletionVectorMapperContext
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, SparkThrowable}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, Or}
import org.apache.spark.sql.functions.{coalesce, col, lit, sum}
import org.apache.spark.util.{ThreadUtils, Utils => SparkUtils}

/**
 * Row-level concurrency resolution for the [[ConflictChecker]]: instead of aborting a concurrent
 * DV-based DELETE/UPDATE that touches the same physical files as the winning transaction, MERGE the
 * two transactions' deletion vectors when they deleted disjoint rows.
 *
 * Mixed into [[ConflictChecker]] as a self-typed trait: it reads and rewrites the checker's
 * transaction state (`currentTransactionInfo`, `winningCommitSummary`, `deltaLog`, `spark`)
 * directly, and lives in its own file so ConflictChecker stays focused on file-level conflict
 * detection.
 */
trait RowLevelConcurrencyResolution extends DeltaLogging { self: ConflictChecker =>
  import RowLevelConcurrencyResolution.MaxParallelFiles

  /** Whether row-level concurrency resolution is enabled and applicable to this table. */
  protected lazy val rowLevelConcurrencyEnabled: Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED) &&
      DeletionVectorUtils.deletionVectorsWritable(
        currentTransactionInfo.protocol, currentTransactionInfo.metadata)

  /** The operation name of the winning commit, if available. */
  protected lazy val winningOperationName: Option[String] =
    winningCommitSummary.commitInfo.map(_.operation)

  /**
   * Resolves "same physical file" conflicts with the winning transaction at the row level, for the
   * files set aside by [[prepareRowLevelConflictResolution]]. Runs once every other check against
   * the winning commit passed, and throws the conflict when any of the files cannot be reconciled.
   *
   * A DV-based DELETE/UPDATE emits, for each touched file `P`, a `RemoveFile(P)` (tombstone of the
   * pre-image) and an `AddFile(P)` carrying a larger deletion vector. When both the winning and the
   * current transaction touch the same file `P` this way, the two operations are logically
   * independent as long as they mark *different* rows deleted. For every such shared file we:
   *   1. decode the winning DV, the current DV and their common base DV (from the pre-image
   *      `RemoveFile`) as [[RoaringBitmapArray]]s;
   *   2. check whether the newly-deleted rows are disjoint, i.e. `(dv_win INTERSECT dv_cur) MINUS
   *      dv_base` is empty. If they overlap, this is a genuine row-level conflict and the commit
   *      aborts;
   *   3. on disjoint sets, merge the DVs (`dv_win UNION dv_cur`), persist them packed into new DV
   *      files, and rebase the current transaction onto the winner's post-image: the current
   *      `AddFile(P)` now carries the merged DV and the current `RemoveFile(P)` now tombstones the
   *      winner's `AddFile(P)`.
   *
   * Worked example: file `P` holds rows 0..99, undeleted at the current txn's read time
   * (`dv_base = {}`). The winner commits `DELETE WHERE id IN (5, 10)` so `dv_win = {5, 10}`; the
   * current txn holds `DELETE WHERE id IN (20, 21)` so `dv_cur = {20, 21}`. The overlap is
   * `({5,10} INTERSECT {20,21}) MINUS {} = {}` -> disjoint, so we merge to `dv = {5, 10, 20, 21}`,
   * point the current `AddFile(P)` at it, and tombstone the winner's `AddFile(P)`; both deletes
   * survive. Had the current txn instead deleted row 5 (`dv_cur = {5, 21}`), the overlap would be
   * `{5}` -> the same row was deleted twice concurrently, a genuine conflict that aborts the
   * commit. (For a 3+ way chain `dv_base` is the previous winner's DV rather than the
   * original pre-image, which keeps the overlap test conservative and the merge correct.)
   *
   * Reconciling `P` hides the winner's deletes in `P` from the delete-read check, so a file is only
   * reconciled when the current transaction's reads are provably unaffected by them: none of the
   * rows the winner newly deleted may match any of the current transaction's read predicates (see
   * [[readVerdictWithoutScan]]). The current transaction then produces the same actions
   * whether it runs before or after the winner, so `winner ; current` -- the commit order -- is a
   * valid serialization. For a DELETE/UPDATE the only read is its own condition, whose matching
   * rows in `P` are exactly the rows it deleted, so the disjointness test above already implies
   * this. A MERGE, or any transaction that also read other rows, can see a winner's delete:
   * `MERGE ... WHEN MATCHED AND cond THEN DELETE WHEN NOT MATCHED THEN INSERT` that matched a row
   * without deleting it must INSERT the source row once the winner deletes that target row.
   *
   * Row identity is preserved for free: the merged file is the same physical file, so its base row
   * ID is unchanged and [[reassignOverlappingRowIds]] (already run) leaves it alone. Deletion
   * vectors index physical row positions within one immutable Parquet file, so the merge needs no
   * row tracking.
   *
   * A storage failure leaves the files unreconciled, so the commit aborts with a conflict, as it
   * does without row-level concurrency: a deletion vector that cannot be read, even a corrupt one
   * (which then surfaces where it is read next), a read-check job that fails, and a merged DV that
   * cannot be written. A cancellation, and any other failure, is thrown as is.
   */
  protected def resolveRowLevelConflicts(): Unit = {
    if (rowLevelCandidates.isEmpty) return
    resolutionStartNs = System.nanoTime()
    recordTime("resolved-row-level-conflicts") {
      val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
      val tablePath = deltaLog.dataPath
      // Decide what the reads allow before any deletion vector is decoded, so a transaction that
      // must abort for its reads does no deletion vector I/O. Only a scan needs the DV check
      // first, as it relies on the winner's DV containing the base DV.
      val conjunctsPerRead = evaluableConjunctsPerRead
      val readVerdict = readVerdictWithoutScan(rowLevelCandidates, conjunctsPerRead)
      readVerdict match {
        case ReadAffected(reason) =>
          throw notReconciled(rowLevelCandidates.head, reason, deleteRead = true)
        case _ =>
      }
      val disjointFiles = checkDisjointDeletionVectors(dvStore, tablePath)
      if (readVerdict == ReadNeedsScan) {
        scanReadsForWinnerDeletes(rowLevelCandidates, conjunctsPerRead).foreach { reason =>
          throw notReconciled(rowLevelCandidates.head, reason, deleteRead = true)
        }
      }
      val replacements = writeMergedDeletionVectors(dvStore, tablePath, disjointFiles)

      // Rebase the current transaction onto the winner's post-image: each AddFile(P) now carries
      // the merged DV and each RemoveFile(P) tombstones the winner's AddFile(P). The winning
      // summary no longer mentions P since the preparation.
      val newActions = currentTransactionInfo.actions.flatMap {
        case a: AddFile if replacements.contains(a.path) => replacements(a.path)._1
        case r: RemoveFile if replacements.contains(r.path) => Some(replacements(r.path)._2)
        case other => Some(other)
      }
      currentTransactionInfo = currentTransactionInfo.copy(actions = newActions)
      recordDeltaEvent(
        deltaLog,
        opType = "delta.rowLevelConcurrency.deletionVectorsMerged",
        data = resolutionMetrics ++ Map("resolvedPaths" -> replacements.size))
    }
  }

  /**
   * Sets aside the files [[resolveRowLevelConflicts]] may reconcile: those whose deletion vector
   * both the winning and the current transaction updated, i.e. a `RemoveFile(P)` plus an
   * `AddFile(P)` with a DV on both sides. Only compares actions.
   *
   * The files are dropped from the winning summary, so the file-level checks that run next see only
   * the other conflicts, and abort before any deletion vector is decoded, any read is scanned or
   * any merged deletion vector is written.
   * More files than [[DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_CANDIDATE_FILES]] are left to
   * the file-level checks, which bounds the work done during conflict detection.
   */
  protected def prepareRowLevelConflictResolution(): Unit = {
    if (!rowLevelConcurrencyEnabled) return
    // A whole-table read observes every row, so any row the winner deleted affects it.
    if (currentTransactionInfo.readWholeTable) return
    // RESTORE is not reconciled: it can bring deleted rows back, which the DV merge does not model.
    if (currentTransactionInfo.op.name == DeltaOperations.OP_RESTORE ||
        winningOperationName.contains(DeltaOperations.OP_RESTORE)) {
      return
    }

    // Winning transaction's DV updates: path present in both an AddFile (with a DV) and a
    // RemoveFile of the winning commit.
    val winningRemovedPaths = winningCommitSummary.removedFiles.map(_.path).toSet
    val winningDvUpdates: Map[String, AddFile] = winningCommitSummary.addedFiles.iterator
      .filter(a => a.deletionVector != null && winningRemovedPaths.contains(a.path))
      .map(a => a.path -> a)
      .toMap
    if (winningDvUpdates.isEmpty) return

    // Current transaction's DV updates and file removes, indexed by path, in a single pass over
    // the actions (the last action for a path wins).
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
    if (sharedPaths.size >
        spark.conf.get(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_CANDIDATE_FILES)) {
      // Left to the file-level checks, which abort on them.
      recordDeltaEvent(
        deltaLog,
        opType = "delta.rowLevelConcurrency.notReconciled",
        data = Map(
          "winningCommitVersion" -> winningCommitVersion,
          "winningOperation" -> winningOperationName.getOrElse("UNKNOWN"),
          "candidateFiles" -> sharedPaths.size,
          "commitLockHeld" -> commitLockHeld,
          "reason" -> NotReconciledReason.TooManyCandidateFiles))
      return
    }

    rowLevelCandidates = sharedPaths.toSeq.map { path =>
      SharedFile(winningDvUpdates(path), currentAddByPath(path), currentRemoveByPath(path))
    }
    winningCommitSummary = pruneReconciledFiles(winningCommitSummary, sharedPaths)
  }

  /** The files set aside by [[prepareRowLevelConflictResolution]]. */
  private var rowLevelCandidates: Seq[SharedFile] = Seq.empty

  /** When [[resolveRowLevelConflicts]] started, and whether its read check ran a scan. */
  private var resolutionStartNs = 0L
  private var readCheckScanned = false

  /** A file whose deletion vector both the winning and the current transaction updated. */
  private case class SharedFile(
      winningAdd: AddFile,
      currentAdd: AddFile,
      currentRemove: RemoveFile) {
    def path: String = currentAdd.path
  }

  /** Why the set-aside files are not reconciled, as recorded in the usage event. */
  private object NotReconciledReason {
    val SameRowDeleted = "sameRowDeleted"
    val DeletedRowsRestored = "deletedRowsRestored"
    val DeletionVectorUnreadable = "deletionVectorUnreadable"
    val ReadAffected = "readAffected"
    val ReadNotCheckable = "readNotCheckable"
    val ReadCheckSkippedUnderCommitLock = "readCheckSkippedUnderCommitLock"
    val ReadCheckFailed = "readCheckFailed"
    val DeletionVectorWriteFailed = "deletionVectorWriteFailed"
    val TooManyCandidateFiles = "tooManyCandidateFiles"
  }

  private def isCancellation(e: Throwable): Boolean =
    RowLevelConcurrencyResolution.isCancellation(e, spark.sparkContext)

  private def resolutionMetrics: Map[String, Any] = Map(
    "winningCommitVersion" -> winningCommitVersion,
    "winningOperation" -> winningOperationName.getOrElse("UNKNOWN"),
    "candidateFiles" -> rowLevelCandidates.size,
    "readCheckScanned" -> readCheckScanned,
    "commitLockHeld" -> commitLockHeld,
    "durationMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - resolutionStartNs))

  /**
   * Records why the set-aside files are not reconciled and returns the conflict to throw for
   * `file`.
   */
  private def notReconciled(file: SharedFile, reason: String, deleteRead: Boolean): Throwable = {
    recordDeltaEvent(
      deltaLog,
      opType = "delta.rowLevelConcurrency.notReconciled",
      data = resolutionMetrics ++ Map("reason" -> reason))
    val partitionOpt = getPrettyPartitionMessage(file.currentRemove.partitionValues)
    if (deleteRead) {
      DeltaErrors.concurrentDeleteReadException(
        winningCommitSummary.commitInfo, getTableNameOrPath, winningCommitVersion, partitionOpt)
    } else {
      DeltaErrors.concurrentDeleteDeleteException(
        winningCommitSummary.commitInfo, getTableNameOrPath, winningCommitVersion, partitionOpt)
    }
  }

  /**
   * Applies `f` to each item, on up to [[MaxParallelFiles]] threads when there are several: the
   * work is driver-side object-store DV I/O, so parallelism shortens the conflict window. A single
   * item stays on the caller thread. Results are awaited in order, and the first failure reached
   * is thrown as `f` threw it, whatever the number of items; the work still running is then
   * interrupted.
   */
  private def forEachFile[A, B](items: Seq[A])(f: A => B): Seq[B] = {
    if (items.size <= 1) return items.map(f)
    // A pool per call, as ThreadUtils.parmap gives DeltaFileOperations' footer reads, so one
    // table's resolution never waits on another's. Unlike parmap, it forwards Spark local
    // properties, such as credentials scoped to the query, and does not wrap failures.
    val executor =
      DeltaThreadPool.newDaemonCachedThreadPool("delta-row-level-conflict", MaxParallelFiles)
    try {
      val futures = items.map { item =>
        executor.submit(new Callable[B] { override def call(): B = spark.withActive(f(item)) })
      }.toList
      futures.map { future =>
        try future.get() catch {
          // Keep the failure's type, adding the caller's stack to the worker thread's.
          case e: ExecutionException => throw ThreadUtils.wrapCallerStacktrace(e.getCause)
        }
      }
    } finally {
      executor.shutdownNow()
    }
  }

  /** What the current transaction's reads allow, as decided by [[readVerdictWithoutScan]]. */
  private sealed trait ReadVerdict
  private case object ReadUnaffected extends ReadVerdict
  private case class ReadAffected(reason: String) extends ReadVerdict
  private case object ReadNeedsScan extends ReadVerdict

  /**
   * Decides, without reading any data, whether rows the winner newly deleted from `files` may
   * match the current transaction's read predicates, i.e. change what its reads return.
   *
   * Decided here: a DELETE/UPDATE whose only read is its own condition, and most MERGEs whose only
   * read is the target scan (which usually matches every row, see
   * [[mergeUnaffectedByDeletesOfUntouchedRows]]), are unaffected. Reads on partition columns only
   * are evaluated on the partition values of the files the winner deleted rows from. A read
   * predicate is evaluated on its deterministic, subquery-free conjuncts over the table's columns
   * (dropping a conjunct only matches more rows), and a read with none of those may match any
   * row. Other reads need [[scanReadsForWinnerDeletes]], which does not run while the commit lock
   * is held: those reads are then treated as affected. Such an abort happens before the deletion
   * vectors are compared, so it is reported for the reads even if both transactions also deleted
   * the same row.
   */
  private def readVerdictWithoutScan(
      files: Seq[SharedFile],
      conjunctsPerRead: Seq[Seq[Expression]]): ReadVerdict = {
    val readPredicates = currentTransactionInfo.readPredicates
    currentTransactionInfo.op match {
      // A DELETE/UPDATE whose only read is its own condition marked every row of the shared files
      // that matches it, so the disjointness test already shows none of the winner's new deletes
      // match. No scan needed.
      case _: DeltaOperations.Delete | _: DeltaOperations.Update if readPredicates.size == 1 =>
        return ReadUnaffected
      // The shortcut reasons about the target scan only, so it applies only when that scan is the
      // transaction's only read: a MERGE whose source also reads the table records more reads.
      case merge: DeltaOperations.Merge
          if readPredicates.size == 1 && mergeUnaffectedByDeletesOfUntouchedRows(merge) =>
        return ReadUnaffected
      case _ =>
    }
    if (conjunctsPerRead.isEmpty || conjunctsPerRead.exists(_.isEmpty)) {
      return ReadAffected(NotReconciledReason.ReadNotCheckable)
    }

    val partitionColumns = currentTransactionInfo.readSnapshot.metadata.partitionColumns
    // The reads are evaluated as one filter that `filterFileList` rewrites, which a read recorded
    // as already rewritten must not be.
    val partitionOnly = readPredicates.forall(_.shouldRewriteFilter) &&
      conjunctsPerRead.forall(_.forall { e =>
        DeltaTableUtils.isPredicatePartitionColumnsOnly(e, partitionColumns, spark)
      })
    if (partitionOnly) {
      // A winner's DV with more rows than the base DV deleted some rows. Their partition values
      // decide whether they match, evaluated as the added-files check does.
      val filesWithNewDeletes = files.filter { f =>
        f.winningAdd.deletionVector.cardinality >
          Option(f.currentRemove.deletionVector).map(_.cardinality).getOrElse(0L)
      }
      if (filesWithNewDeletes.isEmpty) return ReadUnaffected
      val matchingFiles = DeltaLog.filterFileList(
        partitionSchema = currentTransactionInfo.partitionSchemaAtReadTime,
        files = filesWithNewDeletes.map(_.winningAdd).toDF(spark),
        partitionFilters = Seq(conjunctsPerRead.map(_.reduce(And)).reduce(Or)))
      return if (matchingFiles.isEmpty) ReadUnaffected
      else ReadAffected(NotReconciledReason.ReadAffected)
    }
    if (commitLockHeld) ReadAffected(NotReconciledReason.ReadCheckSkippedUnderCommitLock)
    else ReadNeedsScan
  }

  /**
   * The current transaction's read predicates, each reduced to the conjuncts the read check can
   * evaluate: deterministic, subquery-free and over the table's columns.
   */
  private def evaluableConjunctsPerRead: Seq[Seq[Expression]] = {
    val resolver = spark.sessionState.conf.resolver
    val columnNames = currentTransactionInfo.readSnapshot.metadata.schema.fieldNames
    currentTransactionInfo.readPredicates.map { rp =>
      (rp.partitionPredicates ++ rp.dataPredicates).filter { e =>
        e.deterministic && !DeltaTableUtils.containsSubquery(e) &&
          e.references.forall(a => columnNames.exists(resolver(_, a.name)))
      }
    }
  }

  /**
   * Returns why rows the winner newly deleted from `files` may match the current transaction's
   * read predicates, by scanning the files, or `None` when none do.
   *
   * The winner's newly-deleted rows of a file are its live rows under the current transaction's
   * base DV minus its live rows under the winner's DV (a subset, as the winner's DV contains the
   * base DV, see [[disjointFileDeletionVectors]]), so for the read condition `cond` (OR across
   * reads, AND within a read):
   *
   *   matches(newly deleted) = matches(base-DV live rows) - matches(winner-DV live rows)
   *
   * Both images of all files are scanned with signed weights (+1 base, -1 winner) and summed in a
   * single Spark job.
   */
  private def scanReadsForWinnerDeletes(
      files: Seq[SharedFile],
      conjunctsPerRead: Seq[Seq[Expression]]): Option[String] = {
    readCheckScanned = true
    val snapshot = currentTransactionInfo.readSnapshot
    def matchingRowWeights(images: Seq[AddFile], weight: Long): DataFrame = {
      val df = deltaLog.createDataFrame(
        snapshot, images, actionTypeOpt = Some("rowLevelConcurrencyReadCheck"))
      val condition = conjunctsPerRead
        .map(_.map(e => rebindToDataFrame(e, df)).reduce(_ && _))
        .reduce(_ || _)
      df.where(condition).select(lit(weight).as("weight"))
    }
    val baseImages =
      files.map(f => f.currentAdd.copy(deletionVector = f.currentRemove.deletionVector))
    val winnerImages = files.map(_.winningAdd)
    val newlyDeletedWeights = matchingRowWeights(baseImages, 1L)
      .union(matchingRowWeights(winnerImages, -1L))
      .select(coalesce(sum(col("weight")), lit(0L)))
    // Plan the job first, so that only running it is caught: a failure to build or plan it is
    // thrown as is. `collect` runs this plan; `head` would plan another one.
    newlyDeletedWeights.queryExecution.executedPlan
    val newlyDeletedMatches = try newlyDeletedWeights.collect().head.getLong(0) catch {
      case NonFatal(e) if isReadCheckJobFailure(e) =>
        logWarning(log"Row-level concurrency could not run its read check; not reconciling", e)
        return Some(NotReconciledReason.ReadCheckFailed)
    }
    if (newlyDeletedMatches == 0) None else Some(NotReconciledReason.ReadAffected)
  }

  /**
   * Whether `e`, thrown while the read-check job runs, is the job failing, for example on a file
   * it cannot read or on a row its condition cannot be evaluated for. Spark reports a failed job
   * as a [[SparkThrowable]]; an internal error, which Spark reports for a bug, and a cancellation
   * are not job failures.
   */
  private def isReadCheckJobFailure(e: Throwable): Boolean = e match {
    case st: SparkThrowable =>
      !Option(st.getCondition).exists(_.startsWith("INTERNAL_ERROR")) && !isCancellation(e)
    case _ => false
  }

  /**
   * Whether a MERGE's outcome is unchanged when rows it did not modify are deleted first. Each
   * target row's fate depends only on that row and the source: a row that matched a MATCHED (or
   * NOT MATCHED BY SOURCE) clause was modified, so it is in the MERGE's own new deletes and
   * disjointness already excludes it from the winner's. The exception is NOT MATCHED: a source
   * row is inserted only if NO target row matched it, so deleting a matched-but-unmodified target
   * row changes the inserts. That can only happen when no MATCHED clause is unconditional.
   */
  private def mergeUnaffectedByDeletesOfUntouchedRows(merge: DeltaOperations.Merge): Boolean =
    merge.notMatchedPredicates.isEmpty || merge.matchedPredicates.exists(_.predicate.isEmpty)

  /** Rebinds `e`'s attributes to `df`'s columns by name. */
  private def rebindToDataFrame(e: Expression, df: DataFrame): Column =
    Column(e.transformUp {
      case a: AttributeReference => df.col("`" + a.name.replace("`", "``") + "`").expr
    })

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
   *
   * A file whose DV only the winner updated (`RemoveFile(Q)` plus `AddFile(Q)` with a DV) is not
   * pruned either, so the current transaction still conflicts on it: its re-add counts as changed
   * data in the added-files check, and its removal fails the delete-read check if the current
   * transaction read Q. Excluding such re-adds and checking the winner's newly deleted rows of Q
   * against the current transaction's reads is a separate change.
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
   * A set-aside file whose newly-deleted rows are disjoint between the winning and current
   * transactions, with its decoded `(winning, current)` bitmaps when they are kept for the merge.
   */
  private case class DisjointFile(
      file: SharedFile,
      bitmaps: Option[(RoaringBitmapArray, RoaringBitmapArray)])

  /**
   * Decodes the deletion vectors of every set-aside file and returns them when the newly-deleted
   * rows of every file are disjoint, or throws the conflict for the first file that is not. Files
   * not yet decoded are skipped once a conflict is found.
   *
   * The decoded winning and current bitmaps are kept for the merge only while their total size
   * stays within [[DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_RETAINED_DV_BYTES]]; the other
   * files' are read again when merged. Up to [[MaxParallelFiles]] files are being decoded at the
   * same time, each with its winning, current and base bitmaps.
   */
  private def checkDisjointDeletionVectors(
      dvStore: DeletionVectorStore,
      tablePath: Path): Seq[DisjointFile] = {
    val maxRetainedBytes =
      spark.conf.get(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_RETAINED_DV_BYTES)
    var retainedBytes = 0L
    val filesToCheck = rowLevelCandidates.map { f =>
      retainedBytes +=
        f.winningAdd.deletionVector.sizeInBytes + f.currentAdd.deletionVector.sizeInBytes
      f -> (retainedBytes <= maxRetainedBytes)
    }
    val conflictFound = new AtomicBoolean(false)
    val results = forEachFile(filesToCheck) { case (f, retain) =>
      if (conflictFound.get) {
        None
      } else {
        val result =
          disjointFileDeletionVectors(dvStore, tablePath, f, retain).left.map(reason => (f, reason))
        if (result.isLeft) conflictFound.set(true)
        Some(result)
      }
    }
    results.flatten.collectFirst { case Left(conflict) => conflict }.foreach {
      case (file, reason) => throw notReconciled(file, reason, deleteRead = false)
    }
    results.flatten.collect { case Right(disjointFile) => disjointFile }
  }

  /**
   * Decodes the winning, current and base deletion vectors of a single set-aside file `P` and
   * returns it when the two transactions deleted disjoint rows, or why they cannot be merged.
   */
  private def disjointFileDeletionVectors(
      dvStore: DeletionVectorStore,
      tablePath: Path,
      file: SharedFile,
      retainBitmaps: Boolean): Either[String, DisjointFile] = {
    def bitmapOf(dv: DeletionVectorDescriptor): Option[RoaringBitmapArray] =
      tryReadDeletionVector(dvStore, dv, tablePath, file)
    val bitmaps = for {
      base <- bitmapOf(file.currentRemove.deletionVector)
      winning <- bitmapOf(file.winningAdd.deletionVector)
      current <- bitmapOf(file.currentAdd.deletionVector)
    } yield (base, winning, current)
    val (baseBitmap, winningBitmap, currentBitmap) = bitmaps match {
      case Some(decoded) => decoded
      case None => return Left(NotReconciledReason.DeletionVectorUnreadable)
    }
    // `baseBitmap` is the DV of `P` at the current txn's read time (carried on its RemoveFile).
    // DELETE/UPDATE/MERGE only add rows to a DV, so `dv_win` and `dv_cur` contain `base` and each
    // side's *newly* deleted rows are `dv \ base`. A commit that brings deleted rows back breaks
    // this: the union would delete those rows again, and the read check would not see them. Such
    // a file is not reconciled.
    if (!containsAll(winningBitmap, baseBitmap) || !containsAll(currentBitmap, baseBitmap)) {
      return Left(NotReconciledReason.DeletedRowsRestored)
    }
    // The newly-deleted rows overlap in `(dv_win INTERSECT dv_cur) MINUS base`.
    // Empty overlap => the two txns deleted disjoint rows, so the DV union is safe as far as the
    // writes go (the reads are checked separately, see readVerdictWithoutScan).
    // Non-empty => the same row was deleted concurrently: a genuine conflict, and the commit
    // aborts.
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

    if (newlyDeletedOverlap.isEmpty) {
      Right(DisjointFile(file, if (retainBitmaps) Some((winningBitmap, currentBitmap)) else None))
    } else {
      Left(NotReconciledReason.SameRowDeleted)
    }
  }

  /** Whether `bitmap` contains every row of `subset`. */
  private def containsAll(bitmap: RoaringBitmapArray, subset: RoaringBitmapArray): Boolean = {
    val missing = subset.copy()
    missing.andNot(bitmap)
    missing.isEmpty
  }

  /**
   * Merges each disjoint file's deletion vectors and writes them packed into as few new DV files as
   * [[DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE]] allows, with the table's random file
   * prefixes, like the DML write path does. Bitmaps not kept from the disjointness check are read
   * again here, a bounded batch at a time. Returns the current transaction's rebased actions per
   * path, see [[rebased]].
   *
   * NOTE: this writes DV files as a side effect of conflict resolution. If the commit ultimately
   * fails or is retried against another winning version, they are orphaned and later reclaimed by
   * VACUUM (same lifecycle as any DV written by DML).
   */
  private def writeMergedDeletionVectors(
      dvStore: DeletionVectorStore,
      tablePath: Path,
      files: Seq[DisjointFile]): Map[String, (Option[AddFile], RemoveFile)] = {
    val writer = new PackedDeletionVectorWriter(
      dvStore,
      tablePath,
      spark.conf.get(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE),
      DeltaUtils.getRandomPrefixLength(currentTransactionInfo.metadata))
    // Merge a batch in parallel, then append it to the packed file on this thread.
    def mergeAndWrite(): Map[String, (Option[AddFile], RemoveFile)] =
      files.grouped(64).flatMap { batch =>
        val merged = forEachFile(batch) { f =>
          def bitmapOf(dv: DeletionVectorDescriptor): Option[RoaringBitmapArray] =
            tryReadDeletionVector(dvStore, dv, tablePath, f.file)
          val bitmaps = f.bitmaps.orElse(for {
            winning <- bitmapOf(f.file.winningAdd.deletionVector)
            current <- bitmapOf(f.file.currentAdd.deletionVector)
          } yield (winning, current))
          bitmaps.map { case (winningBitmap, currentBitmap) =>
            val mergedBitmap = winningBitmap.copy()
            mergedBitmap.merge(currentBitmap)
            mergedBitmap
          }
        }
        batch.zip(merged).map {
          case (f, Some(mergedBitmap)) =>
            f.file.path ->
              rebased(f.file, () => writer.write(mergedBitmap), mergedBitmap.cardinality)
          case (f, None) =>
            throw notReconciled(
              f.file, NotReconciledReason.DeletionVectorUnreadable, deleteRead = false)
        }
      }.toMap
    // As the DML DV write path does: a DV file that fails to close may be incomplete, so the
    // commit fails with it unless deletionVectors.propagateCloseFailure is off. On any failure,
    // including an interrupt, the file is closed.
    try {
      if (spark.conf.get(DeltaSQLConf.DELETION_VECTOR_PROPAGATE_CLOSE_FAILURE)) {
        SparkUtils.tryWithSafeFinally(mergeAndWrite())(writer.close())
      } else {
        SparkUtils.tryWithResource(writer)(_ => mergeAndWrite())
      }
    } catch {
      case e: DeletionVectorWriteException if !isCancellation(e.getCause) =>
        logWarning(log"Row-level concurrency could not write merged deletion vectors; " +
          log"not reconciling", e.getCause)
        throw notReconciled(
          files.head.file, NotReconciledReason.DeletionVectorWriteFailed, deleteRead = false)
      case e: DeletionVectorWriteException => throw e.getCause
    }
  }

  /**
   * The current transaction's actions for `file`, rebased onto the winner's post-image: its
   * `AddFile(P)` with the merged DV, and the removal of the winner's `AddFile(P)`. Like DML, a file
   * whose rows are all deleted is only removed, and no DV is written for it: `writeMergedDV` is
   * called only for a file that keeps rows.
   */
  private def rebased(
      file: SharedFile,
      writeMergedDV: () => DeletionVectorDescriptor,
      deletedRows: Long): (Option[AddFile], RemoveFile) = {
    val rebasedRemove =
      file.winningAdd.removeWithTimestamp(dataChange = file.currentRemove.dataChange)
    if (file.currentAdd.numPhysicalRecords.contains(deletedRows)) {
      (None, rebasedRemove)
    } else {
      // Keep the current AddFile's identity (base row ID / default row commit version already
      // reconciled by the row-ID phases), applying the merged DV as DML applies a new one.
      val (rebasedAdd, _) = file.currentAdd.removeRows(
        writeMergedDV(), updateStats = true, dataChange = file.currentAdd.dataChange)
      (Some(rebasedAdd), rebasedRemove)
    }
  }

  /**
   * Reads the deletion vector `dv` of `file`, or returns `None` when it cannot be read, for any
   * reason but a cancellation, including a corrupt DV, so that the file is not reconciled. Only
   * the read is caught, not the code that uses the bitmap.
   */
  private def tryReadDeletionVector(
      dvStore: DeletionVectorStore,
      dv: DeletionVectorDescriptor,
      tablePath: Path,
      file: SharedFile): Option[RoaringBitmapArray] = {
    try Some(readDeletionVectorOrEmpty(dvStore, dv, tablePath)) catch {
      case NonFatal(e) if !isCancellation(e) =>
        logWarning(log"Row-level concurrency could not read a deletion vector of " +
          log"${MDC(DeltaLogKeys.PATH, file.path)}; not reconciling", e)
        None
    }
  }

  /** Reads a deletion vector into a [[RoaringBitmapArray]], returning an empty bitmap for none. */
  private def readDeletionVectorOrEmpty(
      dvStore: DeletionVectorStore,
      dv: DeletionVectorDescriptor,
      tablePath: Path): RoaringBitmapArray = {
    if (dv == null || dv.isEmpty) new RoaringBitmapArray() else dvStore.read(dv, tablePath)
  }

  /**
   * Writes deletion vectors packed into as few new files as `targetSize` allows, each file under a
   * random prefix of `prefixLength` characters, as the DML write path does (see
   * [[DeletionVectorWriter.createMapperToStoreDeletionVectors]]).
   */
  private class PackedDeletionVectorWriter(
      dvStore: DeletionVectorStore,
      tablePath: Path,
      targetSize: Long,
      prefixLength: Int) extends Closeable {
    private val tablePathWithFs = dvStore.pathWithFileSystem(tablePath)
    private var context: Option[DeletionVectorMapperContext] = None
    private var bytesInFile = 0L

    /** Writes `bitmap` and returns its descriptor. An empty bitmap needs no file. */
    def write(bitmap: RoaringBitmapArray): DeletionVectorDescriptor = {
      if (bitmap.isEmpty) return DeletionVectorDescriptor.EMPTY
      val serialized =
        DeletionVectorUtils.serialize(bitmap, RoaringBitmapArrayFormat.Portable, Some(tablePath))
      if (context.isEmpty || bytesInFile + serialized.length > targetSize) {
        close()
        val prefix = DeltaUtils.getRandomPrefix(prefixLength)
        val (writer, fileId) =
          storage(DeletionVectorWriter.createWriter(dvStore, tablePathWithFs, prefix))
        context = Some(DeletionVectorMapperContext(dvStore, writer, tablePath, fileId, prefix))
        bytesInFile = 0L
      }
      bytesInFile += serialized.length
      storage(
        DeletionVectorWriter.storeSerializedBitmap(context.get, serialized, bitmap.cardinality))
    }

    override def close(): Unit = {
      val current = context
      context = None
      current.foreach(c => storage(c.writer.close()))
    }

    /** Runs a call into DV storage, marking its failure as a [[DeletionVectorWriteException]]. */
    private def storage[T](body: => T): T =
      try body catch { case NonFatal(e) => throw new DeletionVectorWriteException(e) }
  }
}

/**
 * A failure of row-level concurrency to write or close a merged DV file, as opposed to a failure
 * in the code that merges the DVs.
 */
private class DeletionVectorWriteException(cause: Throwable) extends Exception(cause)

private object RowLevelConcurrencyResolution {
  /** How many files one resolution decodes or merges at a time. */
  private val MaxParallelFiles = 8

  /**
   * Whether `e` cancels the statement rather than failing the reconciliation: a cancelled Spark
   * job, a stopped SparkContext, an interrupt of this thread, or an interrupt as Spark's
   * StreamExecution.isInterruptionException recognizes one, except a SocketTimeoutException,
   * which fails a read. It is thrown as is, so that a cancelled statement does not come back as a
   * conflict to retry; another timeout that is an InterruptedIOException is thrown as is too.
   */
  private[delta] def isCancellation(e: Throwable, sc: SparkContext): Boolean =
    Thread.currentThread().isInterrupted || sc.isStopped || (e match {
      case st: SparkThrowable if st.getCondition == "SPARK_JOB_CANCELLED" => true
      case _: SocketTimeoutException => false
      case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException =>
        true
      case wrapper @ (_: UncheckedIOException | _: ExecutionException)
          if wrapper.getCause != null =>
        isCancellation(wrapper.getCause, sc)
      case _ => false
    })
}
