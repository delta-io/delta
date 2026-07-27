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

import java.io.IOException
import java.util.UUID

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, Metadata, RemoveFile}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Utilities for Row-Level Concurrency (RLC) conflict resolution.
 *
 * RLC allows concurrent DML operations (DELETE/UPDATE/MERGE) that touch disjoint rows in the
 * same physical file to commit without aborting. It works by decoding the Deletion Vector
 * bitmaps from both the winner and loser transactions, checking row-set disjointness, and
 * rebasing the loser's DV on top of the winner's post-image when the rows are disjoint.
 *
 * RLC is strictly opportunistic: it only converts a "would-abort" outcome into a
 * "commits cleanly" outcome, never the reverse. When preconditions fail, the existing
 * file-level conflict checking behavior is preserved exactly.
 *
 * @see [[isSnapshotEligible]] for the table-level eligibility predicates.
 * @see [[tryRebase]] for the rebase algorithm and its preconditions.
 */
object RowLevelConcurrency {

  // ---------------------------------------------------------------------------
  // Telemetry event names
  // ---------------------------------------------------------------------------

  /** Prefix for all RLC-related Delta telemetry events. */
  val TELEMETRY_PREFIX = "delta.conflictDetection.rowLevelConcurrency"

  /** Fired when one or more same-file DV conflicts are successfully resolved. */
  val TELEMETRY_RESOLVED = s"$TELEMETRY_PREFIX.resolved"

  /** Fired for each file where P4 (disjoint delta) fails -- true row-level overlap. */
  val TELEMETRY_ABORTED_OVERLAP = s"$TELEMETRY_PREFIX.abortedOverlap"

  /** Fired when DV decode or resolution exceeds the configured budget. */
  val TELEMETRY_ABORTED_BUDGET = s"$TELEMETRY_PREFIX.abortedBudget"

  /** Fired when winner action shape fails preconditions P1/P2/P3. */
  val TELEMETRY_ABORTED_SHAPE = s"$TELEMETRY_PREFIX.abortedShape"

  /** Fired when DV bytes cannot be read (404, permission, network error). */
  val TELEMETRY_ABORTED_DV_READ_FAILURE = s"$TELEMETRY_PREFIX.abortedDvReadFailure"

  /** Fired when DV bytes are present but cannot be decoded. */
  val TELEMETRY_ABORTED_DECODE_FAILURE = s"$TELEMETRY_PREFIX.abortedDecodeFailure"

  /** Fired when a rebased DV cannot be written. */
  val TELEMETRY_ABORTED_DV_WRITE_FAILURE = s"$TELEMETRY_PREFIX.abortedDvWriteFailure"

  /** Fired when an unexpected, non-fatal error makes RLC fall back to legacy detection. */
  val TELEMETRY_ABORTED_UNEXPECTED = s"$TELEMETRY_PREFIX.abortedUnexpected"

  /** Fired when a successful rebase is followed by another conflict-check failure. */
  val TELEMETRY_ABORTED_AFTER_REBASE = s"$TELEMETRY_PREFIX.abortedAfterRebase"

  /** Detect-only: records what *would* have resolved, without mutating actions. */
  val TELEMETRY_WOULD_RESOLVE = s"$TELEMETRY_PREFIX.wouldResolve"

  // ---------------------------------------------------------------------------
  // Eligibility predicates
  // ---------------------------------------------------------------------------

  /**
   * Snapshot-level eligibility: does the table support RLC at all?
   *
   * A table is eligible when ALL of the following hold:
   *  1. The RLC master switch is enabled.
   *  2. Deletion Vectors are writable on the table.
   *  3. Row Tracking is enabled (required for correct baseRowId handling).
   *  4. The table is unpartitioned (P0 only -- partitioned tables bypass RLC).
   *  5. The table has no identity columns.
   *
   * NOTE: RLC engages at BOTH `Serializable` and `WriteSerializable`; the isolation level
   * controls blind-INSERT vs. DML conflict semantics, not whether RLC engages. Here
   * `DeltaConfigs.ISOLATION_LEVEL` only accepts `Serializable` (see `DeltaConfig.scala`),
   * so the snapshot's table-level isolation is always `Serializable` and there is no
   * isolation-level gate to apply.
   *
   * Persistent-DV mode requires no extra gating: the per-op confs
   * `DELETE/UPDATE/MERGE_USE_PERSISTENT_DELETION_VECTORS` already default to `true`, and
   * each DML command additionally gates DV usage on
   * [[DeletionVectorUtils.deletionVectorsWritable]] at runtime (see
   * `DeleteCommand.shouldWritePersistentDeletionVectors`), which is exactly the
   * "DV mode when the table supports it" behavior RLC needs.
   */
  def isSnapshotEligible(spark: SparkSession, snapshot: Snapshot): Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED) &&
    DeletionVectorUtils.deletionVectorsWritable(snapshot) &&
    RowTracking.isEnabled(snapshot.protocol, snapshot.metadata) &&
    snapshot.metadata.partitionColumns.isEmpty &&
    !ColumnWithDefaultExprUtils.hasIdentityColumn(snapshot.schema)
  }

  /**
   * Operation-level eligibility: does *this* commit's action set safely admit RLC?
   *
   * This refines the snapshot-level check with commit-specific guards:
   *  1. No divergent Metadata mutation (the only safe Metadata is byte-identical to the
   *     read snapshot's metadata).
   *
   * @param snapshot the read snapshot for this transaction
   * @param actions  the prepared actions for this transaction
   */
  def isCommitEligible(
      spark: SparkSession,
      snapshot: Snapshot,
      actions: Seq[Action]): Boolean = {
    isSnapshotEligible(spark, snapshot) &&
    !actions.exists {
      case m: Metadata => m != snapshot.metadata
      case _ => false
    }
  }

  /**
   * Returns a user-facing hint suggesting how to enable Row-Level Concurrency on this table
   * when the RLC SQL conf is on but the loser's snapshot is missing one or both required
   * table features (Deletion Vectors and/or Row Tracking). Returns `""` when no actionable
   * hint applies: RLC is disabled, both features are already enabled, OR the table is
   * partitioned or contains identity columns.
   *
   * The result is a single-line parenthetical phrase suitable for appending after the
   * docLink in an error message such as
   *   "Refer to <docLink> (...hint...) for more information."
   * It must remain single-line so it can flow through the OSS error-class `docLink`
   * parameter slot, which the error-class machinery and tests assume contains no newlines.
   *
   * @param snapshot the read snapshot for the failing transaction
   * @param conf the active session's SQLConf
   */
  def enablementHintIfMissing(snapshot: Snapshot, conf: SQLConf): String = {
    if (!conf.getConf(DeltaSQLConf.ROW_LEVEL_CONCURRENCY_ENABLED)) return ""
    // Partitioned tables aren't supported by RLC, and partitioning isn't a feature the user
    // can toggle without rewriting the table. Suppress the hint to avoid noise.
    if (snapshot.metadata.partitionColumns.nonEmpty) return ""
    // Identity-column tables are intentionally unsupported. Do not suggest enabling table
    // features that would still leave the transaction ineligible.
    if (ColumnWithDefaultExprUtils.hasIdentityColumn(snapshot.schema)) return ""

    val dvOff = !DeletionVectorUtils.deletionVectorsWritable(snapshot)
    val rtOff = !RowTracking.isEnabled(snapshot.protocol, snapshot.metadata)
    if (!dvOff && !rtOff) return ""

    val missing = mutable.ArrayBuffer.empty[String]
    val tblPropFixes = mutable.ArrayBuffer.empty[String]
    if (dvOff) {
      missing += "Deletion Vectors"
      tblPropFixes += s"'${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'true'"
    }
    if (rtOff) {
      missing += "Row Tracking"
      tblPropFixes += s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true'"
    }

    s" (enabling Row-Level Concurrency could allow this DML to commit if its row changes are" +
      s" disjoint from the concurrent write -- missing feature(s): ${missing.mkString(" and ")};" +
      s" enable with: ALTER TABLE <table> SET TBLPROPERTIES (${tblPropFixes.mkString(", ")}))"
  }

  // ---------------------------------------------------------------------------
  // Detect-only analysis (no I/O, no DV decode)
  // ---------------------------------------------------------------------------

  /**
   * Skip reasons recorded by [[wouldResolve]] when a candidate path is rejected from RLC
   * resolution at the action-shape level.
   */
  object SkipReason {

    /** Winner emitted more than one (or zero) AddFile for the path -- not a DV-only mod. */
    val WinnerNotDvOnly = "winner_not_dv_only"

    /** Winner's AddFile path differs from RemoveFile path -- copy-on-write or OPTIMIZE. */
    val WinnerCowOrCompacted = "winner_cow_or_compacted"

    /** Loser has a RemoveFile for the path but no matching AddFile -- not a DV-only mod. */
    val LoserNoSamePathAdd = "loser_no_same_path_add"
  }

  /**
   * Summary of what [[wouldResolve]] found: used to emit telemetry without performing any
   * I/O or action mutation.
   *
   * @param candidateFileCount number of files where action shapes would allow rebase
   * @param candidatePaths     the candidate paths themselves (capped to avoid log blowup)
   * @param skipReasons        per-reason counts for paths skipped at the shape level
   */
  case class WouldResolveSummary(
      candidateFileCount: Int,
      candidatePaths: Seq[String],
      skipReasons: Map[String, Int])

  private val MAX_CANDIDATE_PATHS_LOGGED = 16

  /**
   * Detect-only analysis: walk the loser's and winner's actions and identify files where
   * the action SHAPE would allow row-level conflict resolution. Does NOT decode DVs, does
   * NOT mutate actions, does NOT issue I/O.
   *
   * Resolution preconditions P2 (winner monotonicity) and P4 (disjointness) are NOT checked
   * here -- they require DV bytes, which only [[tryRebase]] reads.
   *
   * A path is a candidate when BOTH of the following hold:
   *  - Loser emits `RemoveFile(path)` AND exactly one `AddFile(path)` (DV-only mod shape)
   *  - Winner emits `RemoveFile(path)` AND exactly one `AddFile(path)` (DV-only mod shape)
   */
  def wouldResolve(
      loserActions: Seq[Action],
      winnerAddedFiles: Seq[AddFile],
      winnerRemovedFiles: Seq[RemoveFile]): WouldResolveSummary = {
    val loserRemovesByPath = loserActions.collect { case r: RemoveFile => r.path -> r }.toMap
    val loserAddsByPath = loserActions.collect { case a: AddFile => a }.groupBy(_.path)
    val winnerRemovesByPath = winnerRemovedFiles.map(r => r.path -> r).toMap
    val winnerAddsByPath = winnerAddedFiles.groupBy(_.path)

    val sharedPaths = loserRemovesByPath.keySet.intersect(winnerRemovesByPath.keySet)

    val candidates = scala.collection.mutable.ArrayBuffer.empty[String]
    val skipReasons = scala.collection.mutable.HashMap.empty[String, Int].withDefaultValue(0)

    sharedPaths.foreach { path =>
      val wAdds = winnerAddsByPath.getOrElse(path, Seq.empty)
      val lAdds = loserAddsByPath.getOrElse(path, Seq.empty)

      if (wAdds.isEmpty) {
        // Winner is a full-delete or CoW that rewrote to a different path.
        skipReasons(SkipReason.WinnerCowOrCompacted) += 1
      } else if (wAdds.size != 1) {
        // Winner produced multiple AddFiles for the path -- not a DV-only mod shape.
        skipReasons(SkipReason.WinnerNotDvOnly) += 1
      } else if (lAdds.size != 1) {
        // Loser does not have exactly one AddFile for the path -- not a DV-only mod shape.
        skipReasons(SkipReason.LoserNoSamePathAdd) += 1
      } else {
        candidates += path
      }
    }

    WouldResolveSummary(
      candidateFileCount = candidates.size,
      candidatePaths = candidates.take(MAX_CANDIDATE_PATHS_LOGGED).toSeq,
      skipReasons = skipReasons.toMap)
  }

  // ---------------------------------------------------------------------------
  // Atomic rebase
  // ---------------------------------------------------------------------------

  sealed trait RebaseFailure {
    def name: String
  }

  object RebaseFailure {
    case object WinnerCowOrCompacted extends RebaseFailure {
      override val name: String = SkipReason.WinnerCowOrCompacted
    }
    case object WinnerNotDvOnly extends RebaseFailure {
      override val name: String = SkipReason.WinnerNotDvOnly
    }
    case object LoserNotDvOnly extends RebaseFailure {
      override val name: String = SkipReason.LoserNoSamePathAdd
    }
    case object DifferentBaseDv extends RebaseFailure {
      override val name: String = "different_base_dv"
    }
    case object WinnerShrunkDv extends RebaseFailure {
      override val name: String = "winner_shrunk_dv"
    }
    case object LoserShrunkDv extends RebaseFailure {
      override val name: String = "loser_shrunk_dv"
    }
    case object Overlap extends RebaseFailure {
      override val name: String = "overlap"
    }
    case object ByteBudgetExceeded extends RebaseFailure {
      override val name: String = "byte_budget_exceeded"
    }
    case object DvReadBudgetExceeded extends RebaseFailure {
      override val name: String = "dv_read_budget_exceeded"
    }
    case object DeadlineExceeded extends RebaseFailure {
      override val name: String = "deadline_exceeded"
    }
    case object DecodeFailure extends RebaseFailure {
      override val name: String = "decode_failure"
    }
    case object DvReadFailure extends RebaseFailure {
      override val name: String = "dv_read_failure"
    }
    case object DvWriteFailure extends RebaseFailure {
      override val name: String = "dv_write_failure"
    }
  }

  sealed trait RebaseStatus
  object RebaseStatus {
    case object NoSharedPaths extends RebaseStatus
    case object Succeeded extends RebaseStatus
    case object Aborted extends RebaseStatus
  }

  case class RebaseStats(
      numDvFilesRead: Int = 0,
      numDvBytesRead: Long = 0L,
      numDvFilesWritten: Int = 0)

  /**
   * A winning commit is applied only when every shared physical path can be rebased. On
   * [[RebaseStatus.Aborted]], `newActions` is always the original action sequence and the
   * resolved winner action lists are empty.
   */
  case class RebaseResult(
      status: RebaseStatus,
      newActions: Seq[Action],
      resolvedAddFiles: Seq[AddFile],
      resolvedRemoveFiles: Seq[RemoveFile],
      failure: Option[RebaseFailure],
      stats: RebaseStats) {
    def resolvedFileCount: Int = resolvedAddFiles.size
    def numDvFilesWritten: Int = stats.numDvFilesWritten
    def skipReasons: Map[String, Int] = failure.map(f => Map(f.name -> 1)).getOrElse(Map.empty)
  }

  /**
   * @param maxDvBytesPerFile maximum serialized size of any DV decoded during the rebase
   * @param maxDvReads maximum number of distinct on-disk DV reads per winning commit
   * @param deadlineNanos absolute deadline in the same time source as `nanoTime`
   * @param nanoTime injectable monotonic time source for deterministic tests
   */
  case class RebaseBudgets(
      maxDvBytesPerFile: Long,
      maxDvReads: Int,
      deadlineNanos: Long,
      nanoTime: () => Long = () => System.nanoTime())

  object RebaseBudgets {
    def unbounded: RebaseBudgets =
      RebaseBudgets(
        maxDvBytesPerFile = Long.MaxValue,
        maxDvReads = Int.MaxValue,
        deadlineNanos = Long.MaxValue)
  }

  private case class RebaseCandidate(
      path: String,
      loserRemove: RemoveFile,
      loserAdd: AddFile,
      winnerRemove: RemoveFile,
      winnerAdd: AddFile)

  private case class PreparedRebase(
      candidate: RebaseCandidate,
      loserDelta: RoaringBitmapArray,
      unionedDv: RoaringBitmapArray)

  private class MutableRebaseStats {
    var numDvFilesRead: Int = 0
    var numDvBytesRead: Long = 0L
    var numDvFilesWritten: Int = 0

    def result: RebaseStats =
      RebaseStats(numDvFilesRead, numDvBytesRead, numDvFilesWritten)
  }

  /**
   * Rebase all same-file conflicts against one winning commit. Shape and DV contents are
   * fully preflighted before any DV is written. If any shared path fails preflight, or if
   * any write fails, the original actions are returned and no winner actions are resolved.
   * A failed write can leave an unreferenced DV file, as with other failed Delta writes, but
   * it can never produce a partially mutated transaction action set.
   */
  def tryRebase(
      loserActions: Seq[Action],
      winnerAddedFiles: Seq[AddFile],
      winnerRemovedFiles: Seq[RemoveFile],
      tablePath: Path,
      hadoopConf: Configuration,
      budgets: RebaseBudgets): RebaseResult = {
    val stats = new MutableRebaseStats

    def aborted(failure: RebaseFailure): RebaseResult = RebaseResult(
      status = RebaseStatus.Aborted,
      newActions = loserActions,
      resolvedAddFiles = Seq.empty,
      resolvedRemoveFiles = Seq.empty,
      failure = Some(failure),
      stats = stats.result)

    val loserRemovesByPath = loserActions.collect { case r: RemoveFile => r }.groupBy(_.path)
    val loserAddsByPath = loserActions.collect { case a: AddFile => a }.groupBy(_.path)
    val winnerRemovesByPath = winnerRemovedFiles.groupBy(_.path)
    val winnerAddsByPath = winnerAddedFiles.groupBy(_.path)
    val sharedPaths = loserRemovesByPath.keySet.intersect(winnerRemovesByPath.keySet).toSeq.sorted

    if (sharedPaths.isEmpty) {
      return RebaseResult(
        status = RebaseStatus.NoSharedPaths,
        newActions = loserActions,
        resolvedAddFiles = Seq.empty,
        resolvedRemoveFiles = Seq.empty,
        failure = None,
        stats = stats.result)
    }

    val candidates = mutable.ArrayBuffer.empty[RebaseCandidate]
    sharedPaths.foreach { path =>
      val loserRemoves = loserRemovesByPath(path)
      val loserAdds = loserAddsByPath.getOrElse(path, Seq.empty)
      val winnerRemoves = winnerRemovesByPath(path)
      val winnerAdds = winnerAddsByPath.getOrElse(path, Seq.empty)

      if (winnerAdds.isEmpty) return aborted(RebaseFailure.WinnerCowOrCompacted)
      if (winnerRemoves.size != 1 || winnerAdds.size != 1) {
        return aborted(RebaseFailure.WinnerNotDvOnly)
      }
      if (loserRemoves.size != 1 || loserAdds.size != 1) {
        return aborted(RebaseFailure.LoserNotDvOnly)
      }

      val candidate = RebaseCandidate(
        path,
        loserRemoves.head,
        loserAdds.head,
        winnerRemoves.head,
        winnerAdds.head)
      if (dvOrEmpty(candidate.loserRemove.deletionVector) !=
          dvOrEmpty(candidate.winnerRemove.deletionVector)) {
        return aborted(RebaseFailure.DifferentBaseDv)
      }
      candidates += candidate
    }

    val distinctDvs = candidates.iterator.flatMap { candidate =>
      Iterator(
        dvOrEmpty(candidate.winnerRemove.deletionVector),
        dvOrEmpty(candidate.winnerAdd.deletionVector),
        dvOrEmpty(candidate.loserAdd.deletionVector))
    }.toSet
    if (distinctDvs.exists(_.sizeInBytes > budgets.maxDvBytesPerFile)) {
      return aborted(RebaseFailure.ByteBudgetExceeded)
    }
    if (distinctDvs.count(_.isOnDisk) > budgets.maxDvReads) {
      return aborted(RebaseFailure.DvReadBudgetExceeded)
    }

    if (deadlineExceeded(budgets)) return aborted(RebaseFailure.DeadlineExceeded)

    val dvStore = try {
      DeletionVectorStore.createInstance(hadoopConf)
    } catch {
      case NonFatal(_) => return aborted(RebaseFailure.DvReadFailure)
    }
    val decoded = mutable.HashMap.empty[DeletionVectorDescriptor, RoaringBitmapArray]

    def decodeDv(
        dv: DeletionVectorDescriptor): Either[RebaseFailure, RoaringBitmapArray] = {
      decoded.get(dv) match {
        case Some(bitmap) => Right(bitmap)
        case None =>
          if (deadlineExceeded(budgets)) return Left(RebaseFailure.DeadlineExceeded)
          if (dv.isOnDisk) {
            stats.numDvFilesRead += 1
            stats.numDvBytesRead += dv.sizeInBytes
          }
          val bitmap = try {
            dvStore.read(dv, tablePath)
          } catch {
            case _: IOException => return Left(RebaseFailure.DvReadFailure)
            case NonFatal(_) => return Left(RebaseFailure.DecodeFailure)
          }
          if (deadlineExceeded(budgets)) return Left(RebaseFailure.DeadlineExceeded)
          decoded(dv) = bitmap
          Right(bitmap)
      }
    }

    val prepared = mutable.ArrayBuffer.empty[PreparedRebase]
    candidates.foreach { candidate =>
      val prior = decodeDv(dvOrEmpty(candidate.winnerRemove.deletionVector)) match {
        case Right(bitmap) => bitmap
        case Left(failure) => return aborted(failure)
      }
      val winner = decodeDv(dvOrEmpty(candidate.winnerAdd.deletionVector)) match {
        case Right(bitmap) => bitmap
        case Left(failure) => return aborted(failure)
      }
      val loser = decodeDv(dvOrEmpty(candidate.loserAdd.deletionVector)) match {
        case Right(bitmap) => bitmap
        case Left(failure) => return aborted(failure)
      }

      val priorMinusWinner = prior.copy()
      priorMinusWinner.andNot(winner)
      if (!priorMinusWinner.isEmpty) return aborted(RebaseFailure.WinnerShrunkDv)

      val priorMinusLoser = prior.copy()
      priorMinusLoser.andNot(loser)
      if (!priorMinusLoser.isEmpty) return aborted(RebaseFailure.LoserShrunkDv)

      val winnerDelta = winner.copy()
      winnerDelta.andNot(prior)
      val loserDelta = loser.copy()
      loserDelta.andNot(prior)
      val intersection = loserDelta.copy()
      intersection.and(winnerDelta)
      if (!intersection.isEmpty) return aborted(RebaseFailure.Overlap)

      val unioned = winner.copy()
      unioned.or(loserDelta)
      prepared += PreparedRebase(candidate, loserDelta, unioned)
    }

    val replacements = mutable.HashMap.empty[String, (RemoveFile, AddFile)]
    prepared.foreach { item =>
      val descriptor = writeDv(item.unionedDv, tablePath, dvStore, budgets, stats) match {
        case Right(dv) => dv
        case Left(failure) => return aborted(failure)
      }
      val newRemove =
        item.candidate.loserRemove.copy(deletionVector = item.candidate.winnerAdd.deletionVector)
      val withNewDv = item.candidate.winnerAdd.copy(deletionVector = descriptor)
      val newAdd =
        if (item.loserDelta.isEmpty) withNewDv else withNewDv.withoutTightBoundStats
      replacements(item.candidate.path) = (newRemove, newAdd)
    }

    val newActions = loserActions.map {
      case remove: RemoveFile if replacements.contains(remove.path) =>
        replacements(remove.path)._1
      case add: AddFile if replacements.contains(add.path) =>
        replacements(add.path)._2
      case action => action
    }
    RebaseResult(
      status = RebaseStatus.Succeeded,
      newActions = newActions,
      resolvedAddFiles = candidates.map(_.winnerAdd).toSeq,
      resolvedRemoveFiles = candidates.map(_.winnerRemove).toSeq,
      failure = None,
      stats = stats.result)
  }

  /**
   * Convenience overload for callers that only need a per-DV byte budget and no read or
   * time limits. Used by unit tests that exercise rebase semantics in isolation.
   */
  def tryRebase(
      loserActions: Seq[Action],
      winnerAddedFiles: Seq[AddFile],
      winnerRemovedFiles: Seq[RemoveFile],
      tablePath: Path,
      hadoopConf: Configuration,
      maxDvBytes: Long): RebaseResult =
    tryRebase(
      loserActions = loserActions,
      winnerAddedFiles = winnerAddedFiles,
      winnerRemovedFiles = winnerRemovedFiles,
      tablePath = tablePath,
      hadoopConf = hadoopConf,
      budgets = RebaseBudgets.unbounded.copy(maxDvBytesPerFile = maxDvBytes))

  private def dvOrEmpty(dv: DeletionVectorDescriptor): DeletionVectorDescriptor =
    if (dv == null) DeletionVectorDescriptor.EMPTY else dv

  private def deadlineExceeded(budgets: RebaseBudgets): Boolean =
    budgets.nanoTime() > budgets.deadlineNanos

  private def writeDv(
      bitmap: RoaringBitmapArray,
      tablePath: Path,
      dvStore: DeletionVectorStore,
      budgets: RebaseBudgets,
      stats: MutableRebaseStats): Either[RebaseFailure, DeletionVectorDescriptor] = {
    if (deadlineExceeded(budgets)) return Left(RebaseFailure.DeadlineExceeded)
    if (bitmap.isEmpty) return Right(DeletionVectorDescriptor.EMPTY)

    val serialized = try {
      DeletionVectorUtils.serialize(
        bitmap,
        RoaringBitmapArrayFormat.Portable,
        tablePath = Some(tablePath))
    } catch {
      case NonFatal(_) => return Left(RebaseFailure.DvWriteFailure)
    }
    if (deadlineExceeded(budgets)) return Left(RebaseFailure.DeadlineExceeded)

    val descriptor = try {
      val tablePathWithFS = dvStore.pathWithFileSystem(tablePath)
      val fileId = UUID.randomUUID()
      val writer = dvStore.createWriter(dvStore.generateFileNameInTable(tablePathWithFS, fileId))
      val range = try {
        writer.write(serialized)
      } finally {
        writer.close()
      }
      stats.numDvFilesWritten += 1
      DeletionVectorDescriptor.onDiskWithRelativePath(
        id = fileId,
        sizeInBytes = serialized.length,
        cardinality = bitmap.cardinality,
        offset = Some(range.offset))
    } catch {
      case NonFatal(_) => return Left(RebaseFailure.DvWriteFailure)
    }
    if (deadlineExceeded(budgets)) Left(RebaseFailure.DeadlineExceeded) else Right(descriptor)
  }
}
