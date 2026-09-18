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

package org.apache.spark.sql.delta.amt

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaErrors, DeltaLog, DeltaOperations, FullAMTWriteFailedWithConflict, LogSegment, MaintenanceOperation, Snapshot, WinningCommitMetrics}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint, FileAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.SparkSession

/**
 * Describes a trigger for AMT.
 *
 * @param name          stable string recorded in metrics.
 * @param isIncremental whether the manifest tree is rebuilt incrementally (true) or fully
 *                      re-materialized from the live file set (false).
 */
sealed abstract class AMTTriggerMode(
    val name: String,
    val isIncremental: Boolean) {
  override def toString: String = name
}

object AMTTriggerMode {
  /** Commits since the last AMT reached the checkpoint interval: incremental follow-up rewrite. */
  case object CheckpointIntervalIncremental extends AMTTriggerMode(
      name = "CHECKPOINT_INTERVAL_INCREMENTAL",
      isIncremental = true)

  /** Commits since the last full AMT reached the full checkpoint interval: full rewrite. */
  case object CheckpointIntervalFull extends AMTTriggerMode(
    name = "CHECKPOINT_INTERVAL_FULL",
    isIncremental = false)

  /** A large business commit writes its AMT inline: incremental inline rewrite. */
  case object InlineWithLargeCommitIncremental extends AMTTriggerMode(
    name = "INLINE_WITH_LARGE_COMMIT_INCREMENTAL",
    isIncremental = true)

  /**
   * An on-demand `DeltaLog.checkpoint` request on an AMT table: full rewrite. Such a request
   * is done by callers like `commitLarge` e.g. RESTORE / CLONE that typically just replaced
   * the file set wholesale, so there is nothing useful to build on incrementally.
   */
  case object OnDemandCheckpointFull extends AMTTriggerMode(
    name = "ON_DEMAND_CHECKPOINT_FULL",
    isIncremental = false)
}

/** Metrics for one complete round of AMT conflict resolution. */
case class AMTConflictResolutionRoundMetrics(
    losingAttemptVersion: Long,
    nextAttemptVersion: Long,
    losingCommitType: String,
    losingTreeType: Option[String],
    winningCommits: WinningAMTCommitSummary,
    backRefRebaseMetrics: BackRefRebaseMetrics,
    var treeOutcome: String,
    var winnerTreeSatisfiesRequirement: Option[Boolean],
    var exceptionThrown: Option[String]) {

  def updateOutcome(
      treeOutcome: String,
      winnerTreeSatisfiesRequirement: Option[Boolean] = None,
      exceptionThrown: Option[String] = None): Unit = {
    this.treeOutcome = treeOutcome
    this.winnerTreeSatisfiesRequirement = winnerTreeSatisfiesRequirement
    this.exceptionThrown = exceptionThrown
  }
}

object AMTConflictResolutionRoundMetrics {
  /** For field [[AMTConflictResolutionRoundMetrics.losingCommitType]]. */
  val LOG_ONLY = "LOG_ONLY"
  val INLINE_INCREMENTAL = "INLINE_INCREMENTAL"
  val INCREMENTAL_CHECKPOINT = "INCREMENTAL_CHECKPOINT"
  val FULL_CHECKPOINT = "FULL_CHECKPOINT"

  /** For field [[AMTConflictResolutionRoundMetrics.losingTreeType]]. */
  val INCREMENTAL_TREE = "INCREMENTAL_TREE"
  val FULL_TREE = "FULL_TREE"

  /** For field [[AMTConflictResolutionRoundMetrics.treeOutcome]]. */
  val NO_TREE_REBASE = "NO_TREE_REBASE"
  val REBUILT_INLINE_TREE = "REBUILT_INLINE_TREE"
  val REUSED_LOSING_TREE = "REUSED_LOSING_TREE"
  val REGENERATE_VIA_TXN_RETRY = "REGENERATE_VIA_TXN_RETRY"
  val SKIP_WINNER_SATISFIES_REQUIREMENT = "SKIP_WINNER_SATISFIES_REQUIREMENT"
}

/** Aggregated shape of all winning commits processed in one conflict-checking round. */
case class WinningAMTCommitSummary(
    firstVersion: Long,
    lastVersion: Long,
    numLogOnly: Int,
    numInlineIncremental: Int,
    numIncrementalCheckpoints: Int,
    numFullCheckpoints: Int,
    allLogWinnersPreserveLosingTree: Option[Boolean])

/** Metrics emitted for one AMT write or conflict-resolution round. */
case class AMTMetrics(
    txnId: String,
    roundId: Int,
    var conflictResolutionMetrics: Option[AMTConflictResolutionRoundMetrics],
    var singleAMTWriteMetrics: Option[SingleAMTWriteMetrics])

/** AMT metrics attached to the successful commit's commit stats. */
case class AMTCommitStats(
    contentRootVersion: Long,
    lastAMTWriteMetrics: SingleAMTWriteMetrics,
    includeActionsInCommitJson: Boolean)

/** Back-reference rebase outcome and optional details for one conflict-resolution round. */
case class BackRefRebaseMetrics(
    skipped: Boolean,
    skipReason: Option[String],
    oldAMTVersion: Option[Long] = None,
    newAMTVersion: Option[Long] = None,
    totalTimeTakenMs: Option[Long] = None,
    numActionsReusingBackref: Option[Int] = None,
    numActionsRegeneratingBackref: Option[Int] = None)

object BackRefRebaseMetrics {
  /** Values for field [[BackRefRebaseMetrics.skipReason]]. */
  val SKIP_REASON_NO_NEW_TREE = "NO_NEW_TREE"
  val SKIP_REASON_BLIND_APPEND = "BLIND_APPEND"
  val SKIP_REASON_NO_FILE_ACTIONS = "NO_FILE_ACTIONS"
  val SKIP_REASON_ALREADY_REBASED = "ALREADY_REBASED"
  val SKIP_REASON_NO_AMT_PROVIDER = "NO_AMT_PROVIDER"

  /** Creates metrics for a skipped back-reference rebase. */
  def apply(skipReason: String): BackRefRebaseMetrics =
    new BackRefRebaseMetrics(skipped = true, skipReason = Some(skipReason))

  /** Creates metrics for an executed back-reference rebase. */
  def apply(
      oldAMTVersion: Long,
      newAMTVersion: Long,
      totalTimeTakenMs: Long,
      numActionsReusingBackref: Int,
      numActionsRegeneratingBackref: Int): BackRefRebaseMetrics =
    new BackRefRebaseMetrics(
      skipped = false,
      skipReason = None,
      oldAMTVersion = Some(oldAMTVersion),
      newAMTVersion = Some(newAMTVersion),
      totalTimeTakenMs = Some(totalTimeTakenMs),
      numActionsReusingBackref = Some(numActionsReusingBackref),
      numActionsRegeneratingBackref = Some(numActionsRegeneratingBackref))
}

/** Metrics for a single AMT write attempt (one per commit attempt that materializes a tree). */
case class SingleAMTWriteMetrics(
    trigger: String,
    incremental: String,
    materializeDurationMs: Long,
    // Detailed shape breakdown of an incremental write; None for a full rewrite.
    incrementalWriteMetrics: Option[IncrementalAMTWriteMetrics] = None)

case class IncrementalAMTWriteMetrics(
    numIntermediateCommits: Int,
    numOldLeavesUpdated: Int,
    numOldLeavesUntouched: Int,
    numNewLeaves: Int,
    // Per-status breakdown over root-resident DATA entries (see [[Tracking.Status]]).
    numRootEntriesAddedStatus: Int,
    numRootEntriesExistingStatus: Int,
    numRootEntriesModifiedStatus: Int,
    numRootEntriesReplacedStatus: Int,
    numRootEntriesDeletedStatus: Int,
    numLeafMdvBitsAdded: Int,
    numLeafDeleteCDFBitsAdded: Int = 0,
    numLeafReplaceCDFBitsAdded: Int = 0,
    // Per-status breakdown over all leaf pointers in the new tree (see [[Tracking.Status]]), plus
    // the stale DELETED tombstones from the previous tree that this rewrite dropped.
    numLeavesAddedStatus: Int = 0,
    numLeavesExistingStatus: Int = 0,
    numLeavesModifiedStatus: Int = 0,
    numLeavesDeletedStatus: Int = 0,
    numStaleDeletedLeavesDropped: Int = 0)

/**
 * The outcome of an AMT write for a single commit attempt.
 *
 * @param contentRootVersion          the table version the manifest tree describes
 * @param checkpoint                  the inline [[Checkpoint]] action to embed in the commit JSON
 * @param leaves                      the root's `DATA_MANIFEST` pointer entries, one per leaf
 * @param includeActionsInCommitJson  whether the transaction should still write the commit's file
 *                                    actions inline in the commit JSON.
 * @param amtWriteMetrics             metrics for the materialization that produced this result
 */
case class AMTWriteResult(
    contentRootVersion: Long,
    checkpoint: Checkpoint,
    leaves: Seq[DataManifestEntry],
    includeActionsInCommitJson: Boolean,
    amtWriteMetrics: SingleAMTWriteMetrics)

/** A lazily-materialized [[AMTCheckpointProvider]] for `checkpointOpt`. */
class LazyAMTCheckpointProvider(
    checkpointOpt: Option[Checkpoint],
    readSnapshot: Snapshot,
    manifestCommitVersion: Long) {
  lazy val providerOpt: Option[AMTCheckpointProvider] = checkpointOpt.map { checkpoint =>
    readSnapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider if amt.checkpointAction.version == checkpoint.version => amt
      case _ =>
        AMTCheckpointProvider.fromCheckpoint(
          readSnapshot.deltaLog, checkpoint, manifestCommitVersion)
    }
  }
}

/**
 * Orchestrates write of an AMT for a given transaction (including reattempts on a conflict).
 * There is one [[AMTWriterManager]] per `OptimisticTransaction`.
 *
 * On the first attempt:
 *  - [[writeAMT]] must be invoked. It writes a new AMT if needed, returns the optional
 *    [[AMTWriteResult]], and sends a usage log when it writes a tree.
 *
 * On each conflict:
 *  - [[updatePreCommitLatestAMTCheckpointProvider]] updates the cached AMT provider after conflict
 *    resolution folds the winning commits.
 *  - [[rebaseBackReferences]] re-derives back references if needed. Its metrics are retained in
 *    `pendingBackRefRebaseMetrics` until the attempt is prepared.
 *  - [[writeAMT]] must be invoked again. It reuses or writes an AMT as needed, returns the optional
 *    [[AMTWriteResult]], and sends the usage log for the conflict round. The event combines the
 *    pending back-reference metrics with metrics for any new tree written by that attempt.
 */
class AMTWriterManager(
    private val txnId: String,
    readSnapshot: Snapshot,
    initialOperation: DeltaOperations.Operation) extends DeltaLogging {

  import AMTConflictResolutionRoundMetrics._
  import BackRefRebaseMetrics._

  private def spark: SparkSession = SparkSession.active
  private def deltaLog: DeltaLog = readSnapshot.deltaLog

  private var lastAMTWriteResultOpt: Option[AMTWriteResult] = None
  private var nextRoundId = 0

  /** The read snapshot's own AMT checkpoint, if it is AMT-backed. */
  private def readSnapshotAMTCheckpointOpt: Option[Checkpoint] = {
    if (!AMTUtils.amtEnabled(readSnapshot)) return None
    readSnapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt.checkpointAction)
      case _ => None
    }
  }

  /**
   * The AMT Checkpoint Provider corresponding to the last manifest commit corresponding to
   * OptimisticTransaction.preCommitLogSegment.
   * This is updated after every round of [[ConflictChecker]] rebase.
   */
  private var preCommitLatestAMTCheckpointProvider: LazyAMTCheckpointProvider =
    new LazyAMTCheckpointProvider(readSnapshotAMTCheckpointOpt, readSnapshot, readSnapshot.version)

  /**
   * The folded-latest AMT provider the committed actions' back references are stamped against after
   * conflict resolution.
   */
  private[delta] def preCommitLatestAMTCheckpointProviderOpt: Option[AMTCheckpointProvider] =
    preCommitLatestAMTCheckpointProvider.providerOpt

  /** The folded AMT tree version the committed actions were last re-stamped against. */
  private var lastRebasedAMTVersion: Option[Long] = None

  /** The version this transaction's previous attempt targeted. */
  private var lastAttemptVersion: Long = readSnapshot.version + 1

  private var pendingBackRefRebaseMetrics =
    BackRefRebaseMetrics(SKIP_REASON_NO_NEW_TREE)

  /**
   * Builds the AMT write for a commit attempt, or `None` when no AMT should be written. Serves both
   * the first attempt and any conflict-resolution retry.
   *
   * @param nextAttemptVersion  the version this attempt targets
   * @param currentTransactionInfo the in-flight transaction (its actions, protocol, metadata)
   * @param preCommitLogSegment the log segment prior to this commit
   * @param winningCommitMetricsForConflictedRange per-winning-commit metrics from conflict
   *   resolution, one per commit in the conflicted range [lastAttemptVersion, nextAttemptVersion)
   * @return the AMT write result; `None` if no AMT write is triggered or it's a non-AMT table.
   */
  def writeAMT(
      nextAttemptVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      preCommitLogSegment: LogSegment,
      winningCommitMetricsForConflictedRange: Seq[WinningCommitMetrics]): Option[AMTWriteResult] = {
    if (!AMTUtils.amtEnabled(readSnapshot)) return None
    val actionsToCommit = currentTransactionInfo.actions
    val roundId = nextRoundId
    nextRoundId += 1
    val metrics = AMTMetrics(
      txnId = txnId,
      roundId = roundId,
      conflictResolutionMetrics = None,
      singleAMTWriteMetrics = None)
    // Whether this attempt would (re)write a manifest tree.
    val writesTree = initialOperation match {
      case _: DeltaOperations.OptimizeCheckpoint => true
      case _ => shouldDoInlineIncrementalCheckpoint(actionsToCommit)
    }

    val rebasing = preCommitLogSegment.version > readSnapshot.version
    if (rebasing) {
      validateWinningCommitMetrics(
        winningCommitMetricsForConflictedRange, lastAttemptVersion, nextAttemptVersion)
      initializeAMTMetricsDuringRebase(
        metrics, nextAttemptVersion, winningCommitMetricsForConflictedRange)
      // A concurrent commit won our target version and we are rebasing. In the table below a
      // "new-tree commit" is a winner that installed a new AMT tree (an OPTIMIZE checkpoint or a
      // large inline commit); scenarios handled:
      //   Winning commit  | Losing commit          | Action taken
      //   Log commit      | Log commit             | usual conflict checking; back refs stay valid
      //   Log commit      | Inline AMT commit      | rebuild the inline tree; back refs stay valid
      //   New-tree commit | Log commit             | rebase onto the new tree; re-derive back refs
      //   New-tree commit | Inline AMT commit      | re-seat + rebuild; re-derive back refs
      //   Log commit      | Full AMT commit        | reuse the base as-is, else regenerate on retry
      //   Log commit      | Incremental AMT commit | reuse the base as-is, else regenerate on retry
    }

    def materializeNewTree(incremental: Boolean, trigger: String): AMTWriteResult = {
      val result = materialize(
        nextAttemptVersion,
        currentTransactionInfo,
        preCommitLogSegment,
        incremental,
        trigger)
      metrics.singleAMTWriteMetrics = Some(result.amtWriteMetrics)
      result
    }

    def writeTree(): Option[AMTWriteResult] = initialOperation match {
      case _: DeltaOperations.OptimizeCheckpoint if rebasing =>
        // A concurrent commit won our target version: reuse the already-written base tree as-is
        // or signal a full-AMT regenerate.
        handleLosingOptimizeCheckpoint(
          preCommitLogSegment,
          currentTransactionInfo,
          winningCommitMetricsForConflictedRange,
          metrics)
      case optimize: DeltaOperations.OptimizeCheckpoint =>
        assert(actionsToCommit.isEmpty,
          s"OPTIMIZE checkpoint commit must carry no actions, got ${actionsToCommit.size}.")
        // An incremental rewrite must extend an existing tree, so the first AMT is always a full
        // rewrite even when the trigger requested incremental (e.g. the JSON-size threshold).
        val incremental =
          optimize.incremental && AMTWriteHelper.previousAMTContentRoot(readSnapshot).isDefined
        Some(materializeNewTree(incremental, optimize.triggerName))
      case _ if shouldDoInlineIncrementalCheckpoint(actionsToCommit) =>
        // A large business commit rebuilds its manifest tree inline (incrementally).
        val mode = AMTTriggerMode.InlineWithLargeCommitIncremental
        val result = materializeNewTree(mode.isIncremental, mode.name)
        metrics.conflictResolutionMetrics.foreach(_.updateOutcome(REBUILT_INLINE_TREE))
        Some(result)
      case _ =>
        // A commit that writes no tree emits no AMT.
        assert(!writesTree,
          s"writeAMT reached the no-tree branch for a tree-writing commit: $initialOperation.")
        None
    }

    val resultOpt = try {
      val result = writeTree()
      recordAMTMetrics(metrics, rebasing)
      result
    } catch {
      case e: FullAMTWriteFailedWithConflict if rebasing =>
        recordAMTMetrics(metrics, rebasing)
        throw e
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog,
          opType = AMTUsageLogs.WRITE_FAILED,
          data = Map(
            "exception" -> e.getMessage,
            "stackTrace" -> e.getStackTrace.take(30).mkString("\n\t")))
        throw e
    }
    lastAMTWriteResultOpt = resultOpt
    // Advance the `lastAttemptVersion` so that if we get a conflict again, we rebase
    // starting from [lastAttemptVersion, ...].
    lastAttemptVersion = nextAttemptVersion
    resultOpt
  }

  /**
   * Asserts the winning commit metrics cover exactly the conflicted range [lastAttemptVersion,
   * nextAttemptVersion) -- one per concurrent winner this attempt lost to.
   */
  private def validateWinningCommitMetrics(
      winningCommitMetrics: Seq[WinningCommitMetrics],
      lastAttemptVersion: Long,
      nextAttemptVersion: Long): Unit = {
    assert(
      winningCommitMetrics.size == nextAttemptVersion - lastAttemptVersion,
      s"winning commit metrics (${winningCommitMetrics.size}) must cover exactly the " +
        s"conflicted range [$lastAttemptVersion, $nextAttemptVersion).")
  }

  /**
   * Decides the AMT write for a losing OPTIMIZE checkpoint on a conflict-resolution retry.
   *
   * A losing OPTIMIZE checkpoint carries no user actions, so there is nothing for doCommit to
   * re-stamp. When its first attempt already wrote a tree (describing the read snapshot) and it
   * lost only to log-only winners, and every winner touched only files created strictly after that
   * tree's content-root version (min defaultRowCommitVersion > the base version), the tree is still
   * exact -- recommit it as-is (this holds whether the base was a full or an incremental rewrite).
   * Otherwise a winner changed content the base tree describes, so the tree must be rebuilt: signal
   * [[FullAMTWriteFailedWithConflict]] for the caller (CheckpointHook) to refresh and redo the
   * rewrite from scratch. A winner that installed its own tree still defers via ConcurrentWrite.
   */
  private def handleLosingOptimizeCheckpoint(
      preCommitLogSegment: LogSegment,
      currentTransactionInfo: CurrentTransactionInfo,
      winningCommitMetricsForConflictedRange: Seq[WinningCommitMetrics],
      metrics: AMTMetrics): Option[AMTWriteResult] = {
    lastAMTWriteResultOpt match {
      case Some(baseResult) =>
        if (winningCommitInstalledNewAMTTree(currentTransactionInfo)) {
          // A winner installed its own tree; rebasing the losing checkpoint onto it is deferred.
          throw DeltaErrors.concurrentWriteException(conflictingCommit = None)
        }
        val baseVersion = baseResult.contentRootVersion
        if (winningCommitMetricsForConflictedRange.forall(
            _.allFileActionsHaveDefaultCommitVersionNewerThan(baseVersion))) {
          // Every file action in every winner commit is a new FileAction with
          // defaultRowCommitVersion > Losing FULL AMT's checkpoint version (X)
          // This means, none of them should have backreferences also.
          // Reasoning: if they have backreferences (which points to an old tree pointing
          // to version < X)
          // => they are old files (readded / removed)
          //  => they should have old defaultRowCommitVersion
          // Which contradicts the above check.
          winningCommitMetricsForConflictedRange.foreach { m =>
            deltaAssertAndThrow(
              check = m.numAddFilesWithBackreferences == 0 &&
                m.numRemoveFilesWithBackreferences == 0,
              name = AMTUsageLogs
                .ALERT_SUFFIX_FILE_CONTAINS_NEW_SEQ_NUMBERS_BUT_NON_EMPTY_BACKREFERENCE,
              msg = "A base-preserving winner must carry no back references into the base tree " +
                s"at version $baseVersion, but found ${m.numAddFilesWithBackreferences} Add and " +
                s"${m.numRemoveFilesWithBackreferences} Remove file actions with back references.",
              throwable = new IllegalStateException(
                s"Base-preserving winner has ${m.numAddFilesWithBackreferences} Add and " +
                  s"${m.numRemoveFilesWithBackreferences} Remove file actions with back " +
                  s"references at base version $baseVersion."),
              deltaLog = deltaLog)
          }
          metrics.conflictResolutionMetrics.foreach(_.updateOutcome(REUSED_LOSING_TREE))
          Some(baseResult)
        } else {
          // A winner changed content the base tree describes, so it cannot be reused as-is.
          // The caller should do a retry in this case.
          metrics.conflictResolutionMetrics.foreach { conflictResolutionMetrics =>
            conflictResolutionMetrics.updateOutcome(
              treeOutcome = REGENERATE_VIA_TXN_RETRY,
              exceptionThrown = Some(classOf[FullAMTWriteFailedWithConflict].getSimpleName))
          }
          throw DeltaErrors.fullAMTWriteFailedWithConflict(
            conflictingCommitVersion = preCommitLogSegment.version)
        }
      case None =>
        // An OPTIMIZE checkpoint always materializes a tree on its first attempt, so by the time it
        // rebases there must be a cached result.
        throw new IllegalStateException(
          "A losing OPTIMIZE checkpoint has no cached AMT write from its first attempt.")
    }
  }

  /**
   * Whether this Writer should write its changed actions inline as part of a new AMT.
   * True only when the commit is large enough (by action count) AND the table already has a full
   * AMT to build on.
   */
  private def shouldDoInlineIncrementalCheckpoint(actionsToCommit: Seq[Action]): Boolean =
    actionsToCommit.size.toLong >= largeCommitActionsCountThresholdForInlineManifestCommit &&
      AMTWriteHelper.previousAMTContentRoot(readSnapshot).isDefined

  /** True when there was a winning manifest commit concurrent to this transaction */
  private def winningCommitInstalledNewAMTTree(
      currentTransactionInfo: CurrentTransactionInfo): Boolean = {
    val readSnapshotAMTVersion = readSnapshot.lastManifestCommitOpt.map(_.contentRootVersion)
    val preCommitAMTVersion = currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version)
    (readSnapshotAMTVersion, preCommitAMTVersion) match {
      case (Some(_), None) =>
        throw new IllegalStateException(
          "The read snapshot has an AMT but the winning commits has no AMT -- this can happen " +
            "only during downgrade -- not supported yet")
      case (Some(readVersion), Some(foldedVersion)) if readVersion > foldedVersion =>
        throw new IllegalStateException(
          s"The rebased AMT moved backwards: read-snapshot tree version $readVersion is newer " +
            s"than the folded tree version $foldedVersion.")
      case (readOpt, foldedOpt) => readOpt != foldedOpt
    }
  }

  // Materializes the manifest tree for this commit and records its metrics. An incremental rewrite
  // packs the post-commit live files into leaves in input order on the driver; a full rewrite
  // clusters the read snapshot's live files and flushes them into leaves distributed across
  // executors.
  private def materialize(
      commitVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      preCommitLogSegment: LogSegment,
      incremental: Boolean,
      trigger: String): AMTWriteResult = {
    val amtProviderOpt = preCommitLatestAMTCheckpointProvider.providerOpt
    assert(
      amtProviderOpt.map(_.checkpointAction.version) ==
        currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version),
      s"Cached AMT provider ${amtProviderOpt.map(_.checkpointAction.version)} is out of sync " +
        "with preCommitLatestAMTCheckpointOpt " +
        s"${currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version)}.")
    if (incremental && amtProviderOpt.isDefined) {
      val amtProvider = amtProviderOpt.get
      val oldAMTVersion = amtProvider.checkpointAction.contentRoot.version
      // The commits written after the old AMT, up to the last committed version.
      val intermediateLogCommits = preCommitLogSegment.deltas
        .filter(f => FileNames.getFileVersion(f) > oldAMTVersion)
      new IncrementalAMTWriter(spark, deltaLog).writeIncremental(
        oldAMTActionsProvider = new BaseAMTCheckpointActionsProvider(deltaLog, amtProvider),
        intermediateLogCommits = intermediateLogCommits,
        attemptVersion = commitVersion,
        actionsToCommit = currentTransactionInfo.actions,
        trigger = trigger)
    } else {
      assert(currentTransactionInfo.actions.isEmpty,
        "A full AMT rewrite must carry no actions, got " +
          s"${currentTransactionInfo.actions.size}.")
      AMTWriteHelper.writeFullMaterialization(
        spark = spark,
        readSnapshot = readSnapshot,
        commitVersion = commitVersion,
        postCommitProtocol = currentTransactionInfo.protocol,
        postCommitMetadata = currentTransactionInfo.metadata,
        trigger = trigger)
    }
  }

  private def largeCommitActionsCountThresholdForInlineManifestCommit: Long =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT)

  /**
   * Updates the pre-commit AMTCheckpointProvider after resolving conflicts via [[ConflictChecker]].
   */
  def updatePreCommitLatestAMTCheckpointProvider(
      currentTransactionInfo: CurrentTransactionInfo): Unit = {
    val manifestCommitVersion = currentTransactionInfo.commitInfo
      .flatMap(_.lastManifestCommit).map(_.version)
      .orElse(currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version)).getOrElse(0L)
    preCommitLatestAMTCheckpointProvider = new LazyAMTCheckpointProvider(
      currentTransactionInfo.preCommitLatestAMTCheckpointOpt, readSnapshot, manifestCommitVersion)
  }

  /**
   * Re-derives the file actions' back references against the AMT this attempt builds on. Only runs
   * on a rebase where a winning commit installed a new tree; `reStampBackReferences` re-derives
   * each file action whose back reference that tree invalidated (a leaf it dropped or a position it
   * newly MDV-masked because the file moved or was removed) and leaves the rest -- those still
   * pointing at a live leaf entry -- unchanged. A blind append is skipped entirely: it only adds
   * brand-new files, so none of its actions can point at a leaf the winner's tree invalidated.
   */
  def rebaseBackReferences(
      currentTransactionInfo: CurrentTransactionInfo): CurrentTransactionInfo = {
    pendingBackRefRebaseMetrics =
      BackRefRebaseMetrics(SKIP_REASON_NO_NEW_TREE)
    val actions = currentTransactionInfo.actions
    if (!AMTUtils.amtEnabled(readSnapshot)) {
      return currentTransactionInfo
    }
    if (!winningCommitInstalledNewAMTTree(currentTransactionInfo)) {
      return currentTransactionInfo
    }
    // A blind append only adds brand-new files and reads or removes nothing, so none of its actions
    // reference a leaf the winner's tree could have dropped or MDV-masked. Those new files are
    // absent from the winner's tree and get a fresh back reference when this attempt's own tree
    // folds them in, so skip the re-stamp rather than re-deriving back references that do not exist
    // in the winner's tree.
    if (currentTransactionInfo.commitInfo.flatMap(_.isBlindAppend).getOrElse(false)) {
      pendingBackRefRebaseMetrics =
        BackRefRebaseMetrics(SKIP_REASON_BLIND_APPEND)
      return currentTransactionInfo
    }
    // A commit with no file actions has no back references to re-derive, so short-circuit before
    // materializing the winning tree's (potentially expensive) AMT provider.
    if (!actions.exists(_.isInstanceOf[FileAction])) {
      pendingBackRefRebaseMetrics =
        BackRefRebaseMetrics(SKIP_REASON_NO_FILE_ACTIONS)
      return currentTransactionInfo
    }
    val foldedAMTVersion = currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version)
    if (foldedAMTVersion == lastRebasedAMTVersion) {
      // No new tree was installed since the last rebase, so the actions are already re-stamped
      // against it -- nothing to re-derive.
      pendingBackRefRebaseMetrics =
        BackRefRebaseMetrics(SKIP_REASON_ALREADY_REBASED)
      return currentTransactionInfo
    }
    // The tree the actions were last stamped against: the previous rebase target, or -- on the
    // first rebase -- the read snapshot's own tree, which is what the writer originally stamped.
    val oldAMTVersion = lastRebasedAMTVersion
      .orElse(readSnapshot.lastManifestCommitOpt.map(_.contentRootVersion))
      .getOrElse(0L)
    val foldedContentRootVersion =
      currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.contentRoot.version)
    val providerContentRootVersion =
      preCommitLatestAMTCheckpointProvider.providerOpt.map(_.checkpointAction.contentRoot.version)
    assert(foldedContentRootVersion == providerContentRootVersion,
      "the cached AMT provider must correspond to the transaction's folded AMT checkpoint.")
    val startNs = System.nanoTime()
    val restampedActions = preCommitLatestAMTCheckpointProvider.providerOpt match {
      case Some(provider) =>
        val result = provider.reStampBackReferences(spark, deltaLog, actions)
        val rebaseMetrics = BackRefRebaseMetrics(
          oldAMTVersion = oldAMTVersion,
          newAMTVersion = foldedAMTVersion.getOrElse(oldAMTVersion),
          totalTimeTakenMs =
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs),
          numActionsReusingBackref = result.numActionsReusingBackref,
          numActionsRegeneratingBackref = result.numActionsRegeneratingBackref)
        pendingBackRefRebaseMetrics = rebaseMetrics
        result.actions
      case None =>
        pendingBackRefRebaseMetrics =
          BackRefRebaseMetrics(SKIP_REASON_NO_AMT_PROVIDER)
        actions
    }
    lastRebasedAMTVersion = foldedAMTVersion
    currentTransactionInfo.copy(actions = restampedActions)
  }

  // **************** AMT Metric related helpers ****************

  private def initializeAMTMetricsDuringRebase(
      metrics: AMTMetrics,
      nextAttemptVersion: Long,
      winningCommitMetrics: Seq[WinningCommitMetrics]): Unit = {
    metrics.conflictResolutionMetrics = Some(AMTConflictResolutionRoundMetrics(
      losingAttemptVersion = lastAttemptVersion,
      nextAttemptVersion = nextAttemptVersion,
      losingCommitType = initialOperation match {
        case _: DeltaOperations.OptimizeCheckpoint =>
          if (lastAMTWriteResultOpt.exists(
              _.checkpoint.contentRoot.isIncremental.contains(true))) {
            INCREMENTAL_CHECKPOINT
          } else {
            FULL_CHECKPOINT
          }
        case _ if lastAMTWriteResultOpt.isDefined => INLINE_INCREMENTAL
        case _ => LOG_ONLY
      },
      losingTreeType = lastAMTWriteResultOpt.map { result =>
        if (result.checkpoint.contentRoot.isIncremental.contains(true)) {
          INCREMENTAL_TREE
        } else {
          FULL_TREE
        }
      },
      winningCommits = summarizeWinningCommits(
        winningCommitMetrics, lastAttemptVersion, nextAttemptVersion),
      backRefRebaseMetrics = pendingBackRefRebaseMetrics,
      treeOutcome = NO_TREE_REBASE,
      winnerTreeSatisfiesRequirement = None,
      exceptionThrown = None))
  }

  private def summarizeWinningCommits(
      winningCommitMetrics: Seq[WinningCommitMetrics],
      firstVersion: Long,
      nextAttemptVersion: Long): WinningAMTCommitSummary = {
    var numLogOnly = 0
    var numInlineIncremental = 0
    var numIncrementalCheckpoints = 0
    var numFullCheckpoints = 0
    winningCommitMetrics.foreach { winner =>
      winner.checkpointAction match {
        case None => numLogOnly += 1
        case Some(checkpoint) if checkpoint.contentRoot.isIncremental.contains(false) =>
          numFullCheckpoints += 1
        case Some(_) if winner.numAdds + winner.numRemoves > 0 =>
          numInlineIncremental += 1
        case Some(_) => numIncrementalCheckpoints += 1
      }
    }
    val allWinnersAreLogOnly = numLogOnly == winningCommitMetrics.size
    val allLogWinnersPreserveLosingTree = lastAMTWriteResultOpt.flatMap { result =>
      if (winningCommitMetrics.nonEmpty && allWinnersAreLogOnly) {
        Some(winningCommitMetrics.forall(
          _.allFileActionsHaveDefaultCommitVersionNewerThan(result.contentRootVersion)))
      } else {
        None
      }
    }
    WinningAMTCommitSummary(
      firstVersion = firstVersion,
      lastVersion = nextAttemptVersion - 1,
      numLogOnly = numLogOnly,
      numInlineIncremental = numInlineIncremental,
      numIncrementalCheckpoints = numIncrementalCheckpoints,
      numFullCheckpoints = numFullCheckpoints,
      allLogWinnersPreserveLosingTree = allLogWinnersPreserveLosingTree)
  }

  private def recordAMTMetrics(metrics: AMTMetrics, rebasing: Boolean): Unit = {
    assert(!rebasing || metrics.conflictResolutionMetrics.isDefined)
    if (metrics.conflictResolutionMetrics.isEmpty && metrics.singleAMTWriteMetrics.isEmpty) return
    recordDeltaEvent(
      deltaLog, opType = AMTUsageLogs.CONFLICT_RESOLUTION_ROUND, data = metrics)
    if (rebasing) {
      pendingBackRefRebaseMetrics = BackRefRebaseMetrics(SKIP_REASON_NO_NEW_TREE)
    }
  }

}
object AMTWriterManager {

  /**
   * The maintenance work a committed transaction should schedule for after it commits.
   * The maintenance work will be done by CheckpointHook
   */
  def planMaintenance(
      spark: SparkSession,
      readSnapshot: Snapshot,
      initialOperation: DeltaOperations.Operation,
      commitVersion: Long,
      postCommitSnapshot: Snapshot): MaintenanceOperation = {
    // if the commit itself was to do a checkpoint, don't schedule any maintenance as part
    // of its post-commit hook.
    if (!AMTUtils.amtEnabled(readSnapshot)
        || initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return MaintenanceOperation()
    }


    val amtTriggerModeOpt =
      followUpTriggerMode(spark, readSnapshot, commitVersion, postCommitSnapshot)
    MaintenanceOperation(
      shouldCheckpoint = amtTriggerModeOpt.isDefined,
      amtTriggerModeOpt = amtTriggerModeOpt)
  }

  /**
   * The maintenance work to schedule after a large commit wrote its AMT inline.
   *
   * An inline write is always incremental. If a table keeps getting inline AMTs, we still want it
   * to get a full AMT once in a while when the last full AMT was older than
   * checkpointInterval * fullRewriteCheckpointIntervalMultiplier.
   */
  def planMaintenanceAfterInlineWrite(
      spark: SparkSession,
      readSnapshot: Snapshot,
      initialOperation: DeltaOperations.Operation,
      commitVersion: Long,
      postCommitSnapshot: Snapshot): MaintenanceOperation = {
    // The follow-up OPTIMIZE CHECKPOINT commit itself must never schedule more maintenance.
    if (!AMTUtils.amtEnabled(readSnapshot)
        || initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return MaintenanceOperation()
    }
    val checkpointInterval = readSnapshot.deltaLog.checkpointInterval(postCommitSnapshot.metadata)
    if (isFullCheckpointOverdue(spark, commitVersion, postCommitSnapshot, checkpointInterval)) {
      MaintenanceOperation(
        shouldCheckpoint = true,
        amtTriggerModeOpt = Some(AMTTriggerMode.CheckpointIntervalFull))
    } else {
      MaintenanceOperation()
    }
  }

  /** [[AMTTriggerMode]] for a followup AMT Checkpoint commit if any. */
  private def followUpTriggerMode(
      spark: SparkSession,
      readSnapshot: Snapshot,
      commitVersion: Long,
      postCommitSnapshot: Snapshot): Option[AMTTriggerMode] = {
    val checkpointInterval = readSnapshot.deltaLog.checkpointInterval(postCommitSnapshot.metadata)
    // -- case-1 --
    // Assume v0 has an AMT. This is to make sure future AMTs land on even boundaries
    // e.g. 10/20/30 instead of 9/19/29 (as classic checkpoints do).
    val lastCheckpointVersion = postCommitSnapshot.logSegment.checkpointProvider.version
    val lastAMTVersion = math.max(0L, lastCheckpointVersion)
    val versionDiff = commitVersion - lastAMTVersion
    // Emit only on the exact interval boundary (versionDiff a positive multiple of the interval),
    // not >= the interval. This is what CheckpointTrigger does: if v10's follow-up AMT has not
    // landed yet, a racing v11 still sees lastAMTVersion == 0, but 11 % 10 != 0 so it does not
    // re-trigger; only v10, v20, ... do.
    if (versionDiff > 0 && versionDiff % checkpointInterval == 0) {
      // If checkpointInterval is 200 and fullRewriteCheckpointIntervalMultiplier is 5
      // Then if 10220 is full tree, then 10420, 10620, 10820, 11020 will be incremental
      // and then 11220 will be full tree again.
      val fullRewriteSpan =
        checkpointInterval.toLong * fullRewriteCheckpointIntervalMultiplier(spark)
      val needsFullRewrite = AMTWriteHelper.previousAMTContentRoot(postCommitSnapshot)
        .flatMap(_.lastManifestCommitWithFullRewrite)
        .forall(lastFull => commitVersion - lastFull >= fullRewriteSpan)
      return Some(
        if (needsFullRewrite) {
          AMTTriggerMode.CheckpointIntervalFull
        } else {
          AMTTriggerMode.CheckpointIntervalIncremental
        })
    }

    // -- case-1b --
    // Backstop for an overdue full rewrite off the interval boundary. case-1 only fires at an
    // interval boundary relative to the last AMT, and interval-boundary commits can be inlined.
    // The inline path i.e. [[planMaintenanceAfterInlineWrite]] only schedules a full when it lands
    // exactly on the full-rewrite cadence i.e. if checkpoint interval=10 and multiplier = 5 and
    // last full is at 14 and then we say always have inline AMTs except 64/114/164/214 etc.). Such
    // a table would never take case-1 and never get a follow-up full rewrite. Anchor this check to
    // the last full rewrite (not the last AMT) and gate it on fullRewriteSpan: it fires the first
    // version a full span has elapsed, and a racing follow-up that has not landed yet does not
    // re-trigger on the very next commit (only once per interval), matching case-1's racing
    // behavior.
    if (isFullCheckpointOverdue(spark, commitVersion, postCommitSnapshot, checkpointInterval)) {
      return Some(AMTTriggerMode.CheckpointIntervalFull)
    }


    None
  }

  /**
   * Whether a full rewrite is overdue at `commitVersion`: a full span has elapsed since the last
   * full rewrite AND `commitVersion` sits on an interval boundary relative to that anchor. The
   * boundary gate keeps this racing-safe -- while a scheduled follow-up is in flight it re-triggers
   * at most once per interval, not on every commit -- matching `followUpTriggerMode`'s case-1.
   */
  private def isFullCheckpointOverdue(
      spark: SparkSession,
      commitVersion: Long,
      postCommitSnapshot: Snapshot,
      checkpointInterval: Long): Boolean = {
    val fullRewriteSpan = checkpointInterval * fullRewriteCheckpointIntervalMultiplier(spark)
    AMTWriteHelper.previousAMTContentRoot(postCommitSnapshot)
      .flatMap(_.lastManifestCommitWithFullRewrite)
      .exists { lastFull =>
        val versionsSinceFull = commitVersion - lastFull
        versionsSinceFull > 0 && versionsSinceFull % checkpointInterval == 0 &&
          versionsSinceFull >= fullRewriteSpan
      }
  }

  private def fullRewriteCheckpointIntervalMultiplier(spark: SparkSession): Int =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER)
}
