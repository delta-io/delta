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

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaErrors, DeltaLog, DeltaOperations, LogSegment, MaintenanceOperation, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint, FileAction}
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
}

/** Aggregated AMT metrics collected across all attempts of a single [[AMTWriterManager]]. */
case class AMTMetrics(
    private[delta] var writeAttempts: Seq[SingleAMTWriteMetrics] = Seq.empty,
    private[delta] var backrefRebaseAttempts: Seq[BackRefRebaseMetrics] = Seq.empty)

/** Metrics for one back-reference rebase pass (see [[AMTWriterManager.rebaseBackReferences]]). */
case class BackRefRebaseMetrics(
    oldAMTVersion: Long,
    newAMTVersion: Long,
    totalTimeTakenMs: Long,
    numActionsReusingBackref: Int,
    numActionsRegeneratingBackref: Int)

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
 */
case class AMTWriteResult(
    contentRootVersion: Long,
    checkpoint: Checkpoint,
    leaves: Seq[DataManifestEntry],
    includeActionsInCommitJson: Boolean)

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
 */
class AMTWriterManager(
    readSnapshot: Snapshot,
    initialOperation: DeltaOperations.Operation) {

  private def spark: SparkSession = SparkSession.active
  private def deltaLog: DeltaLog = readSnapshot.deltaLog

  val metrics = AMTMetrics()
  private var lastAMTWriteResultOpt: Option[AMTWriteResult] = None

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

  /** The folded AMT tree version the committed actions were last re-stamped against. */
  private var lastRebasedAMTVersion: Option[Long] = None

  /**
   * Builds the AMT write for a commit attempt, or `None` when no AMT should be written. Serves both
   * the first attempt and any conflict-resolution retry.
   *
   * @param commitVersion       the version this attempt targets
   * @param currentTransactionInfo the in-flight transaction (its actions, protocol, metadata)
   * @param preCommitLogSegment the log segment prior to this commit
   * @return the AMT write result; `None` if no AMT write is triggered or it's a non-AMT table.
   */
  def writeAMT(
      commitVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      preCommitLogSegment: LogSegment): Option[AMTWriteResult] = {
    if (!AMTUtils.amtEnabled(readSnapshot)) return None
    val actionsToCommit = currentTransactionInfo.actions
    // Whether this attempt would (re)write a manifest tree.
    val writesTree = initialOperation match {
      case _: DeltaOperations.OptimizeCheckpoint => true
      case _ => shouldDoInlineIncrementalCheckpoint(actionsToCommit)
    }

    if (preCommitLogSegment.version > readSnapshot.version) {
      // A concurrent commit won our target version and we are rebasing. In the table below a
      // "new-tree commit" is a winner that installed a new AMT tree (an OPTIMIZE checkpoint or a
      // large inline commit); scenarios handled:
      //   Winning commit  | Losing commit     | Action taken
      //   Log commit      | Log commit        | usual conflict checking; back refs stay valid
      //   Log commit      | Inline AMT commit | rebuild the inline tree; back refs stay valid
      //   New-tree commit | Log commit        | rebase onto the new tree; re-derive back refs
      //   New-tree commit | Inline AMT commit | re-seat + rebuild; re-derive back refs
      // All other scenarios are not handled.
      val losingOptimizeCheckpoint =
        initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]
      if (losingOptimizeCheckpoint) {
        throw DeltaErrors.concurrentWriteException(conflictingCommit = None)
      }
    }

    val resultOpt = initialOperation match {
      case optimize: DeltaOperations.OptimizeCheckpoint =>
        assert(actionsToCommit.isEmpty,
          s"OPTIMIZE checkpoint commit must carry no actions, got ${actionsToCommit.size}.")
        // An incremental rewrite must extend an existing tree, so the first AMT is always a full
        // rewrite even when the trigger requested incremental (e.g. the JSON-size threshold).
        val incremental =
          optimize.incremental && AMTWriteHelper.previousAMTContentRoot(readSnapshot).isDefined
        Some(materialize(
          commitVersion, currentTransactionInfo, preCommitLogSegment,
          incremental = incremental, trigger = optimize.triggerName))
      case _ if shouldDoInlineIncrementalCheckpoint(actionsToCommit) =>
        // A large business commit rebuilds its manifest tree inline (incrementally).
        val mode = AMTTriggerMode.InlineWithLargeCommitIncremental
        Some(materialize(
          commitVersion, currentTransactionInfo, preCommitLogSegment,
          incremental = mode.isIncremental, trigger = mode.name))
      case _ =>
        // A commit that writes no tree emits no AMT.
        assert(!writesTree,
          s"writeAMT reached the no-tree branch for a tree-writing commit: $initialOperation.")
        None
    }
    lastAMTWriteResultOpt = resultOpt
    resultOpt
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
    val (result, singleMetric) =
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
    metrics.writeAttempts :+= singleMetric
    result
  }

  /**
   * The maintenance work a committed transaction should schedule for after it commits.
   * The maintenance work will be done by CheckpointHook
   */
  def planMaintenance(
      commitVersion: Long,
      postCommitSnapshot: Snapshot): MaintenanceOperation = {
    // if the commit itself was to do a checkpoint, don't schedule any maintenance as part
    // of its post-commit hook.
    if (!AMTUtils.amtEnabled(readSnapshot)
        || initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return MaintenanceOperation()
    }


    val amtTriggerModeOpt = followUpTriggerMode(commitVersion, postCommitSnapshot)
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
      commitVersion: Long,
      postCommitSnapshot: Snapshot): MaintenanceOperation = {
    // The follow-up OPTIMIZE CHECKPOINT commit itself must never schedule more maintenance.
    if (!AMTUtils.amtEnabled(readSnapshot)
        || initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return MaintenanceOperation()
    }
    val checkpointInterval = deltaLog.checkpointInterval(postCommitSnapshot.metadata)
    if (isFullCheckpointOverdue(commitVersion, postCommitSnapshot, checkpointInterval)) {
      MaintenanceOperation(
        shouldCheckpoint = true,
        amtTriggerModeOpt = Some(AMTTriggerMode.CheckpointIntervalFull))
    } else {
      MaintenanceOperation()
    }
  }

  /** [[AMTTriggerMode]] for a followup AMT Checkpoint commit if any. */
  private def followUpTriggerMode(
      commitVersion: Long,
      postCommitSnapshot: Snapshot): Option[AMTTriggerMode] = {
    val checkpointInterval = deltaLog.checkpointInterval(postCommitSnapshot.metadata)
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
      val fullRewriteSpan = checkpointInterval.toLong * fullRewriteCheckpointIntervalMultiplier
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
    if (isFullCheckpointOverdue(commitVersion, postCommitSnapshot, checkpointInterval)) {
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
      commitVersion: Long,
      postCommitSnapshot: Snapshot,
      checkpointInterval: Long): Boolean = {
    val fullRewriteSpan = checkpointInterval * fullRewriteCheckpointIntervalMultiplier
    AMTWriteHelper.previousAMTContentRoot(postCommitSnapshot)
      .flatMap(_.lastManifestCommitWithFullRewrite)
      .exists { lastFull =>
        val versionsSinceFull = commitVersion - lastFull
        versionsSinceFull > 0 && versionsSinceFull % checkpointInterval == 0 &&
          versionsSinceFull >= fullRewriteSpan
      }
  }

  private def largeCommitActionsCountThresholdForInlineManifestCommit: Long =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT)

  private def fullRewriteCheckpointIntervalMultiplier: Int =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER)

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
    val actions = currentTransactionInfo.actions
    if (!AMTUtils.amtEnabled(readSnapshot) ||
        !winningCommitInstalledNewAMTTree(currentTransactionInfo)) {
      return currentTransactionInfo
    }
    // A blind append only adds brand-new files and reads or removes nothing, so none of its actions
    // reference a leaf the winner's tree could have dropped or MDV-masked. Those new files are
    // absent from the winner's tree and get a fresh back reference when this attempt's own tree
    // folds them in, so skip the re-stamp rather than re-deriving back references that do not exist
    // in the winner's tree.
    if (currentTransactionInfo.commitInfo.flatMap(_.isBlindAppend).getOrElse(false)) {
      return currentTransactionInfo
    }
    // A commit with no file actions has no back references to re-derive, so short-circuit before
    // materializing the winning tree's (potentially expensive) AMT provider.
    if (!actions.exists(_.isInstanceOf[FileAction])) {
      return currentTransactionInfo
    }
    val foldedAMTVersion = currentTransactionInfo.preCommitLatestAMTCheckpointOpt.map(_.version)
    if (foldedAMTVersion == lastRebasedAMTVersion) {
      // No new tree was installed since the last rebase, so the actions are already re-stamped
      // against it -- nothing to re-derive.
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
        metrics.backrefRebaseAttempts :+= BackRefRebaseMetrics(
          oldAMTVersion = oldAMTVersion,
          newAMTVersion = foldedAMTVersion.getOrElse(oldAMTVersion),
          totalTimeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs),
          numActionsReusingBackref = result.numActionsReusingBackref,
          numActionsRegeneratingBackref = result.numActionsRegeneratingBackref)
        result.actions
      case None => actions
    }
    lastRebasedAMTVersion = foldedAMTVersion
    currentTransactionInfo.copy(actions = restampedActions)
  }
}
