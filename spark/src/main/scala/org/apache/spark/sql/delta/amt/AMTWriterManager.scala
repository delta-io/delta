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

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CurrentTransactionInfo, DeltaErrors, DeltaLog, DeltaOperations, LogSegment, MaintenanceOperation, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

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

/** Metrics describing an AMT (Adaptive Metadata Tree) write, one entry per attempt. */
case class AMTWriteMetrics(
    private[delta] var attempts: Seq[SingleAMTWriteMetrics] = Seq.empty)

/** Metrics for a single AMT write attempt (one per commit attempt that materializes a tree). */
case class SingleAMTWriteMetrics(
    trigger: String,
    materializeDurationMs: Long)

/**
 * The outcome of an AMT write for a single commit attempt.
 *
 * @param contentRootVersion          the table version the manifest tree describes
 * @param checkpoint                  the inline [[Checkpoint]] action to embed in the commit JSON
 * @param leaves                      pointer metadata for each leaf written
 * @param includeActionsInCommitJson  whether the transaction should still write the commit's file
 *                                    actions inline in the commit JSON.
 */
case class AMTWriteResult(
    contentRootVersion: Long,
    checkpoint: Checkpoint,
    leaves: Seq[AMTCheckpointProvider.LeafInfo],
    includeActionsInCommitJson: Boolean)

/**
 * Orchestrates write of an AMT for a given transaction (including reattempts on a conflict).
 */
class AMTWriterManager(
    readSnapshot: Snapshot,
    initialOperation: DeltaOperations.Operation) {

  private def spark: SparkSession = SparkSession.active
  private def deltaLog: DeltaLog = readSnapshot.deltaLog

  val metrics = AMTWriteMetrics()
  private var lastAMTWriteResultOpt: Option[AMTWriteResult] = None

  /**
   * Builds the AMT write for a commit attempt, or `None` when no AMT should be written. Serves both
   * the first attempt and any conflict-resolution retry.
   *
   * @param commitVersion       the version this attempt targets
   * @param currentTransactionInfo the in-flight transaction (its actions, protocol, metadata)
   * @param preCommitLogSegment the log segment prior to this commit
   */
  def writeAMT(
      commitVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      preCommitLogSegment: LogSegment): Option[AMTWriteResult] = {
    if (!amtEnabled(commitVersion)) return None
    if (preCommitLogSegment.version > readSnapshot.version) {
      throw DeltaErrors.concurrentWriteException(conflictingCommit = None)
    }
    val actionsToCommit = currentTransactionInfo.actions
    val resultOpt = initialOperation match {
      case optimize: DeltaOperations.OptimizeCheckpoint =>
        assert(actionsToCommit.isEmpty,
          s"OPTIMIZE checkpoint commit must carry no actions, got ${actionsToCommit.size}.")
        Some(materialize(
          commitVersion, currentTransactionInfo,
          incremental = optimize.incremental, trigger = optimize.triggerName))
      case _ if shouldDoInlineIncrementalCheckpoint(actionsToCommit) =>
        // A large business commit rebuilds its manifest tree inline (incrementally).
        val mode = AMTTriggerMode.InlineWithLargeCommitIncremental
        Some(materialize(
          commitVersion, currentTransactionInfo,
          incremental = mode.isIncremental, trigger = mode.name))
      case _ =>
        None
    }
    lastAMTWriteResultOpt = resultOpt
    resultOpt
  }

  /**
   * Whether this business commit is large enough (by action count) to write its
   * changed actions as part of new AMT.
   */
  private def shouldDoInlineIncrementalCheckpoint(actionsToCommit: Seq[Action]): Boolean =
    actionsToCommit.size.toLong >= largeCommitActionsCountThresholdForInlineManifestCommit

  // Materializes the manifest tree for this commit and records its metrics. An incremental rewrite
  // packs the post-commit live files into leaves in input order on the driver; a full rewrite
  // clusters the read snapshot's live files and flushes them into leaves distributed across
  // executors.
  private def materialize(
      commitVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      incremental: Boolean,
      trigger: String): AMTWriteResult = {
    val (result, singleMetric) =
      if (incremental) {
        AMTWriteHelper.writeIncrementalMaterialization(
          spark = spark,
          readSnapshot = readSnapshot,
          commitVersion = commitVersion,
          actionsToCommit = currentTransactionInfo.actions,
          postCommitProtocol = currentTransactionInfo.protocol,
          postCommitMetadata = currentTransactionInfo.metadata,
          trigger = trigger,
          incremental = incremental)
      } else {
        // A full rewrite re-materializes the whole live file set; the commit carries no actions.
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
    metrics.attempts :+= singleMetric
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
    if (!amtEnabled(commitVersion)
        || initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return MaintenanceOperation()
    }


    val amtTriggerModeOpt = followUpTriggerMode(commitVersion, postCommitSnapshot)
    MaintenanceOperation(
      shouldCheckpoint = amtTriggerModeOpt.isDefined,
      amtTriggerModeOpt = amtTriggerModeOpt)
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


    None
  }

  /**
   * Whether AMT write is possible at all for this commit.
   * Note: We don't create AMT at commit 0 as of now but this could be relaxed in future.
   */
  private def amtEnabled(commitVersion: Long): Boolean = {
    if (commitVersion <= 0) return false
    readSnapshot.protocol.isFeatureSupported(AdaptiveMetadataTableFeature)
  }

  private def largeCommitActionsCountThresholdForInlineManifestCommit: Long =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT)

  private def fullRewriteCheckpointIntervalMultiplier: Int =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER)
}
