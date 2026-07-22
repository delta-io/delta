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

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CurrentTransactionInfo, DeltaErrors, DeltaLog, DeltaOperations, LogSegment, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint, Metadata, Protocol}

import org.apache.spark.sql.SparkSession

/** What made an AMT write happen. */
sealed abstract class AmtTrigger(val name: String) {
  override def toString: String = name
}

object AmtTrigger {
  /** An OPTIMIZE checkpoint forced a full manifest-tree rewrite. */
  case object OptimizeCheckpoint extends AmtTrigger("OPTIMIZE_CHECKPOINT")
  /** Commits since the last AMT reached the table's checkpoint interval. */
  case object CheckpointInterval extends AmtTrigger("CHECKPOINT_INTERVAL")
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
    val resultOpt = getAMTTriggerMode(
      commitVersion, actionsToCommit, preCommitLogSegment, currentTransactionInfo.metadata)
      .map {
        case AmtTrigger.OptimizeCheckpoint =>
          // An OPTIMIZE checkpoint only rewrites the manifest tree; it commits no user actions.
          assert(actionsToCommit.isEmpty,
            s"OPTIMIZE checkpoint commit must carry no actions, got ${actionsToCommit.size}.")
          throw new UnsupportedOperationException(
            "Full AMT rewrite for OPTIMIZE checkpoints is not yet supported.")
        case trigger =>
          val (result, singleMetric) = AMTWriteHelper.writeFullMaterialization(
            spark = spark,
            readSnapshot = readSnapshot,
            commitVersion = commitVersion,
            actionsToCommit = actionsToCommit,
            postCommitProtocol = currentTransactionInfo.protocol,
            postCommitMetadata = currentTransactionInfo.metadata,
            trigger = trigger)
          metrics.attempts :+= singleMetric
          result
      }
    lastAMTWriteResultOpt = resultOpt
    resultOpt
  }

  /**
   * Whether AMT write is possible at all for this commit.
   * Note: We don't create AMT at commit 0 as of now but this could be relaxed in future.
   */
  private def amtEnabled(commitVersion: Long): Boolean = {
    if (commitVersion <= 0) return false
    readSnapshot.protocol.isFeatureSupported(AdaptiveMetadataTableFeature)
  }

  /**
   * The trigger reason for why an AMT write is needed for this commit, or `None` if AMT write is
   * not needed.
   */
  private def getAMTTriggerMode(
      commitVersion: Long,
      actionsToCommit: Seq[Action],
      preCommitLogSegment: LogSegment,
      postCommitMetadata: Metadata): Option[AmtTrigger] = {
    if (!amtEnabled(commitVersion)) return None
    // -- case-1 --
    if (initialOperation.isInstanceOf[DeltaOperations.OptimizeCheckpoint]) {
      return Some(AmtTrigger.OptimizeCheckpoint)
    }

    // -- case-2 --
    // Assume 0 as version -- this is to make sure future AMTs are at even boundaries e.g.
    // 10/20/30 instead of 9/19/29. (similar logic in checkpoints as well)
    val lastAMTVersion = math.max(0L, preCommitLogSegment.checkpointProvider.version)
    val commitsSinceLastAMT = commitVersion - lastAMTVersion
    if (commitsSinceLastAMT >= deltaLog.checkpointInterval(postCommitMetadata)) {
      return Some(AmtTrigger.CheckpointInterval)
    }


    None
  }
}
