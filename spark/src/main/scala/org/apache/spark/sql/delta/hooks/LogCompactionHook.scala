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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/**
 * Post-commit hook that creates log compaction files (`<x>.<y>.compacted.json`) to speed up
 * snapshot construction without the cost of a full checkpoint.
 *
 * When enabled (`deltaLog.minorCompaction.useForWrites`), after a commit whose version is a
 * multiple of the configured interval (table property `delta.logCompactionInterval`), this hook
 * reconciles the most recent window of commits into a single compaction file. The window is
 * `[startVersion, committedVersion]` where
 * `startVersion = max(committedVersion - interval + 1, lastCheckpointVersion + 1)`. Bounding
 * the window below by the last checkpoint avoids producing a compaction that spans (and is thus
 * subsumed by) a checkpoint.
 *
 * Reconciling a fixed window keeps the produced compaction files non-overlapping so that, when the
 * checkpoint interval is a multiple of the compaction interval, they tile the commit range and can
 * be chained by the reader (see `useCompactedDeltasForLogSegment`). Producing an ever-growing range
 * would instead yield overlapping files where the reader only ever uses the smallest one.
 *
 * If a checkpoint was just written for this commit, the hook does nothing, because a checkpoint
 * already subsumes the commits a compaction would cover.
 *
 * Per the protocol, compaction files (like checkpoints) may only be produced for commit versions
 * already published in `_delta_log`. On catalog-managed / coordinated-commits tables the hook
 * therefore synchronously backfills the window's commits before compacting, mirroring how
 * checkpoint writing publishes commits via `Snapshot.ensureCommitFilesBackfilled`.
 *
 * Log compaction files are optional and do not require any protocol or table-feature upgrade.
 * Readers that support compacted deltas (`deltaLog.minorCompaction.useForReads`) use them to speed
 * up snapshot construction; readers that don't simply ignore them.
 */
object LogCompactionHook extends PostCommitHook {

  override val name: String = "Post commit log compaction"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!spark.conf.get(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES)) return
    // A checkpoint already subsumes the commits a compaction would cover, so skip when one was
    // just written for this commit.
    if (txn.needsCheckpoint) return

    val interval =
      DeltaConfigs.LOG_COMPACTION_INTERVAL.fromMetaData(txn.postCommitSnapshot.metadata)
    val endVersion = txn.committedVersion
    // Only compact on interval boundaries to keep the produced windows non-overlapping.
    if (endVersion <= 0 || endVersion % interval != 0) return

    // Don't start a compaction at or before the latest checkpoint: those commits are already
    // subsumed by the checkpoint, so such a compaction would never be used by the reader.
    val checkpointVersion = txn.postCommitSnapshot.logSegment.checkpointProvider.version
    val startVersion = math.max(endVersion - interval + 1, checkpointVersion + 1)

    // Need at least two commits in the range to produce a useful compaction.
    if (endVersion <= startVersion) return

    // Per the protocol, a log compaction file may only be produced for versions already published
    // (backfilled) in `_delta_log` (i.e. `_delta_log/<v>.json` must exist for every `v` in the
    // range). On coordinated-commits / catalog-managed tables recent commits may still be staged
    // under `_delta_log/_staged_commits`, so we synchronously backfill them first - the same way
    // checkpoint writing ensures published commits (see `Snapshot.ensureCommitFilesBackfilled`,
    // invoked from `Checkpoints.writeCheckpointFiles`). This is a no-op on filesystem-based tables
    // (no commit coordinator).
    txn.postCommitSnapshot.ensureCommitFilesBackfilled(txn.catalogTable)

    LogCompaction.compact(txn.deltaLog, txn.postCommitSnapshot, startVersion, endVersion)
  }
}
