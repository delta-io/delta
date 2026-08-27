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

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.exceptions.ConcurrentWriteException

/**
 * Tests for [[AMTWriterManager]]: the emission policy (checkpoint-interval and accumulated-size
 * triggers), the unsupported OPTIMIZE-checkpoint branch, and the conflict-rebase hard-fail.
 */
class AMTWriterManagerSuite extends AMTCheckpointTestBase {

  // Reads the current snapshot and returns (manager, snapshot) for direct method-level tests.
  private def managerFor(
      tableName: String,
      operation: DeltaOperations.Operation = DeltaOperations.ManualUpdate):
      (AMTWriterManager, Snapshot) = {
    val snapshot = deltaLogForName(tableName).update()
    (new AMTWriterManager(snapshot, operation), snapshot)
  }

  // A minimal transaction info over `snapshot` carrying `actions`, for direct writeAMT calls.
  // `preCommitLatestAMTCheckpointOpt` models the base AMT the attempt would build on: on a rebase
  // it is the tree the conflict fold advanced to (the winner's, if the winner wrote one).
  private def txnInfoFor(
      snapshot: Snapshot,
      actions: Seq[Action],
      preCommitLatestAMTCheckpointOpt: Option[Checkpoint] = None) =
    CurrentTransactionInfo(
      txnId = "txn",
      readPredicates = Vector.empty,
      readFiles = Set.empty,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = snapshot.metadata,
      protocol = snapshot.protocol,
      actions = actions,
      readSnapshot = snapshot,
      commitInfo = None,
      readRowIdHighWatermark = 0L,
      catalogTable = None,
      domainMetadata = Seq.empty,
      op = DeltaOperations.ManualUpdate,
      preCommitLatestAMTCheckpointOpt = preCommitLatestAMTCheckpointOpt)

  test("writeAMT performs a clustered full rewrite for an OPTIMIZE checkpoint operation") {
    withTable("amt_optimize_ckpt") {
      val name = "amt_optimize_ckpt"
      createAMTTable(name, checkpointInterval = 2)
      withSQLConf(leafPackingConfs: _*) {
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles)

        val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
          incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
        val result = manager.writeAMT(
          commitVersion = snapshot.version + 1,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = snapshot.logSegment).getOrElse(
            fail("OPTIMIZE checkpoint must materialize an AMT."))
        assertLeafCount(result.leaves)
        // The commit carries no user actions, so the tree describes state as of the read version.
        assert(result.contentRootVersion == snapshot.version)
        // The metric records the trigger name carried on the operation.
        assert(manager.metrics.attempts.head.trigger == AMTTriggerMode.CheckpointIntervalFull.name)
      }
    }
  }

  // End-to-end emission-policy scenarios (interval / full-rewrite cadence / size trigger / minor
  // compaction) live in AMTCheckpointPolicySuite. This suite covers writeAMT's direct behavior.

  test("writeAMT hard-fails a tree-writing commit on a conflict-resolution retry") {
    withTable("amt_conflict_tree_writer") {
      val name = "amt_conflict_tree_writer"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")

      // An OPTIMIZE checkpoint writes a tree, so it still hard-fails on a rebase (the tree-rebuild
      // rebase is a later milestone).
      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
      // A retry: conflict resolution advanced the segment past the read snapshot's version.
      val retrySegment = snapshot.logSegment.copy(version = snapshot.version + 1)
      intercept[ConcurrentWriteException] {
        manager.writeAMT(
          commitVersion = snapshot.version + 2,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = retrySegment)
      }
    }
  }

  test("writeAMT lets a log-only commit rebase past a log-only winner on retry") {
    withTable("amt_conflict_log_rebase") {
      val name = "amt_conflict_log_rebase"
      createAMTTable(name, checkpointInterval = 2)
      commitCheckpoint(deltaLogForName(name), incremental = false)

      val (manager, snapshot) = managerFor(name)
      val baseTree = amtProvider(snapshot).map(_.checkpointAction)
      assert(baseTree.isDefined, "the table must be AMT-backed for this case.")
      // The winner wrote no tree, so the base AMT is unchanged (the folded pointer still equals the
      // read snapshot's tree): a log-only commit rebases with no AMT write instead of hard-failing.
      val retrySegment = snapshot.logSegment.copy(version = snapshot.version + 1)
      val result = manager.writeAMT(
        commitVersion = snapshot.version + 2,
        currentTransactionInfo =
          txnInfoFor(snapshot, actions = Seq.empty, preCommitLatestAMTCheckpointOpt = baseTree),
        preCommitLogSegment = retrySegment)
      assert(result.isEmpty,
        "a log-only commit that lost to a log-only winner must rebase without an AMT write.")
    }
  }

  test("writeAMT hard-fails a log-only commit when the winner installed a new tree") {
    withTable("amt_conflict_log_vs_tree") {
      val name = "amt_conflict_log_vs_tree"
      createAMTTable(name, checkpointInterval = 2)
      commitCheckpoint(deltaLogForName(name), incremental = false)

      val (manager, snapshot) = managerFor(name)
      val baseTree = amtProvider(snapshot).map(_.checkpointAction).getOrElse(
        fail("the table must be AMT-backed for this case."))
      // A winner installed a newer tree than the read snapshot's, so a log-only commit's back
      // references are stale and it must hard-fail until they are re-derived (a later milestone).
      val winnerTree = baseTree.copy(version = baseTree.version + 1)
      val retrySegment = snapshot.logSegment.copy(version = snapshot.version + 1)
      intercept[ConcurrentWriteException] {
        manager.writeAMT(
          commitVersion = snapshot.version + 2,
          currentTransactionInfo = txnInfoFor(
            snapshot, actions = Seq.empty, preCommitLatestAMTCheckpointOpt = Some(winnerTree)),
          preCommitLogSegment = retrySegment)
      }
    }
  }

  test("writeAMT does not hard-fail a non-AMT table on a conflict-resolution retry") {
    withTable("amt_non_amt_conflict") {
      val name = "amt_non_amt_conflict"
      // A vanilla Delta table without the AMT feature must not be hard-failed on a conflict.
      sql(s"CREATE TABLE $name (id INT) USING DELTA")
      sql(s"INSERT INTO $name VALUES (1)")

      val (manager, snapshot) = managerFor(name)
      val retrySegment = snapshot.logSegment.copy(version = snapshot.version + 1)
      val result = manager.writeAMT(
        commitVersion = snapshot.version + 2,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = retrySegment)
      assert(result.isEmpty, "Non-AMT tables emit no AMT and are not hard-failed on a retry.")
    }
  }
}
