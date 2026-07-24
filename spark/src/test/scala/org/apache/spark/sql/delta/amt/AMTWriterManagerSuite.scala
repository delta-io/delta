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
  private def txnInfoFor(
      snapshot: Snapshot, actions: Seq[org.apache.spark.sql.delta.actions.Action]) =
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
      op = DeltaOperations.ManualUpdate)

  test("writeAMT materializes a tree for an OPTIMIZE checkpoint operation") {
    withTable("amt_optimize_ckpt") {
      val name = "amt_optimize_ckpt"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")

      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
      val result = manager.writeAMT(
        commitVersion = snapshot.version + 1,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = snapshot.logSegment).getOrElse(
          fail("OPTIMIZE checkpoint must materialize an AMT."))
      // The commit carries no user actions, so the tree describes state as of the read version.
      assert(result.contentRootVersion == snapshot.version)
      // The metric records the trigger name carried on the operation.
      assert(manager.metrics.attempts.head.trigger == AMTTriggerMode.CheckpointIntervalFull.name)
    }
  }

  // End-to-end emission-policy scenarios (interval / full-rewrite cadence / size trigger / minor
  // compaction) live in AMTCheckpointPolicySuite. This suite covers writeAMT's direct behavior.

  test("writeAMT hard-fails an AMT table on a conflict-resolution retry") {
    withTable("amt_conflict_fail") {
      val name = "amt_conflict_fail"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")

      val (manager, snapshot) = managerFor(name)
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
