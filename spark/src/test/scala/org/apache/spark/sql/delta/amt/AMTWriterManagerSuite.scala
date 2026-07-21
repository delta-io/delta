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

  test("writeAMT throws UnsupportedOperationException for an OPTIMIZE checkpoint operation") {
    withTable("amt_optimize_ckpt") {
      val name = "amt_optimize_ckpt"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")

      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint())
      val ex = intercept[UnsupportedOperationException] {
        manager.writeAMT(
          commitVersion = snapshot.version + 1,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = snapshot.logSegment)
      }
      assert(ex.getMessage.contains("OPTIMIZE checkpoints"))
    }
  }

  test("emits AMT when commit count since last checkpoint reaches the checkpoint interval") {
    withTable("amt_count_trigger") {
      val name = "amt_count_trigger"
      createAMTTable(name, checkpointInterval = 3)
      sql(s"INSERT INTO $name VALUES (1)") // v1: 1 commit since genesis, < 3.
      sql(s"INSERT INTO $name VALUES (2)") // v2: 2 commits since genesis, < 3.
      sql(s"INSERT INTO $name VALUES (3)") // v3: 3 commits since genesis, >= 3 -> emit.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      assert(checkpointsAt(deltaLog, 1).isEmpty, "v1 is below the interval; no emission.")
      assert(checkpointsAt(deltaLog, 2).isEmpty, "v2 is below the interval; no emission.")
      assert(checkpointsAt(deltaLog, 3).size == 1, "v3 reaches the interval; AMT must be emitted.")
      assert(rootFiles(path).size == 1 && leafFiles(path).nonEmpty,
        "A manifest tree must be written at the interval boundary.")
      assert(amtProvider(deltaLog.update()).isDefined)
    }
  }


  test("does not emit AMT when no trigger fires") {
    withTable("amt_no_trigger") {
      val name = "amt_no_trigger"
      // Interval far away, and a tiny commit stays well below the default size threshold, so
      // neither the count nor the (edge-only) size trigger fires.
      createAMTTable(name, checkpointInterval = 1000)
      sql(s"INSERT INTO $name VALUES (1)")

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      assert(checkpointsAt(deltaLog, 1).isEmpty, "No trigger fires; no emission.")
      assert(rootFiles(path).isEmpty && leafFiles(path).isEmpty, "No manifest tree written.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

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
