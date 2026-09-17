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

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaOperations, FullAMTWriteFailedWithConflict, LogSegment, Snapshot, SnapshotManagement, WinningCommitMetrics, WinningCommitSummary}
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, RemoveFile}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * Tests for [[AMTWriterManager]]: the emission policy (checkpoint-interval and accumulated-size
 * triggers), the unsupported OPTIMIZE-checkpoint branch, and the conflict-rebase hard-fail.
 */
class AMTWriterManagerSuite extends AMTCheckpointTestBase {

  // Models conflict resolution advancing the read snapshot's segment by one log-only commit.
  private def advanceSegmentByOneCommit(segment: LogSegment): LogSegment = {
    val newVersion = segment.version + 1
    val commitFileStatus =
      new FileStatus(1L, false, 1, 1L, 1L, FileNames.unsafeDeltaFile(segment.logPath, newVersion))
    SnapshotManagement.appendCommitToLogSegment(segment, commitFileStatus, newVersion)
  }

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

  // A neutral single-winner metric for a one-version conflicted range, for direct writeAMT rebase
  // tests that do not exercise the base-reuse decision (the loser is log-only, or the manager has
  // no full base to reuse).
  private def anyWinner: WinningCommitMetrics = WinningCommitMetrics(
    isBlindAppend = true,
    minDefaultRowCommitVersion = None,
    numAdds = 0,
    numRemoves = 0,
    numAddFilesWithBackreferences = 0,
    numRemoveFilesWithBackreferences = 0,
    checkpointAction = None,
    commitInfo = None)

  test("writeAMT performs a clustered full rewrite for an OPTIMIZE checkpoint operation") {
    withTable("amt_optimize_ckpt") {
      val name = "amt_optimize_ckpt"
      createAMTTable(name, checkpointInterval = 2)
      withSQLConf(leafPackingConfs: _*) {
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles)

        val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
          incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
        val result = manager.writeAMT(
          nextAttemptVersion = snapshot.version + 1,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = snapshot.logSegment,
          winningCommitMetricsForConflictedRange = Seq.empty).getOrElse(
            fail("OPTIMIZE checkpoint must materialize an AMT."))
        assertLeafCount(result.leaves)
        // The commit carries no user actions, so the tree describes state as of the read version.
        assert(result.contentRootVersion == snapshot.version)
        // The metric records the trigger name carried on the operation.
        assert(manager.metrics.writeAttempts.head.trigger ==
          AMTTriggerMode.CheckpointIntervalFull.name)
      }
    }
  }

  // End-to-end emission-policy scenarios (interval / full-rewrite cadence / size trigger / minor
  // compaction) live in AMTCheckpointPolicySuite. This suite covers writeAMT's direct behavior.

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
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val result = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 2,
        currentTransactionInfo =
          txnInfoFor(snapshot, actions = Seq.empty, preCommitLatestAMTCheckpointOpt = baseTree),
        preCommitLogSegment = retrySegment,
        winningCommitMetricsForConflictedRange = Seq(anyWinner))
      assert(result.isEmpty,
        "a log-only commit that lost to a log-only winner must rebase without an AMT write.")
    }
  }

  test("writeAMT writes no tree for a log-only commit rebasing past a tree-installing winner") {
    withTable("amt_conflict_log_vs_tree") {
      val name = "amt_conflict_log_vs_tree"
      createAMTTable(name, checkpointInterval = 2)
      commitCheckpoint(deltaLogForName(name), incremental = false)

      val (manager, snapshot) = managerFor(name)
      val baseTree = amtProvider(snapshot).map(_.checkpointAction).getOrElse(
        fail("the table must be AMT-backed for this case."))
      // A winner installed a newer tree than the read snapshot's. writeAMT for a log-only commit no
      // longer hard-fails on this -- it writes no tree; re-deriving the file actions' back
      // references against the winner tree happens in doCommit's rebaseBackReferences, exercised
      // end-to-end in AMTConflictResolutionSuite.
      val winnerTree = baseTree.copy(version = baseTree.version + 1)
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val result = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 2,
        currentTransactionInfo = txnInfoFor(
          snapshot, actions = Seq.empty, preCommitLatestAMTCheckpointOpt = Some(winnerTree)),
        preCommitLogSegment = retrySegment,
        winningCommitMetricsForConflictedRange = Seq(anyWinner))
      assert(result.isEmpty,
        "a log-only commit rebasing past a winner tree writes no AMT (re-derivation is elsewhere).")
    }
  }

  test("writeAMT does not hard-fail a non-AMT table on a conflict-resolution retry") {
    withTable("amt_non_amt_conflict") {
      val name = "amt_non_amt_conflict"
      // A vanilla Delta table without the AMT feature must not be hard-failed on a conflict.
      sql(s"CREATE TABLE $name (id INT) USING DELTA")
      sql(s"INSERT INTO $name VALUES (1)")

      val (manager, snapshot) = managerFor(name)
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val result = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 2,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = retrySegment,
        winningCommitMetricsForConflictedRange = Seq.empty)
      assert(result.isEmpty, "Non-AMT tables emit no AMT and are not hard-failed on a retry.")
    }
  }

  test("WinningCommitMetrics summarizes a winning commit and computes base-preservation") {
    val logPath = new Path("/tmp/winning-commit-metrics/_delta_log")
    def summaryOf(actions: Seq[Action], version: Long): WinningCommitSummary = {
      val deltaFile = FileNames.unsafeDeltaFile(logPath, version)
      val fileStatus = new FileStatus(1L, false, 1, 1L, 1L, deltaFile)
      new WinningCommitSummary(actions, fileStatus, readTimeMs = 0L)
    }
    def add(path: String, drcv: Option[Long]): AddFile =
      AddFile(path, Map.empty[String, String], size = 1L, modificationTime = 1L,
        dataChange = true, defaultRowCommitVersion = drcv)
    def remove(path: String, drcv: Option[Long]): RemoveFile =
      AddFile(path, Map.empty[String, String], size = 1L, modificationTime = 1L,
        dataChange = true, defaultRowCommitVersion = drcv).removeWithTimestamp(1L)

    // Every add/remove file was created at version 5 or later.
    val allNew = WinningCommitMetrics.fromWinningCommitSummary(
      summaryOf(Seq(add("a", Some(5L)), add("b", Some(7L)), remove("c", Some(6L))), version = 8L))
    assert(allNew.numAdds == 2 && allNew.numRemoves == 1)
    assert(allNew.minDefaultRowCommitVersion.contains(5L))
    assert(allNew.allFileActionsHaveDefaultCommitVersionNewerThan(4L),
      "min DRCV 5 > base 4 must preserve the base")
    // A file created at the base version is a leaf of that base tree, so it does not preserve it.
    assert(!allNew.allFileActionsHaveDefaultCommitVersionNewerThan(5L),
      "min DRCV 5 is a leaf of base 5, so must not preserve it")
    assert(!allNew.allFileActionsHaveDefaultCommitVersionNewerThan(6L),
      "min DRCV 5 < base 6 must not preserve the base")
    // Files without a back reference contribute nothing to the back-reference counts.
    assert(allNew.numAddFilesWithBackreferences == 0 &&
      allNew.numRemoveFilesWithBackreferences == 0)

    // Only the file actions carrying a backReference into a base leaf are counted.
    val backRef = Some(BackReference("leaf", 0))
    val withBackrefs = WinningCommitMetrics.fromWinningCommitSummary(summaryOf(
      Seq(add("a", Some(5L)).copy(backReference = backRef),
        add("b", Some(6L)),
        remove("c", Some(7L)).copy(backReference = backRef)),
      version = 8L))
    assert(withBackrefs.numAddFilesWithBackreferences == 1,
      "only the one Add file carrying a back reference must be counted")
    assert(withBackrefs.numRemoveFilesWithBackreferences == 1,
      "the Remove file carrying a back reference must be counted")

    // A file missing a defaultRowCommitVersion leaves the minimum unknown, so the commit is
    // conservatively treated as not base-preserving.
    val missingDrcv = WinningCommitMetrics.fromWinningCommitSummary(
      summaryOf(Seq(add("a", Some(9L)), add("b", None)), version = 9L))
    assert(missingDrcv.minDefaultRowCommitVersion.isEmpty)
    assert(!missingDrcv.allFileActionsHaveDefaultCommitVersionNewerThan(0L),
      "a missing DRCV is never base-preserving")

    // A commit with no file actions touches no leaf, so it preserves any base.
    val noFiles = WinningCommitMetrics.fromWinningCommitSummary(summaryOf(Seq.empty, version = 3L))
    assert(noFiles.numAdds == 0 && noFiles.numRemoves == 0)
    assert(noFiles.minDefaultRowCommitVersion.isEmpty)
    assert(noFiles.allFileActionsHaveDefaultCommitVersionNewerThan(100L),
      "a commit with no file actions preserves any base")
  }

  test("writeAMT recommits the full base as-is when every winner is base-preserving") {
    withTable("amt_full_ckpt_reuse_base_preserving") {
      val name = "amt_full_ckpt_reuse_base_preserving"
      createAMTTable(name, checkpointInterval = 2)
      appendRowsAsSeparateFiles(name, numFiles = 2, startId = 1)

      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))

      // First attempt materializes the full tree for the read snapshot (caches it for the retry).
      val firstAttempt = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 1,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = snapshot.logSegment,
        winningCommitMetricsForConflictedRange = Seq.empty).getOrElse(
          fail("an OPTIMIZE checkpoint must materialize a full AMT."))
      assert(firstAttempt.checkpoint.contentRoot.isIncremental.contains(false),
        "the first attempt must write a full (non-incremental) tree.")

      // Retry: a log-only winner won the target version, and its files were all created at or after
      // the full base's content-root version (min defaultRowCommitVersion >= base), so the base
      // tree is still exact -- writeAMT recommits it as-is instead of folding the winner in.
      val basePreservingWinner = WinningCommitMetrics(
        isBlindAppend = false,
        minDefaultRowCommitVersion = Some(firstAttempt.contentRootVersion + 1),
        numAdds = 1,
        numRemoves = 0,
        numAddFilesWithBackreferences = 0,
        numRemoveFilesWithBackreferences = 0,
        checkpointAction = None,
        commitInfo = None)
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val reused = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 2,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = retrySegment,
        winningCommitMetricsForConflictedRange = Seq(basePreservingWinner)).getOrElse(
          fail("a base-preserving rebase must reuse the full base."))
      assert(reused.contentRootVersion == firstAttempt.contentRootVersion,
        "reuse must keep the full base's content-root version, not advance it by folding.")
      assert(reused.checkpoint.contentRoot.isIncremental.contains(false),
        "the reused checkpoint must remain the full base, not an incremental fold.")
    }
  }

  test("writeAMT asserts a base-preserving winner carries no back references") {
    withTable("amt_full_ckpt_reuse_backref_guard") {
      val name = "amt_full_ckpt_reuse_backref_guard"
      createAMTTable(name, checkpointInterval = 2)
      appendRowsAsSeparateFiles(name, numFiles = 2, startId = 1)

      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
      val firstAttempt = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 1,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = snapshot.logSegment,
        winningCommitMetricsForConflictedRange = Seq.empty).getOrElse(
          fail("an OPTIMIZE checkpoint must materialize a full AMT."))

      // A contradictory winner: its min DRCV says base-preserving, yet a file carries a back
      // reference into the base tree. The two signals disagree, so reuse must hard-fail.
      val contradictoryWinner = WinningCommitMetrics(
        isBlindAppend = false,
        minDefaultRowCommitVersion = Some(firstAttempt.contentRootVersion + 1),
        numAdds = 1,
        numRemoves = 0,
        numAddFilesWithBackreferences = 1,
        numRemoveFilesWithBackreferences = 0,
        checkpointAction = None,
        commitInfo = None)
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val ex = intercept[IllegalStateException] {
        manager.writeAMT(
          nextAttemptVersion = snapshot.version + 2,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = retrySegment,
          winningCommitMetricsForConflictedRange = Seq(contradictoryWinner))
      }
      assert(ex.getMessage.contains("back references"))
    }
  }

  test("writeAMT signals a full-AMT regenerate for a non-base-preserving winner") {
    withTable("amt_full_ckpt_regenerate") {
      val name = "amt_full_ckpt_regenerate"
      createAMTTable(name, checkpointInterval = 2)
      appendRowsAsSeparateFiles(name, numFiles = 2, startId = 1)

      val (manager, snapshot) = managerFor(name, DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
      val firstAttempt = manager.writeAMT(
        nextAttemptVersion = snapshot.version + 1,
        currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
        preCommitLogSegment = snapshot.logSegment,
        winningCommitMetricsForConflictedRange = Seq.empty).getOrElse(
          fail("an OPTIMIZE checkpoint must materialize a full AMT."))

      // A non-base-preserving winner (its min DRCV is the base version, so it changed a base leaf)
      // cannot be reused: the writer signals a full-AMT regenerate for CheckpointHook to refresh
      // and retry, rather than folding or deferring.
      val nonBasePreservingWinner = WinningCommitMetrics(
        isBlindAppend = false,
        minDefaultRowCommitVersion = Some(firstAttempt.contentRootVersion),
        numAdds = 1,
        numRemoves = 0,
        numAddFilesWithBackreferences = 0,
        numRemoveFilesWithBackreferences = 0,
        checkpointAction = None,
        commitInfo = None)
      val retrySegment = advanceSegmentByOneCommit(snapshot.logSegment)
      val ex = intercept[FullAMTWriteFailedWithConflict] {
        manager.writeAMT(
          nextAttemptVersion = snapshot.version + 2,
          currentTransactionInfo = txnInfoFor(snapshot, actions = Seq.empty),
          preCommitLogSegment = retrySegment,
          winningCommitMetricsForConflictedRange = Seq(nonBasePreservingWinner))
      }
      assert(ex.conflictingCommitVersion == retrySegment.version,
        "the regenerate signal must carry the winners' latest commit version.")
    }
  }
}
