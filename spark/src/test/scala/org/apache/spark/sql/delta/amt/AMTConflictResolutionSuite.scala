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

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.{ConcurrentWriteException, DeltaLog}
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.util.ThreadUtils

/**
 * Conflict-resolution behavior for AMT-backed tables.
 */
class AMTConflictResolutionSuite
  extends AMTCheckpointTestBase
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  // A large interval keeps the checkpoint-interval maintenance hook from firing its own checkpoint
  // mid-test; every AMT write here is one the test drives explicitly.
  private val noAutoCheckpointInterval = 1000

  /** A transaction body that commits an incremental OPTIMIZE CHECKPOINT (writes a new AMT tree). */
  private def optimizeCheckpointTxn(deltaLog: DeltaLog): () => Array[Row] = () => {
    commitCheckpoint(deltaLog, incremental = true)
    Array.empty
  }

  /** A transaction body that commits a full-rewrite OPTIMIZE CHECKPOINT (a brand-new AMT tree). */
  private def fullCheckpointTxn(deltaLog: DeltaLog): () => Array[Row] = () => {
    commitCheckpoint(deltaLog, incremental = false)
    Array.empty
  }

  /** A transaction body that appends `id` as a plain (log-only, no tree) business commit. */
  private def appendTxn(tableName: String, id: Int): () => Array[Row] = () => {
    spark.sql(s"INSERT INTO $tableName VALUES ($id)").collect()
  }

  /** A transaction body that deletes `id` -- a non-blind commit. */
  private def deleteTxn(tableName: String, id: Int): () => Array[Row] = () => {
    spark.sql(s"DELETE FROM $tableName WHERE id = $id").collect()
  }

  /**
   * Creates an AMT table with a full checkpoint base and a few seeded files, then returns its
   * [[DeltaLog]].
   */
  private def setupAMTTable(tableName: String): DeltaLog = {
    createAMTTable(tableName, checkpointInterval = noAutoCheckpointInterval)
    val deltaLog = deltaLogForName(tableName)
    // Do a full checkpoint -- it is to make sure that incremental checkpoints can be done in tests.
    commitCheckpoint(deltaLog, incremental = false)
    appendRowsAsSeparateFiles(tableName, numFiles = 3, startId = 1)
    deltaLog
  }

  /** Awaits `future`, asserting it fails a rebase with an AMT [[ConcurrentWriteException]]. */
  private def assertConcurrentWriteFailure(future: scala.concurrent.Future[Array[Row]]): Unit = {
    val ex = intercept[SparkException] {
      ThreadUtils.awaitResult(future, Duration.Inf)
    }
    // The commit runs on a worker thread and a SQL command re-wraps its failure, so the
    // ConcurrentWriteException can sit anywhere in the cause chain rather than at `getCause`.
    val causes = Iterator
      .iterate[Throwable](ex)(t => if (t.getCause eq t) null else t.getCause)
      .takeWhile(_ != null)
      .take(50)
      .toList
    assert(causes.exists(_.isInstanceOf[ConcurrentWriteException]),
      s"expected a ConcurrentWriteException in the cause chain, got " +
        causes.map(_.getClass.getName).mkString(" -> "))
  }

  test("Winning Commit [Log Commit] vs Losing commit [Manifest Commit] - FAILS") {
    withTable("amt_conflict_tree_writer_loses") {
      val name = "amt_conflict_tree_writer_loses"
      val deltaLog = setupAMTTable(name)

      // A is an OPTIMIZE checkpoint (a maintenance tree write); B is a plain append that wins A's
      // target version. Rebasing a losing maintenance checkpoint is deferred (it can be rescheduled
      // against the new snapshot instead), so A still hard-fails.
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
        optimizeCheckpointTxn(deltaLog),
        appendTxn(name, id = 100))
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      assertConcurrentWriteFailure(futureA)
    }
  }

  test("Winning Commit [Log Commit] vs Losing commit [inline-incremental commit] - Success") {
    withTable("amt_conflict_inline_rebase") {
      val name = "amt_conflict_inline_rebase"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // Threshold 6: A's 10-file insert inlines its AMT tree, while B's single-file append (a
      // couple of actions) stays log-only. So A is a tree writer losing to a log-only winner -- it
      // must rebase and rebuild its tree with B folded into the incremental window rather than
      // hard-failing. Capture A's per-attempt AMT write metrics to inspect the window growth.
      val perAttempt = trackIncrementalAMTWriteMetricsPerAttempt(deltaLog.update().version) {
        withInlineThreshold(6) {
          val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
            () => { appendRowsAsSeparateFiles(name, numFiles = 10, startId = 100); Array.empty },
            appendTxn(name, id = 200))
          ThreadUtils.awaitResult(futureB, Duration.Inf)
          ThreadUtils.awaitResult(futureA, Duration.Inf)
        }
      }

      // Reading the live set back goes through A's rebased inline tree, so a correct id set proves
      // the tree folded the winning append.
      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore ++ (100 to 109).toSet ++ Set(200),
        s"both commits must survive the rebase; before=$liveIdsBefore after=$liveIdsAfter")
      // A's commit rode an inline AMT checkpoint, confirming it took the tree-writing rebase path.
      val latest = deltaLog.update()
      assert(checkpointAt(deltaLog, latest.version).isDefined,
        "the rebased inline commit must carry an AMT checkpoint at its version.")
      assert(amtProvider(latest).isDefined, "the table must remain AMT-backed after the rebase.")

      // A materialized its tree exactly twice -- once on its first attempt and once on the rebase
      // -- and the rebase's incremental window folds in exactly one extra commit, the winning
      // append, so its numIntermediateCommits is precisely one more than the first attempt's.
      val numIntermediateCommits = perAttempt.map(_.numIntermediateCommits)
      assert(numIntermediateCommits.size == 2,
        s"A must materialize exactly twice (first attempt + rebase); got $numIntermediateCommits")
      assert(numIntermediateCommits(1) == numIntermediateCommits(0) + 1,
        "the rebase must fold exactly one more intermediate commit (the winning append) than the " +
          s"first attempt; got $numIntermediateCommits")
    }
  }

  test("Winning Commit [Incremental checkpoint commit] vs Losing commit " +
    "[inline-incremental commit] - Success") {
    withTable("amt_conflict_inline_vs_tree_winner") {
      val name = "amt_conflict_inline_vs_tree_winner"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // A's 10-file insert inlines a tree; B installs a new tree (OPTIMIZE CHECKPOINT) and wins A's
      // target version. A must re-seat its incremental write onto B's tree and rebuild its own tree
      // on top (re-deriving back references if B's leaf set moved), rather than hard-failing.
      withInlineThreshold(6) {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
          () => { appendRowsAsSeparateFiles(name, numFiles = 10, startId = 100); Array.empty },
          optimizeCheckpointTxn(deltaLog))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        ThreadUtils.awaitResult(futureA, Duration.Inf)
      }

      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore ++ (100 to 109).toSet,
        s"A's files must survive the rebase onto the winner's tree; before=$liveIdsBefore " +
          s"after=$liveIdsAfter")
      // A's rebased commit rode an inline AMT checkpoint built on the winner's tree.
      val latest = deltaLog.update()
      assert(checkpointAt(deltaLog, latest.version).isDefined,
        "the rebased inline commit must carry an AMT checkpoint at its version.")
      assert(amtProvider(latest).isDefined, "the table must remain AMT-backed after the rebase.")
    }
  }

  test("Winning Commit [log commit] vs Losing commit [log commit] - SUCCESS") {
    withTable("amt_conflict_log_vs_log") {
      val name = "amt_conflict_log_vs_log"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // Both A and B are plain appends of distinct rows: neither writes a tree and blind appends do
      // not logically conflict, so A must rebase past B and commit.
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
        appendTxn(name, id = 100),
        appendTxn(name, id = 200))

      // Neither future may fail: B wins its version and A rebases onto it.
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)

      // Neither commit writes a tree, so the base AMT is unchanged. Reading the live set back
      // still goes through it, and a correct id set proves A rebased its log actions past B.
      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore ++ Set(100, 200),
        s"both appends must survive the rebase; before=$liveIdsBefore after=$liveIdsAfter")
      assert(amtProvider(deltaLog.update()).isDefined,
        "the table must remain AMT-backed after the rebase.")
    }
  }

  test("Winning Commit [Incremental checkpoint commit] vs Losing commit [log commit] - " +
    "Success") {
    withTable("amt_conflict_log_vs_tree_winner") {
      val name = "amt_conflict_log_vs_tree_winner"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // A is a non-blind DELETE (it removes id 1's file); B installs a new tree (OPTIMIZE
      // CHECKPOINT) and wins A's target version. A must rebase past the new tree -- re-deriving its
      // RemoveFile's back reference against it when the tree's leaf set moved -- rather than
      // hard-failing.
      val rebaseMetrics = trackBackrefRebaseMetricsAt(deltaLog.update().version) {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
          deleteTxn(name, id = 1),
          optimizeCheckpointTxn(deltaLog))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        ThreadUtils.awaitResult(futureA, Duration.Inf)
      }

      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore - 1,
        s"A's delete must survive the rebase onto the winner's tree; before=$liveIdsBefore " +
          s"after=$liveIdsAfter")
      assert(amtProvider(deltaLog.update()).isDefined,
        "the table must remain AMT-backed after the rebase.")
      // The rebase really ran: one round re-derived A's RemoveFile back reference against B's tree.
      assert(rebaseMetrics.size == 1 && rebaseMetrics.head.numActionsRegeneratingBackref >= 1,
        s"A must record one back-ref rebase that re-derived its RemoveFile; got $rebaseMetrics")
    }
  }

  test("Winning Commit [Full checkpoint commit] vs Losing commit [log commit] - Success") {
    withTable("amt_conflict_log_vs_full_tree_winner") {
      val name = "amt_conflict_log_vs_full_tree_winner"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // A is a non-blind DELETE (it removes id 1's file); B installs a brand-new FULL-rewrite tree
      // (OPTIMIZE CHECKPOINT, incremental = false) and wins A's target version. A full rewrite
      // moves every leaf position, so A's RemoveFile back reference is re-derived from scratch
      // against B's tree before A commits, rather than hard-failing.
      val rebaseMetrics = trackBackrefRebaseMetricsAt(deltaLog.update().version) {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
          deleteTxn(name, id = 1),
          fullCheckpointTxn(deltaLog))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        ThreadUtils.awaitResult(futureA, Duration.Inf)
      }

      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore - 1,
        s"A's delete must survive the rebase onto the full-rewrite winner's tree; " +
          s"before=$liveIdsBefore after=$liveIdsAfter")
      assert(amtProvider(deltaLog.update()).isDefined,
        "the table must remain AMT-backed after the rebase.")
      // The rebase really ran: the full rewrite forced A's RemoveFile back reference to be
      // re-derived against B's tree.
      assert(rebaseMetrics.size == 1 && rebaseMetrics.head.numActionsRegeneratingBackref >= 1,
        s"A must record one back-ref rebase that re-derived its RemoveFile; got $rebaseMetrics")
    }
  }

  test("Winning Commit [Full checkpoint commit] vs Losing commit " +
    "[inline-incremental commit] - Success") {
    withTable("amt_conflict_inline_vs_full_tree_winner") {
      val name = "amt_conflict_inline_vs_full_tree_winner"
      val deltaLog = setupAMTTable(name)
      val liveIdsBefore = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet

      // A's 10-file insert inlines a tree; B installs a brand-new FULL-rewrite tree and wins A's
      // target version. A must re-seat its incremental write onto B's full tree and rebuild its
      // own tree on top, re-deriving all its back references, rather than hard-failing.
      withInlineThreshold(6) {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
          () => { appendRowsAsSeparateFiles(name, numFiles = 10, startId = 100); Array.empty },
          fullCheckpointTxn(deltaLog))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        ThreadUtils.awaitResult(futureA, Duration.Inf)
      }

      val liveIdsAfter = spark.sql(s"SELECT id FROM $name").collect().map(_.getInt(0)).toSet
      assert(liveIdsAfter == liveIdsBefore ++ (100 to 109).toSet,
        s"A's files must survive the rebase onto the full-rewrite winner's tree; " +
          s"before=$liveIdsBefore after=$liveIdsAfter")
      val latest = deltaLog.update()
      assert(checkpointAt(deltaLog, latest.version).isDefined,
        "the rebased inline commit must carry an AMT checkpoint at its version.")
      assert(amtProvider(latest).isDefined, "the table must remain AMT-backed after the rebase.")
    }
  }

}
