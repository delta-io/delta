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

  /** A transaction body that appends `id` as a plain (log-only, no tree) business commit. */
  private def appendTxn(tableName: String, id: Int): () => Array[Row] = () => {
    spark.sql(s"INSERT INTO $tableName VALUES ($id)").collect()
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

  test("Winning Commit [Manifest Commit] vs Losing commit [inline-incremental commit]" +
    " - FAILS") {
    withTable("amt_conflict_inline_vs_tree_winner") {
      val name = "amt_conflict_inline_vs_tree_winner"
      val deltaLog = setupAMTTable(name)

      // A's 10-file insert inlines a tree; B installs a new tree (OPTIMIZE CHECKPOINT) and wins A's
      // target version. The winner's tree moves A's base, so A hard-fails until the base re-seat
      // lands.
      withInlineThreshold(6) {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
          () => { appendRowsAsSeparateFiles(name, numFiles = 10, startId = 100); Array.empty },
          optimizeCheckpointTxn(deltaLog))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        assertConcurrentWriteFailure(futureA)
      }
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

  test("Winning Commit [Manifest Commit] vs Losing commit [Log commit] - FAILS") {
    withTable("amt_conflict_log_vs_tree_winner") {
      val name = "amt_conflict_log_vs_tree_winner"
      val deltaLog = setupAMTTable(name)

      // A is a plain (blind) append, so its AddFiles carry no back references. B writes a new
      // tree (OPTIMIZE CHECKPOINT) and wins A's target version.
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
        appendTxn(name, id = 100),
        optimizeCheckpointTxn(deltaLog))

      // B commits its tree; A loses and hard-fails. Every commit that lost to a tree-installing
      // winner is blocked for now -- regardless of back references -- because re-seating it onto
      // the winner's tree is a later milestone.
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      assertConcurrentWriteFailure(futureA)
    }
  }

  test("Winning Commit [Manifest Commit] vs Losing commit [Manifest Commit] - FAILS") {
    withTable("amt_conflict_tree_vs_tree") {
      val name = "amt_conflict_tree_vs_tree"
      val deltaLog = setupAMTTable(name)

      // Both A and B are OPTIMIZE CHECKPOINTs (tree writers). B wins A's target version; A loses
      // and, because it (re)writes a tree, still hard-fails -- rebasing a losing tree writer is a
      // later milestone.
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
        optimizeCheckpointTxn(deltaLog),
        optimizeCheckpointTxn(deltaLog))

      ThreadUtils.awaitResult(futureB, Duration.Inf)
      assertConcurrentWriteFailure(futureA)
    }
  }
}
