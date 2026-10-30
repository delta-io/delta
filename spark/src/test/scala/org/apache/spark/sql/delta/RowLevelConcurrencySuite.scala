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

package org.apache.spark.sql.delta

import java.io.{File, InterruptedIOException, IOException}
import java.net.URI
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import com.databricks.spark.util.Log4jUsageLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{DelegateToFileSystem, FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Progressable

import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.ThreadUtils

/**
 * End-to-end tests for deletion-vector-based row-level concurrency
 * ([[DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED]]).
 *
 * Two concurrent DML operations that touch the same physical file but modify disjoint rows should
 * commit cleanly by merging their deletion vectors, instead of aborting the losing transaction.
 *
 * Scope note: this is the sound *same-file DV union*. A rewrite-only DML that also writes *new
 * image* files (an UPDATE emits updated row values to a fresh path) is NOT reconciled here. Those
 * image files are ordinary non-blind changed-data files that can carry a genuine conflict (a
 * value-flip write-skew), so they fall back to today's abort. Reconciling them on proven
 * non-overlap is conflict-time reader-side data skipping (a separate change).
 */
class RowLevelConcurrencySuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  // Enable deletion vectors on every table created by this suite.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "true")

  private def tableRef(dir: File): String = s"delta.`${dir.getCanonicalPath}`"

  /**
   * Creates a single-file Delta table with `id` in [0, numRows) and deletion vectors enabled.
   * `extraProperties` are applied as `delta.*` table properties at creation (e.g. row tracking or
   * change data feed).
   */
  private def createSingleFileTableWithDVs(
      dir: File,
      numRows: Int = 100,
      extraProperties: Map[String, String] = Map.empty): DeltaLog = {
    spark.range(start = 0, end = numRows, step = 1, numPartitions = 1)
      .write.format("delta").options(extraProperties).mode("append").save(dir.getAbsolutePath)
    val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
    val snapshot = log.update()
    assert(
      snapshot.metadata.configuration
        .get(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key).contains("true"),
      "deletion vectors must be enabled on the test table")
    assert(snapshot.allFiles.collect().length === 1, "test table must have a single data file")
    log
  }

  /** A DELETE/UPDATE transaction that runs under the given row-level-concurrency setting. */
  private def sqlTxn(sqlText: String, rowLevelConcurrency: Boolean): () => Array[Row] =
    () => {
      withSQLConf(
        DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> rowLevelConcurrency.toString) {
        sql(sqlText).collect()
      }
      Array.empty[Row]
    }

  private def deletionVectorCardinalities(log: DeltaLog): Seq[Long] =
    log.update().allFiles.collect()
      .filter(_.deletionVector != null)
      .map(_.deletionVector.cardinality)
      .toSeq

  private def assertConcurrentModificationException(e: SparkException): Unit = {
    val causeName = e.getCause.getClass.getName
    assert(
      Seq("ConcurrentAppend", "ConcurrentDeleteRead", "ConcurrentDeleteDelete")
        .exists(causeName.contains),
      s"Expected a concurrency conflict, got: $causeName")
  }

  /** Runs `f` and returns how many row-level read-check scans it issued. */
  private def countReadCheckScans(f: => Unit): Int = {
    val count = new AtomicInteger(0)
    def isReadCheck(qe: QueryExecution): Boolean = qe.analyzed.exists {
      case l: LogicalRelation => l.relation match {
        case h: HadoopFsRelation => h.location match {
          case index: TahoeBatchFileIndex => index.actionType == "rowLevelConcurrencyReadCheck"
          case _ => false
        }
        case _ => false
      }
      case _ => false
    }
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        if (isReadCheck(qe)) count.incrementAndGet()
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        if (isReadCheck(qe)) count.incrementAndGet()
    }
    spark.listenerManager.register(listener)
    try {
      f
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.listenerManager.unregister(listener)
    }
    count.get()
  }

  /** Runs `f` and returns why row-level concurrency did not reconcile the set-aside files. */
  private def notReconciledReasons(f: => Unit): Seq[String] =
    Log4jUsageLogger.track(f)
      .filter(_.tags.get("opType").contains("delta.rowLevelConcurrency.notReconciled"))
      .map { r =>
        val blob = JsonUtils.fromJson[Map[String, Any]](r.blob)
        blob("reason").toString
      }

  private def withCommitLock[T](enabled: Boolean)(f: => T): T =
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED.key -> enabled.toString)(f)

  private def ids(dir: File): Seq[Long] =
    spark.read.format("delta").load(dir.getAbsolutePath).select("id")
      .collect().map(_.getLong(0)).sorted.toSeq

  // ---------------------------------------------------------------------------
  // DELETE vs DELETE (same file)
  // ---------------------------------------------------------------------------

  test("disjoint concurrent DELETEs on the same file both commit by merging deletion vectors") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      // One surviving file carrying the merged deletion vector (cardinality 2).
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("disjoint concurrent DELETEs reconcile on a file that already carries a base DV") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // Establish a non-empty base DV: delete id=5 and commit (the file now carries a DV of
      // cardinality 1). Both concurrent txns below read this DV as their common base, so the
      // overlap test must subtract it (`(dv_win INTERSECT dv_cur) MINUS base`); without that
      // subtraction the shared row 5 would look like a false conflict and abort the merge.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 5")
      assert(deletionVectorCardinalities(log) === Seq(1L))

      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => Set(5L, 10L, 20L).contains(id)))
      // Base row 5 plus the two disjoint new deletes -> merged deletion vector cardinality 3.
      assert(deletionVectorCardinalities(log) === Seq(3L))
    }
  }

  test("disjoint concurrent DELETEs reconcile under Serializable isolation") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      // Each DELETE reads only the rows matching its own condition, which are the rows it deletes,
      // so neither read observes the other's disjoint delete. Committing in `winner ; current`
      // order is a valid serialization even under Serializable, so the DV union still commits.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("Serializable: a MERGE matched-delete (read-then-modify) reconciles vs a concurrent " +
      "DELETE") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      // The disjoint-DELETE Serializable test above already covers a delete-only loser. Here the
      // loser is a MERGE, which *scans* the file to evaluate its ON condition, so the shared path
      // enters the current txn's readFiles as a genuine read (not merely as a delete target). The
      // winner's disjoint DELETE removes that same file, which under Serializable would trip the
      // delete-read check -- unless row-level resolution drops the winner's removal of that path.
      // A MERGE without a NOT MATCHED clause cannot observe the winner deleting a row it did not
      // modify, so this reconciles under the stricter isolation.
      val txnA = sqlTxn(mergeMatched(dir, 10, "DELETE"), rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("overlapping concurrent DELETEs still conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)

      var e: SparkException = null
      val reasons = notReconciledReasons {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      }
      assertConcurrentModificationException(e)
      assert(reasons === Seq("sameRowDeleted"))
      // Clean abort: only the winner's delete (id=10) is applied; its DV has cardinality 1.
      assert(ids(dir) === (0L to 99L).filterNot(_ == 10))
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  test("feature disabled: disjoint concurrent DELETEs still conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = false)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = false)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Only the winner's delete (id=20) is applied; DVs still used, so its DV cardinality is 1.
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20))
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  test("deletion vectors disabled: row-level concurrency gate is off, disjoint DELETEs conflict") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false')")
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      // Flag ON, but DVs are not writable -> resolveRowLevelConflicts no-ops.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Winner's delete (id=20) applied by rewriting the file (no DVs), so no DV is present.
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20))
      assert(deletionVectorCardinalities(log) === Seq.empty[Long])
    }
  }

  // ---------------------------------------------------------------------------
  // UPDATE image files: rewrite-only DML writes new data files that same-file DV union does not
  // reconcile. They conservatively conflict here; conflict-time reader-side data skipping (a
  // separate change) reconciles the provably-disjoint case.
  // ---------------------------------------------------------------------------

  test("DELETE (loser) vs UPDATE (winner): winner image file conservatively conflicts") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // A (loser) deletes id=10; B (winner) updates id=20 -> 1020 (masks row 20, appends image).
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = 1020 WHERE id = 20",
        rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      // The shared file's DVs are disjoint and would merge, but the winner's UPDATE also writes an
      // *image* file (new path) that the append check cannot prove disjoint from the loser's read
      // without conflict-time reader-side data skipping (a separate change). It conservatively
      // conflicts, so the loser aborts. This is one-way safe: it never reconciles a genuine
      // value-flip write-skew (e.g. winner `SET x = 15`, loser `DELETE WHERE x > 10`), which the
      // DV union cannot detect.
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Loser aborted cleanly: only the winner's update is applied (row 20 masked, 1020 appended).
      assert(ids(dir) === ((0L to 99L).filterNot(_ == 20) :+ 1020L).sorted)
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  test("overlapping UPDATE vs UPDATE (same row) still conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = 1010 WHERE id = 20",
        rowLevelConcurrency = true)
      val txnB = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = 2020 WHERE id = 20",
        rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Only the winner's update (20 -> 2020) is applied; original file's DV masks row 20.
      assert(ids(dir) === ((0L to 99L).filterNot(_ == 20) :+ 2020L).sorted)
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  // ---------------------------------------------------------------------------
  // MERGE (matched clauses only): a matched-DELETE writes an in-place deletion vector, exactly like
  // a standalone DELETE, so disjoint matched-deletes reconcile through the same DV union. A
  // matched-UPDATE additionally writes an *image* file (like a standalone UPDATE), so it
  // conservatively conflicts. MERGE inserts / WHEN NOT MATCHED BY SOURCE are out of scope here
  // (they need per-file row-tracking classification) and are not exercised.
  // ---------------------------------------------------------------------------

  /** `MERGE INTO t USING (single-row source) ... ON t.id = s.id` for the given matched action. */
  private def mergeMatched(dir: File, matchId: Long, action: String): String =
    s"""MERGE INTO ${tableRef(dir)} t USING (SELECT id FROM range($matchId, ${matchId + 1})) s
       |ON t.id = s.id WHEN MATCHED THEN $action""".stripMargin

  test("disjoint concurrent MERGE matched-deletes on the same file both commit by merging DVs") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(mergeMatched(dir, 10, "DELETE"), rowLevelConcurrency = true)
      val txnB = sqlTxn(mergeMatched(dir, 20, "DELETE"), rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      // A matched-DELETE MERGE writes only a DV (no image file), so the two disjoint deletes merge
      // into a single surviving file's deletion vector of cardinality 2 -- identical to DELETE.
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("MERGE matched-delete reconciles against a concurrent DELETE on the same file") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // The DV union is op-agnostic: a MERGE matched-delete and a plain DELETE on disjoint rows
      // reconcile just like two DELETEs.
      val txnA = sqlTxn(mergeMatched(dir, 10, "DELETE"), rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("overlapping concurrent MERGE matched-deletes still conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(mergeMatched(dir, 10, "DELETE"), rowLevelConcurrency = true)
      val txnB = sqlTxn(mergeMatched(dir, 10, "DELETE"), rowLevelConcurrency = true)

      var e: SparkException = null
      val reasons = notReconciledReasons {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      }
      assertConcurrentModificationException(e)
      assert(reasons === Seq("sameRowDeleted"))
      // Clean abort: only the winner's matched-delete (id=10) is applied; its DV has cardinality 1.
      assert(ids(dir) === (0L to 99L).filterNot(_ == 10))
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  test("crossed MERGEs whose sources read the target table are not reconciled") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir)
      // A reads row 20 as its source and deletes row 5; B reads row 5 and deletes row 20. The
      // deletes are disjoint, but either serial order deletes only one row: the second MERGE's
      // source is empty. A's source read of row 20 must abort it.
      def selfMerge(sourceId: Long, targetId: Long): String =
        s"""MERGE INTO ${tableRef(dir)} t
           |USING (SELECT id + ${targetId - sourceId} AS id FROM ${tableRef(dir)}
           |       WHERE id = $sourceId) s
           |ON t.id = s.id WHEN MATCHED THEN DELETE""".stripMargin
      val txnA = sqlTxn(selfMerge(sourceId = 20, targetId = 5), rowLevelConcurrency = true)
      val txnB = sqlTxn(selfMerge(sourceId = 5, targetId = 20), rowLevelConcurrency = true)

      var e: SparkException = null
      val reasons = withCommitLock(enabled = false) {
        notReconciledReasons {
          val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
          ThreadUtils.awaitResult(futureB, Duration.Inf)
          e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
        }
      }
      assertConcurrentModificationException(e)
      assert(reasons === Seq("readAffected"))
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20L))
    }
  }

  test("DELETE (loser) vs MERGE matched-update (winner): winner image file conservatively " +
      "conflicts") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // A (loser) deletes id=10; B (winner) matched-updates id=20 -> 1020, which masks row 20 with
      // a DV and appends an image file for 1020. Like the standalone-UPDATE case above, that image
      // file is a non-blind changed-data add the append check cannot prove disjoint, so the loser
      // conservatively aborts rather than reconciling.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(mergeMatched(dir, 20, "UPDATE SET id = 1020"), rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Loser aborted cleanly: only the winner's update is applied (row 20 masked, 1020 appended).
      assert(ids(dir) === ((0L to 99L).filterNot(_ == 20) :+ 1020L).sorted)
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  // ---------------------------------------------------------------------------
  // Winner fully removes the file -> not reconcilable -> conflict
  // ---------------------------------------------------------------------------

  test("winner that fully removes a file conflicts with a concurrent row-level delete") {
    withTempDir { dir =>
      // Two files: [0,50) and [50,100).
      spark.range(start = 0, end = 100, step = 1, numPartitions = 2)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      // A (loser) DV-deletes one row in the first file; B (winner) deletes the whole first file.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id < 50", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Winner fully removed the first file (no DV); loser aborted cleanly.
      assert(ids(dir) === (50L to 99L))
      assert(deletionVectorCardinalities(log) === Seq.empty[Long])
    }
  }

  // ---------------------------------------------------------------------------
  // N-way: three concurrent transactions on the same file
  // ---------------------------------------------------------------------------

  test("three concurrent disjoint DELETEs on the same file all commit") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)
      val txnC = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 30", rowLevelConcurrency = true)

      // A starts; B commits; C commits; A commits last (reconciles against both B and C).
      val (futureA, futureB, futureC) =
        runTxnsWithOrder__A_Start__B__C__A_End(txnA, txnB, txnC)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureC, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => Set(10L, 20L, 30L).contains(id)))
      assert(deletionVectorCardinalities(log) === Seq(3L))
    }
  }

  test("three concurrent DELETEs: two disjoint commit, the overlapping last one aborts") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // B (id=20) and C (id=10) touch disjoint rows and both commit. A also deletes id=10 and,
      // committing last, reconciles cleanly against B (disjoint) but then discovers its overlap
      // with C on row 10, so it aborts.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)
      val txnC = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)

      // A starts; B commits; C commits (reading B's state); A commits last.
      var e: SparkException = null
      val reasons = notReconciledReasons {
        val (futureA, futureB, futureC) =
          runTxnsWithOrder__A_Start__B__C__A_End(txnA, txnB, txnC)
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        ThreadUtils.awaitResult(futureC, Duration.Inf)
        e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      }
      assertConcurrentModificationException(e)
      assert(reasons === Seq("sameRowDeleted"))

      // Only B (id=20) and C (id=10) applied; A aborted cleanly on the id=10 overlap.
      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("three concurrent disjoint DELETEs reconcile on a file that already carries a base DV") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // Establish a non-empty base DV before any concurrency: delete id=5 (file DV cardinality 1).
      // All three concurrent txns read this DV as their common base, and the last one (A) then
      // reconciles against a two-deep winner chain (B, then C) stacked on that base. This is the
      // deepest chain the ordering helpers reach, and it combines the base-DV subtlety with the
      // N-way induction: each step must subtract the *prior winner's* DV, not the read-time base,
      // or the accumulated deletes would look like a false overlap.
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 5")
      assert(deletionVectorCardinalities(log) === Seq(1L))

      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)
      val txnC = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 30", rowLevelConcurrency = true)

      // A starts; B commits; C commits (reading B's state); A commits last (reconciles vs B, C).
      val (futureA, futureB, futureC) =
        runTxnsWithOrder__A_Start__B__C__A_End(txnA, txnB, txnC)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureC, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => Set(5L, 10L, 20L, 30L).contains(id)))
      // Base row 5 plus three disjoint concurrent deletes -> merged deletion vector cardinality 4.
      assert(deletionVectorCardinalities(log) === Seq(4L))
    }
  }

  // ---------------------------------------------------------------------------
  // Two shared files in one conflict -> bounded-parallel reconcile (ThreadUtils.parmap).
  // Every test above conflicts on one file (sharedPaths.size == 1 -> caller-thread branch).
  // A DELETE whose predicate spans two files DV-updates both in ONE commit, so the loser
  // reconciles two shared paths at once, crossing the `parallelism <= 1` boundary into parmap.
  // parmap preserves input order, so the reconciled result matches the sequential branch.
  // ---------------------------------------------------------------------------

  for ((name, confs, expectedDvFiles) <- Seq(
      ("", Seq.empty[(String, String)], 1),
      (", re-reading bitmaps that were not kept",
        Seq(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_RETAINED_DV_BYTES.key -> "0"), 1),
      (", one DV file per packing target size",
        Seq(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key -> "1"), 2)))
  test(s"disjoint DELETEs spanning two files reconcile via the bounded-parallel path$name") {
    withTempDir { dir => withSQLConf(confs: _*) {
      // Two data files: ids [0,100) and [100,200), one file each (separate appends).
      spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      assert(log.update().allFiles.collect().length === 2, "test table must have two data files")

      // Each DELETE's IN list spans both files (one id < 100, one id >= 100), so each commit
      // removes and re-adds BOTH files with a per-file DV. The loser therefore has two shared
      // paths in one resolveRowLevelConflicts call -> parallelism == 2 (the parmap branch).
      val txnA =
        sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id IN (20, 120)", rowLevelConcurrency = true)
      val txnB =
        sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id IN (10, 110)", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 199L).filterNot(id => Set(10L, 20L, 110L, 120L).contains(id)))
      // Both files survive, each carrying its own merged DV (winner + loser delete): cardinality 2.
      assert(deletionVectorCardinalities(log).sorted === Seq(2L, 2L))
      // The merged DVs are packed into one DV file unless the packing target size forbids it.
      val dvFiles = log.update().allFiles.collect().map(_.deletionVector.pathOrInlineDv).distinct
      assert(dvFiles.length === expectedDvFiles)
    }}
  }

  // ---------------------------------------------------------------------------
  // Partitioned table (DV merge is partition-agnostic)
  // ---------------------------------------------------------------------------

  test("disjoint concurrent DELETEs on a partitioned table's file both commit") {
    withTempDir { dir =>
      // Single partition p=0 with a single data file.
      spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
        .withColumn("p", lit(0))
        .write.partitionBy("p").format("delta").mode("append").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      assert(log.update().allFiles.collect().length === 1)

      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  // ---------------------------------------------------------------------------
  // Row tracking: the merge keeps the same physical file, so stable row IDs are preserved
  // ---------------------------------------------------------------------------

  test("row tracking: reconciled disjoint DELETEs preserve surviving rows' stable row IDs") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir,
        extraProperties = Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"))
      // Snapshot each row's stable row id before the concurrent deletes.
      val before = spark.read.format("delta").load(dir.getAbsolutePath)
        .select("id", "_metadata.row_id")
        .collect().map(r => r.getLong(0) -> r.getLong(1)).toMap

      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))

      val after = spark.read.format("delta").load(dir.getAbsolutePath)
        .select("id", "_metadata.row_id")
        .collect().map(r => r.getLong(0) -> r.getLong(1)).toMap
      // Merging DVs on the same physical file leaves base row IDs untouched, so every surviving
      // row keeps the exact stable row id it had before the concurrent deletes.
      assert(after === (before -- Seq(10L, 20L)))
    }
  }

  // ---------------------------------------------------------------------------
  // Change Data Feed: each deleted row is emitted once, at the version that deleted it
  // ---------------------------------------------------------------------------

  test("change data feed: reconciled disjoint DELETEs each emit one delete at their own version") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir,
        extraProperties = Map(DeltaConfigs.CHANGE_DATA_FEED.key -> "true"))
      // Table creation is version 0; the winner commits at version 1 and the reconciled current
      // txn at version 2.
      val firstDeleteVersion = log.update().version + 1

      // A starts first but commits last, so B (id=20) is the winner at version 1 and A (id=10) is
      // the reconciled current txn at version 2.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))

      val changes = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", firstDeleteVersion)
        .load(dir.getAbsolutePath)
        .select("id", "_change_type", "_commit_version")
        .where("_change_type = 'delete'")
        .collect()
        .map(r => (r.getLong(0), r.getString(1), r.getLong(2)))
        .sortBy(_._1)
        .toSeq
      // Each deleted row appears exactly once, attributed to the version that deleted it: the
      // winner's id=20 at version 1 and the reconciled txn's id=10 at version 2.
      assert(changes === Seq(
        (10L, "delete", firstDeleteVersion + 1),
        (20L, "delete", firstDeleteVersion)))
    }
  }

  // ---------------------------------------------------------------------------
  // Reads of the current transaction that observe the winner's deletes
  // ---------------------------------------------------------------------------

  /**
   * The file actions of `DELETE FROM t WHERE id = <id>` against the table at `dir`, produced on a
   * copy so they can be committed by a hand-driven transaction on `dir`. The copy's DV files are
   * copied back so the actions resolve.
   */
  private def deleteActionsFromCopy(dir: File, id: Long): Seq[Action] =
    fileActionsFromCopy(dir)(copy => s"DELETE FROM ${tableRef(copy)} WHERE id = $id")

  /** The file actions of the `sqlOnCopy(copy)` command, produced on a copy of the table. */
  private def fileActionsFromCopy(dir: File)(sqlOnCopy: File => String): Seq[Action] = {
    val copy = new File(dir.getParentFile, s"${dir.getName}-copy-${java.util.UUID.randomUUID()}")
    org.apache.commons.io.FileUtils.copyDirectory(dir, copy)
    sql(sqlOnCopy(copy))
    val copyLog = DeltaLog.forTable(spark, copy.getCanonicalPath)
    copy.listFiles().filter(_.getName.contains("deletion_vector"))
      .foreach(f => org.apache.commons.io.FileUtils.copyFileToDirectory(f, dir))
    copyLog.getChanges(copyLog.update().version).next()._2
      .filter(a => a.isInstanceOf[AddFile] || a.isInstanceOf[RemoveFile])
  }

  private val idAttr = AttributeReference("id", LongType)()

  /**
   * A hand-driven transaction that runs `read`, then commits `DELETE ... WHERE id = 5`'s actions
   * after a concurrent `DELETE ... WHERE id = 20` committed. The two deletes are row-disjoint.
   */
  private def deleteFiveAfterRead(dir: File, log: DeltaLog)(read: OptimisticTransaction => Unit)
      : scala.util.Try[Long] = {
    val loserActions = deleteActionsFromCopy(dir, 5)
    withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
      val loser = log.startTransaction()
      read(loser)
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
      scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
    }
  }

  private def assertConflict(result: scala.util.Try[Long]): Unit = assert(
    result.failed.toOption.exists(e =>
      Seq("ConcurrentAppend", "ConcurrentDeleteRead", "ConcurrentDeleteDelete")
        .exists(e.getClass.getName.contains)),
    s"Expected a concurrency conflict, got: $result")

  test("a read of a row the winner deleted is not reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      // The loser read row 20 ("delete 5 if 20 exists"); the winner deleted row 20. Merging the
      // DVs would commit a read the winner invalidated.
      var result: scala.util.Try[Long] = null
      val reasons = withCommitLock(enabled = false) {
        notReconciledReasons {
          result = deleteFiveAfterRead(dir, log)(_.filterFiles(Seq(EqualTo(idAttr, Literal(20L)))))
        }
      }
      assertConflict(result)
      assert(reasons === Seq("readAffected"))
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20L))
    }
  }

  for ((name, loserSql) <- Seq(
      "DELETE" -> ((dir: File) => s"DELETE FROM ${tableRef(dir)} WHERE id = 50"),
      "UPDATE" -> ((dir: File) => s"UPDATE ${tableRef(dir)} SET id = 1050 WHERE id = 50"))) {
    test(s"a $name whose only read is its condition reconciles without a read-check scan") {
      withTempDir { dir =>
        createSingleFileTableWithDVs(dir)
        val txnA = sqlTxn(loserSql(dir), rowLevelConcurrency = true)
        val txnB =
          sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)
        val scans = countReadCheckScans {
          val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
          ThreadUtils.awaitResult(futureA, Duration.Inf)
          ThreadUtils.awaitResult(futureB, Duration.Inf)
        }
        assert(scans === 0)
        val expected = (0L to 99L).filterNot(id => id == 20L || id == 50L)
        assert(ids(dir) === (if (name == "UPDATE") (expected :+ 1050L).sorted else expected))
      }
    }
  }

  test("a loser that aborts on another conflict scans no read and writes no merged DV") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val loserActions = deleteActionsFromCopy(dir, 5)
      def dvFilesOnDisk(): Set[String] =
        dir.listFiles().map(_.getName).filter(_.contains("deletion_vector_")).toSet
      var result: scala.util.Try[Long] = null
      var dvFilesWritten = Set.empty[String]
      val scans = countReadCheckScans {
        withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
          val loser = log.startTransaction()
          loser.filterFiles(Seq(EqualTo(idAttr, Literal(30L))))
          // The winner's UPDATE masks row 20 in the shared file, which alone would reconcile, and
          // appends an image file with id 1020, which the loser's read cannot rule out.
          sql(s"UPDATE ${tableRef(dir)} SET id = 1020 WHERE id = 20")
          val before = dvFilesOnDisk()
          result = scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
          dvFilesWritten = dvFilesOnDisk() -- before
        }
      }
      assert(result.failed.toOption.exists(_.isInstanceOf[ConcurrentAppendException]), result)
      assert(scans === 0)
      assert(dvFilesWritten.isEmpty, dvFilesWritten)
    }
  }

  test("a whole-table read is not reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      assertConflict(deleteFiveAfterRead(dir, log)(_.readWholeTable()))
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20L))
    }
  }

  test("a read the winner's deletes do not match is reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      var result: scala.util.Try[Long] = null
      val scans = withCommitLock(enabled = false) {
        countReadCheckScans {
          result = deleteFiveAfterRead(dir, log)(_.filterFiles(Seq(EqualTo(idAttr, Literal(30L)))))
        }
      }
      assert(result.isSuccess, s"Expected the DVs to merge, got: $result")
      // A read that is not the DML's own condition is checked by scanning the shared file.
      assert(scans === 1)
      assert(ids(dir) === (0L to 99L).filterNot(id => id == 5L || id == 20L))
      assert(deletionVectorCardinalities(log) === Seq(2L))
    }
  }

  test("a read-check job that fails leaves the files to the conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      val loserActions = deleteActionsFromCopy(dir, 5)
      var result: scala.util.Try[Long] = null
      val reasons = withCommitLock(enabled = false) {
        notReconciledReasons {
          withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
            val loser = log.startTransaction()
            loser.filterFiles(Seq(EqualTo(idAttr, Literal(30L))))
            sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
            // The shared data file disappears, so the scan of the loser's read fails.
            val dataFile = log.update().allFiles.head().absolutePath(log)
            assert(new File(dataFile.toUri).delete())
            result = scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
          }
        }
      }
      assertConflict(result)
      assert(reasons === Seq("readCheckFailed"))
    }
  }

  test("a read-check job whose condition fails on a row leaves the files to the conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        // Only data skipping evaluated this read; the read-check scan divides by zero on row 5.
        val failingRead = spark.read.format("delta").load(dir.getAbsolutePath)
          .where("10 / (id - 5) > 1").queryExecution.analyzed
          .collectFirst { case f: Filter => f.condition }.get
        var result: scala.util.Try[Long] = null
        val reasons = withCommitLock(enabled = false) {
          notReconciledReasons {
            result = deleteFiveAfterRead(dir, log)(_.filterFiles(Seq(failingRead)))
          }
        }
        assertConflict(result)
        assert(reasons === Seq("readCheckFailed"))
      }
    }
  }

  test("isCancellation: cancellations and interrupts, but not timeouts or other failures") {
    val sc = spark.sparkContext
    def cancelled(e: Throwable): Boolean = RowLevelConcurrencyResolution.isCancellation(e, sc)
    val jobCancelled = new SparkException(
      errorClass = "SPARK_JOB_CANCELLED",
      messageParameters = Map("jobId" -> "1", "reason" -> ""),
      cause = null)
    assert(cancelled(jobCancelled))
    assert(cancelled(new InterruptedIOException()))
    assert(cancelled(new java.nio.channels.ClosedByInterruptException()))
    assert(cancelled(new java.io.UncheckedIOException(new InterruptedIOException())))
    assert(!cancelled(new java.net.SocketTimeoutException()))
    assert(!cancelled(new java.util.concurrent.ExecutionException(
      new java.net.SocketTimeoutException())))
    assert(!cancelled(new java.io.IOException("unreadable")))
    assert(!cancelled(new SparkException("Job aborted due to stage failure")))
    // An interrupt of this thread, however the failure that it caused looks.
    Thread.currentThread().interrupt()
    try assert(cancelled(new java.io.IOException("unreadable"))) finally Thread.interrupted()
  }

  test("a cancelled read-check job is thrown as is, not as a conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      // A read whose predicate holds the read-check scan until its job is cancelled, or, should
      // the cancellation never come, until one deadline for the whole scan.
      ReadCheckBlocker.reached.drainPermits()
      val deadline = System.currentTimeMillis() + timeout.toMillis
      spark.udf.register("rlcBlockReadCheck", (_: Long) => {
        ReadCheckBlocker.reached.release()
        val task = TaskContext.get()
        while (!task.isInterrupted() && System.currentTimeMillis() < deadline) Thread.sleep(10)
        true
      })
      var result: scala.util.Try[Long] = null
      val reasons = try {
        val blockingRead = spark.read.format("delta").load(dir.getAbsolutePath)
          .where("rlcBlockReadCheck(id)").queryExecution.analyzed
          .collectFirst { case f: Filter => f.condition }.get
        val loserActions = deleteActionsFromCopy(dir, 5)
        withCommitLock(enabled = false) {
          notReconciledReasons {
            withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
              val loser = log.startTransaction()
              loser.filterFiles(Seq(blockingRead))
              sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
              val canceller = new Thread(() => {
                if (ReadCheckBlocker.reached.tryAcquire(timeout.toSeconds, TimeUnit.SECONDS)) {
                  spark.sparkContext.cancelAllJobs()
                }
              })
              canceller.start()
              result = scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
              canceller.join()
            }
          }
        }
      } finally {
        spark.sessionState.catalog.dropTempFunction("rlcBlockReadCheck", ignoreIfNotExists = true)
      }
      result.failed.get match {
        case e: SparkException => assert(e.getCondition === "SPARK_JOB_CANCELLED", e)
        case e => fail(s"Expected the cancelled job, got: $e", e)
      }
      assert(reasons.isEmpty)
      assert(log.update().version === 2)
    }
  }

  // ---------------------------------------------------------------------------
  // RESTORE and other commits that bring deleted rows back
  // ---------------------------------------------------------------------------

  private def restoreTo(dir: File, version: Long): String =
    s"RESTORE TABLE ${tableRef(dir)} TO VERSION AS OF $version"

  test("RESTORE winner that brings back a deleted row is not reconciled") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir) // v0
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 1") // v1: dv = {1}
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 2") // v2: dv = {1, 2}
      // A prepares dv = {1, 2, 50}; B restores dv = {1}. Merging them would delete row 2 again.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 50", rowLevelConcurrency = true)
      val txnB = sqlTxn(restoreTo(dir, 1), rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      assert(ids(dir) === (0L to 99L).filterNot(_ == 1L))
    }
  }

  test("RESTORE winner that only deletes rows is not reconciled") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir) // v0
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 1") // v1: dv = {1}
      sql(restoreTo(dir, 0)) // v2: no dv
      // B restores dv = {1}, a superset of A's base. RESTORE is still left to the file checks.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 50", rowLevelConcurrency = true)
      val txnB = sqlTxn(restoreTo(dir, 1), rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      assert(ids(dir) === (0L to 99L).filterNot(_ == 1L))
    }
  }

  test("RESTORE loser aborts on a concurrent DELETE") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir) // v0
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 1") // v1: dv = {1}
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 2") // v2: dv = {1, 2}
      // A restores dv = {1}; B deletes row 50 from the same file. RESTORE commits through
      // `commitLarge`, which aborts on any concurrent commit without running conflict resolution.
      val txnA = sqlTxn(restoreTo(dir, 1), rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 50", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assert(e.getCause.isInstanceOf[io.delta.exceptions.ConcurrentWriteException], e.getCause)
      assert(ids(dir) === (0L to 99L).filterNot(id => Set(1L, 2L, 50L).contains(id)))
    }
  }

  test("RESTORE loser committed through conflict resolution is not reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir) // v0
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 1") // v1: dv = {1}
      sql(restoreTo(dir, 0)) // v2: no dv
      // The loser restores dv = {1} (rows only added) after reading row 1; the winner deletes row
      // 20. Both DV checks pass, so only the RESTORE exclusion keeps this from reconciling.
      val loserActions = fileActionsFromCopy(dir)(restoreTo(_, 1))
      val result = withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
        val loser = log.startTransaction()
        loser.filterFiles(Seq(EqualTo(idAttr, Literal(1L))))
        sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
        scala.util.Try(loser.commit(loserActions, DeltaOperations.Restore(Some(1L), None)))
      }
      assertConflict(result)
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20L))
    }
  }

  test("a non-RESTORE commit that brings back a deleted row is not reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir) // v0
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 1") // v1: dv = {1}
      sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 2") // v2: dv = {1, 2}
      // The winner un-deletes row 2 like RESTORE does, but under another operation name (e.g.
      // written by another engine). The DVs themselves show the winner's DV lost a base row.
      val winnerActions = fileActionsFromCopy(dir)(restoreTo(_, 1))
      val loserActions = deleteActionsFromCopy(dir, 50)
      var result: scala.util.Try[Long] = null
      // Without the commit lock, the loser's read waits for a scan, so the DV check decides.
      val reasons = withCommitLock(enabled = false) {
        notReconciledReasons {
          result = withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
            val loser = log.startTransaction()
            loser.filterFiles(Seq(EqualTo(idAttr, Literal(50L))))
            log.startTransaction().commit(winnerActions, DeltaOperations.ManualUpdate)
            scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
          }
        }
      }
      assertConflict(result)
      assert(reasons === Seq("deletedRowsRestored"))
      assert(ids(dir) === (0L to 99L).filterNot(_ == 1L))
    }
  }

  test("MERGE with a conditional matched clause and an insert is not reconciled") {
    withTempDir { dir =>
      createSingleFileTableWithDVs(dir)
      // Source {5, 20}: 5 matches and is deleted; 20 matches the ON condition but not the clause,
      // so it is left alone and NOT inserted. After the winner deletes 20, the same MERGE would
      // insert 20 -- so committing the MERGE after the winner without rerunning it is wrong.
      val txnA = sqlTxn(
        s"""MERGE INTO ${tableRef(dir)} t USING (SELECT explode(array(5L, 20L)) AS id) s
           |ON t.id = s.id
           |WHEN MATCHED AND s.id = 5 THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin,
        rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      var e: SparkException = null
      val reasons = withCommitLock(enabled = false) {
        notReconciledReasons {
          val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
          ThreadUtils.awaitResult(futureB, Duration.Inf)
          e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
        }
      }
      assertConcurrentModificationException(e)
      assert(reasons === Seq("readNotCheckable"))
      assert(ids(dir) === (0L to 99L).filterNot(_ == 20L))
    }
  }

  test("MERGE with an unconditional matched clause and an insert reconciles") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // Every matched row is modified, so a winner can only delete rows the MERGE did not match,
      // which cannot change what it inserts.
      val txnA = sqlTxn(
        s"""MERGE INTO ${tableRef(dir)} t USING (SELECT explode(array(5L, 1000L)) AS id) s
           |ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin,
        rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      assert(ids(dir) === ((0L to 99L).filterNot(id => id == 5L || id == 20L) :+ 1000L))
      assert(deletionVectorCardinalities(log).contains(2L))
    }
  }

  // ---------------------------------------------------------------------------
  // Merged deletion vectors written like DML writes them
  // ---------------------------------------------------------------------------

  test("a merged DV that deletes every row removes the file") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir, numRows = 2)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 0", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 1", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assert(ids(dir).isEmpty)
      assert(log.update().allFiles.collect().isEmpty)
      // Only the two DELETEs' own DV files: no merged DV is written for a removed file.
      val dvFiles = dir.listFiles().map(_.getName)
        .filter(name => name.contains("deletion_vector_") && name.endsWith(".bin"))
      assert(dvFiles.length === 2, dvFiles.toSeq)
    }
  }

  test("merged DV files use the table's random file prefixes") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.RANDOMIZE_FILE_PREFIXES.key}' = 'true')")
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      val mergedDv = log.update().allFiles.collect().head.deletionVector
      assert(mergedDv.cardinality === 2L)
      assert(mergedDv.absolutePath(log.dataPath).getParent != log.dataPath)
    }
  }

  // ---------------------------------------------------------------------------
  // Bounded work during conflict detection
  // ---------------------------------------------------------------------------

  test("under the commit lock, a read that needs a scan is not reconciled") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      var result: scala.util.Try[Long] = null
      var reasons = Seq.empty[String]
      val scans = withCommitLock(enabled = true) {
        countReadCheckScans {
          reasons = notReconciledReasons {
            result =
              deleteFiveAfterRead(dir, log)(_.filterFiles(Seq(EqualTo(idAttr, Literal(30L)))))
          }
        }
      }
      assertConflict(result)
      assert(reasons === Seq("readCheckSkippedUnderCommitLock"))
      assert(scans === 0)
    }
  }

  test("under the commit lock, a read that needs a scan aborts before any DV is decoded") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // The loser deletes the same row as the winner, which the DV check would report, but its
      // read is decided first, without decoding any deletion vector.
      val loserActions = deleteActionsFromCopy(dir, 20)
      var result: scala.util.Try[Long] = null
      val reasons = withCommitLock(enabled = true) {
        notReconciledReasons {
          result = withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_ENABLED.key -> "true") {
            val loser = log.startTransaction()
            loser.filterFiles(Seq(EqualTo(idAttr, Literal(30L))))
            sql(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
            scala.util.Try(loser.commit(loserActions, DeltaOperations.ManualUpdate))
          }
        }
      }
      assertConflict(result)
      assert(reasons === Seq("readCheckSkippedUnderCommitLock"))
    }
  }

  test("more shared files than rowLevelConcurrency.maxCandidateFiles are not reconciled") {
    withTempDir { dir =>
      spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      val txnA =
        sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id IN (20, 120)", rowLevelConcurrency = true)
      val txnB =
        sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id IN (10, 110)", rowLevelConcurrency = true)

      var e: SparkException = null
      val reasons =
        withSQLConf(DeltaSQLConf.DELTA_ROW_LEVEL_CONCURRENCY_MAX_CANDIDATE_FILES.key -> "1") {
          notReconciledReasons {
            val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
            ThreadUtils.awaitResult(futureB, Duration.Inf)
            e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
          }
        }
      assertConcurrentModificationException(e)
      // The two shared files are left to the file-level checks, so none is set aside.
      assert(reasons === Seq("tooManyCandidateFiles"))
      assert(ids(dir) === (0L to 199L).filterNot(id => id == 10L || id == 110L))
    }
  }

  for (overlap <- Seq(false, true)) {
    test(s"concurrent DELETEs on more shared files than one merge batch (overlap: $overlap)") {
      withTempDir { dir =>
        // 70 files of 3 rows each, more than the 64 merged per batch; both DELETEs touch every
        // file and leave one row in it.
        spark.range(start = 0, end = 210, step = 1, numPartitions = 70)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        assert(log.update().allFiles.count() === 70)
        val deletedByB = if (overlap) "id % 3 = 1 OR id = 150" else "id % 3 = 1"
        val txnA =
          sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id % 3 = 0", rowLevelConcurrency = true)
        val txnB =
          sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE $deletedByB", rowLevelConcurrency = true)

        var resultA: scala.util.Try[Array[Row]] = null
        val reasons = notReconciledReasons {
          val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
          ThreadUtils.awaitResult(futureB, Duration.Inf)
          resultA = scala.util.Try(ThreadUtils.awaitResult(futureA, Duration.Inf))
        }
        if (overlap) {
          assertConcurrentModificationException(resultA.failed.get.asInstanceOf[SparkException])
          assert(reasons === Seq("sameRowDeleted"))
          assert(ids(dir) === (0L to 209L).filterNot(id => id % 3 == 1 || id == 150))
        } else {
          assert(resultA.isSuccess, resultA)
          assert(reasons.isEmpty)
          assert(ids(dir) === (0L to 209L).filter(_ % 3 == 2))
          assert(deletionVectorCardinalities(log) === Seq.fill(70)(2L))
        }
      }
    }
  }

  test("a merged deletion vector that cannot be written leaves the file to the conflict") {
    withTempDir { tempDir =>
      withSQLConf(
          "fs.faildvwrites.impl" -> classOf[FailDVWritesLocalFileSystem].getName,
          "fs.AbstractFileSystem.faildvwrites.impl" ->
            classOf[FailDVWritesAbstractFileSystem].getName) {
        val path = s"${FailDVWritesLocalFileSystem.scheme}://${tempDir.getCanonicalPath}"
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").save(path)
        val log = DeltaLog.forTable(spark, path)
        val txnA = sqlTxn(s"DELETE FROM delta.`$path` WHERE id = 10", rowLevelConcurrency = true)
        val txnB = sqlTxn(s"DELETE FROM delta.`$path` WHERE id = 20", rowLevelConcurrency = true)

        var e: SparkException = null
        val reasons = try {
          notReconciledReasons {
            val Seq(futureA, futureB) = runFunctionsWithOrderingFromObserver(Seq(txnA, txnB)) {
              case (observerA :: observerB :: Nil) =>
                unblockUntilPreCommit(observerA)
                busyWaitFor(observerA.phases.preparePhase.hasEntered, timeout)
                unblockAllPhases(observerB)
                busyWaitFor(observerB.phases.postCommitPhase.hasLeft, timeout)
                // DV files can no longer be created once A checks conflicts.
                FailDVWritesLocalFileSystem.failDeletionVectorWrites = true
                unblockCommit(observerA)
            }
            ThreadUtils.awaitResult(futureB, Duration.Inf)
            e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
          }
        } finally {
          FailDVWritesLocalFileSystem.failDeletionVectorWrites = false
        }
        assertConcurrentModificationException(e)
        assert(reasons === Seq("deletionVectorWriteFailed"))
        // A did not commit.
        assert(log.update().version === 1)
      }
    }
  }

  for (numFiles <- Seq(1, 3)) {
    test(s"a winner's unreadable deletion vector leaves the files to the conflict " +
        s"($numFiles shared files)") {
      withTempDir { dir =>
        spark.range(start = 0, end = 10 * numFiles, step = 1, numPartitions = numFiles)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        assert(log.update().allFiles.count() === numFiles)
        val txnA =
          sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id % 10 = 0", rowLevelConcurrency = true)
        val txnB =
          sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id % 10 = 1", rowLevelConcurrency = true)

        var e: SparkException = null
        val reasons = notReconciledReasons {
          val Seq(futureA, futureB) = runFunctionsWithOrderingFromObserver(Seq(txnA, txnB)) {
            case (observerA :: observerB :: Nil) =>
              unblockUntilPreCommit(observerA)
              busyWaitFor(observerA.phases.preparePhase.hasEntered, timeout)
              unblockAllPhases(observerB)
              busyWaitFor(observerB.phases.postCommitPhase.hasLeft, timeout)
              // B's deletion vector files disappear before A checks conflicts.
              val dvFiles = log.update().allFiles.collect().toSeq
                .map(add => new File(add.deletionVector.absolutePath(log.dataPath).toUri))
                .distinct
              assert(dvFiles.forall(_.delete()))
              unblockCommit(observerA)
          }
          ThreadUtils.awaitResult(futureB, Duration.Inf)
          e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
        }
        // The conflict A gets without row-level concurrency, whatever the number of files.
        assertConcurrentModificationException(e)
        assert(reasons === Seq("deletionVectorUnreadable"))
        // A did not commit.
        assert(log.update().version === 1)
      }
    }
  }

  private val partitionAttr = AttributeReference("p", IntegerType)()

  for ((readPartition, reconciled) <- Seq(1 -> true, 0 -> false)) {
    test(s"a read of partition $readPartition is checked on the partition values of the " +
        s"winner's deletes in partition 0 (reconciled: $reconciled)") {
      withTempDir { dir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .withColumn("p", lit(0))
          .write.partitionBy("p").format("delta").mode("append").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        var result: scala.util.Try[Long] = null
        var reasons = Seq.empty[String]
        val scans = countReadCheckScans {
          reasons = notReconciledReasons {
            result = deleteFiveAfterRead(dir, log)(
              _.filterFiles(Seq(EqualTo(partitionAttr, Literal(readPartition)))))
          }
        }
        // Decided from the files' partition values even under the commit lock, without a scan.
        assert(scans === 0)
        if (reconciled) {
          assert(result.isSuccess, s"Expected the DVs to merge, got: $result")
          assert(ids(dir) === (0L to 99L).filterNot(id => id == 5L || id == 20L))
        } else {
          assertConflict(result)
          assert(reasons === Seq("readAffected"))
        }
      }
    }
  }
}

/** Signals that the read-check scan of a test reached its blocking read predicate. */
private object ReadCheckBlocker {
  val reached = new Semaphore(0)
}

/** A local file system that fails to create DV files while `failDeletionVectorWrites` is set. */
class FailDVWritesLocalFileSystem extends RawLocalFileSystem {
  private var uri: URI = _

  override def getScheme: String = FailDVWritesLocalFileSystem.scheme

  override def initialize(name: URI, conf: Configuration): Unit = {
    uri = URI.create(name.getScheme + ":///")
    super.initialize(name, conf)
  }

  // RawLocalFileSystem's constructor calls this before `initialize`.
  override def getUri(): URI = if (uri == null) super.getUri else uri

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (FailDVWritesLocalFileSystem.failDeletionVectorWrites &&
        f.getName.contains("deletion_vector_")) {
      throw new IOException(s"Injected failure creating $f")
    }
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}

class FailDVWritesAbstractFileSystem(uri: URI, conf: Configuration)
  extends DelegateToFileSystem(
    uri, new FailDVWritesLocalFileSystem, conf, FailDVWritesLocalFileSystem.scheme, false)

object FailDVWritesLocalFileSystem {
  val scheme = "faildvwrites"
  @volatile var failDeletionVectorWrites = false
}

