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

import java.io.File

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
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
 * non-overlap is conflict-time data skipping, owned by Case 1 (fork issue #4).
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

  /** Creates a single-file Delta table with `id` in [0, numRows) and deletion vectors enabled. */
  private def createSingleFileTableWithDVs(dir: File, numRows: Int = 100): DeltaLog = {
    spark.range(start = 0, end = numRows, step = 1, numPartitions = 1)
      .write.format("delta").mode("append").save(dir.getAbsolutePath)
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

  test("overlapping concurrent DELETEs still conflict") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
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
  // UPDATE image files: rewrite-only DML writes new data files that Layer-1 (DV union) does not
  // reconcile. They conservatively conflict here; conflict-time data skipping (Case 1 / #4)
  // reconciles the provably-disjoint case.
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
      // without conflict-time data skipping (Case 1 / #4). It conservatively conflicts, so the
      // loser aborts. This is one-way safe: it never reconciles a genuine value-flip write-skew
      // (e.g. winner `SET x = 15`, loser `DELETE WHERE x > 10`), which the DV union cannot detect.
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
}
