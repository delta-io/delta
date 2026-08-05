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
      // The `current ; winner` schedule of two disjoint-row deletes is a valid serialization even
      // under Serializable (neither read a row the other wrote), so the DV union still commits.
      val txnA = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 10", rowLevelConcurrency = true)
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

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)
      // Clean abort: only the winner's matched-delete (id=10) is applied; its DV has cardinality 1.
      assert(ids(dir) === (0L to 99L).filterNot(_ == 10))
      assert(deletionVectorCardinalities(log) === Seq(1L))
    }
  }

  test("DELETE (loser) vs MERGE matched-update (winner): winner image file conservatively " +
      "conflicts") {
    withTempDir { dir =>
      val log = createSingleFileTableWithDVs(dir)
      // A (loser) deletes id=10; B (winner) matched-updates id=20 -> 1020, which masks row 20 with a
      // DV and appends an image file for 1020. Like the standalone-UPDATE case above, that image
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
      val (futureA, futureB, futureC) =
        runTxnsWithOrder__A_Start__B__C__A_End(txnA, txnB, txnC)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureC, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentModificationException(e)

      // Only B (id=20) and C (id=10) applied; A aborted cleanly on the id=10 overlap.
      assert(ids(dir) === (0L to 99L).filterNot(id => id == 10 || id == 20))
      assert(deletionVectorCardinalities(log) === Seq(2L))
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
}
