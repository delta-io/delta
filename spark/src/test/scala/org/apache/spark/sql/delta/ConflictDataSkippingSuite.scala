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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThanOrEqual, LessThan, Literal, Remainder}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.ThreadUtils

/**
 * Tests for reader-side conflict-time data skipping
 * ([[DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED]]).
 *
 * A concurrently-added file whose column stats prove it cannot match the current transaction's read
 * predicates should NOT cause an append conflict, especially on unpartitioned tables, where the
 * append check would otherwise conflict on any added file. Skipping must be one-way safe: a file
 * with missing stats is always kept, so a real conflict is never missed.
 */
class ConflictDataSkippingSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  private def tableRef(dir: File): String = s"delta.`${dir.getCanonicalPath}`"

  /**
   * Unpartitioned table with `id` in [0, 1000) across 10 files (file i covers [100i, 100i+100)),
   * committed at Serializable isolation so that concurrent (blind) appends are conflict-checked.
   * When `numIndexedCols` is set, stats collection is limited accordingly (0 = no stats).
   */
  private def createTable(dir: File, numIndexedCols: Option[Int] = None): Unit = {
    spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
      .write.format("delta").mode("append").save(dir.getAbsolutePath)
    val extraProps = numIndexedCols
      .map(n => s", '${DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key}' = '$n'")
      .getOrElse("")
    sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
      s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable'$extraProps)")
  }

  /**
   * A DELETE that runs under the given conflict-time skipping settings (evaluated on its commit
   * thread). `valueExact` enables the tier-2 actual-value scan on top of tier-1 stats skipping.
   */
  private def deleteTxn(
      dir: File,
      condition: String,
      dataSkipping: Boolean,
      valueExact: Boolean = false): () => Array[Row] =
    () => {
      withSQLConf(
        DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> dataSkipping.toString,
        DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_VALUE_EXACT_ENABLED.key ->
          valueExact.toString) {
        sql(s"DELETE FROM ${tableRef(dir)} WHERE $condition").collect()
      }
      Array.empty[Row]
    }

  /** A blind append of `id` in [start, end), optionally into partition `p`. */
  private def appendTxn(
      dir: File, start: Long, end: Long, partition: Option[Int] = None): () => Array[Row] =
    () => {
      var df = spark.range(start, end).toDF()
      partition.foreach(p => df = df.withColumn("p", lit(p)))
      df.write.format("delta").mode("append").save(dir.getAbsolutePath)
      Array.empty[Row]
    }

  /** A blind append of the EVEN ids in [start, end), optionally into partition `p`. */
  private def appendEvenTxn(
      dir: File, start: Long, end: Long, partition: Option[Int] = None): () => Array[Row] =
    () => {
      var df = spark.range(start, end, step = 2).toDF()
      partition.foreach(p => df = df.withColumn("p", lit(p)))
      df.write.format("delta").mode("append").save(dir.getAbsolutePath)
      Array.empty[Row]
    }

  /** Expected surviving ids after deleting id<50 and appending [1000, 1100). */
  private val disjointExpected: Seq[Row] =
    ((50L until 1000L) ++ (1000L until 1100L)).map(Row(_))

  private def assertConcurrentAppend(e: SparkException): Unit =
    assert(e.getCause.isInstanceOf[io.delta.exceptions.ConcurrentAppendException],
      s"Expected ConcurrentAppendException, got: ${e.getCause}")

  test("disjoint data ranges: added file is skipped, no conflict") {
    withTempDir { dir =>
      createTable(dir)
      // A (loser) deletes id<50; B (winner) appends [1000,1100), disjoint from A's predicate.
      val txnA = deleteTxn(dir, "id < 50", dataSkipping = true)
      val txnB = appendTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      // Both committed: id<50 deleted, [1000,1100) appended.
      checkAnswer(
        spark.read.format("delta").load(dir.getAbsolutePath).select("id"), disjointExpected)
    }
  }

  test("overlapping data ranges: added file matches the predicate, still conflicts") {
    withTempDir { dir =>
      createTable(dir)
      // A's predicate id>=950 overlaps B's appended [1000,1100) file range -> not skippable.
      val txnA = deleteTxn(dir, "id >= 950", dataSkipping = true)
      val txnB = appendTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("feature disabled: disjoint data ranges still conflict") {
    withTempDir { dir =>
      createTable(dir)
      val txnA = deleteTxn(dir, "id < 50", dataSkipping = false)
      val txnB = appendTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("missing stats: disjoint ranges still conflict (one-way safety)") {
    withTempDir { dir =>
      // No indexed columns -> the appended file has no id stats -> must NOT be skipped.
      createTable(dir, numIndexedCols = Some(0))
      val txnA = deleteTxn(dir, "id < 50", dataSkipping = true)
      val txnB = appendTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("partitioned table: data skipping on a non-partition column avoids the conflict") {
    withTempDir { dir =>
      // Partitioned by `p`; the appended file shares the partition but its id range is disjoint.
      spark.range(start = 0, end = 1000, step = 1, numPartitions = 10).withColumn("p", lit(0))
        .write.partitionBy("p").format("delta").mode("append").save(dir.getAbsolutePath)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")

      val txnA = deleteTxn(dir, "id < 50", dataSkipping = true)
      val txnB = appendTxn(dir, 1000, 1100, partition = Some(0))

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      checkAnswer(
        spark.read.format("delta").load(dir.getAbsolutePath).select("id"), disjointExpected)
    }
  }

  private def manufacturedAdd(name: String): AddFile =
    AddFile(name, Map.empty[String, String], size = 1L, modificationTime = 1L, dataChange = true)

  test("empty read predicates on a non-blind-append txn: added file still conflicts") {
    // Soundness of the Serializable blind-append case. A transaction can be a NON-blind append
    // (it removes a file, so onlyAddFiles = false) and yet record no read predicates. With data
    // skipping enabled such a txn must still conflict-check every concurrently added file, exactly
    // as it does with the feature disabled. Before the guard on non-empty read predicates, the
    // empty read set produced an empty survivor set and the conflict was silently suppressed.
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      // An existing file to remove, which makes the loser a non-blind append with no reads.
      val existingRemove = log.update().allFiles.collect().head.remove
      withSQLConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> "true") {
        val loser = log.startTransaction()
        // Winner: a blind append committed while the loser is still open.
        log.startTransaction().commit(
          Seq(manufacturedAdd("winner.parquet")), DeltaOperations.Write(SaveMode.Append))
        // Loser adds AND removes without reading -> not a blind append, no read predicates.
        intercept[io.delta.exceptions.ConcurrentAppendException] {
          loser.commit(
            Seq(manufacturedAdd("loser.parquet"), existingRemove),
            DeltaOperations.Write(SaveMode.Append))
        }
      }
    }
  }

  test("whole-table read is never skipped: conflicts with a concurrent append") {
    // A whole-table read must treat every concurrently added file as a conflict, even with data
    // skipping enabled -- the feature is deliberately bypassed for readWholeTable.
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      withSQLConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> "true") {
        val loser = log.startTransaction()
        loser.readWholeTable()
        log.startTransaction().commit(
          Seq(manufacturedAdd("winner.parquet")), DeltaOperations.Write(SaveMode.Append))
        intercept[io.delta.exceptions.ConcurrentAppendException] {
          loser.commit(
            Seq(manufacturedAdd("loser.parquet")), DeltaOperations.Write(SaveMode.Append))
        }
      }
    }
  }

  test("filterFilesByDataSkipping AND-combines predicates within one read") {
    // Predicates from a SINGLE read have AND semantics (OR is only across independent reads, which
    // ConflictChecker unions by construction). This exercises that AND directly.
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = log.update()
      val allFiles = snapshot.allFiles.collect().toSeq
      assert(allFiles.size == 10)

      // Resolved catalyst predicates on the `id` column; built directly so the literal stays a
      // bare Long (a parsed `id >= 200` would wrap it in a Cast, which is not skipping-eligible).
      val id = AttributeReference("id", LongType)()
      val ge200 = GreaterThanOrEqual(id, Literal(200L))
      val lt300 = LessThan(id, Literal(300L))

      // Only the [200, 300) file can match `id >= 200 AND id < 300`. OR semantics would keep every
      // file with id >= 200 or id < 300, i.e. all 10.
      val survivors = snapshot.filterFilesByDataSkipping(allFiles, Seq(ge200, lt300))
      assert(survivors.size == 1,
        s"expected exactly the [200,300) file, got ${survivors.map(_.path).sorted}")

      // Sanity: a single predicate keeps its whole matching range (files with max id >= 200).
      val geOnly = snapshot.filterFilesByDataSkipping(allFiles, Seq(ge200))
      assert(geOnly.size == 8, s"expected 8 files with id >= 200, got ${geOnly.size}")
    }
  }

  test("filterFilesMatchingAnyReadPredicate OR-combines independent reads in one call") {
    // Independent reads have OR semantics: a file survives if it could match ANY read. This is the
    // multi-read path ConflictChecker uses -- all reads are evaluated together in a single job.
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = log.update()
      val allFiles = snapshot.allFiles.collect().toSeq
      assert(allFiles.size == 10)

      val id = AttributeReference("id", LongType)()
      // Read 1: id in [200, 300) -> the single [200,300) file. Read 2: id >= 800 -> the [800,900)
      // and [900,1000) files. Their union (OR across reads) is exactly 3 files.
      val read1 = Seq[Expression](
        GreaterThanOrEqual(id, Literal(200L)), LessThan(id, Literal(300L)))
      val read2 = Seq[Expression](GreaterThanOrEqual(id, Literal(800L)))
      val survivors =
        snapshot.filterFilesMatchingAnyReadPredicate(allFiles, Seq(read1, read2))
      assert(survivors.size == 3,
        s"expected [200,300) + [800,1000), got ${survivors.map(_.path).sorted}")

      // A read with no usable predicate matches everything, so nothing can be skipped -> all kept.
      val allKept = snapshot.filterFilesMatchingAnyReadPredicate(allFiles, Seq(read1, Seq.empty))
      assert(allKept.size == allFiles.size,
        s"a read with no predicate must keep all files, got ${allKept.size}")
    }
  }

  test("value-exact: modulo predicate min/max cannot skip is reconciled by actual-value scan") {
    withTempDir { dir =>
      createTable(dir)
      // A (loser) deletes ODD ids; B (winner) appends the all-EVEN ids in [1000,1100). The parities
      // are disjoint, but the appended file's min/max [1000,1098] spans odd values, so tier-1 stats
      // cannot prove `id % 2 = 1` unsatisfiable. Tier-2 value-exact reads the rows (all even) and
      // finds zero matches -> no conflict.
      val txnA = deleteTxn(dir, "id % 2 = 1", dataSkipping = true, valueExact = true)
      val txnB = appendEvenTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      // Both committed: odd ids in [0,1000) deleted; all-even [1000,1100) appended.
      val expected =
        ((0L until 1000L).filter(_ % 2 == 0) ++ (1000L until 1100L by 2)).map(Row(_))
      checkAnswer(
        spark.read.format("delta").load(dir.getAbsolutePath).select("id"), expected)
    }
  }

  test("value-exact disabled: modulo predicate still conflicts (stats cannot skip)") {
    // The gap value-exact closes: with only tier-1 stats, a modulo predicate the min/max cannot
    // resolve keeps the added file as a conflict candidate, so the disjoint-parity append
    // conflicts.
    withTempDir { dir =>
      createTable(dir)
      val txnA = deleteTxn(dir, "id % 2 = 1", dataSkipping = true, valueExact = false)
      val txnB = appendEvenTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("value-exact: a genuinely matching added file still conflicts (no over-skip)") {
    withTempDir { dir =>
      createTable(dir)
      // Loser deletes EVEN ids; winner appends all-even [1000,1100). Every appended row matches
      // `id % 2 = 0`, so value-exact must KEEP the file and the conflict must still stand.
      val txnA = deleteTxn(dir, "id % 2 = 0", dataSkipping = true, valueExact = true)
      val txnB = appendEvenTxn(dir, 1000, 1100)

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("filterFilesByValueExactScan drops a file whose rows never match, keeps one that does") {
    withTempDir { dir =>
      // A single file of all-even ids in [0,100). min/max = [0,98] spans odd values, so min/max
      // stats cannot prove `id % 2 = 1` unsatisfiable, but no row actually matches it.
      spark.range(0, 100, step = 2).repartition(1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = log.update()
      val allFiles = snapshot.allFiles.collect().toSeq
      assert(allFiles.size == 1, s"expected a single file, got ${allFiles.size}")

      val id = AttributeReference("id", LongType)()
      val odd = EqualTo(Remainder(id, Literal(2L)), Literal(1L))
      val even = EqualTo(Remainder(id, Literal(2L)), Literal(0L))

      // Value-exact: no row is odd -> file dropped; every row is even -> file kept (no over-drop).
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(odd))).isEmpty,
        "all-even file must be dropped for id % 2 = 1")
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(even))).size == 1,
        "all-even file must be kept for id % 2 = 0")

      // Contrast tier 1: min/max stats cannot resolve modulo, so it keeps the file regardless.
      assert(snapshot.filterFilesByDataSkipping(allFiles, Seq(odd)).size == 1,
        "stats skipping cannot resolve modulo -> keeps the file")

      // A read with no eligible filter matches everything -> keep all (cannot prove non-match).
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq.empty)).size == 1,
        "a read with no eligible filter must keep all files")
    }
  }

  test("filterFilesByValueExactScan keeps all candidates if any matches, drops all if none do") {
    withTempDir { dir =>
      // Two single-row-group files: one all-odd, one all-even. The scan is an existence check, not
      // per-file attribution -- the caller only needs a boolean -- so a predicate matched by EITHER
      // file keeps BOTH, and a predicate no file matches drops both.
      spark.range(1, 100, step = 2).repartition(1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath) // all-odd file
      spark.range(0, 100, step = 2).repartition(1)
        .write.format("delta").mode("append").save(dir.getAbsolutePath) // all-even file
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = log.update()
      val allFiles = snapshot.allFiles.collect().toSeq
      assert(allFiles.size == 2, s"expected two files, got ${allFiles.size}")

      val id = AttributeReference("id", LongType)()
      val odd = EqualTo(Remainder(id, Literal(2L)), Literal(1L))
      val even = EqualTo(Remainder(id, Literal(2L)), Literal(0L))
      val noMatch = EqualTo(Remainder(id, Literal(2L)), Literal(5L)) // id % 2 is 0 or 1, never 5

      // A row in either file matches -> keep every candidate.
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(odd))).size == 2,
        "the all-odd file matches -> keep both candidates")
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(even))).size == 2,
        "the all-even file matches -> keep both candidates")
      // No file has a matching row -> drop every candidate (safe: nothing can conflict).
      assert(snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(noMatch))).isEmpty,
        "no candidate matches -> drop all")
    }
  }

  test("filterFilesByValueExactScan keeps all files when a predicate is unresolvable (fail-safe)") {
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = log.update()
      val allFiles = snapshot.allFiles.collect().toSeq
      assert(allFiles.size == 10)

      // A predicate on a column that is not in the table: rebinding throws, and the fail-safe must
      // keep every file as a conflict candidate rather than let the scan failure abort a commit.
      val ghost = EqualTo(AttributeReference("does_not_exist", LongType)(), Literal(1L))
      val kept = snapshot.filterFilesByValueExactScan(allFiles, Seq(Seq(ghost)))
      assert(kept.size == allFiles.size,
        s"an unresolvable predicate must keep all files (fail-safe), got ${kept.size}")
    }
  }

  test("value-exact on a partitioned table: modulo predicate reconciled by actual-value scan") {
    withTempDir { dir =>
      // Partitioned by `p`; loser deletes ODD ids, winner appends all-EVEN ids into the same
      // partition. The data predicate `id % 2 = 1` is non-partition, so stats cannot skip the
      // appended file, but its rows are all even -> value-exact finds zero matches -> no conflict.
      spark.range(start = 0, end = 1000, step = 1, numPartitions = 10).withColumn("p", lit(0))
        .write.partitionBy("p").format("delta").mode("append").save(dir.getAbsolutePath)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")

      val txnA = deleteTxn(dir, "id % 2 = 1", dataSkipping = true, valueExact = true)
      val txnB = appendEvenTxn(dir, 1000, 1100, partition = Some(0))

      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      val expected =
        ((0L until 1000L).filter(_ % 2 == 0) ++ (1000L until 1100L by 2)).map(Row(_))
      checkAnswer(
        spark.read.format("delta").load(dir.getAbsolutePath).select("id"), expected)
    }
  }
}
