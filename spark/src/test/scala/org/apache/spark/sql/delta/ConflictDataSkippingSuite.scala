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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThanOrEqual, LessThan, Literal}
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

  /** A DELETE that runs under the given data-skipping setting (evaluated on its commit thread). */
  private def deleteTxn(dir: File, condition: String, dataSkipping: Boolean): () => Array[Row] =
    () => {
      withSQLConf(
        DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> dataSkipping.toString) {
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
}
