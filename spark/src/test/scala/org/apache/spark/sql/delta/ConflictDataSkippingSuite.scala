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

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.DeltaTestUtils.filterUsageRecords
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, EqualTo, Expression, GreaterThanOrEqual, LessThan, Literal, Multiply, Rand, Remainder}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
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

  /** Runs `sqlText` with `confs` set while it runs (including its commit). */
  private def sqlTxn(sqlText: String, confs: (String, String)*): () => Array[Row] =
    () => {
      withSQLConf(confs: _*) { sql(sqlText).collect() }
      Array.empty[Row]
    }

  private def dataSkippingConf(enabled: Boolean): (String, String) =
    DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> enabled.toString

  /** A DELETE that runs under the given data-skipping setting (evaluated on its commit thread). */
  private def deleteTxn(
      dir: File,
      condition: String,
      dataSkipping: Boolean,
      confs: (String, String)*): () => Array[Row] =
    sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE $condition",
      dataSkippingConf(dataSkipping) +: confs: _*)

  /**
   * The subset of `files` that conflict-time data skipping keeps as conflict candidates for the
   * reads `dataFiltersPerRead`: the files that could still match any of them.
   */
  private def survivingFiles(
      snapshot: Snapshot,
      files: Seq[AddFile],
      dataFiltersPerRead: Seq[Seq[Expression]]): Seq[AddFile] = {
    import org.apache.spark.sql.delta.implicits._
    snapshot.conflictSkippingCondition(dataFiltersPerRead) match {
      case None => files
      case Some(condition) =>
        val paths = snapshot.applyConflictSkippingCondition(files.toDF(spark), condition)
          .select("path").collect().map(_.getString(0)).toSet
        files.filter(f => paths.contains(f.path))
    }
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

  private def manufacturedAdd(
      name: String, partitionValues: Map[String, String] = Map.empty): AddFile =
    AddFile(name, partitionValues, size = 1L, modificationTime = 1L, dataChange = true)

  private val appliedEvent = "delta.conflictDetection.dataSkipping.applied"

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

  test("data skipping AND-combines predicates within one read") {
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
      val survivors = survivingFiles(snapshot, allFiles, Seq(Seq(ge200, lt300)))
      assert(survivors.size == 1,
        s"expected exactly the [200,300) file, got ${survivors.map(_.path).sorted}")

      // Sanity: a single predicate keeps its whole matching range (files with max id >= 200).
      val geOnly = survivingFiles(snapshot, allFiles, Seq(Seq(ge200)))
      assert(geOnly.size == 8, s"expected 8 files with id >= 200, got ${geOnly.size}")
    }
  }

  test("data skipping OR-combines independent reads in one condition") {
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
      val survivors = survivingFiles(snapshot, allFiles, Seq(read1, read2))
      assert(survivors.size == 3,
        s"expected [200,300) + [800,1000), got ${survivors.map(_.path).sorted}")

      // A read with no usable predicate matches everything, so nothing can be skipped -> all kept.
      val allKept = survivingFiles(snapshot, allFiles, Seq(read1, Seq.empty))
      assert(allKept.size == allFiles.size,
        s"a read with no predicate must keep all files, got ${allKept.size}")
    }
  }

  test("files with null stats are kept while files whose stats prove no match are skipped") {
    // Exercises the per-file `!verifyStatsForFilter` keep path: the skipping predicate IS built
    // (the table has min/max stats), but one candidate file carries no stats at all.
    withTempDir { dir =>
      createTable(dir)
      val snapshot = DeltaLog.forTable(spark, dir.getCanonicalPath).update()
      val allFiles = snapshot.allFiles.collect().toSeq
      val noStats = manufacturedAdd("no-stats.parquet")
      assert(noStats.stats == null)

      val id = AttributeReference("id", LongType)()
      val survivors = survivingFiles(snapshot, allFiles :+ noStats,
        Seq(Seq(LessThan(id, Literal(50L)))))
      // Only the [0,100) file can match id<50; the other 9 are skipped by their stats. The file
      // without stats cannot be proven to miss, so it is kept.
      assert(survivors.size == 2, s"got ${survivors.map(_.path).sorted}")
      assert(survivors.exists(_.path == noStats.path))
    }
  }

  test("concurrently added file with null stats still conflicts") {
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val id = AttributeReference("id", LongType)()
      withSQLConf(DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> "true") {
        val loser = log.startTransaction()
        loser.filterFiles(Seq(LessThan(id, Literal(50L))))
        log.startTransaction().commit(
          Seq(manufacturedAdd("winner-no-stats.parquet")), DeltaOperations.Write(SaveMode.Append))
        intercept[io.delta.exceptions.ConcurrentAppendException] {
          loser.commit(
            Seq(manufacturedAdd("loser.parquet")), DeltaOperations.Write(SaveMode.Append))
        }
      }
    }
  }

  test("multiple reads: data predicates are paired with their own partition predicate") {
    // Reads `p=1 AND id<50` and `p=2 AND id>=100`. A file in p=1 with id=150 matches neither read,
    // but matches (p=1 OR p=2) AND (id<50 OR id>=100), so evaluating partition and data
    // predicates independently would report a false conflict.
    withTempDir { dir =>
      spark.range(0, 1000, 1, 4).withColumn("p", lit(1))
        .union(spark.range(0, 1000, 1, 4).withColumn("p", lit(2)))
        .write.partitionBy("p").format("delta").save(dir.getAbsolutePath)
      sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
        s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val id = AttributeReference("id", LongType)()
      val p = AttributeReference("p", IntegerType)()
      var loserCount = 0

      /** Runs the two-read loser against a winner appending `id` in [start, end) to `winnerP`. */
      def run(start: Long, end: Long, winnerP: Int, dataSkipping: Boolean): Try[Long] = {
        loserCount += 1
        withSQLConf(DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key ->
            dataSkipping.toString) {
          val loser = log.startTransaction()
          loser.filterFiles(Seq(EqualTo(p, Literal(1)), LessThan(id, Literal(50L))))
          loser.filterFiles(Seq(EqualTo(p, Literal(2)), GreaterThanOrEqual(id, Literal(100L))))
          appendTxn(dir, start, end, partition = Some(winnerP))()
          Try(loser.commit(
            Seq(manufacturedAdd(s"loser-$loserCount.parquet", Map("p" -> "1"))),
            DeltaOperations.Write(SaveMode.Append)))
        }
      }

      def assertConflict(result: Try[Long]): Unit = assert(
        result.failed.toOption.exists(
          _.isInstanceOf[io.delta.exceptions.ConcurrentAppendException]),
        s"Expected ConcurrentAppendException, got: $result")

      // Cross terms (P1 AND D2) and (P2 AND D1): no read matches -> no conflict.
      val usageLogs = Log4jUsageLogger.track {
        assert(run(150, 151, winnerP = 1, dataSkipping = true).isSuccess)
        assert(run(10, 11, winnerP = 2, dataSkipping = true).isSuccess)
      }
      assert(filterUsageRecords(usageLogs, appliedEvent).size == 2)
      // A file matching one of the reads still conflicts, and records no applied event.
      val conflictLogs = Log4jUsageLogger.track {
        assertConflict(run(10, 11, winnerP = 1, dataSkipping = true))
        assertConflict(run(150, 151, winnerP = 2, dataSkipping = true))
      }
      assert(filterUsageRecords(conflictLogs, appliedEvent).isEmpty)
      // Feature disabled: partition pruning alone keeps the file -> conflict.
      assertConflict(run(150, 151, winnerP = 1, dataSkipping = false))
    }
  }

  private val fallbackEvent = "delta.conflictDetection.dataSkipping.fallback"

  test("skipping evaluation failure falls back to treating added files as conflicts") {
    // `id < 'abc'` builds a skipping predicate over the long `id` stats that fails once evaluated:
    // under ANSI mode the string does not cast to a long. (Without ANSI the cast yields NULL and
    // nothing fails, so the mode is pinned.) The loser records it with stats skipping off so its
    // own scan does not fail, then commits with skipping on.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTempDir { dir =>
        createTable(dir)
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val id = AttributeReference("id", LongType)()
        val loser = withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false") {
          val txn = log.startTransaction()
          txn.filterFiles(Seq(LessThan(id, Literal("abc"))))
          txn
        }
        // A stats-bearing winner file that the predicate cannot match if evaluated.
        appendTxn(dir, 1000, 1100)()
        val usageLogs = Log4jUsageLogger.track {
          withSQLConf(dataSkippingConf(enabled = true)) {
            intercept[io.delta.exceptions.ConcurrentAppendException] {
              loser.commit(
                Seq(manufacturedAdd("loser.parquet")), DeltaOperations.Write(SaveMode.Append))
            }
          }
        }
        assert(filterUsageRecords(usageLogs, fallbackEvent).size == 1)
        assert(filterUsageRecords(usageLogs, appliedEvent).isEmpty)
      }
    }
  }

  test("skipping evaluation failure on a partitioned table keeps partition filtering") {
    // Same failing skipping predicate, paired with the partition predicate `p = 1`. The fallback
    // drops only the data skipping: a file in another partition still does not conflict.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTempDir { dir =>
        spark.range(0, 1000, 1, 4).withColumn("p", lit(1))
          .union(spark.range(0, 1000, 1, 4).withColumn("p", lit(2)))
          .write.partitionBy("p").format("delta").save(dir.getAbsolutePath)
        sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
          s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val id = AttributeReference("id", LongType)()
        val p = AttributeReference("p", IntegerType)()

        /** Commits a loser reading `p = 1 AND id < 'abc'` after a winner appends to `winnerP`. */
        def run(winnerP: Int): (Try[Long], Seq[UsageRecord]) = {
          val loser = withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false") {
            val txn = log.startTransaction()
            txn.filterFiles(Seq(EqualTo(p, Literal(1)), LessThan(id, Literal("abc"))))
            txn
          }
          appendTxn(dir, 1000, 1100, partition = Some(winnerP))()
          var result: Try[Long] = null
          val usageLogs = Log4jUsageLogger.track {
            withSQLConf(dataSkippingConf(enabled = true)) {
              result = Try(loser.commit(
                Seq(manufacturedAdd(s"loser-$winnerP.parquet", Map("p" -> "1"))),
                DeltaOperations.Write(SaveMode.Append)))
            }
          }
          (result, usageLogs)
        }

        val (otherPartition, otherLogs) = run(winnerP = 2)
        assert(otherPartition.isSuccess, s"got $otherPartition")
        assert(filterUsageRecords(otherLogs, fallbackEvent).size == 1)
        val (samePartition, sameLogs) = run(winnerP = 1)
        assert(samePartition.failed.toOption.exists(
          _.isInstanceOf[io.delta.exceptions.ConcurrentAppendException]), s"got $samePartition")
        // Once the widened probe finds a file, the probe re-runs without widening (default on),
        // which falls back again.
        assert(filterUsageRecords(sameLogs, fallbackEvent).nonEmpty)
      }
    }
  }

  test("skipping evaluates candidates in batches and finds a conflict in a later batch") {
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val id = AttributeReference("id", LongType)()
      // Winner adds four files: three whose stats prove they miss `id < 50`, then the [0,100) file,
      // which matches. Reusing the table's files gives them real stats.
      val (matching, missing) = log.update().allFiles.collect().toSeq
        .partition(_.stats.contains("\"minValues\":{\"id\":0}"))
      val winnerAdds = missing.take(3) ++ matching
      assert(winnerAdds.size == 4)

      def run(adds: Seq[AddFile]): scala.util.Try[Long] = withSQLConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_BATCH_SIZE.key -> "1") {
        val loser = log.startTransaction()
        loser.filterFiles(Seq(LessThan(id, Literal(50L))))
        log.startTransaction().commit(adds, DeltaOperations.Write(SaveMode.Append))
        scala.util.Try(loser.commit(
          Seq(manufacturedAdd(s"loser-${adds.size}.parquet")),
          DeltaOperations.Write(SaveMode.Append)))
      }
      // Only the three non-matching files: every batch is skipped, no conflict.
      assert(run(winnerAdds.take(3).map(f => f.copy(path = "a-" + f.path))).isSuccess)
      // The matching file is the fourth batch: still found.
      assert(run(winnerAdds.map(f => f.copy(path = "b-" + f.path))).failed.toOption
        .exists(_.isInstanceOf[io.delta.exceptions.ConcurrentAppendException]))
    }
  }

  test("winner adding no files to check: data skipping is not reported as applied") {
    // The winner only removes files, so there is no added file to check and nothing is skipped.
    // The partitioned path still builds a skipping condition for its probe.
    Seq(true, false).foreach { partitioned =>
      withTempDir { dir =>
        val df = spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
        if (partitioned) {
          df.withColumn("p", (df("id") % 2).cast("int"))
            .write.partitionBy("p").format("delta").save(dir.getAbsolutePath)
        } else {
          df.write.format("delta").save(dir.getAbsolutePath)
        }
        sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
          s"('${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val id = AttributeReference("id", LongType)()
        val p = AttributeReference("p", IntegerType)()
        withSQLConf(dataSkippingConf(enabled = true)) {
          val loser = log.startTransaction()
          val readFilters =
            if (partitioned) Seq(EqualTo(p, Literal(0)), LessThan(id, Literal(50L)))
            else Seq(LessThan(id, Literal(50L)))
          val readPaths = loser.filterFiles(readFilters).map(_.path).toSet
          // Remove files the loser did not read, so the removal is not a conflict either.
          val removes = log.update().allFiles.collect().filterNot(f => readPaths.contains(f.path))
            .take(2).map(_.remove)
          assert(removes.nonEmpty)
          log.startTransaction().commit(removes, DeltaOperations.ManualUpdate)
          val usageLogs = Log4jUsageLogger.track {
            val partitionValues = if (partitioned) Map("p" -> "0") else Map.empty[String, String]
            loser.commit(
              Seq(manufacturedAdd("loser.parquet", partitionValues)),
              DeltaOperations.Write(SaveMode.Append))
          }
          assert(filterUsageRecords(usageLogs, appliedEvent).isEmpty,
            s"partitioned=$partitioned: no added file was checked, so skipping was not applied")
        }
      }
    }
  }

  test("a batch whose DataFrame fails to build is retried by the next probe, not skipped") {
    // A probe that fails while building a batch's DataFrame falls back; the next probe traverses
    // the same memoized batches and must still see that batch's files.
    val files = (0 until 3).map(i => manufacturedAdd(s"f$i.parquet"))
    var failOnce = true
    val built = mutable.ArrayBuffer[Seq[String]]()
    val candidates = new ConflictCandidateFiles(files, batchSize = 1, toDF = { group =>
      if (failOnce && group.head.path == "f1.parquet") {
        failOnce = false
        throw new IllegalStateException("injected toDF failure")
      }
      built += group.map(_.path)
      spark.emptyDataFrame
    })
    intercept[IllegalStateException] { candidates.batches.foreach(_ => ()) }
    assert(candidates.batches.size == 3)
    assert(built == Seq(Seq("f0.parquet"), Seq("f1.parquet"), Seq("f2.parquet")))
  }

  test("concurrent UPDATE moving a row into the read range conflicts, moving it elsewhere not") {
    // The loser UPDATEs id < 50. The winner UPDATEs id 500, which lives in a file the loser did not
    // read; the winner's rewritten file holds the new value.
    def run(dir: File, newValue: Long): Try[Array[Row]] = {
      createTable(dir)
      val txnA = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = -id WHERE id < 50",
        dataSkippingConf(enabled = true))
      val txnB = sqlTxn(s"UPDATE ${tableRef(dir)} SET id = $newValue WHERE id = 500")
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      Try(ThreadUtils.awaitResult(futureA, Duration.Inf))
    }
    // Moved to 10: the rewritten file's stats [10, 599] overlap id < 50 -> must conflict.
    withTempDir { dir =>
      val result = run(dir, newValue = 10L)
      assert(result.failed.toOption.exists(
        _.getCause.isInstanceOf[io.delta.exceptions.ConcurrentAppendException]), s"got $result")
    }
    // Moved to 700: the rewritten file's stats [501, 700] do not overlap -> both commit.
    withTempDir { dir =>
      val result = run(dir, newValue = 700L)
      assert(result.isSuccess, s"got $result")
      checkAnswer(spark.read.format("delta").load(dir.getAbsolutePath).select("id"),
        ((0L until 50L).map(-_) ++ (50L until 1000L).filter(_ != 500L) :+ 700L).map(Row(_)))
    }
  }

  test("concurrent MERGE: a disjoint append is skipped, an UPDATE moving a row into range is not") {
    // The MERGE reads the target with its target-only predicate `t.id < 50`.
    def mergeTxn(dir: File): () => Array[Row] = sqlTxn(
      s"""MERGE INTO ${tableRef(dir)} t USING (SELECT id FROM range(0, 10)) s
         |ON t.id = s.id AND t.id < 50
         |WHEN MATCHED THEN DELETE""".stripMargin,
      dataSkippingConf(enabled = true))

    withTempDir { dir =>
      createTable(dir)
      val (futureA, futureB) =
        runTxnsWithOrder__A_Start__B__A_End(mergeTxn(dir), appendTxn(dir, 1000, 1100))
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      checkAnswer(spark.read.format("delta").load(dir.getAbsolutePath).select("id"),
        ((10L until 1000L) ++ (1000L until 1100L)).map(Row(_)))
    }
    withTempDir { dir =>
      createTable(dir)
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(
        mergeTxn(dir), sqlTxn(s"UPDATE ${tableRef(dir)} SET id = 5 WHERE id = 500"))
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  /** [[createTable]] with deletion vectors enabled. */
  private def createDvTable(dir: File): Unit = {
    createTable(dir)
    sql(s"ALTER TABLE ${tableRef(dir)} SET TBLPROPERTIES " +
      s"('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'true')")
  }

  /** The files added by the latest commit, which must carry deletion vectors. */
  private def lastCommitDvAdds(dir: File): Seq[AddFile] = {
    val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
    val adds = log.getChanges(log.update().version).next()._2.collect { case a: AddFile => a }
    assert(adds.nonEmpty && adds.forall(_.deletionVector != null), s"expected DV adds: $adds")
    adds
  }

  test("deletion vectors: DELETEs in disjoint files do not conflict") {
    // The winner's DELETE re-adds the [500, 600) file with a deletion vector and that file's stats,
    // which do not overlap the loser's id < 50.
    Seq(true, false).foreach { dataSkipping =>
      withTempDir { dir =>
        createDvTable(dir)
        val txnA = deleteTxn(dir, "id < 50", dataSkipping)
        val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 550")
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        if (dataSkipping) {
          ThreadUtils.awaitResult(futureA, Duration.Inf)
          assert(lastCommitDvAdds(dir).size == 1)
          checkAnswer(spark.read.format("delta").load(dir.getAbsolutePath).select("id"),
            (50L until 1000L).filter(_ != 550L).map(Row(_)))
        } else {
          // Without skipping, the re-added file conflicts like any added file.
          val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
          assertConcurrentAppend(e)
        }
      }
    }
  }

  test("deletion vectors: DELETEs of different rows in the same file conflict") {
    withTempDir { dir =>
      createDvTable(dir)
      val txnA = deleteTxn(dir, "id = 10", dataSkipping = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id = 20")
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
    }
  }

  test("deletion vectors: stats that still cover deleted rows are used conservatively") {
    // The winner deletes id < 30 from the [0, 100) file. Its re-added file keeps the file's stats
    // [0, 99] (not tight: they still cover the deleted rows), so the loser's id < 20, which only
    // matches rows the winner deleted, still conflicts.
    withTempDir { dir =>
      createDvTable(dir)
      val txnA = deleteTxn(dir, "id < 20", dataSkipping = true)
      val txnB = sqlTxn(s"DELETE FROM ${tableRef(dir)} WHERE id < 30")
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
      assertConcurrentAppend(e)
      val Seq(winnerAdd) = lastCommitDvAdds(dir)
      assert(winnerAdd.tightBounds.contains(false), winnerAdd.stats)
    }
  }

  test("column mapping: stats keyed by physical names are matched to the logical column") {
    Seq((1000L, 1100L, true), (0L, 10L, false)).foreach { case (start, end, commits) =>
      withTempDir { dir =>
        sql(s"CREATE TABLE ${tableRef(dir)} (id BIGINT) USING delta TBLPROPERTIES (" +
          s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name', " +
          s"'${DeltaConfigs.ISOLATION_LEVEL.key}' = 'Serializable')")
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        val snapshot = DeltaLog.forTable(spark, dir.getCanonicalPath).update()
        val physicalName = DeltaColumnMapping.getPhysicalName(snapshot.schema("id"))
        assert(physicalName != "id")
        assert(snapshot.allFiles.collect().forall(_.stats.contains(s""""$physicalName":""")))

        val txnA = deleteTxn(dir, "id < 50", dataSkipping = true)
        val (futureA, futureB) =
          runTxnsWithOrder__A_Start__B__A_End(txnA, appendTxn(dir, start, end))
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        if (commits) {
          ThreadUtils.awaitResult(futureA, Duration.Inf)
          checkAnswer(spark.read.format("delta").load(dir.getAbsolutePath).select("id"),
            disjointExpected)
        } else {
          val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
          assertConcurrentAppend(e)
        }
      }
    }
  }

  test("non-deterministic and unusable predicates are not used to skip, other conjuncts are") {
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val id = AttributeReference("id", LongType)()
      val lt50 = LessThan(id, Literal(50L))
      // id < cast(rand(0) * 50 as bigint): references id, but non-deterministic.
      val nonDeterministic =
        LessThan(id, Cast(Multiply(Rand(Literal(0L)), Literal(50.0)), LongType))
      assert(!nonDeterministic.deterministic)
      // id % 7 = 3: deterministic, but min/max stats cannot evaluate it.
      val unusable = EqualTo(Remainder(id, Literal(7L)), Literal(3L))
      var appended = 0L

      /** Commits a loser reading `readFilters` after a winner appends a disjoint id range. */
      def run(readFilters: Seq[Expression]): Try[Long] =
        withSQLConf(dataSkippingConf(enabled = true)) {
          val loser = log.startTransaction()
          loser.filterFiles(readFilters)
          val start = 1000L + appended
          appended += 100
          appendTxn(dir, start, start + 100)()
          Try(loser.commit(
            Seq(manufacturedAdd(s"loser-$start.parquet")), DeltaOperations.Write(SaveMode.Append)))
        }
      def conflicted(result: Try[Long]): Boolean = result.failed.toOption.exists(
        _.isInstanceOf[io.delta.exceptions.ConcurrentAppendException])

      Seq(Seq(nonDeterministic), Seq(unusable), Seq(nonDeterministic, unusable)).foreach { read =>
        val result = run(read)
        assert(conflicted(result), s"$read: nothing to skip with, expected a conflict: $result")
      }
      Seq(Seq(nonDeterministic, lt50), Seq(unusable, lt50)).foreach { read =>
        val result = run(read)
        assert(result.isSuccess, s"$read: id < 50 alone proves the file cannot match: $result")
      }
    }
  }

  test("stats skipping disabled: conflict-time data skipping does not apply") {
    withTempDir { dir =>
      createTable(dir)
      val txnA = deleteTxn(dir, "id < 50", dataSkipping = true,
        DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false")
      val txnB = appendTxn(dir, 1000, 1100)
      val usageLogs = Log4jUsageLogger.track {
        val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
        ThreadUtils.awaitResult(futureB, Duration.Inf)
        val e = intercept[SparkException] { ThreadUtils.awaitResult(futureA, Duration.Inf) }
        assertConcurrentAppend(e)
      }
      assert(filterUsageRecords(usageLogs, appliedEvent).isEmpty)
    }
  }

  test("applied event is reported once per commit across several winning commits") {
    withTempDir { dir =>
      createTable(dir)
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val id = AttributeReference("id", LongType)()

      /** Commits a loser reading id < 50 after winners appending each of `winnerRanges`. */
      def run(winnerRanges: (Long, Long)*): (Try[Long], Seq[UsageRecord]) =
        withSQLConf(dataSkippingConf(enabled = true)) {
          val loser = log.startTransaction()
          loser.filterFiles(Seq(LessThan(id, Literal(50L))))
          winnerRanges.foreach { case (start, end) => appendTxn(dir, start, end)() }
          var result: Try[Long] = null
          val usageLogs = Log4jUsageLogger.track {
            result = Try(loser.commit(
              Seq(manufacturedAdd(s"loser-${log.update().version}.parquet")),
              DeltaOperations.Write(SaveMode.Append)))
          }
          (result, usageLogs)
        }

      // Two winners, both skipped: one event, for the version the loser committed.
      val (committed, committedLogs) = run((1000L, 1100L), (1100L, 1200L))
      assert(committed.isSuccess, s"got $committed")
      val events = filterUsageRecords(committedLogs, appliedEvent)
      assert(events.size == 1)
      val data = JsonUtils.mapper.readValue[Map[String, Any]](events.head.blob)
      assert(data("committedVersion").toString == committed.get.toString)
      // The first winner is skipped, the second conflicts: the commit fails, no event.
      val (conflicted, conflictedLogs) = run((1200L, 1300L), (0L, 10L))
      assert(conflicted.failed.toOption.exists(
        _.isInstanceOf[io.delta.exceptions.ConcurrentAppendException]), s"got $conflicted")
      assert(filterUsageRecords(conflictedLogs, appliedEvent).isEmpty)
    }
  }
}
