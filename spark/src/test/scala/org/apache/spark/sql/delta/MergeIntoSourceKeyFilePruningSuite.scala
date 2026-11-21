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

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions, UsageRecord}
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSourceReason, MergeStats}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for pruning the target files of a MERGE by the join key values of a small source
 * (`merge.sourceKeyFilePruning.enabled`).
 */
class MergeIntoSourceKeyFilePruningSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private val numFiles = 10
  private val rowsPerFile = 100

  private val pruningEnabled = DeltaSQLConf.MERGE_SOURCE_KEY_FILE_PRUNING_ENABLED.key
  private val maxSourceRows = DeltaSQLConf.MERGE_SOURCE_KEY_FILE_PRUNING_MAX_SOURCE_ROWS.key

  /**
   * Creates a target table with `numFiles` files. File `i` holds the keys
   * `[i * rowsPerFile, (i + 1) * rowsPerFile)` with `value = key`, `p = i`, `d = key` and
   * `s` = the key padded to 6 digits followed by 40 'x', longer than the 32-character prefix
   * that the string statistics keep.
   */
  private def createTarget(name: String, partitioned: Boolean): Unit = {
    val partitionClause = if (partitioned) "PARTITIONED BY (p)" else ""
    sql(s"CREATE TABLE $name (key INT, value INT, p INT, d DOUBLE, s STRING) USING delta " +
      partitionClause)
    (0 until numFiles).foreach { i =>
      spark.range(i * rowsPerFile, (i + 1) * rowsPerFile)
        .selectExpr(
          "CAST(id AS INT) AS key",
          "CAST(id AS INT) AS value",
          s"$i AS p",
          "CAST(id AS DOUBLE) AS d",
          "concat(lpad(CAST(id AS STRING), 6, '0'), repeat('x', 40)) AS s")
        .coalesce(1)
        .write.format("delta").mode("append").saveAsTable(name)
    }
  }

  private def isTahoeEvent(e: UsageRecord, opType: String): Boolean =
    e.metric == MetricDefinitions.EVENT_TAHOE.name && e.tags.get("opType").contains(opType)

  /** Runs a MERGE and returns its stats and its source-key file pruning events. */
  private def runMerge(mergeSql: String): (MergeStats, Seq[Map[String, Any]]) = {
    val events = Log4jUsageLogger.track { sql(mergeSql) }
    val stats = events.filter(isTahoeEvent(_, "delta.dml.merge.stats"))
    assert(stats.size === 1)
    val pruningEvents = events
      .filter(isTahoeEvent(_, "delta.dml.merge.sourceKeyFilePruning"))
      .map(e => JsonUtils.fromJson[Map[String, Any]](e.blob))
    (JsonUtils.fromJson[MergeStats](stats.head.blob), pruningEvents)
  }

  private case class MergeRun(
      statsWithPruning: MergeStats,
      statsWithoutPruning: MergeStats,
      pruningEvents: Seq[Map[String, Any]]) {
    def filesReadWithPruning: Long = statsWithPruning.targetAfterSkipping.files.get
    def filesReadWithoutPruning: Long = statsWithoutPruning.targetAfterSkipping.files.get
  }

  /**
   * Creates the source view `src` from `sourceQuery`, runs `mergeSql(targetTable)` on two copies
   * of the target, with and without source-key file pruning, and checks that both copies end with
   * the same rows. With `sourceIsDeltaTable`, `src` is a Delta table holding the rows of
   * `sourceQuery` (not a view, whose `View` node makes MERGE treat the source as
   * non-deterministic), which MERGE does not materialize unless `materializeSource` is `all`.
   */
  private def mergeWithAndWithoutPruning(
      sourceQuery: String,
      partitioned: Boolean = false,
      sourceIsDeltaTable: Boolean = false)(
      mergeSql: String => String): MergeRun = {
    var run: MergeRun = null
    withTable("target_pruned", "target_unpruned", "src") {
      withTempView("src") {
        if (sourceIsDeltaTable) {
          sql(s"CREATE TABLE src USING delta AS $sourceQuery")
        } else {
          sql(s"CREATE TEMP VIEW src AS $sourceQuery")
        }
        createTarget("target_pruned", partitioned)
        createTarget("target_unpruned", partitioned)
        val (statsWithPruning, pruningEvents) = withSQLConf(pruningEnabled -> "true") {
          runMerge(mergeSql("target_pruned"))
        }
        val (statsWithoutPruning, eventsWithoutPruning) = withSQLConf(pruningEnabled -> "false") {
          runMerge(mergeSql("target_unpruned"))
        }
        assert(eventsWithoutPruning.isEmpty)
        checkAnswer(spark.table("target_pruned"), spark.table("target_unpruned"))
        assert(statsWithoutPruning.targetAfterSkipping.files.contains(numFiles.toLong))
        run = MergeRun(statsWithPruning, statsWithoutPruning, pruningEvents)
      }
    }
    run
  }

  private def assertApplied(run: MergeRun, applied: Boolean): Unit = {
    assert(run.pruningEvents.size === 1)
    assert(run.pruningEvents.head("applied") === applied)
    if (!applied) {
      assert(run.filesReadWithPruning === numFiles)
    }
  }

  private def upsert(target: String, condition: String = "t.key = s.key"): String =
    s"""MERGE INTO $target t USING src s ON $condition
       |WHEN MATCHED THEN UPDATE SET t.value = s.value
       |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""".stripMargin

  // Two keys that update rows in files 1 and 9, and one key that matches no file.
  private val smallSource = "SELECT * FROM VALUES (150, -1), (950, -2), (5000, -3) AS v(key, value)"

  for {
    (materializeSource, sourceIsDeltaTable, materializeSourceReason) <- Seq(
      ("auto", false, MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA),
      ("all", false, MergeIntoMaterializeSourceReason.MATERIALIZE_ALL),
      ("all", true, MergeIntoMaterializeSourceReason.MATERIALIZE_ALL))
    deletionVectors <- Seq(true, false)
  } {
    test(s"upsert reads only the files that can hold the source keys - " +
        s"materializeSource=$materializeSource, sourceIsDeltaTable=$sourceIsDeltaTable, " +
        s"deletionVectors=$deletionVectors") {
      withSQLConf(
          DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> materializeSource,
          DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey ->
            deletionVectors.toString) {
        val run = mergeWithAndWithoutPruning(smallSource, sourceIsDeltaTable = sourceIsDeltaTable)(
          upsert(_))
        assertApplied(run, applied = true)
        assert(run.pruningEvents.head("materializeSourceReason") ===
          materializeSourceReason.toString)
        assert(run.filesReadWithPruning === 2)
        assert(run.statsWithPruning.targetRowsUpdated === 2)
        assert(run.statsWithPruning.targetRowsInserted === 1)
      }
    }
  }

  test("not applied when the source is not materialized") {
    val insertOnly = (target: String) =>
      s"""MERGE INTO $target t USING src s ON t.key = s.key
         |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""".stripMargin
    for ((materializeSource, sourceIsDeltaTable, mergeSql, reason) <- Seq(
        ("none", false, upsert(_: String),
          MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_NONE),
        ("none", true, upsert(_: String),
          MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_NONE),
        // A deterministic Delta scan is read again instead of being materialized.
        ("auto", true, upsert(_: String),
          MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO),
        // An insert-only MERGE reads its source once, without materializing it.
        ("auto", false, insertOnly,
          MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO_INSERT_ONLY))) {
      withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> materializeSource) {
        val run = mergeWithAndWithoutPruning(smallSource, sourceIsDeltaTable = sourceIsDeltaTable)(
          mergeSql)
        assertApplied(run, applied = false)
        assert(run.pruningEvents.head("isSourceMaterialized") === false)
        assert(run.pruningEvents.head("numSourceRowsRead") === 0)
        assert(run.pruningEvents.head("materializeSourceReason") === reason.toString)
      }
    }
  }

  test("delete, conditional update and multiple clauses return the same rows") {
    val source =
      "SELECT * FROM VALUES (150, -1), (151, 151), (950, -2), (5000, -3) AS v(key, value)"
    val run = mergeWithAndWithoutPruning(source) { target =>
      s"""MERGE INTO $target t USING src s ON t.key = s.key
         |WHEN MATCHED AND s.value = t.value THEN DELETE
         |WHEN MATCHED AND s.value < -1 THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED AND s.key > 1000 THEN INSERT (key, value) VALUES (s.key, s.value)
         |""".stripMargin
    }
    assertApplied(run, applied = true)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsDeleted === 1)
    assert(run.statsWithPruning.targetRowsUpdated === 1)
  }

  test("matched-only merge") {
    val run = mergeWithAndWithoutPruning(smallSource) { target =>
      s"MERGE INTO $target t USING src s ON t.key = s.key WHEN MATCHED THEN DELETE"
    }
    assertApplied(run, applied = true)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsDeleted === 2)
  }

  test("insert-only merge with a materialized source reads only the files that can hold the " +
      "source keys") {
    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "all") {
      val run = mergeWithAndWithoutPruning(smallSource) { target =>
        s"""MERGE INTO $target t USING src s ON t.key = s.key
           |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""".stripMargin
      }
      assertApplied(run, applied = true)
      assert(run.filesReadWithPruning === 2)
      assert(run.statsWithPruning.targetRowsInserted === 1)
    }
  }

  test("multi-column keys") {
    val source = "SELECT * FROM VALUES (150, 150), (950, 950), (5000, 0) AS v(key, value)"
    val run = mergeWithAndWithoutPruning(source) { target =>
      s"""MERGE INTO $target t USING src s ON t.key = s.key AND s.value = t.value
         |WHEN MATCHED THEN UPDATE SET t.value = -t.value
         |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""".stripMargin
    }
    assertApplied(run, applied = true)
    assert(run.pruningEvents.head("numKeyColumns") === 2)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsUpdated === 2)
  }

  test("source key expression") {
    val source = "SELECT * FROM VALUES (149, -1), (4999, -3) AS v(k, value)"
    val run = mergeWithAndWithoutPruning(source) { target =>
      s"""MERGE INTO $target t USING src s ON t.key = s.k + 1
         |WHEN MATCHED THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.k + 1, s.value)""".stripMargin
    }
    assertApplied(run, applied = true)
    assert(run.filesReadWithPruning === 1)
  }

  test("string keys longer than the statistics prefix") {
    val source =
      """SELECT concat(lpad(CAST(key AS STRING), 6, '0'), repeat('x', 40)) AS s, value
        |FROM VALUES (150, -1), (950, -2) AS v(key, value)""".stripMargin
    val run = mergeWithAndWithoutPruning(source) { target =>
      s"""MERGE INTO $target t USING src s ON t.s = s.s
         |WHEN MATCHED THEN UPDATE SET t.value = s.value""".stripMargin
    }
    assertApplied(run, applied = true)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsUpdated === 2)
  }

  test("partition column key prunes partitions") {
    val source = "SELECT * FROM VALUES (1, 150, -1), (9, 950, -2) AS v(p, key, value)"
    // `abs(t.key)` is not a column, so only the partition column `p` prunes files.
    val run = mergeWithAndWithoutPruning(source, partitioned = true) { target =>
      s"""MERGE INTO $target t USING src s ON t.p = s.p AND abs(t.key) = s.key
         |WHEN MATCHED THEN UPDATE SET t.value = s.value""".stripMargin
    }
    assertApplied(run, applied = true)
    assert(run.pruningEvents.head("numKeyColumns") === 1)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsUpdated === 2)
  }

  test("source without non-null keys reads no target file") {
    for (source <- Seq(
        "SELECT CAST(NULL AS INT) AS key, -1 AS value",
        "SELECT * FROM VALUES (150, -1) AS v(key, value) WHERE key < 0")) {
      val run = mergeWithAndWithoutPruning(source)(upsert(_))
      assertApplied(run, applied = true)
      assert(run.filesReadWithPruning === 0)
      assert(run.statsWithPruning.targetRowsUpdated === 0)
    }
  }

  test("source with maxSourceRows rows") {
    // 50 keys in file 1, 50 keys in file 9 and 900 keys that match no file.
    val source =
      """SELECT CAST(CASE WHEN id < 50 THEN 100 + id WHEN id < 100 THEN 850 + id
        |  ELSE 10000 + id END AS INT) AS key, -1 AS value
        |FROM range(1000)""".stripMargin
    val run = mergeWithAndWithoutPruning(source)(upsert(_))
    assertApplied(run, applied = true)
    assert(run.pruningEvents.head("numSourceRowsRead") === 1000)
    assert(run.filesReadWithPruning === 2)
    assert(run.statsWithPruning.targetRowsUpdated === 100)
    assert(run.statsWithPruning.targetRowsInserted === 900)
  }

  test("not applied when the source has more rows than maxSourceRows") {
    withSQLConf(maxSourceRows -> "2") {
      val run = mergeWithAndWithoutPruning(smallSource)(upsert(_))
      assertApplied(run, applied = false)
      assert(run.pruningEvents.head("numSourceRowsRead") === 3)
    }
    withSQLConf(maxSourceRows -> "3") {
      val run = mergeWithAndWithoutPruning(smallSource)(upsert(_))
      assertApplied(run, applied = true)
      assert(run.filesReadWithPruning === 2)
    }
    withSQLConf(maxSourceRows -> "0") {
      val run = mergeWithAndWithoutPruning(smallSource)(upsert(_))
      assert(run.pruningEvents.isEmpty)
      assert(run.filesReadWithPruning === numFiles)
    }
  }

  test("not applied with NOT MATCHED BY SOURCE clauses") {
    val run = mergeWithAndWithoutPruning(smallSource) { target =>
      s"""MERGE INTO $target t USING src s ON t.key = s.key
         |WHEN MATCHED THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED BY SOURCE AND t.key < 5 THEN DELETE""".stripMargin
    }
    assert(run.pruningEvents.isEmpty)
    assert(run.filesReadWithPruning === numFiles)
  }

  test("not applied without a usable equality on a target column") {
    val source = "SELECT * FROM VALUES (150, -1, 150D), (950, -2, 950D) AS v(key, value, d)"
    for (condition <- Seq(
        // NULL source keys would have to match NULL target keys.
        "t.key <=> s.key",
        // FLOAT/DOUBLE equality treats -0.0 and 0.0 (and NaNs) as equal.
        "t.d = s.d",
        // The target side is not a column.
        "abs(t.key) = s.key",
        // Not an equality between the target and the source.
        "t.key = t.value AND s.key = 150")) {
      val run = mergeWithAndWithoutPruning(source) { target =>
        s"MERGE INTO $target t USING src s ON $condition WHEN MATCHED THEN DELETE"
      }
      assert(run.pruningEvents.isEmpty, condition)
      assert(run.filesReadWithPruning === numFiles, condition)
    }
  }
}
