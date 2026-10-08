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

package io.delta.spark.internal.v2.read

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

private[read] trait DeltaV2LimitPushdownE2ETests {
  self: DeltaV2ScanE2ETestUtils =>

  test("a basic LIMIT returns the requested rows and reaches the scan") {
    val table = "v2_scan_e2e_limit_basic"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT, name STRING) USING $tableProvider")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 3")
      assert(df.count() == 3L, "LIMIT 3 should return exactly 3 rows")
      assertExpectedPushedLimit(df, 3, "basic LIMIT read")
    }
  }

  test("a LIMIT larger than the table returns every row and reaches the scan") {
    val table = "v2_scan_e2e_limit_larger_than_table"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1), (2), (3)")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 100")
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
      assertExpectedPushedLimit(df, 100, "LIMIT larger than table")
    }
  }

  test("LIMIT one returns one row and reaches the scan") {
    val table = "v2_scan_e2e_limit_one"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1), (2), (3)")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 1")
      assert(df.count() == 1L, "LIMIT 1 should return exactly 1 row")
      assertExpectedPushedLimit(df, 1, "LIMIT one read")
    }
  }

  test("LIMIT zero returns no rows without planning a scan") {
    val table = "v2_scan_e2e_limit_zero"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1), (2), (3)")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 0")
      checkAnswer(df, Seq.empty)
      val plan = df.queryExecution.optimizedPlan
      assert(
        plan.collect { case relation: DataSourceV2ScanRelation => relation }.isEmpty,
        s"LIMIT 0 should be optimized away before scan planning, got:\n$plan")
    }
  }

  test("LIMIT on an empty table returns no rows and reaches the scan") {
    val table = "v2_scan_e2e_limit_empty"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 10")
      checkAnswer(df, Seq.empty)
      assertExpectedPushedLimit(df, 10, "LIMIT on empty table")
    }
  }

  test("LIMIT with deletion vectors returns live rows and reaches the scan") {
    val table = "v2_scan_e2e_limit_dv"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, value STRING) USING $tableProvider " +
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        spark
          .range(1000)
          .selectExpr("id", "cast(id as string) as value")
          .write
          .mode("append")
          .insertInto(table)
        sql(s"DELETE FROM $table WHERE id % 2 = 0")
      }

      val query = s"SELECT * FROM $table LIMIT 50"
      assertAnySelectedFileHasDeletionVector(
        sql(query),
        "LIMIT read with deletion vectors")
      val df = sql(query)
      val rows = df.collect().toSeq
      assert(rows.length == 50, "LIMIT 50 with DVs should return exactly 50 rows")
      assert(
        rows.forall(_.getLong(0) % 2 == 1),
        s"LIMIT 50 with DVs returned a deleted even id: ${rows.mkString(", ")}")
      assertExpectedPushedLimit(df, 50, "LIMIT read with deletion vectors")
      assertExpectedScan(df, "LIMIT read with deletion vectors")
    }
  }

  test("LIMIT with heavy deletion vectors spans enough live rows") {
    val table = "v2_scan_e2e_limit_heavy_dv"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, value STRING) USING $tableProvider " +
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        spark
          .range(0, 100)
          .selectExpr("id", "cast(id as string) as value")
          .coalesce(1)
          .write
          .mode("append")
          .insertInto(table)
        spark
          .range(100, 200)
          .selectExpr("id", "cast(id as string) as value")
          .coalesce(1)
          .write
          .mode("append")
          .insertInto(table)
        sql(
          s"DELETE FROM $table WHERE " +
            "(id >= 40 AND id < 100) OR (id >= 140 AND id < 200)")
      }

      val numFiles = fileCount(table)
      assert(numFiles == 2L)
      val query = s"SELECT * FROM $table LIMIT 50"
      assertAllSelectedFilesHaveDeletionVectors(
        sql(query),
        "LIMIT read with heavy deletion vectors")
      val df = sql(query)
      val rows = df.collect().toSeq
      assert(rows.length == 50, "LIMIT 50 with heavy DVs should return exactly 50 rows")
      assert(
        rows.forall { row =>
          val id = row.getLong(0)
          (id >= 0 && id < 40) || (id >= 100 && id < 140)
        },
        s"LIMIT 50 with heavy DVs returned a deleted id: ${rows.mkString(", ")}")
      assertExpectedPushedLimit(df, 50, "LIMIT read with heavy deletion vectors")
      assertExpectedScan(df, "LIMIT read with heavy deletion vectors")
      assertRouteSpecificInputFileCount(
        df,
        expected = 2,
        context = "LIMIT 50 must select both heavy-DV files")
    }
  }

  test("a partition filter with LIMIT returns matching rows") {
    val table = "v2_scan_e2e_limit_partition_filter"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id INT, part STRING) USING $tableProvider " +
            "PARTITIONED BY (part)")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b')")
      }

      val df = sql(s"SELECT * FROM $table WHERE part = 'a' LIMIT 2")
      val rows = df.collect().toSeq
      assert(rows.length == 2, "Partition filter + LIMIT should return 2 rows")
      assert(
        rows.forall(_.getString(1) == "a"),
        s"Partition filter + LIMIT returned a non-matching row: ${rows.mkString(", ")}")
      if (spark.version.startsWith("4.0")) {
        // Spark 4.0 keeps the partition filter above the scan, which prevents LIMIT pushdown.
        assertExpectedNoPushedLimit(df, "partition filter with LIMIT")
      } else {
        assertExpectedPushedLimit(df, 2, "partition filter with LIMIT")
      }
    }
  }

  test("a data filter with LIMIT returns matching rows without pushdown") {
    val table = "v2_scan_e2e_limit_data_filter"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT, name STRING) USING $tableProvider")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
      }

      val df = sql(s"SELECT * FROM $table WHERE id > 2 LIMIT 2")
      val rows = df.collect().toSeq
      assert(rows.length == 2, "Data filter + LIMIT should return 2 rows")
      assert(
        rows.forall(_.getInt(0) > 2),
        s"Data filter + LIMIT returned an id <= 2: ${rows.mkString(", ")}")
      assertExpectedNoPushedLimit(df, "data filter with LIMIT")
    }
  }

  test("a projected LIMIT returns only the requested column and reaches the scan") {
    val table = "v2_scan_e2e_limit_projection"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id INT, name STRING, value DOUBLE) USING $tableProvider")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)")
      }

      val df = sql(s"SELECT name FROM $table LIMIT 2")
      val rows = df.collect().toSeq
      assert(rows.length == 2, "Column projection + LIMIT should return 2 rows")
      assert(df.columns.toSeq == Seq("name"), "projected result should contain only name")
      val expectedNames = Set("a", "b", "c")
      assert(
        rows.forall(row => !row.isNullAt(0) && expectedNames.contains(row.getString(0))),
        s"Column projection + LIMIT returned an unexpected value: ${rows.mkString(", ")}")
      assertExpectedPushedLimit(df, 2, "projected LIMIT read")
    }
  }

  test("LIMIT over multiple files returns the requested rows and reaches the scan") {
    val table = "v2_scan_e2e_limit_multiple_files"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        // Preserve the source's deterministic one-row-per-file layout.
        (0 until 20).foreach { id =>
          sql(s"INSERT INTO $table VALUES ($id)")
        }
      }

      val numFiles = fileCount(table)
      assert(numFiles == 20L, s"expected 20 source files, got $numFiles")

      val df = sql(s"SELECT * FROM $table LIMIT 3")
      assert(df.count() == 3L, "LIMIT 3 over 20 single-row files should return 3 rows")
      assertExpectedPushedLimit(df, 3, "LIMIT read over multiple files")
      assertRouteSpecificInputFileCount(
        df,
        expected = 3,
        context = "LIMIT 3 route-specific input-file count")
    }
  }

  test("LIMIT pushdown retains files without numRecords") {
    val table = "v2_scan_e2e_limit_missing_num_records"
    withTable(table) {
      withV1Mode {
        withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
          sql(
            s"CREATE TABLE $table (id INT) USING $tableProvider " +
              "TBLPROPERTIES ('delta.enableRowTracking' = 'false')")
          sql(s"INSERT INTO $table VALUES (1)")
          sql(s"INSERT INTO $table VALUES (2)")
        }
      }

      val numFiles = fileCount(table)
      assert(numFiles == 2L, s"expected two source files, got $numFiles")

      val df = sql(s"SELECT * FROM $table LIMIT 1")
      val rows = df.collect().toSeq
      assert(rows.length == 1, s"expected one row, got ${rows.length}")
      assert(Set(Row(1), Row(2)).contains(rows.head), s"unexpected source row ${rows.head}")
      assertExpectedPushedLimit(df, 1, "LIMIT read with missing numRecords")
      assertExpectedScan(df, "LIMIT read with missing numRecords")
      assertRouteSpecificInputFileCount(
        df,
        expected = 2,
        // With file statistics disabled, neither file counts toward the pushed limit.
        context = "LIMIT 1 must retain files without numRecords")
    }
  }

  test("a non-deterministic filter prevents LIMIT pushdown") {
    val table = "v2_scan_e2e_limit_nondeterministic_filter"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        spark
          .range(100)
          .selectExpr("cast(id as int) as id")
          .write
          .mode("append")
          .insertInto(table)
      }

      val df = sql(s"SELECT * FROM $table WHERE rand(0) > 0.5 LIMIT 5")
      assert(df.count() == 5L)
      assertExpectedNoPushedLimit(df, "non-deterministic filter with LIMIT")
    }
  }

  test("LIMIT with OFFSET pushes the combined row requirement") {
    val table = "v2_scan_e2e_limit_offset"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1), (2), (3), (4), (5)")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 2 OFFSET 1")
      assert(df.count() == 2L)
      assertExpectedPushedLimit(df, 3, "LIMIT with OFFSET read")
    }
  }

  test("ORDER BY with LIMIT returns ordered rows without pushdown") {
    val table = "v2_scan_e2e_limit_order_by"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT, name STRING) USING $tableProvider")
        sql(
          s"INSERT INTO $table VALUES " +
            "(3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')")
      }

      val df = sql(s"SELECT * FROM $table ORDER BY id LIMIT 3")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
      assertExpectedNoPushedLimit(df, "ORDER BY with LIMIT")
    }
  }

  // Spark offers LIMIT only to batch scan builders; a streaming LIMIT stays a stateful operator.
  test("a streaming LIMIT is applied above the micro-batch scan") {
    val table = "v2_scan_e2e_limit_streaming"
    val queryName = "v2_scan_e2e_limit_streaming_sink"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1), (2), (3)")
        sql(s"INSERT INTO $table VALUES (4), (5)")
      }

      val batch = sql(s"SELECT * FROM $table LIMIT 3")
      assert(batch.count() == 3L, "batch LIMIT 3 should return exactly 3 rows")
      assertExpectedPushedLimit(batch, 3, "batch LIMIT")
      withTempView(queryName) {
        val stream = spark.readStream.option("startingVersion", "1").table(table).limit(3)
        val (sinkRows, execution) = runStreamToCompletion(stream, queryName)
        val rows = sinkRows.map(_.getInt(0))
        assert(rows.length == 3, s"streaming LIMIT 3 should emit 3 rows, got $rows")
        assert(
          rows.distinct.length == 3 && rows.forall((1 to 5).contains),
          s"streaming LIMIT 3 emitted unexpected rows: $rows")
        assertExpectedStreamingScan(execution, "streaming LIMIT read")
      }
    }
  }

  test("dynamic partition pruning prevents LIMIT pushdown") {
    val fact = "v2_scan_e2e_limit_dpp_fact"
    val dimension = "v2_scan_e2e_limit_dpp_dimension"
    withTable(fact, dimension) {
      withV1Mode {
        sql(
          s"CREATE TABLE $fact (id INT, part INT) USING $tableProvider " +
            "PARTITIONED BY (part)")
        sql(s"INSERT INTO $fact VALUES (1, 1), (2, 2), (3, 3)")
        sql(s"CREATE TABLE $dimension (part INT, tag STRING) USING $tableProvider")
        sql(
          s"INSERT INTO $dimension VALUES " +
            "(1, 'drop'), (2, 'keep'), (3, 'drop')")
      }

      val query =
        s"SELECT fact.id FROM $fact fact " +
          s"JOIN $dimension dimension ON fact.part = dimension.part " +
          "WHERE dimension.tag = 'keep' LIMIT 1"
      withSQLConf(
          "spark.sql.adaptive.enabled" -> "false",
          "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true") {
        val df = sql(query)
        checkAnswer(df, Seq(Row(2)))
        assertExpectedScanCount(df, 1, "DPP LIMIT read")
        assertExpectedDynamicPruning(df, "DPP LIMIT read")
        assertExpectedNoPushedLimit(df, "DPP LIMIT read")
      }
    }
  }

  test("LIMIT on a column-mapped table returns source rows and reaches the scan") {
    val table = "v2_scan_e2e_limit_column_mapping"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id INT, name STRING) USING $tableProvider " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      }

      val df = sql(s"SELECT * FROM $table LIMIT 2")
      val rows = df.collect().toSeq
      val expectedRows = Set(Row(1, "a"), Row(2, "b"), Row(3, "c"))
      assert(rows.length == 2, s"expected two rows, got ${rows.length}")
      assert(
        rows.forall(row => !row.anyNull && expectedRows.contains(row)),
        s"expected two non-null source rows, got ${rows.mkString(", ")}")
      assertExpectedPushedLimit(df, 2, "LIMIT read with column mapping")
    }
  }
}

/** Covers LIMIT correctness and pushdown semantics shared across Delta V2 scan paths, including
 * dynamic partition pruning, column mapping, and streaming reads.
 */
class DeltaV2LimitPushdownE2ESuite
  extends DeltaV2ScanE2ETestUtils
  with DeltaV2LimitPushdownE2ETests
{
}
