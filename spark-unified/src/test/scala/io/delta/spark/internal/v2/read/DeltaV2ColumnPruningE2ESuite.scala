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

import org.apache.spark.sql.Row

private[read] trait DeltaV2ColumnPruningE2ETests {
  self: DeltaV2ScanE2ETestUtils =>

  // V2ScanColumnPruningIntegrationTest.testSelectSubsetPrunesDataColumns
  test("selecting a subset prunes unused data columns") {
    val table = "v2_scan_e2e_pruning_subset"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (a INT, b INT, c INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1, 2, 3), (4, 5, 6)")
      }

      val df = sql(s"SELECT a FROM $table")
      checkAnswer(df, Seq(Row(1), Row(4)))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 1, "subset column-pruning read")
      assert(
        requiredSchemas == Seq(Seq("a")),
        s"expected the scan to read only a, got $requiredSchemas")
    }
  }

  // V2ScanColumnPruningIntegrationTest.testFilterPushdownLeavesOnlyProjectedColumns
  test("a residual filter retains its column and prunes unused columns") {
    val table = "v2_scan_e2e_pruning_filter"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (a INT, b INT, c INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1, 2, 3), (4, 5, 6)")
      }

      val df = sql(s"SELECT a FROM $table WHERE b > 3")
      checkAnswer(df, Seq(Row(4)))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 1, "residual-filter column-pruning read")
      assert(
        requiredSchemas == Seq(Seq("a", "b")),
        s"expected the scan to read a and b but not c, got $requiredSchemas")
    }
  }

  // V2ScanColumnPruningIntegrationTest.testAllColumnsUsedMeansNoPruning
  test("using every data column leaves the read schema unchanged") {
    val table = "v2_scan_e2e_pruning_all_columns"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (a INT, b INT) USING $tableProvider")
        sql(s"INSERT INTO $table VALUES (1, 2), (3, 4)")
      }

      val df = sql(s"SELECT a, b FROM $table")
      checkAnswer(df, Seq(Row(1, 2), Row(3, 4)))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 1, "all-columns pruning read")
      assert(
        requiredSchemas == Seq(Seq("a", "b")),
        s"expected the scan to read a and b, got $requiredSchemas")
    }
  }

  // V2ScanColumnPruningIntegrationTest.testPartitionOnlyQueryHasEmptyReadDataSchema
  test("a partition-only query has an empty data read schema") {
    val table = "v2_scan_e2e_pruning_partition_only"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (a INT, p STRING) USING $tableProvider " +
            "PARTITIONED BY (p)")
        sql(s"INSERT INTO $table VALUES (1, 'x'), (2, 'y')")
      }

      val df = sql(s"SELECT p FROM $table")
      checkAnswer(df, Seq(Row("x"), Row("y")))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 1, "partition-only column-pruning read")
      assert(
        requiredSchemas == Seq(Seq.empty[String]),
        s"expected an empty data requiredSchema, got $requiredSchemas")
    }
  }

  // V2ScanColumnPruningIntegrationTest.testSubqueryDecorrelation_prunesUnusedColumns
  test("subquery decorrelation prunes unused columns") {
    val orders = "v2_scan_e2e_pruning_orders"
    val vips = "v2_scan_e2e_pruning_vips"
    withTable(orders, vips) {
      withV1Mode {
        sql(
          s"CREATE TABLE $orders (" +
            s"order_id INT, customer_id INT, amount INT, status STRING) " +
            s"USING $tableProvider")
        sql(
          s"INSERT INTO $orders VALUES " +
            "(1, 10, 100, 'open'), (2, 20, 200, 'closed')")
        sql(s"CREATE TABLE $vips (customer_id INT) USING $tableProvider")
        sql(s"INSERT INTO $vips VALUES (10), (30)")
      }

      val df = sql(
        s"SELECT order_id, amount FROM $orders " +
          s"WHERE customer_id IN (SELECT customer_id FROM $vips)")
      checkAnswer(df, Seq(Row(1, 100)))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 2, "decorrelated subquery column-pruning read")
      val expectedSchemas = Set(
        Seq("order_id", "customer_id", "amount"),
        Seq("customer_id"))
      assert(
        requiredSchemas.toSet == expectedSchemas,
        s"expected orders and VIP required schemas $expectedSchemas, got $requiredSchemas")
    }
  }

  // V2ScanColumnPruningIntegrationTest.testJoinPrunesUnreferencedColumns
  test("a join prunes unreferenced columns from both sides") {
    val left = "v2_scan_e2e_pruning_join_left"
    val right = "v2_scan_e2e_pruning_join_right"
    withTable(left, right) {
      withV1Mode {
        sql(s"CREATE TABLE $left (a INT, b INT, c INT) USING $tableProvider")
        sql(s"INSERT INTO $left VALUES (1, 2, 3), (4, 5, 6)")
        sql(s"CREATE TABLE $right (x INT, y INT, z INT) USING $tableProvider")
        sql(s"INSERT INTO $right VALUES (2, 20, 200), (5, 50, 500)")
      }

      val df =
        sql(s"SELECT a, y FROM $left t1 JOIN $right t2 ON t1.b = t2.x")
      checkAnswer(df, Seq(Row(1, 20), Row(4, 50)))
      val requiredSchemas =
        expectedScanRequiredFieldNames(df, 2, "join column-pruning read")
      val expectedSchemas = Set(Seq("a", "b"), Seq("x", "y"))
      assert(
        requiredSchemas.toSet == expectedSchemas,
        s"expected left and right required schemas $expectedSchemas, got $requiredSchemas")
    }
  }
}

/** Covers required-schema and column-pruning contracts shared across Delta V2 scan paths.
 */
class DeltaV2ColumnPruningE2ESuite
  extends DeltaV2ScanE2ETestUtils
  with DeltaV2ColumnPruningE2ETests
{
}
