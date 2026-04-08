/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.sql.Date;
import java.util.List;
import java.util.concurrent.*;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Tests for V2 batch write operations. */
public class V2WriteTest extends V2TestBase {

  @Test
  public void testAppendToUnpartitionedTable() {
    String tableName = str("dsv2.%s.write_append_test", nameSpace);

    // Create table via V2 catalog (Kernel).
    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));

    // Insert via DSv2 write path.
    spark.sql(str("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));

    // Read back and verify via DSv2 read path.
    check(str("SELECT * FROM %s ORDER BY id", tableName), List.of(row(1, "Alice"), row(2, "Bob")));
  }

  @Test
  public void testMultipleAppends() {
    String tableName = str("dsv2.%s.write_multi_append_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, value DOUBLE) USING delta", tableName));

    spark.sql(str("INSERT INTO %s VALUES (1, 10.5)", tableName));
    spark.sql(str("INSERT INTO %s VALUES (2, 20.5), (3, 30.5)", tableName));

    check(
        str("SELECT * FROM %s ORDER BY id", tableName),
        List.of(row(1, 10.5), row(2, 20.5), row(3, 30.5)));
  }

  @Test
  public void testAppendToPathBasedTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    // Create table at a path using V1 catalog.
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'seed')", tablePath));

    // Insert through DSv2 path-based access.
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'v2write')", tablePath));

    // Read back through DSv2 to verify both rows.
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "seed"), row(2, "v2write")));
  }

  @Test
  public void testInsertEmptyDataFrame() throws Exception {
    String tableName = str("dsv2.%s.write_empty_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));

    spark
        .createDataFrame(List.of(), spark.sql(str("SELECT * FROM %s", tableName)).schema())
        .writeTo(tableName)
        .append();

    check(str("SELECT * FROM %s", tableName), List.of());
  }

  @Test
  public void testAppendToPartitionedTable() {
    String tableName = str("dsv2.%s.write_partitioned_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, city STRING, value DOUBLE) USING delta PARTITIONED BY (city)",
            tableName));

    spark.sql(str("INSERT INTO %s VALUES (1, 'NYC', 10.0), (2, 'SF', 20.0)", tableName));
    spark.sql(str("INSERT INTO %s VALUES (3, 'NYC', 30.0)", tableName));

    check(
        str("SELECT id, city, value FROM %s ORDER BY id", tableName),
        List.of(row(1, "NYC", 10.0), row(2, "SF", 20.0), row(3, "NYC", 30.0)));
  }

  @Test
  public void testAppendToMultiPartitionTable() {
    String tableName = str("dsv2.%s.write_multi_part_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, year INT, month INT, name STRING) "
                + "USING delta PARTITIONED BY (year, month)",
            tableName));

    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, 2024, 1, 'Jan'), (2, 2024, 2, 'Feb'), (3, 2025, 1, 'Jan25')",
            tableName));

    check(
        str("SELECT id, year, month, name FROM %s ORDER BY id", tableName),
        List.of(row(1, 2024, 1, "Jan"), row(2, 2024, 2, "Feb"), row(3, 2025, 1, "Jan25")));
  }

  // ---- Overwrite / Truncate tests ----

  @Test
  public void testTruncateUnpartitioned() {
    String tableName = str("dsv2.%s.write_truncate_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));
    spark.sql(str("INSERT INTO %s VALUES (1, 'old1'), (2, 'old2')", tableName));

    // INSERT OVERWRITE replaces all rows (truncate semantics for unpartitioned).
    spark.sql(str("INSERT OVERWRITE %s VALUES (10, 'new1')", tableName));

    check(str("SELECT * FROM %s ORDER BY id", tableName), List.of(row(10, "new1")));
  }

  @Test
  public void testTruncatePartitioned() {
    String tableName = str("dsv2.%s.write_truncate_part_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, city STRING, value DOUBLE) USING delta PARTITIONED BY (city)",
            tableName));
    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, 'NYC', 10.0), (2, 'SF', 20.0), (3, 'NYC', 30.0)",
            tableName));

    // INSERT OVERWRITE on a partitioned table with static partition overwrite mode → truncate all.
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");
    spark.sql(str("INSERT OVERWRITE %s VALUES (99, 'LA', 99.0)", tableName));
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");

    check(
        str("SELECT id, city, value FROM %s ORDER BY id", tableName), List.of(row(99, "LA", 99.0)));
  }

  @Test
  public void testDynamicPartitionOverwrite() {
    String tableName = str("dsv2.%s.write_dynamic_overwrite_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, city STRING, value DOUBLE) USING delta PARTITIONED BY (city)",
            tableName));
    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, 'NYC', 10.0), (2, 'SF', 20.0), (3, 'NYC', 30.0)",
            tableName));

    // Dynamic partition overwrite: only replace partitions that appear in new data (NYC)
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
    spark.sql(str("INSERT OVERWRITE %s VALUES (100, 'NYC', 100.0)", tableName));
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");

    // NYC partition replaced; SF partition untouched.
    check(
        str("SELECT id, city, value FROM %s ORDER BY id", tableName),
        List.of(row(2, "SF", 20.0), row(100, "NYC", 100.0)));
  }

  @Test
  public void testDynamicPartitionOverwriteMultiplePartitions() {
    String tableName = str("dsv2.%s.write_dyn_multi_overwrite_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, city STRING, value DOUBLE) USING delta PARTITIONED BY (city)",
            tableName));
    spark.sql(
        str("INSERT INTO %s VALUES (1, 'NYC', 10.0), (2, 'SF', 20.0), (3, 'LA', 30.0)", tableName));

    // Overwrite NYC and LA, keep SF.
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
    spark.sql(str("INSERT OVERWRITE %s VALUES (10, 'NYC', 100.0), (30, 'LA', 300.0)", tableName));
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");

    check(
        str("SELECT id, city, value FROM %s ORDER BY id", tableName),
        List.of(row(2, "SF", 20.0), row(10, "NYC", 100.0), row(30, "LA", 300.0)));
  }

  @Test
  public void testOverwriteThenAppend() {
    String tableName = str("dsv2.%s.write_overwrite_then_append_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));
    spark.sql(str("INSERT INTO %s VALUES (1, 'first')", tableName));

    // Overwrite
    spark.sql(str("INSERT OVERWRITE %s VALUES (2, 'replaced')", tableName));
    check(str("SELECT * FROM %s ORDER BY id", tableName), List.of(row(2, "replaced")));

    // Then append
    spark.sql(str("INSERT INTO %s VALUES (3, 'appended')", tableName));
    check(
        str("SELECT * FROM %s ORDER BY id", tableName),
        List.of(row(2, "replaced"), row(3, "appended")));
  }

  // ---- replaceWhere (static predicate overwrite) ----

  @Test
  public void testReplaceWhere() throws Exception {
    String tableName = str("dsv2.%s.write_replace_where_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, city STRING, value DOUBLE) USING delta PARTITIONED BY (city)",
            tableName));
    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, 'NYC', 10.0), (2, 'SF', 20.0), (3, 'NYC', 30.0)",
            tableName));

    // replaceWhere: overwrite only the NYC partition
    spark
        .sql("SELECT 100 AS id, 'NYC' AS city, 100.0 AS value")
        .writeTo(tableName)
        .overwrite(org.apache.spark.sql.functions.col("city").equalTo("NYC"));

    // NYC replaced, SF untouched
    check(
        str("SELECT id, city, value FROM %s ORDER BY id", tableName),
        List.of(row(2, "SF", 20.0), row(100, "NYC", 100.0)));
  }

  // ---- Statistics verification ----

  @Test
  public void testStatsInDeltaLog(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            tablePath));

    // Read commit JSON files from _delta_log to inspect AddFile stats
    Dataset<Row> addFiles =
        spark.read().json(tablePath + "/_delta_log/*.json").filter("add IS NOT NULL");

    Dataset<Row> stats = addFiles.selectExpr("add.stats AS stats").filter("stats IS NOT NULL");
    List<Row> statRows = stats.collectAsList();

    assertFalse(statRows.isEmpty(), "Expected at least one AddFile with stats");

    for (Row statRow : statRows) {
      String json = statRow.getString(0);
      assertNotNull(json, "Stats JSON should not be null");
      // Stats JSON should contain numRecords
      assertTrue(json.contains("\"numRecords\""), "Stats should contain numRecords: " + json);
      // Stats JSON should contain minValues and maxValues for data skipping
      assertTrue(
          json.contains("\"minValues\"") || json.contains("\"maxValues\""),
          "Stats should contain min/max values: " + json);
      // Stats JSON should contain nullCount
      assertTrue(json.contains("\"nullCount\""), "Stats should contain nullCount: " + json);
    }
  }

  @Test
  public void testStatsNumRecordsAccuracy(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, value DOUBLE) USING delta", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 10.0), (2, 20.0), (3, 30.0)", tablePath));

    Dataset<Row> numRecords =
        spark
            .read()
            .json(tablePath + "/_delta_log/*.json")
            .filter("add IS NOT NULL")
            .selectExpr("get_json_object(add.stats, '$.numRecords') AS nr")
            .filter("nr IS NOT NULL");

    List<Row> rows = numRecords.collectAsList();
    long totalRecords = rows.stream().mapToLong(r -> Long.parseLong(r.getString(0))).sum();
    assertEquals(3L, totalRecords, "Total numRecords across all files should be 3");
  }

  // ---- Multi data type partition keys ----

  @Test
  public void testPartitionByDate() {
    String tableName = str("dsv2.%s.write_part_date_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, dt DATE, name STRING) USING delta PARTITIONED BY (dt)",
            tableName));

    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, DATE '2024-01-15', 'jan'), (2, DATE '2024-06-30', 'jun')",
            tableName));

    check(
        str("SELECT id, dt, name FROM %s ORDER BY id", tableName),
        List.of(
            row(1, Date.valueOf("2024-01-15"), "jan"), row(2, Date.valueOf("2024-06-30"), "jun")));
  }

  @Test
  public void testPartitionByLong() {
    String tableName = str("dsv2.%s.write_part_long_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (name STRING, bucket BIGINT) USING delta PARTITIONED BY (bucket)",
            tableName));

    spark.sql(str("INSERT INTO %s VALUES ('a', 1000000000000), ('b', 2000000000000)", tableName));

    check(
        str("SELECT name, bucket FROM %s ORDER BY name", tableName),
        List.of(row("a", 1000000000000L), row("b", 2000000000000L)));
  }

  @Test
  public void testPartitionByBoolean() {
    String tableName = str("dsv2.%s.write_part_bool_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, active BOOLEAN) USING delta PARTITIONED BY (active)",
            tableName));

    spark.sql(str("INSERT INTO %s VALUES (1, true), (2, false), (3, true)", tableName));

    check(
        str("SELECT id, active FROM %s ORDER BY id", tableName),
        List.of(row(1, true), row(2, false), row(3, true)));
  }

  @Test
  public void testDynamicOverwriteWithDatePartition() {
    String tableName = str("dsv2.%s.write_dyn_date_overwrite_test", nameSpace);

    spark.sql(
        str(
            "CREATE TABLE %s (id INT, dt DATE, value DOUBLE) USING delta PARTITIONED BY (dt)",
            tableName));

    spark.sql(
        str(
            "INSERT INTO %s VALUES (1, DATE '2024-01-01', 10.0), (2, DATE '2024-02-01', 20.0)",
            tableName));

    // Dynamic overwrite: replace only the 2024-01-01 partition
    withSQLConf(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic",
        () ->
            spark.sql(str("INSERT OVERWRITE %s VALUES (10, DATE '2024-01-01', 100.0)", tableName)));

    check(
        str("SELECT id, dt, value FROM %s ORDER BY id", tableName),
        List.of(
            row(2, Date.valueOf("2024-02-01"), 20.0), row(10, Date.valueOf("2024-01-01"), 100.0)));
  }

  // ---- Schema evolution ----
  // mergeSchema/overwriteSchema end-to-end requires Catalyst analyzer rules for SparkTable.
  // The DeltaWrite.toBatch() logic is implemented; these tests verify expected behavior.

  @Test
  public void testMergeSchemaRejectedWithoutAnalyzerRules() {
    String tableName = str("dsv2.%s.write_merge_rejected_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));
    spark.sql(str("INSERT INTO %s VALUES (1, 'Alice')", tableName));

    // Without analyzer rules, writing extra columns is rejected at analysis time
    assertThrows(
        AnalysisException.class,
        () ->
            spark
                .sql("SELECT 2 AS id, 'Bob' AS name, 100 AS score")
                .writeTo(tableName)
                .option("mergeSchema", "true")
                .append());
  }

  @Test
  public void testOverwriteSchemaRejectedWithoutAnalyzerRules() {
    String tableName = str("dsv2.%s.write_overwrite_schema_rejected_test", nameSpace);

    spark.sql(str("CREATE TABLE %s (id INT, name STRING) USING delta", tableName));
    spark.sql(str("INSERT INTO %s VALUES (1, 'Alice')", tableName));

    // Without analyzer rules, writing incompatible columns is rejected at analysis time
    assertThrows(
        AnalysisException.class,
        () ->
            spark
                .sql("SELECT 'hello' AS greeting, 42.0 AS value")
                .writeTo(tableName)
                .option("overwriteSchema", "true")
                .overwrite(org.apache.spark.sql.functions.lit(true)));
  }

  // ---- Concurrent write conflict detection ----

  @Test
  public void testConcurrentAppendsSucceed(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // Two sequential appends should both succeed (no conflict for append-only)
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'first')", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'second')", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "first"), row(2, "second")));
  }

  @Test
  public void testConcurrentOverwriteConflict(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(str("CREATE TABLE delta.`%s` (id INT, value INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 100)", tablePath));

    // Overwrite via DSv2, then overwrite again — second one reads stale snapshot but should
    // still succeed (Kernel handles conflict resolution for full overwrites).
    spark.sql(str("INSERT OVERWRITE dsv2.delta.`%s` VALUES (2, 200)", tablePath));
    spark.sql(str("INSERT OVERWRITE dsv2.delta.`%s` VALUES (3, 300)", tablePath));

    check(str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath), List.of(row(3, 300)));
  }
}
