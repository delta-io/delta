/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproduces the doc-claimed MANAGED-specific edge-data streaming bugs (NPE in
 * OnHeapColumnVector.putNotNulls for null columns / boolean nulls / complex types) over the Unity
 * Catalog catalog path, alongside the EXTERNAL counterparts.
 *
 * <p>Task D (`V2StreamingEdgeDataReadTest`) already proved these don't fire on EXTERNAL via
 * file-path access (`dsv2.delta.<path>`). This suite exercises the same shapes through Unity
 * Catalog's `unity.default.<table>` access path for both `EXTERNAL` and `MANAGED` table types.
 *
 * <p>Each test runs twice via `@TestAllTableTypes`. The MANAGED variant is the relevant repro
 * target; the EXTERNAL variant via UC catalog is a useful control (UC catalog read path may differ
 * from raw file-path read path even for external tables).
 */
public class UCDeltaStreamingEdgeDataReadTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /**
   * Allocates a fresh local checkpoint directory. Checkpoints must be on local FS since UC server
   * holds the cloud credentials, not Spark.
   */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  // -------------------------------------------------------------------------
  // Case 2: All-null INT column
  // Doc claim: NPE in OnHeapColumnVector.putNotNulls (this.nulls uninitialized)
  //            on MANAGED. EXTERNAL was clean (Task D).
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedNullsInColumns(TableType tableType) throws Exception {
    withNewTable(
        "edge_all_null_col",
        "v INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (NULL), (NULL), (NULL)", tableName);

          String queryName =
              "edge_all_null_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> input = spark().readStream().format("delta").table(tableName);

          // AvailableNow gives a deterministic single batch over the existing 3 nulls.
          input
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), "expected 3 null rows; got: " + rows);
          for (Row r : rows) {
            assertEquals(true, r.isNullAt(0), "expected null value, got: " + r);
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 3: BOOLEAN column with nulls
  // Doc claim: same NPE through bit-packing path on MANAGED.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedBooleanNulls(TableType tableType) throws Exception {
    withNewTable(
        "edge_bool_nulls",
        "b BOOLEAN",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (true), (false), (NULL), (true), (NULL)", tableName);

          String queryName =
              "edge_bool_nulls_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(5, rows.size(), "expected 5 rows; got: " + rows);
          long nullCount = rows.stream().filter(r -> r.isNullAt(0)).count();
          assertEquals(2L, nullCount, "expected 2 null booleans; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 4a: Complex types initial-snapshot (ARRAY / MAP / STRUCT)
  // Sanity: Task D showed EXTERNAL passes initial-snapshot. Confirm same on UC.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedComplexTypesInitialSnapshot(TableType tableType) throws Exception {
    withNewTable(
        "edge_complex_initial",
        "id INT, arr ARRAY<INT>, mp MAP<STRING,INT>, st STRUCT<a: INT, b: STRING>",
        tableType,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, array(1, 2, 3), map('k1', 10), named_struct('a', 1, 'b', 'foo')), "
                  + "(2, array(), map(), named_struct('a', 2, 'b', 'bar')), "
                  + "(3, NULL, NULL, NULL)",
              tableName);

          String queryName =
              "edge_complex_init_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 4b: Complex types INCREMENTAL (commits mid-stream)
  // Doc claim: NPE in SparkMicroBatchStreaming for INCREMENTAL on MANAGED.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedComplexTypesIncremental(TableType tableType) throws Exception {
    withNewTable(
        "edge_complex_incr",
        "id INT, arr ARRAY<INT>, mp MAP<STRING,INT>, st STRUCT<a: INT, b: STRING>",
        tableType,
        tableName -> {
          // Seed one initial row.
          sql(
              "INSERT INTO %s VALUES (1, array(1,2), map('a',1), named_struct('a',1,'b','x'))",
              tableName);

          String queryName =
              "edge_complex_incr_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery query = null;
          try {
            // Continuous stream: process initial commit, then commit two more rows mid-stream
            // to exercise the INCREMENTAL micro-batch path the doc flagged.
            query =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .option("checkpointLocation", checkpoint())
                    .start();
            query.processAllAvailable();

            sql(
                "INSERT INTO %s VALUES (2, array(3,4,5), map('b',2,'c',3), "
                    + "named_struct('a',2,'b','y'))",
                tableName);
            query.processAllAvailable();

            sql("INSERT INTO %s VALUES (3, NULL, NULL, NULL)", tableName);
            query.processAllAvailable();

            List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          } finally {
            if (query != null) {
              query.processAllAvailable();
              query.stop();
              query.awaitTermination(10000);
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  // -------------------------------------------------------------------------
  // Bonus 1: Empty table — initial snapshot
  // Task D showed EXTERNAL passes; confirm UC catalog (both types) is also clean.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedEmptyTableInitialSnapshot(TableType tableType) throws Exception {
    withNewTable(
        "edge_empty_table",
        "v INT",
        tableType,
        tableName -> {
          // No data inserted (table is empty) -- but for MANAGED we need at least the
          // catalogManaged metadata commit, which CREATE TABLE produces.
          String queryName = "edge_empty_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(0, rows.size(), "empty table should yield 0 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Bonus 2: NULL partition value (HIVE_DEFAULT_PARTITION sentinel)
  // Task D's external file-path test passed; UC catalog's path-style table read may differ.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testManagedNullPartitionValue(TableType tableType) throws Exception {
    withNewTable(
        "edge_null_part",
        "id INT, part STRING",
        "part",
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, NULL), (3, 'c')", tableName);

          String queryName =
              "edge_null_part_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          long nullParts = rows.stream().filter(r -> r.isNullAt(r.fieldIndex("part"))).count();
          assertEquals(1L, nullParts, "expected 1 null partition value; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
