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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports the 5 currently-passing cases of {@link
 * io.delta.spark.internal.v2.V2StreamingColumnMappingTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so the same shapes run on both EXTERNAL and MANAGED tables
 * via {@code @TestAllTableTypes}.
 *
 * <p>Schema-tracking cases (drop column, rename column, add-then-rename) are intentionally skipped:
 * they exercise the schema-tracking-log path which is WIP (Bug #8) on DSv2.
 *
 * <p>The MANAGED variant is the relevant repro target -- catalogManaged tables surface the
 * MANAGED-only reader-factory bugs described in the task brief.
 */
public class UCDeltaStreamingColumnMappingTest extends UCDeltaTableIntegrationBaseTest {

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
  // Case 1: Column mapping mode=name x stream basic
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testStreamingBasic_columnMappingModeName(TableType tableType) throws Exception {
    withNewTable(
        "cm_name_basic",
        "id INT, name STRING, value DOUBLE",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tableName);

          String queryName =
              "cm_name_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF = spark().readStream().format("delta").table(tableName);
          assertTrue(streamingDF.isStreaming());

          StreamingQuery q =
              streamingDF
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(1, 2), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 2: Column mapping mode=id x stream basic
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testStreamingBasic_columnMappingModeId(TableType tableType) throws Exception {
    withNewTable(
        "cm_id_basic",
        "id INT, name STRING, value DOUBLE",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'id', "
            + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", tableName);

          String queryName =
              "cm_id_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF = spark().readStream().format("delta").table(tableName);
          assertTrue(streamingDF.isStreaming());

          StreamingQuery q =
              streamingDF
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(1, 2), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 3: Column mapping x DV. DELETE rows then stream the latest snapshot.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testColumnMapping_withDeletionVectors(TableType tableType) throws Exception {
    withNewTable(
        "cm_dv",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql(
              "INSERT INTO %s SELECT cast(id as int), cast(id as string) FROM range(10)",
              tableName);
          sql("DELETE FROM %s WHERE id < 3", tableName);

          String queryName = "cm_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Initial snapshot is read from the latest version, after DELETE -> expect ids 3..9.
          assertEquals(7, rows.size(), "expected 7 surviving rows; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(3, 4, 5, 6, 7, 8, 9), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // -------------------------------------------------------------------------
  // Case 4: Column mapping x startingVersion. Stream from a version > 0.
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testColumnMapping_startingVersion(TableType tableType) throws Exception {
    withNewTable(
        "cm_starting_version",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          long createVersion = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (2, 'Bob')", tableName);
          sql("INSERT INTO %s VALUES (3, 'Charlie')", tableName);

          // Start from the second user insert so we should see Bob and Charlie only.
          long startVersion = v1 + 1;
          String queryName =
              "cm_starting_v_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(startVersion))
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(
              2,
              rows.size(),
              "expected 2 rows after startingVersion=" + startVersion + "; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(2, 3), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
          // Reference createVersion so unused-variable warnings stay quiet across linters.
          assertTrue(createVersion >= 0L);
        });
  }

  // -------------------------------------------------------------------------
  // Case 5: Column mapping x Trigger.AvailableNow
  // -------------------------------------------------------------------------

  @TestAllTableTypes
  public void testColumnMapping_triggerAvailableNow(TableType tableType) throws Exception {
    withNewTable(
        "cm_avail_now",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          String queryName =
              "cm_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
                  .start();
          assertTrue(q.awaitTermination(60_000), "AvailableNow query should terminate");

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(1, 2), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
