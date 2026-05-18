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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports {@link io.delta.spark.internal.v2.V2StreamingRowTrackingTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so each case runs against both EXTERNAL and MANAGED tables
 * via {@code @TestAllTableTypes}.
 *
 * <p>Fix-10 (commit {@code 6d2e568d5}, "wire row-tracking metadata columns through DSv2 streaming")
 * landed against the EXTERNAL reader-factory wiring. MANAGED tables go through a separate
 * reader-factory wiring path inside the catalog-managed read flow, so the {@code _metadata.row_id}
 * / {@code _metadata.row_commit_version} projection cases (Cases 2-9) are the highest-probability
 * MANAGED-only failures.
 */
public class UCDeltaStreamingRowTrackingTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Default row-tracking property; UC MANAGED tables already enable RT, but EXTERNAL needs it. */
  private static final String RT_PROPS = "'delta.enableRowTracking' = 'true'";

  @TestAllTableTypes
  public void testStreamFromRowTrackedTableBasic(TableType tableType) throws Exception {
    withNewTable(
        "rt_basic",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tableName);

          String queryName = "rt_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(3, rows.size());
          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
          }
          assertEquals(Set.of(1L, 2L, 3L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: Fix-10 wired _metadata.row_id through the EXTERNAL streaming
  // reader-factory. MANAGED has separate reader-factory wiring inside the catalog-managed read
  // flow that may not project _metadata.row_id, so this case is expected to fail for MANAGED.
  @TestAllTableTypes
  public void testStreamProjectsRowId(TableType tableType) throws Exception {
    withNewTable(
        "rt_row_id",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tableName);

          String queryName = "rt_row_id_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");

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
          assertEquals(3, rows.size());
          Map<Long, Long> idToRowId = new HashMap<>();
          for (Row r : rows) {
            idToRowId.put(r.getLong(0), r.getLong(1));
          }
          assertEquals(
              Set.of(0L, 1L, 2L), new HashSet<>(idToRowId.values()), "Expected row_ids 0,1,2");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: same _metadata.row_commit_version projection -- Fix-10 only fixed
  // EXTERNAL.
  @TestAllTableTypes
  public void testStreamProjectsRowCommitVersionMonotonic(TableType tableType) throws Exception {
    withNewTable(
        "rt_rcv_monotonic",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (2, 'Bob')", tableName);
          long v2 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (3, 'Charlie')", tableName);
          long v3 = currentVersion(tableName);

          String queryName =
              "rt_rcv_monotonic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_commit_version AS rcv");

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
          assertEquals(3, rows.size());
          Map<Long, Long> idToRcv = new HashMap<>();
          for (Row r : rows) {
            idToRcv.put(r.getLong(0), r.getLong(1));
          }
          assertEquals(v1, idToRcv.get(1L), "Alice's rcv == version of insert 1");
          assertEquals(v2, idToRcv.get(2L), "Bob's rcv == version of insert 2");
          assertEquals(v3, idToRcv.get(3L), "Charlie's rcv == version of insert 3");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: _metadata.row_id projection through AvailableNow.
  @TestAllTableTypes
  public void testRowTrackingWithAvailableNowTrigger(TableType tableType) throws Exception {
    withNewTable(
        "rt_avail_now",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tableName);

          String queryName =
              "rt_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr(
                      "id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");

          StreamingQuery q =
              streamingDF
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint())
                  .trigger(Trigger.AvailableNow())
                  .start();
          assertTrue(q.awaitTermination(60_000L));

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), "AvailableNow should drain all rows");
          Set<Long> rowIds = new HashSet<>();
          for (Row r : rows) {
            rowIds.add(r.getLong(1));
          }
          assertEquals(Set.of(0L, 1L, 2L), rowIds);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: same _metadata projection across stream restart on MANAGED.
  @TestAllTableTypes
  public void testRowTrackingAcrossStreamRestart(TableType tableType) throws Exception {
    withNewTable(
        "rt_restart",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          String queryName = "rt_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr(
                      "id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");

          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          sql("INSERT INTO %s VALUES (3, 'Charlie'), (4, 'Dave')", tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr(
                      "id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(4, rows.size(), "All 4 rows should arrive across restart");
          Map<Long, Long> idToRowId = new HashMap<>();
          for (Row r : rows) {
            idToRowId.put(r.getLong(0), r.getLong(1));
          }
          // row_ids should be unique across the table.
          Set<Long> rowIds = new HashSet<>(idToRowId.values());
          assertEquals(4, rowIds.size(), "row_ids must be unique");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: row_id projection on a DV-filtered scan.
  @TestAllTableTypes
  public void testRowTrackingWithDeletionVectorsPreservesIds(TableType tableType) throws Exception {
    withNewTable(
        "rt_dv",
        "id LONG, name STRING",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql("INSERT INTO %s SELECT id, cast(id as string) FROM range(1000)", tableName);
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName = "rt_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");

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
          assertEquals(500, rows.size(), "Expected only odd ids (DV-filtered)");
          for (Row r : rows) {
            long id = r.getLong(0);
            assertEquals(1L, id % 2, "Only odd IDs should survive deletion");
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: combined row-tracking + column-mapping projection.
  @TestAllTableTypes
  public void testRowTrackingWithColumnMapping(TableType tableType) throws Exception {
    withNewTable(
        "rt_cm",
        "id LONG, name STRING",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true', 'delta.columnMapping.mode' = 'name', "
            + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tableName);

          String queryName = "rt_cm_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "name", "_metadata.row_id AS row_id");

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
          assertEquals(3, rows.size());
          Map<Long, Long> idToRowId = new HashMap<>();
          for (Row r : rows) {
            idToRowId.put(r.getLong(0), r.getLong(2));
          }
          assertEquals(Set.of(0L, 1L, 2L), new HashSet<>(idToRowId.values()));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: row_id projection after INSERT OVERWRITE.
  @TestAllTableTypes
  public void testRowTrackingWithInsertOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "rt_overwrite",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);
          sql("INSERT OVERWRITE TABLE %s VALUES (10, 'X'), (20, 'Y')", tableName);

          String queryName =
              "rt_overwrite_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");

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
          assertEquals(2, rows.size(), "Stream should see only post-overwrite rows");
          Set<Long> ids = new HashSet<>();
          Set<Long> rowIds = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
            rowIds.add(r.getLong(1));
          }
          assertEquals(Set.of(10L, 20L), ids);
          assertEquals(2, rowIds.size(), "row_ids should be unique");
          for (long rid : rowIds) {
            assertTrue(rid >= 2L, "Expected row_id >= 2 after overwrite; got " + rid);
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // BUG-LIKELY ON MANAGED: row_id preservation across MERGE rewrites + ignoreChanges streaming.
  @TestAllTableTypes
  public void testRowTrackingWithMergeIgnoreChanges(TableType tableType) throws Exception {
    withNewTable(
        "rt_merge_ignore",
        "id LONG, name STRING",
        null,
        tableType,
        RT_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", tableName);

          // Pre-MERGE row_ids via a batch read against the same table.
          List<Row> beforeRows =
              spark()
                  .sql("SELECT id, _metadata.row_id AS row_id FROM " + tableName + " ORDER BY id")
                  .collectAsList();
          Map<Long, Long> beforeIdToRowId = new HashMap<>();
          for (Row r : beforeRows) {
            beforeIdToRowId.put(r.getLong(0), r.getLong(1));
          }

          spark().sql("DROP VIEW IF EXISTS rt_merge_src");
          spark()
              .sql("SELECT 1L AS id, 'ALICE' AS name UNION ALL SELECT 99L AS id, 'New' AS name")
              .createOrReplaceTempView("rt_merge_src");

          sql(
              "MERGE INTO %s t USING rt_merge_src s ON t.id = s.id "
                  + "WHEN MATCHED THEN UPDATE SET name = s.name "
                  + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
              tableName);

          String queryName =
              "rt_merge_ignore_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");

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
          Map<Long, Long> afterIdToRowId = new HashMap<>();
          for (Row r : rows) {
            afterIdToRowId.put(r.getLong(0), r.getLong(1));
          }
          assertTrue(afterIdToRowId.containsKey(2L), "Bob (id=2) should still be present");
          assertTrue(afterIdToRowId.containsKey(3L), "Charlie (id=3) should still be present");
          assertEquals(
              beforeIdToRowId.get(2L),
              afterIdToRowId.get(2L),
              "Bob's row_id should be preserved across MERGE rewrites");
          assertEquals(
              beforeIdToRowId.get(3L),
              afterIdToRowId.get(3L),
              "Charlie's row_id should be preserved across MERGE rewrites");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
          spark().sql("DROP VIEW IF EXISTS rt_merge_src");
        });
  }

  @TestAllTableTypes
  public void testRowTrackingEnabledAfterTableCreate(TableType tableType) throws Exception {
    // No row-tracking property on the create -- enable it via ALTER TABLE mid-life.
    withNewTable(
        "rt_late_enable",
        "id LONG, name STRING",
        null,
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);
          sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')", tableName);
          sql("INSERT INTO %s VALUES (3, 'Charlie')", tableName);

          String queryName =
              "rt_late_enable_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");

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
          assertEquals(3, rows.size(), "All rows should arrive even though RT was enabled later");
          Set<Long> rowIds = new HashSet<>();
          for (Row r : rows) {
            rowIds.add(r.getLong(1));
          }
          assertEquals(3, rowIds.size(), "row_ids should be unique across the whole table");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Sanity: projecting _metadata.row_id on a non-RT table must surface a clear analysis error.
  @TestAllTableTypes
  public void testStreamMetadataStructOnNonRowTrackedTable(TableType tableType) throws Exception {
    withNewTable(
        "rt_nort",
        "id LONG, name STRING",
        null,
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          AnalysisException ex =
              assertThrows(
                  AnalysisException.class,
                  () ->
                      spark()
                          .readStream()
                          .format("delta")
                          .table(tableName)
                          .selectExpr("_metadata.row_id AS row_id"));
          String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
          assertTrue(
              msg.contains("row_id") || msg.contains("_metadata"),
              "Expected analysis error mentioning row_id or _metadata; got: " + ex.getMessage());
        });
  }
}
