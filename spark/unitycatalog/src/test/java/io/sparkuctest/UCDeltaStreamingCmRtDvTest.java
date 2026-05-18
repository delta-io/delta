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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingCmRtDvTest} - the column-mapping x
 * row-tracking x deletion-vectors triple-feature composition. Each {@code @TestAllTableTypes}
 * exercises EXTERNAL and MANAGED tables.
 *
 * <p>The {@code _metadata.row_id} / {@code _metadata.row_commit_version} projection cases mirror
 * those in {@link UCDeltaStreamingRowTrackingTest} - row-tracking projection is fix-10's surface
 * and may surface MANAGED-only regressions because catalog-managed reader-factory wiring is a
 * separate code path from the EXTERNAL DSv2 reader factory.
 *
 * <p>UC-infra-blocked cases (skipped via {@link Assumptions#assumeTrue}):
 *
 * <ul>
 *   <li>ADD COLUMN on MANAGED: {@code UCSingleCatalog.alterTable()} throws unconditionally for
 *       MANAGED tables. EXTERNAL runs the full case.
 *   <li>RENAME COLUMN: requires {@code schemaTrackingLocation} which DSv2 streaming does not yet
 *       wire through. Skipped on both table types. Re-enable when {@code SparkScan.SUPPORTED_-
 *       STREAMING_OPTIONS} accepts the option.
 *   <li>OPTIMIZE on MANAGED: blocked via {@code DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_-
 *       OPERATION} (see {@code UCDeltaUtilityTest#testMaintenanceOpsBlockedOnManagedTable}).
 * </ul>
 */
public class UCDeltaStreamingCmRtDvTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private static final String CM_NAME_RT_DV_PROPS =
      "'delta.columnMapping.mode' = 'name', "
          + "'delta.enableRowTracking' = 'true', "
          + "'delta.enableDeletionVectors' = 'true', "
          + "'delta.minReaderVersion' = '3', "
          + "'delta.minWriterVersion' = '7'";

  private static final String CM_ID_RT_DV_PROPS =
      "'delta.columnMapping.mode' = 'id', "
          + "'delta.enableRowTracking' = 'true', "
          + "'delta.enableDeletionVectors' = 'true', "
          + "'delta.minReaderVersion' = '3', "
          + "'delta.minWriterVersion' = '7'";

  /** Single-file initial write of {@code numRows} rows of (id, "name-<id>"). */
  private void writeInitialSingleFile(String tableName, long numRows) {
    spark()
        .range(numRows)
        .selectExpr("id", "concat('name-', cast(id as string)) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(tableName);
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testBasic_cmName_rt_dv}. */
  @TestAllTableTypes
  public void testBasic_cmName_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_basic_name",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 10);
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "cmrt_dv_basic_name_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(5, rows.size(), "Expected 5 surviving odd rows after DV delete");
          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
          }
          assertEquals(Set.of(1L, 3L, 5L, 7L, 9L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testBasic_cmId_rt_dv}. */
  @TestAllTableTypes
  public void testBasic_cmId_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_basic_id",
        "id LONG, name STRING",
        null,
        tableType,
        CM_ID_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 10);
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "cmrt_dv_basic_id_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(5, rows.size(), "Expected 5 surviving odd rows after DV delete");
          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
          }
          assertEquals(Set.of(1L, 3L, 5L, 7L, 9L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testProjectRowId_cmName_rt_dv}. */
  @TestAllTableTypes
  public void testProjectRowId_cmName_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_row_id",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 10);
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "cmrt_dv_row_id_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(5, rows.size(), "Expected 5 surviving rows after DV delete");

          Map<Long, Long> idToRowId = new HashMap<>();
          for (Row r : rows) {
            long id = r.getLong(0);
            String name = r.getString(1);
            long rowId = r.getLong(2);
            assertEquals(1L, id % 2, "Only odd ids should survive");
            assertEquals("name-" + id, name, "name column must align with id under CM rewrite");
            idToRowId.put(id, rowId);
          }
          // Single coalesced file with stable physical row positions: row_id == id.
          for (Map.Entry<Long, Long> e : idToRowId.entrySet()) {
            assertEquals(
                e.getKey(),
                e.getValue(),
                () ->
                    "Bug #25-style regression under CM x RT x DV: id="
                        + e.getKey()
                        + " produced row_id="
                        + e.getValue());
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testProjectRowCommitVersion_cmName_rt_dv}. */
  @TestAllTableTypes
  public void testProjectRowCommitVersion_cmName_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_rcv",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 10);
          long insertVersion = currentVersion(tableName);
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "cmrt_dv_rcv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(5, rows.size());
          for (Row r : rows) {
            long id = r.getLong(0);
            long rcv = r.getLong(1);
            assertEquals(
                insertVersion,
                rcv,
                () ->
                    "Surviving row id="
                        + id
                        + " has row_commit_version="
                        + rcv
                        + " (expected "
                        + insertVersion
                        + "; DV delete must not bump rcv on rows it did not touch)");
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingCmRtDvTest#testDvDeleteAfterAddColumn_cmName_rt}. Aborted on
   * MANAGED because {@code UCSingleCatalog.alterTable()} blocks ADD COLUMN. EXTERNAL runs the full
   * case.
   */
  @TestAllTableTypes
  public void testDvDeleteAfterAddColumn_cmName_rt(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: ALTER TABLE ADD COLUMN is blocked on MANAGED via UCSingleCatalog");
    withNewTable(
        "cmrt_dv_add_col",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 6);
          sql("ALTER TABLE %s ADD COLUMN extra STRING", tableName);
          // DV-delete after the ADD COLUMN: rows 0,2,4 removed; rows 1,3,5 survive with extra=NULL.
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "cmrt_dv_add_col_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "name", "extra", "_metadata.row_id AS row_id");

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
          assertEquals(3, rows.size(), "Expected 3 odd rows surviving DV-delete after ADD COLUMN");
          Map<Long, Long> idToRowId = new HashMap<>();
          for (Row r : rows) {
            long id = r.getLong(0);
            assertEquals("name-" + id, r.getString(1));
            assertNull(r.get(2), "extra should project as NULL for rows written before ADD COLUMN");
            idToRowId.put(id, r.getLong(3));
          }
          // Single-file insert => row_id == id for survivors.
          assertEquals(1L, (long) idToRowId.get(1L));
          assertEquals(3L, (long) idToRowId.get(3L));
          assertEquals(5L, (long) idToRowId.get(5L));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testRestart_cmName_rt_dv_dvDeleteBetweenRuns}. */
  @TestAllTableTypes
  public void testRestart_cmName_rt_dv_dvDeleteBetweenRuns(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_restart_name",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 6);

          String queryName =
              "cmrt_dv_restart_name_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .selectExpr("id", "name", "_metadata.row_id AS row_id");
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // Between runs: DV-delete + new append. skipChangeCommits drops the DV-delete commit;
          // the surviving rows arrive via the post-delete append commit.
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);
          sql("INSERT INTO %s VALUES (100, 'name-100')", tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName)
                  .selectExpr("id", "name", "_metadata.row_id AS row_id");
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
          assertEquals(
              7,
              rows.size(),
              () -> "Expected 6 run-1 rows + 1 appended row across restart; got: " + rows);

          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
            assertEquals("name-" + r.getLong(0), r.getString(1));
          }
          assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L, 100L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testRestart_cmId_rt_dv}. */
  @TestAllTableTypes
  public void testRestart_cmId_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_restart_id",
        "id LONG, name STRING",
        null,
        tableType,
        CM_ID_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 6);

          String queryName =
              "cmrt_dv_restart_id_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark().readStream().format("delta").table(tableName).selectExpr("id", "name");
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);
          sql("INSERT INTO %s VALUES (100, 'name-100')", tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName)
                  .selectExpr("id", "name");
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
          assertEquals(
              7,
              rows.size(),
              () ->
                  "Expected 6 run-1 rows + 1 appended row across restart with CM-id; got: " + rows);
          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
            assertEquals("name-" + r.getLong(0), r.getString(1));
          }
          assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L, 100L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testMaxFilesPerTrigger_cmName_rt_dv}. */
  @TestAllTableTypes
  public void testMaxFilesPerTrigger_cmName_rt_dv(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_max_files",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          // Three separate single-file commits so maxFilesPerTrigger has work to partition.
          for (long i = 0; i < 3; i++) {
            spark()
                .range(i * 2, i * 2 + 2)
                .selectExpr("id", "concat('name-', cast(id as string)) as name")
                .coalesce(1)
                .write()
                .format("delta")
                .mode("append")
                .saveAsTable(tableName);
          }
          // DV-delete one row in the middle of the table - preserves all files (DV-only).
          sql("DELETE FROM %s WHERE id = 3", tableName);

          String queryName =
              "cmrt_dv_max_files_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .selectExpr("id", "name");
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
          // Surviving ids: {0,1,2,4,5}. (id=3 was DV-deleted.)
          Set<Long> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getLong(0));
            assertEquals("name-" + r.getLong(0), r.getString(1));
          }
          assertEquals(Set.of(0L, 1L, 2L, 4L, 5L), ids);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testRowIdMonotonicAcrossDvDelete}. */
  @TestAllTableTypes
  public void testRowIdMonotonicAcrossDvDelete(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_row_id_monotonic",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 12);

          // Three DV-delete commits that each remove a disjoint set.
          sql("DELETE FROM %s WHERE id IN (0, 1)", tableName);
          sql("DELETE FROM %s WHERE id IN (4, 5)", tableName);
          sql("DELETE FROM %s WHERE id IN (8, 9)", tableName);

          String queryName =
              "cmrt_dv_row_id_mono_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(6, rows.size());
          for (Row r : rows) {
            long id = r.getLong(0);
            long rowId = r.getLong(1);
            assertEquals(
                id, rowId, () -> "RT contract violation: id=" + id + " produced row_id=" + rowId);
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingCmRtDvTest#testRowCommitVersionAcrossDvDelete}. */
  @TestAllTableTypes
  public void testRowCommitVersionAcrossDvDelete(TableType tableType) throws Exception {
    withNewTable(
        "cmrt_dv_rcv_stable",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 8);
          long insertVersion = currentVersion(tableName);

          // Two DV-delete commits - each bumps the table version but leaves the original file.
          sql("DELETE FROM %s WHERE id = 0", tableName);
          sql("DELETE FROM %s WHERE id = 7", tableName);
          long postDeleteVersion = currentVersion(tableName);
          assertTrue(
              postDeleteVersion > insertVersion, "DV-deletes must advance the table version");

          String queryName =
              "cmrt_dv_rcv_stable_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
          assertEquals(6, rows.size(), "Expected 6 survivors after deleting ids 0 and 7");
          for (Row r : rows) {
            long id = r.getLong(0);
            long rcv = r.getLong(1);
            assertEquals(
                insertVersion,
                rcv,
                () ->
                    "RT contract violation: surviving id="
                        + id
                        + " has row_commit_version="
                        + rcv
                        + " (expected "
                        + insertVersion
                        + ", the original insert commit)");
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingCmRtDvTest#testRenameColumn_cmName_rt_dv}. Aborted unconditionally:
   * RENAME COLUMN requires {@code schemaTrackingLocation} which DSv2 streaming does not yet plumb
   * through, and {@code UCSingleCatalog.alterTable()} also blocks RENAME on MANAGED.
   */
  @TestAllTableTypes
  public void testRenameColumn_cmName_rt_dv(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: schemaTrackingLocation unsupported in DSv2 streaming, and UCSingleCatalog "
            + "blocks ALTER on MANAGED. Re-enable when both gaps close.");
  }

  /**
   * Mirror of {@code V2StreamingCmRtDvTest#testOptimizeAfterDvDelete_cmName_rt}. Aborted on MANAGED
   * because OPTIMIZE is blocked via {@code DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_- OPERATION}
   * (see {@code UCDeltaUtilityTest#testMaintenanceOpsBlockedOnManagedTable}). EXTERNAL runs the
   * full case.
   */
  @TestAllTableTypes
  public void testOptimizeAfterDvDelete_cmName_rt(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE throws DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION on "
            + "MANAGED");
    withNewTable(
        "cmrt_optimize_dv",
        "id LONG, name STRING",
        null,
        tableType,
        CM_NAME_RT_DV_PROPS,
        tableName -> {
          writeInitialSingleFile(tableName, 10);

          // Capture pre-OPTIMIZE row_ids via a batch query (single-file write => row_id == id).
          List<Row> beforeRows =
              spark()
                  .sql("SELECT id, _metadata.row_id AS row_id FROM " + tableName + " ORDER BY id")
                  .collectAsList();
          Map<Long, Long> beforeIdToRowId = new HashMap<>();
          for (Row r : beforeRows) {
            beforeIdToRowId.put(r.getLong(0), r.getLong(1));
          }

          sql("DELETE FROM %s WHERE id < 3", tableName);

          // Force OPTIMIZE to compact: raise minFileSize well above the actual file size.
          String prevMinFileSize = null;
          try {
            prevMinFileSize = spark().conf().get("spark.databricks.delta.optimize.minFileSize");
          } catch (java.util.NoSuchElementException ignored) {
            // not previously set
          }
          spark()
              .conf()
              .set(
                  "spark.databricks.delta.optimize.minFileSize",
                  Long.toString(1024L * 1024L * 1024L));
          try {
            sql("OPTIMIZE %s", tableName);
          } finally {
            if (prevMinFileSize != null) {
              spark().conf().set("spark.databricks.delta.optimize.minFileSize", prevMinFileSize);
            } else {
              spark().conf().unset("spark.databricks.delta.optimize.minFileSize");
            }
          }

          String queryName =
              "cmrt_optimize_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> streamingDF =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
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
          // Initial snapshot after DELETE+OPTIMIZE reflects survivors {3..9} (7 rows).
          assertEquals(
              7, rows.size(), () -> "Expected 7 survivors after DELETE+OPTIMIZE; got: " + rows);
          for (Row r : rows) {
            long id = r.getLong(0);
            assertEquals("name-" + id, r.getString(1));
            long rowId = r.getLong(2);
            Long expected = beforeIdToRowId.get(id);
            assertEquals(
                expected,
                rowId,
                () ->
                    "row_id mismatch under CM x RT after OPTIMIZE rewrite of DV-deleted file: id="
                        + id
                        + " pre="
                        + beforeIdToRowId.get(id)
                        + " post="
                        + rowId);
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
