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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingVariantCmRestartTest} - the VARIANT x
 * column-mapping x stream-restart triple composition.
 *
 * <p>The catch surface is per-row payload alignment: each row's variant payload is constructed so
 * {@code variant_get(v, '$.row', 'int') == id} by construction; any silent row-reordering surfaces
 * as a payload mismatch (the catch surface for the Bug-#1 and Bug-#25 composition).
 *
 * <p>UC-infra-blocked cases (skipped via {@link Assumptions#assumeTrue}):
 *
 * <ul>
 *   <li>Nested VARIANT in STRUCT under CM (case 5): VARIANT inside CM-rewritten STRUCT is a known
 *       edge that the task brief calls out as UC-rejectable. Skipped on both table types.
 *   <li>ADD COLUMN on MANAGED (case 6): {@code UCSingleCatalog.alterTable()} blocks ALTER. EXTERNAL
 *       runs the full case.
 *   <li>RENAME COLUMN (case 7): requires {@code schemaTrackingLocation} - the V2 test was already
 *       {@code @Disabled} for this gap. UC also blocks ALTER on MANAGED.
 * </ul>
 */
public class UCDeltaStreamingVariantCmRestartTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private static final String CM_NAME_PROPS = "'delta.columnMapping.mode' = 'name'";
  private static final String CM_ID_PROPS =
      "'delta.columnMapping.mode' = 'id', "
          + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'";

  /** Appends {@code count} rows starting at id {@code startId}; v = parse_json({"row":id}). */
  private void appendVariantRows(String tableName, int startId, int count) {
    spark()
        .range(startId, startId + count)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(tableName);
  }

  /**
   * Asserts the memory sink contains rows for ids in [startInclusive, endExclusive) and each row's
   * variant payload equals its id.
   */
  private void assertSinkVariantAligned(String queryName, int startInclusive, int endExclusive) {
    List<Row> rows =
        spark()
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                    + queryName
                    + " ORDER BY id")
            .collectAsList();
    int expectedCount = endExclusive - startInclusive;
    assertEquals(expectedCount, rows.size(), () -> "Expected " + expectedCount + " rows: " + rows);
    int expectedId = startInclusive;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id in sink: " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () ->
              "Silent corruption across VARIANT x CM x restart: id="
                  + id
                  + " variant_get(v,'$.row','int')="
                  + vRow);
      expectedId++;
    }
  }

  /** Mirror of {@code V2StreamingVariantCmRestartTest#testBasic_variant_cmName_restart}. */
  @TestAllTableTypes
  public void testBasic_variant_cmName_restart(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_basic_name",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          appendVariantRows(tableName, 1, 3); // ids 1..3

          String queryName =
              "var_cm_basic_name_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          appendVariantRows(tableName, 4, 3); // ids 4..6

          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          assertSinkVariantAligned(queryName, 1, 7);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingVariantCmRestartTest#testBasic_variant_cmId_restart}. */
  @TestAllTableTypes
  public void testBasic_variant_cmId_restart(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_basic_id",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_ID_PROPS,
        tableName -> {
          appendVariantRows(tableName, 1, 3);

          String queryName =
              "var_cm_basic_id_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          appendVariantRows(tableName, 4, 3);

          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          assertSinkVariantAligned(queryName, 1, 7);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code
   * V2StreamingVariantCmRestartTest#testVariantPayload_assertedAcrossRestart_cmName}.
   */
  @TestAllTableTypes
  public void testVariantPayload_assertedAcrossRestart_cmName(TableType tableType)
      throws Exception {
    withNewTable(
        "var_cm_payload",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          // 5 single-row commits pre-restart - multi-file scan across CM rewrites.
          for (int i = 1; i <= 5; i++) {
            appendVariantRows(tableName, i, 1);
          }

          String queryName =
              "var_cm_payload_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          for (int i = 6; i <= 10; i++) {
            appendVariantRows(tableName, i, 1);
          }

          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          assertSinkVariantAligned(queryName, 1, 11);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingVariantCmRestartTest#testVariantWithDv_cmName_restart}. */
  @TestAllTableTypes
  public void testVariantWithDv_cmName_restart(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_dv",
        "id INT, v VARIANT",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          appendVariantRows(tableName, 1, 10); // ids 1..10 in a single Parquet file

          String queryName = "var_cm_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // DV-only DELETE between runs: file kept, only a DV is written. Even-id rows removed.
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          // Append new commit so restart has something to drain - odd ids 11, 13.
          spark()
              .range(11, 14)
              .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
              .filter("id % 2 = 1")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          // Run 1 ingested ids 1..10. Run 2 with skipChangeCommits drops the DV-DELETE commit and
          // ingests post-DELETE append (ids 11, 13). All variant payloads must align with their id.
          List<Row> rows =
              spark()
                  .sql(
                      "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                          + queryName
                          + " ORDER BY id")
                  .collectAsList();
          // Expected: 1..10 (run 1) + 11, 13 (run 2) = 12 rows.
          assertEquals(
              12, rows.size(), () -> "Expected 12 rows across DV + CM + restart; got: " + rows);
          for (Row row : rows) {
            int id = row.getInt(0);
            Object vRowObj = row.get(1);
            assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
            int vRow = ((Number) vRowObj).intValue();
            assertEquals(
                id,
                vRow,
                () ->
                    "Silent corruption across VARIANT x CM x DV x restart: id="
                        + id
                        + " v_row="
                        + vRow);
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingVariantCmRestartTest#testVariantNestedInStruct_cmName_restart}.
   * Aborted: nested VARIANT inside CM-rewritten STRUCT is a known UC edge per the task brief. Plain
   * nested VARIANT without CM is covered by {@code UCDeltaStreamingDataEdgesTest#testVariant-
   * NestedThreeDeepWithDV}; non-nested VARIANT + CM is covered by case 1.
   */
  @TestAllTableTypes
  public void testVariantNestedInStruct_cmName_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "UC-INFRA-BLOCKED: nested VARIANT inside CM-rewritten STRUCT is an unsupported edge per "
            + "the task brief. Plain nested VARIANT coverage is in UCDeltaStreamingDataEdgesTest; "
            + "VARIANT + CM coverage is in case 1 of this file.");
  }

  /**
   * Mirror of {@code V2StreamingVariantCmRestartTest#testAddColumn_variant_cmName_restart}. Aborted
   * on MANAGED because {@code UCSingleCatalog.alterTable()} blocks ALTER. EXTERNAL runs the full
   * case.
   */
  @TestAllTableTypes
  public void testAddColumn_variant_cmName_restart(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: ALTER TABLE ADD COLUMN is blocked on MANAGED via UCSingleCatalog");
    withNewTable(
        "var_cm_addcol",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          appendVariantRows(tableName, 1, 3); // ids 1..3

          String queryName =
              "var_cm_addcol_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // Additive change: ADD COLUMN extra STRING. New writes carry the extra column.
          sql("ALTER TABLE %s ADD COLUMN extra STRING", tableName);
          spark()
              .range(4, 7)
              .selectExpr(
                  "cast(id as int) as id",
                  "parse_json(concat('{\"row\":', id, '}')) as v",
                  "cast(id as string) as extra")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows =
              spark()
                  .sql(
                      "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                          + queryName
                          + " ORDER BY id")
                  .collectAsList();
          assertEquals(6, rows.size(), () -> "Expected 6 rows after ADD COLUMN + restart: " + rows);
          int expectedId = 1;
          for (Row row : rows) {
            int id = row.getInt(0);
            assertEquals(expectedId, id, () -> "Unexpected id after ADD COLUMN: " + id);
            Object vRowObj = row.get(1);
            assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
            int vRow = ((Number) vRowObj).intValue();
            assertEquals(
                id,
                vRow,
                () -> "Variant payload misaligned after ADD COLUMN: id=" + id + " v_row=" + vRow);
            expectedId++;
          }
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingVariantCmRestartTest#testRenameColumn_variant_cmName_restart}.
   * Aborted: V2 was already {@code @Disabled} for schemaTrackingLocation, and UC blocks ALTER on
   * MANAGED.
   */
  @TestAllTableTypes
  public void testRenameColumn_variant_cmName_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: Cluster S-3 schemaTrackingLocation unsupported in DSv2 streaming. UC also "
            + "blocks ALTER on MANAGED.");
  }

  /** Mirror of {@code V2StreamingVariantCmRestartTest#testStartingVersion_variant_cmName}. */
  @TestAllTableTypes
  public void testStartingVersion_variant_cmName(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_starting_v",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          appendVariantRows(tableName, 1, 1); // ids 1
          appendVariantRows(tableName, 2, 1); // ids 2
          long v2 = currentVersion(tableName); // commit for id=2
          appendVariantRows(tableName, 3, 1); // ids 3
          appendVariantRows(tableName, 4, 1); // ids 4

          long startVersion = v2 + 1; // points at the id=3 commit
          String queryName =
              "var_cm_starting_v_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          // First run: startingVersion=v2+1 -> consume ids 3, 4.
          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(startVersion))
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // Append more commits. Restart picks them up from the checkpoint.
          appendVariantRows(tableName, 5, 1); // id 5
          appendVariantRows(tableName, 6, 1); // id 6

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(startVersion))
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          assertSinkVariantAligned(queryName, 3, 7);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code
   * V2StreamingVariantCmRestartTest#testMaxFilesPerTrigger_variant_cmName_restart}.
   */
  @TestAllTableTypes
  public void testMaxFilesPerTrigger_variant_cmName_restart(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_mfpt",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          for (int i = 1; i <= 5; i++) {
            appendVariantRows(tableName, i, 1);
          }

          String queryName =
              "var_cm_mfpt_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          for (int i = 6; i <= 10; i++) {
            appendVariantRows(tableName, i, 1);
          }

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          assertSinkVariantAligned(queryName, 1, 11);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingVariantCmRestartTest#testAvailableNow_variant_cmName_restart}. */
  @TestAllTableTypes
  public void testAvailableNow_variant_cmName_restart(TableType tableType) throws Exception {
    withNewTable(
        "var_cm_avail_now",
        "id INT, v VARIANT",
        null,
        tableType,
        CM_NAME_PROPS,
        tableName -> {
          appendVariantRows(tableName, 1, 4); // ids 1..4

          String queryName =
              "var_cm_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .start();
          q1.awaitTermination();

          appendVariantRows(tableName, 5, 4); // ids 5..8

          Dataset<Row> df2 = spark().readStream().format("delta").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .start();
          q2.awaitTermination();

          assertSinkVariantAligned(queryName, 1, 9);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
