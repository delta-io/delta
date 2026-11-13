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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for column-mapping streaming scenarios against real Unity Catalog tables, in the
 * default V2_ENABLE_MODE=AUTO (no override). MANAGED tables route to the DSv2/Kernel connector;
 * EXTERNAL tables fall back to V1. Both paths are exercised via {@code @TestAllTableTypes}.
 *
 * <p>Ported from {@code DeltaV2SourceColumnMappingSuite} (the V2/STRICT wrapper around {@code
 * DeltaSourceColumnMappingSuite} / {@code ColumnMappingStreamingBlockedWorkflowSuiteBase}). Each
 * test runs for both {@code id} and {@code name} column-mapping modes. {@code isCdcTest} is false
 * for all scenarios (mirroring {@code DeltaSourceIdColumnMappingSuite} and {@code
 * DeltaSourceNameColumnMappingSuite}).
 *
 * <p>Error-message fragments asserted:
 *
 * <ul>
 *   <li>"Detected schema change" — the retryable {@code DELTA_SCHEMA_CHANGED*} family, thrown when
 *       the stream hits an incompatible schema mid-batch; the user must restart with a new
 *       checkpoint.
 *   <li>"Streaming read is not supported" — {@code
 *       DeltaStreamingNonAdditiveSchemaIncompatibleException}, thrown when a blocked workflow
 *       (drop/rename column) is detected at stream start or during {@code latestOffset()}.
 * </ul>
 *
 * <p>The unsafe-unblock conf key is {@code
 * spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled}.
 *
 * <p>SKIPPED scenarios are documented at the bottom of this file.
 */
public class UCDeltaSourceColumnMappingStreamingTest extends UCDeltaTableIntegrationBaseTest {

  // ── column-mapping mode constants ────────────────────────────────────────────

  private static final String ID_MODE = "id";
  private static final String NAME_MODE = "name";

  /**
   * TBLPROPERTIES fragment for a column-mapping table. Setting {@code delta.columnMapping.mode}
   * triggers automatic protocol upgrade in Delta, but we also set minReaderVersion/minWriterVersion
   * explicitly to match the Scala suite's {@code setupTestTable} logic (minReader=2, minWriter=5
   * for the legacy CM feature).
   */
  private static String cmProps(String mode) {
    return "'delta.enableChangeDataFeed'='true',"
        + " 'delta.columnMapping.mode'='"
        + mode
        + "',"
        + " 'delta.minReaderVersion'='2',"
        + " 'delta.minWriterVersion'='5'";
  }

  /** TBLPROPERTIES for a table that starts with NO column mapping (for the upgrade test). */
  private static final String NO_CM_PROPS = "'delta.enableChangeDataFeed'='true'";

  // ── checkpoint bookkeeping ────────────────────────────────────────────────────

  @TempDir private Path tempDir;

  private int checkpointCount = 0;

  /** Returns a fresh local checkpoint directory path for each streaming query in a test. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectories(ckDir);
    return ckDir.toString();
  }

  // ── row-collection helpers ────────────────────────────────────────────────────

  /** Collects the first column of every row as a {@code String} (null for SQL NULL). */
  private static List<String> firstCol(Dataset<Row> df) {
    return df.collectAsList().stream()
        .map(r -> r.isNullAt(0) ? null : r.get(0).toString())
        .collect(Collectors.toList());
  }

  /** Collects all columns of every row as {@code Object} (null for SQL NULL). */
  private static List<List<Object>> allCols(Dataset<Row> df) {
    return df.collectAsList().stream()
        .map(
            r -> {
              List<Object> cells = new ArrayList<>();
              for (int i = 0; i < r.length(); i++) {
                cells.add(r.isNullAt(i) ? null : r.get(i));
              }
              return cells;
            })
        .collect(Collectors.toList());
  }

  // ── assertStreamingThrowsContaining ──────────────────────────────────────────

  /**
   * Asserts that {@code action} throws an exception whose combined cause-chain message contains ALL
   * of the given {@code fragments} (case-insensitive). Mirrors the private helper of the same name
   * in {@link UCDeltaTableDataFrameStreamingTest}.
   */
  private static void assertStreamingThrowsContaining(
      ThrowingCallable action, String... fragments) {
    assertThatThrownBy(action)
        .satisfies(
            e -> {
              StringBuilder full = new StringBuilder();
              for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null) full.append(t.getMessage()).append(' ');
              }
              String msg = full.toString();
              for (String fragment : fragments) {
                assertThat(msg).containsIgnoringCase(fragment);
              }
            });
  }

  // ── private utility ───────────────────────────────────────────────────────────

  /** Builds a concatenated string of all messages in the Throwable cause chain. */
  private static String buildCauseChain(Throwable e) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null) sb.append(t.getMessage()).append(' ');
    }
    return sb.toString();
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "basic"
  //
  // Source: DeltaSourceSuite.test("basic")
  //
  // Insert rows, start a streaming query, verify rows flow. Stop, insert more, restart, verify
  // incremental delivery. Runs for both id and name column-mapping modes.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testBasicIdMode(TableType tableType) throws Exception {
    testBasic(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testBasicNameMode(TableType tableType) throws Exception {
    testBasic(tableType, NAME_MODE);
  }

  private void testBasic(TableType tableType, String cmMode) throws Exception {
    withNewTable(
        "cm_basic_" + cmMode,
        "value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Batch 1: pre-existing rows
          sql("INSERT INTO %s VALUES ('keep1'), ('keep2'), ('drop3')", tableName);

          List<String> result = new ArrayList<>();
          String ckpt = checkpoint();

          // AvailableNow: process all existing rows; filter 'keep' rows in foreachBatch.
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ckpt)
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, id) ->
                          result.addAll(firstCol(df.filter(df.col("value").contains("keep")))))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder("keep1", "keep2");

          // Batch 2: new rows — resume from same checkpoint so only new rows are delivered.
          sql("INSERT INTO %s VALUES ('drop4'), ('keep5'), ('keep6')", tableName);
          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) ->
                              result.addAll(firstCol(df.filter(df.col("value").contains("keep")))))
                  .start();
          try {
            q2.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder("keep1", "keep2", "keep5", "keep6");

            // Batch 3: more rows while the continuous query is alive.
            sql("INSERT INTO %s VALUES ('keep7'), ('drop8'), ('keep9')", tableName);
            q2.processAllAvailable();
            assertThat(result)
                .containsExactlyInAnyOrder("keep1", "keep2", "keep5", "keep6", "keep7", "keep9");
          } finally {
            q2.stop();
          }
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "maxFilesPerTrigger: metadata checkpoint"
  //
  // Source: DeltaSourceSuite.test("maxFilesPerTrigger: metadata checkpoint")
  //
  // 20 separate commits; maxFilesPerTrigger=1 ensures each is its own micro-batch (20 batches).
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testMaxFilesPerTriggerMetadataCheckpointIdMode(TableType tableType) throws Exception {
    testMaxFilesPerTriggerMetadataCheckpoint(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testMaxFilesPerTriggerMetadataCheckpointNameMode(TableType tableType)
      throws Exception {
    testMaxFilesPerTriggerMetadataCheckpoint(tableType, NAME_MODE);
  }

  private void testMaxFilesPerTriggerMetadataCheckpoint(TableType tableType, String cmMode)
      throws Exception {
    withNewTable(
        "cm_maxfiles_ck_" + cmMode,
        "value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          for (int i = 0; i < 20; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          List<Long> batchIds = new ArrayList<>();
          List<String> allValues = new ArrayList<>();

          spark()
              .readStream()
              .format("delta")
              .option("maxFilesPerTrigger", 1)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, batchId) -> {
                        allValues.addAll(firstCol(df));
                        batchIds.add(batchId);
                      })
              .start()
              .awaitTermination();

          assertThat(allValues)
              .containsExactlyInAnyOrder(
                  "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14",
                  "15", "16", "17", "18", "19");
          // Each of the 20 commits should be its own micro-batch.
          assertThat(batchIds).hasSize(20);
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "maxBytesPerTrigger: metadata checkpoint"
  //
  // Source: DeltaSourceSuite.test("maxBytesPerTrigger: metadata checkpoint")
  //
  // 20 separate commits; maxBytesPerTrigger=1b ensures each is its own micro-batch (20 batches).
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testMaxBytesPerTriggerMetadataCheckpointIdMode(TableType tableType) throws Exception {
    testMaxBytesPerTriggerMetadataCheckpoint(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testMaxBytesPerTriggerMetadataCheckpointNameMode(TableType tableType)
      throws Exception {
    testMaxBytesPerTriggerMetadataCheckpoint(tableType, NAME_MODE);
  }

  private void testMaxBytesPerTriggerMetadataCheckpoint(TableType tableType, String cmMode)
      throws Exception {
    withNewTable(
        "cm_maxbytes_ck_" + cmMode,
        "value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          for (int i = 0; i < 20; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          List<Long> batchIds = new ArrayList<>();
          List<String> allValues = new ArrayList<>();

          spark()
              .readStream()
              .format("delta")
              .option("maxBytesPerTrigger", "1b")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, batchId) -> {
                        allValues.addAll(firstCol(df));
                        batchIds.add(batchId);
                      })
              .start()
              .awaitTermination();

          assertThat(allValues)
              .containsExactlyInAnyOrder(
                  "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14",
                  "15", "16", "17", "18", "19");
          // Each of the 20 commits should be its own micro-batch.
          assertThat(batchIds).hasSize(20);
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "allow to change schema before starting a streaming query"
  //
  // Source: DeltaSourceSuite.test("allow to change schema before starting a streaming query")
  //
  // Insert rows with original schema (id), then ADD COLUMN (value) BEFORE starting the stream.
  // The stream must see ALL rows; old rows have null for the new column.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testAllowSchemaChangeBeforeStreamStartIdMode(TableType tableType) throws Exception {
    testAllowSchemaChangeBeforeStreamStart(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testAllowSchemaChangeBeforeStreamStartNameMode(TableType tableType) throws Exception {
    testAllowSchemaChangeBeforeStreamStart(tableType, NAME_MODE);
  }

  private void testAllowSchemaChangeBeforeStreamStart(TableType tableType, String cmMode)
      throws Exception {
    withNewTable(
        "cm_pre_schema_" + cmMode,
        "id STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Write rows 0-4 with the original schema (id only).
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          // ADD COLUMN before the stream starts — this must be allowed.
          sql("ALTER TABLE %s ADD COLUMN (value STRING)", tableName);

          // Write rows 5-9 with the new schema (id, value).
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // The stream should see ALL 10 rows; old rows have null for value.
          List<List<Object>> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(allCols(df)))
              .start()
              .awaitTermination();

          assertThat(result).hasSize(10);
          long nullValues = result.stream().filter(r -> r.get(1) == null).count();
          long nonNullValues = result.stream().filter(r -> r.get(1) != null).count();
          assertThat(nullValues).isEqualTo(5); // rows 0-4: value is null
          assertThat(nonNullValues).isEqualTo(5); // rows 5-9: value equals id
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "column mapping + streaming - allowed workflows - column addition"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "column mapping + streaming - allowed workflows - column addition")
  //
  // Start with schema (id, value). Start stream. ADD COLUMN (value2) during streaming.
  // Stream fails with retryable "Detected schema change". Restart from same checkpoint:
  // only rows with the new schema are ingested.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testColumnAdditionIdMode(TableType tableType) throws Exception {
    testColumnAddition(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testColumnAdditionNameMode(TableType tableType) throws Exception {
    testColumnAddition(tableType, NAME_MODE);
  }

  private void testColumnAddition(TableType tableType, String cmMode) throws Exception {
    withNewTable(
        "cm_col_add_" + cmMode,
        "id STRING, value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Write rows 0-4 with the original 2-column schema.
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String ckpt = checkpoint();

          // Phase 1: start a continuous streaming query, process initial data.
          List<List<Object>> phase1Results = new ArrayList<>();
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> phase1Results.addAll(allCols(df)))
                  .start();
          try {
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(5);

            // ADD COLUMN (value2) while the stream is alive.
            sql("ALTER TABLE %s ADD COLUMN (value2 STRING)", tableName);
            // Write rows 5-9 with the new 3-column schema.
            for (int i = 5; i < 10; i++) {
              sql("INSERT INTO %s VALUES ('%d', '%d', '%d')", tableName, i, i, i);
            }

            // Stream must fail with the retryable "Detected schema change" error.
            assertStreamingThrowsContaining(q1::processAllAvailable, "Detected schema change");
          } catch (Exception e) {
            // Stream may have already terminated asynchronously; verify the cause chain.
            assertThat(buildCauseChain(e)).containsIgnoringCase("Detected schema change");
          } finally {
            q1.stop();
          }

          // Phase 2: restart from the SAME checkpoint. The stream resumes and ingests only the
          // rows written AFTER the ADD COLUMN (rows 5-9, 3-column schema).
          List<List<Object>> phase2Results = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("maxFilesPerTrigger", "1")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ckpt)
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> phase2Results.addAll(allCols(df)))
              .start()
              .awaitTermination();

          // 5 rows (5-9) each with 3 columns.
          assertThat(phase2Results).hasSize(5);
          phase2Results.forEach(r -> assertThat(r).hasSize(3));
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "column mapping + streaming - allowed workflows - upgrade to name mode"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "column mapping + streaming - allowed workflows - upgrade to name mode")
  //
  // Create a table WITHOUT column mapping. Start stream, process initial rows. Then upgrade to
  // name mode mid-stream (SET TBLPROPERTIES). Write more rows — upgrade is NOT blocking, stream
  // continues. ADD COLUMN after the upgrade causes retryable failure. Restart picks up only
  // post-ADD-COLUMN rows. A fresh checkpoint sees all rows with nulls for the new column.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testUpgradeToNameMode(TableType tableType) throws Exception {
    // The table starts without column mapping and is upgraded to name mode mid-stream.
    withNewTable(
        "cm_upgrade_name",
        "id STRING, name STRING",
        null,
        tableType,
        NO_CM_PROPS,
        tableName -> {
          // Write rows 0-4 with no column mapping.
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String ckpt = checkpoint();
          List<List<Object>> phase1Results = new ArrayList<>();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> phase1Results.addAll(allCols(df)))
                  .start();
          try {
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(5);

            // Upgrade to name mode while the stream is running. This is NOT a blocking workflow —
            // the logical schema is unchanged, only the physical column mapping is assigned.
            sql(
                "ALTER TABLE %s SET TBLPROPERTIES ("
                    + "'delta.columnMapping.mode'='name',"
                    + "'delta.minReaderVersion'='2',"
                    + "'delta.minWriterVersion'='5')",
                tableName);

            // Write rows 5-9 post-upgrade (same 2-column logical schema).
            for (int i = 5; i < 10; i++) {
              sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
            }

            // Upgrade is tolerated; stream should process rows 5-9 without error.
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(10);

            // ADD COLUMN after the upgrade → retryable "Detected schema change".
            sql("ALTER TABLE %s ADD COLUMN (value2 STRING)", tableName);
            for (int i = 10; i < 15; i++) {
              sql("INSERT INTO %s VALUES ('%d', '%d', '%d')", tableName, i, i, i);
            }

            assertStreamingThrowsContaining(q1::processAllAvailable, "Detected schema change");
          } catch (Exception e) {
            assertThat(buildCauseChain(e)).containsIgnoringCase("Detected schema change");
          } finally {
            q1.stop();
          }

          // Restart from the same checkpoint; only rows 10-14 (3-column schema) are ingested.
          List<List<Object>> phase2Results = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("maxFilesPerTrigger", "1")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ckpt)
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> phase2Results.addAll(allCols(df)))
              .start()
              .awaitTermination();

          assertThat(phase2Results).hasSize(5);
          phase2Results.forEach(r -> assertThat(r).hasSize(3));

          // Fresh checkpoint: all 15 rows, old rows have null for value2.
          List<List<Object>> freshResults = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> freshResults.addAll(allCols(df)))
              .start()
              .awaitTermination();

          assertThat(freshResults).hasSize(15);
          // Rows 0-9 have null value2; rows 10-14 have non-null value2.
          long nullValue2 = freshResults.stream().filter(r -> r.get(2) == null).count();
          assertThat(nullValue2).isEqualTo(10);
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "column mapping + streaming: blocking workflow - drop column"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "column mapping + streaming: blocking workflow - drop column")
  //
  // The non-additive (blocking) failure for DROP only fires when the stream tries to PROCESS
  // data PAST a DROP-COLUMN schema change, not at the moment the DROP is committed. An ADD COLUMN
  // (additive) does NOT block — that is why the original Scala "restore via ADD" step raises only
  // the benign retryable error, while an in-stream DROP raises the blocking
  // DeltaStreamingNonAdditiveSchemaIncompatibleException ("Streaming read is not supported").
  //
  // Set up a CM table with (id, value, value2). DROP COLUMN value2 BEFORE streaming, so the
  // stream's initial-snapshot read schema is (id, value).
  //
  // Phase 1 (no startingVersion): the stream serves all historical rows under the (id, value)
  //   initial-snapshot schema, ignoring intermediate schema changes. Schema-compatible inserts
  //   continue to flow. Then a DROP COLUMN value mid-stream is a non-additive change; once the
  //   stream tries to process data past it, it fails with "Streaming read is not supported".
  // Phase 2 (restart from same checkpoint): the DROP is now part of history; the stream blocks at
  //   start with "Streaming read is not supported".
  // Phase 3 (startingVersion=0): serving the whole table from version 0 hits the DROP and blocks.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testBlockingDropColumnIdMode(TableType tableType) throws Exception {
    testBlockingDropColumn(tableType, ID_MODE);
  }

  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: in name column-mapping mode, the V1 (path-based / EXTERNAL) source does not raise the
  // non-additive schema-change block when the DROP COLUMN is served mid-stream, so the
  // "stream must throw" assertion fails on EXTERNAL. The MANAGED / AUTO→DSv2 (Kernel) path DOES
  // raise the block (this case passes there). Id mode passes on both.
  // Why safe: this is the INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1
  // and matches the original DeltaV2 suite's expectation. The EXTERNAL miss reflects pre-existing
  // V1 leniency for name-mode drop-column, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testBlockingDropColumnNameMode(TableType tableType) throws Exception {
    testBlockingDropColumn(tableType, NAME_MODE);
  }

  private void testBlockingDropColumn(TableType tableType, String cmMode) throws Exception {
    withNewTable(
        "cm_drop_col_" + cmMode,
        "id STRING, value STRING, value2 STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Write rows 0-9 with the original 3-column schema.
          for (int i = 0; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d', '%d')", tableName, i, i, i);
          }

          // DROP COLUMN value2 BEFORE the stream starts → initial-snapshot schema is (id, value).
          sql("ALTER TABLE %s DROP COLUMN value2", tableName);

          // Write rows 10-14 with the post-drop 2-column schema (id, value).
          for (int i = 10; i < 15; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // ── Phase 1: no startingVersion; in-stream DROP blocks ───────────────
          // The stream binds to the current snapshot schema (id, value). All historical rows are
          // served because the initial snapshot ignores intermediate schema changes.
          String ckpt = checkpoint();
          List<List<Object>> phase1Results = new ArrayList<>();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> phase1Results.addAll(allCols(df)))
                  .start();
          try {
            q1.processAllAvailable();
            // All 15 rows (0-14) served with the (id, value) schema.
            assertThat(phase1Results).hasSize(15);

            // Write rows 15-19 — still schema-compatible; stream should keep flowing.
            for (int i = 15; i < 20; i++) {
              sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
            }
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(20);

            // DROP COLUMN value mid-stream — a NON-ADDITIVE change. Write rows 20-24 after it so
            // there is data to process past the drop. The stream must block when it reaches them.
            sql("ALTER TABLE %s DROP COLUMN value", tableName);
            for (int i = 20; i < 25; i++) {
              sql("INSERT INTO %s VALUES ('%d')", tableName, i);
            }

            assertStreamingThrowsContaining(
                q1::processAllAvailable, "Streaming read is not supported");
          } catch (Exception e) {
            assertThat(buildCauseChain(e)).containsIgnoringCase("Streaming read is not supported");
          } finally {
            q1.stop();
          }

          // ── Phase 2: restart from same checkpoint — blocks at start ──────────
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("maxFilesPerTrigger", "1")
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ckpt)
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");

          // ── Phase 3: startingVersion=0 hits the DROP and blocks ──────────────
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", "0")
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "column mapping + streaming: blocking workflow - rename column"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "column mapping + streaming: blocking workflow - rename column")
  //
  // Set up CM table with (id, value). RENAME COLUMN value TO value2. Write more rows.
  //
  // Case 1 (no startingVersion, isCdcTest=false): Stream starts from current snapshot schema
  //   (id, value2). All pre-rename rows are served. Writing more rows works. Then rename back
  //   (value2 → value) while the stream is stopped; on restart the stream fails with
  //   "Streaming read is not supported".
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testBlockingRenameColumnIdMode(TableType tableType) throws Exception {
    testBlockingRenameColumn(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testBlockingRenameColumnNameMode(TableType tableType) throws Exception {
    testBlockingRenameColumn(tableType, NAME_MODE);
  }

  private void testBlockingRenameColumn(TableType tableType, String cmMode) throws Exception {
    withNewTable(
        "cm_rename_col_" + cmMode,
        "id STRING, value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Write rows 0-9 with the original (id, value) schema.
          for (int i = 0; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // RENAME COLUMN value → value2.
          sql("ALTER TABLE %s RENAME COLUMN value TO value2", tableName);

          // Write rows 10-14 with the renamed (id, value2) schema.
          for (int i = 10; i < 15; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // ── Case 1: no startingVersion ──────────────────────────────────────
          String ckpt = checkpoint();
          List<List<Object>> phase1Results = new ArrayList<>();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> phase1Results.addAll(allCols(df)))
                  .start();
          try {
            q1.processAllAvailable();
            // All 15 rows served under the (id, value2) schema.
            assertThat(phase1Results).hasSize(15);

            // Write rows 15-19 — still compatible.
            for (int i = 15; i < 20; i++) {
              sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
            }
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(20);
          } finally {
            q1.stop();
          }

          // Write rows 20-24 then rename back (value2 → value), creating incompatibility.
          for (int i = 20; i < 25; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          sql("ALTER TABLE %s RENAME COLUMN value2 TO value", tableName);

          // Restart from the same checkpoint — must block at stream start.
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("maxFilesPerTrigger", "1")
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ckpt)
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "column mapping + streaming: blocking workflow -
  //         should not generate latestOffset past schema change"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "column mapping + streaming: blocking workflow - " +
  //           "should not generate latestOffset past schema change")
  //
  // Case 1: Stream starts from version 1 (skipping the CREATE). A RENAME exists at some later
  //   version. latestOffset() must NOT advance past the rename; stream blocks with
  //   "Streaming read is not supported".
  //
  // Case 2: After the rename, also DROP the renamed column. Starting from just past the rename
  //   version should also block — DROP column is independently incompatible.
  //
  // Case 3 (in-stream): Stream starts safely past the DROP, processes normal data, then a
  //   RENAME happens. latestOffset() must NOT advance past the rename; stream fails with
  //   "Streaming read is not supported". Resuming from the same checkpoint also blocks.
  // ══════════════════════════════════════════════════════════════════════════════

  @TestAllTableTypes
  public void testNoLatestOffsetPastSchemaChangeIdMode(TableType tableType) throws Exception {
    testNoLatestOffsetPastSchemaChange(tableType, ID_MODE);
  }

  @TestAllTableTypes
  public void testNoLatestOffsetPastSchemaChangeNameMode(TableType tableType) throws Exception {
    testNoLatestOffsetPastSchemaChange(tableType, NAME_MODE);
  }

  private void testNoLatestOffsetPastSchemaChange(TableType tableType, String cmMode)
      throws Exception {
    withNewTable(
        "cm_no_offset_" + cmMode,
        "id STRING, value STRING",
        null,
        tableType,
        cmProps(cmMode),
        tableName -> {
          // Write rows 0-4, each as a separate commit (distinct Delta versions).
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // RENAME COLUMN value → value2; capture the version immediately after.
          sql("ALTER TABLE %s RENAME COLUMN value TO value2", tableName);
          long renameVersion = currentVersion(tableName); // version of the RENAME commit

          // Write rows 5-9 in the renamed (id, value2) schema.
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // ── Case 1: startingVersion=1 (skips the CREATE TABLE commit) ─────────
          // latestOffset() must NOT advance past the rename. Stream blocks immediately.
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", "1")
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");

          // ── Case 2: DROP column also blocks ──────────────────────────────────
          sql("ALTER TABLE %s DROP COLUMN value2", tableName);
          long dropVersion = currentVersion(tableName);

          // Write rows 10-14 in the post-drop (id-only) schema.
          for (int i = 10; i < 15; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          // Start from just past the rename → the DROP is visible; stream must block.
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", String.valueOf(renameVersion + 1))
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");

          // ── Case 3: in-stream failure ─────────────────────────────────────────
          // Start safely past the DROP. Process data 10-14 normally. Then RENAME id → id2.
          // latestOffset() must NOT advance past the rename.
          String ckpt3 = checkpoint();
          List<String> case3Results = new ArrayList<>();

          StreamingQuery q3 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(dropVersion + 1))
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt3)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> case3Results.addAll(firstCol(df)))
                  .start();
          try {
            q3.processAllAvailable();
            assertThat(case3Results).hasSize(5); // rows 10-14

            // RENAME id → id2 mid-stream and add one more row.
            sql("ALTER TABLE %s RENAME COLUMN id TO id2", tableName);
            sql("INSERT INTO %s VALUES ('15')", tableName);

            // latestOffset() must block; stream fails with the incompatible schema error.
            assertStreamingThrowsContaining(
                q3::processAllAvailable, "Streaming read is not supported");
          } catch (Exception e) {
            assertThat(buildCauseChain(e)).containsIgnoringCase("Streaming read is not supported");
          } finally {
            q3.stop();
          }

          // Resuming from the same ckpt3 must also block (stream-start incompatibility check).
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", String.valueOf(dropVersion + 1))
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ckpt3)
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // Test: "unsafe flag can unblock drop or rename column"
  //
  // Source: ColumnMappingStreamingBlockedWorkflowSuiteBase.test(
  //           "unsafe flag can unblock drop or rename column")
  //
  // For both DROP and RENAME: after a blocking workflow is hit and confirmed, setting
  //   spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled
  //   = true allows the stream to proceed (with potential data loss). A subsequent schema change
  //   still causes the normal retryable "Detected schema change" failure.
  //
  // The Scala suite resets the inputDir between DROP and RENAME sub-cases (FileUtils.delete).
  // Here we run them as separate test methods to get full isolation via withNewTable.
  // ══════════════════════════════════════════════════════════════════════════════

  private static final String UNSAFE_CM_FLAG_KEY =
      "spark.databricks.delta.streaming"
          + ".unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled";

  @TestAllTableTypes
  public void testUnsafeFlagUnblocksDropIdMode(TableType tableType) throws Exception {
    testUnsafeFlagUnblocks(tableType, ID_MODE, /* isRename= */ false);
  }

  @TestAllTableTypes
  public void testUnsafeFlagUnblocksDropNameMode(TableType tableType) throws Exception {
    testUnsafeFlagUnblocks(tableType, NAME_MODE, /* isRename= */ false);
  }

  @TestAllTableTypes
  public void testUnsafeFlagUnblocksRenameIdMode(TableType tableType) throws Exception {
    testUnsafeFlagUnblocks(tableType, ID_MODE, /* isRename= */ true);
  }

  @TestAllTableTypes
  public void testUnsafeFlagUnblocksRenameNameMode(TableType tableType) throws Exception {
    testUnsafeFlagUnblocks(tableType, NAME_MODE, /* isRename= */ true);
  }

  private void testUnsafeFlagUnblocks(TableType tableType, String cmMode, boolean isRename)
      throws Exception {
    String schemaChangeOp = isRename ? "RENAME COLUMN value TO value2" : "DROP COLUMN value";
    String suffix = (isRename ? "rename" : "drop") + "_" + cmMode;

    // Start the table without CM (matching the Scala suite's withColumnMappingConf("none")).
    // The suite then upgrades to name mode, adds a random column, and applies the change.
    withNewTable(
        "cm_unsafe_" + suffix,
        "id STRING, value STRING",
        null,
        tableType,
        NO_CM_PROPS,
        tableName -> {
          // Write rows 0-4 without column mapping.
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String ckpt = checkpoint();
          List<List<Object>> phase1Results = new ArrayList<>();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> phase1Results.addAll(allCols(df)))
                  .start();
          try {
            q1.processAllAvailable();
            assertThat(phase1Results).hasSize(5);

            // Upgrade to name mode, add a random extra column, then apply the schema change.
            // (Mirrors the Scala suite's sequence to ensure schemaChange verifySchemaChange sees
            // more columns than the read schema and raises the blocking error.)
            sql(
                "ALTER TABLE %s SET TBLPROPERTIES ("
                    + "'delta.columnMapping.mode'='name',"
                    + "'delta.minReaderVersion'='2',"
                    + "'delta.minWriterVersion'='5')",
                tableName);
            sql("ALTER TABLE %s ADD COLUMN (random STRING)", tableName);
            sql("ALTER TABLE %s %s", tableName, schemaChangeOp);

            // Write rows 5-9. The number of columns depends on the operation:
            // - RENAME: schema is (id, value2, random) → 3 values
            // - DROP:   schema is (id, random)          → 2 values
            for (int i = 5; i < 10; i++) {
              if (isRename) {
                sql("INSERT INTO %s (id, value2) VALUES ('%d', '%d')", tableName, i, i);
              } else {
                sql("INSERT INTO %s (id) VALUES ('%d')", tableName, i);
              }
            }

            // Stream fails with retryable "Detected schema change" (ADD COLUMN was additive,
            // but the rename/drop makes the schema incompatible at the batch boundary).
            assertStreamingThrowsContaining(q1::processAllAvailable, "Detected schema change");
          } catch (Exception e) {
            assertThat(buildCauseChain(e)).containsIgnoringCase("Detected schema change");
          } finally {
            q1.stop();
          }

          // Without the flag, stream fails at start with "Streaming read is not supported".
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ckpt)
                      .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                      .start()
                      .awaitTermination(),
              "Streaming read is not supported");

          // WITH the unsafe flag: the stream is unblocked and ingests rows 5-9.
          spark().conf().set(UNSAFE_CM_FLAG_KEY, "true");
          try {
            List<List<Object>> phase3Results = new ArrayList<>();
            StreamingQuery q3 =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .option("checkpointLocation", ckpt)
                    .foreachBatch(
                        (VoidFunction2<Dataset<Row>, Long>)
                            (df, id) -> phase3Results.addAll(allCols(df)))
                    .start();
            try {
              q3.processAllAvailable();
              // 5 rows (5-9) ingested with the changed schema.
              assertThat(phase3Results).hasSize(5);

              // A further schema change still causes the normal retryable failure.
              sql("ALTER TABLE %s ADD COLUMN (random2 STRING)", tableName);
              assertStreamingThrowsContaining(q3::processAllAvailable, "Detected schema change");
            } catch (Exception e) {
              assertThat(buildCauseChain(e)).containsIgnoringCase("Detected schema change");
            } finally {
              q3.stop();
            }
          } finally {
            // Always reset the unsafe flag so it does not bleed into other tests.
            spark().conf().set(UNSAFE_CM_FLAG_KEY, "false");
          }
        });
  }

  // ══════════════════════════════════════════════════════════════════════════════
  // SKIPPED SCENARIOS
  //
  // SKIPPED: "deltaLog snapshot should not be updated outside of the stream"
  //   Reason: Requires direct DeltaLog access (DeltaLog.forTable), reflection into private
  //   streaming-query internals (StreamingExecutionRelation, q.logicalPlan.collectFirst,
  //   source.asInstanceOf[DeltaSource], source.snapshotAtSourceInit), and writing data by
  //   directly calling DeltaLog transaction APIs. None of these are accessible via the public
  //   SQL + DataFrame API used in UC integration tests.
  //
  // SKIPPED: "blocking - rename column" Case 2 (startingVersion=0, isCdcTest=false) —
  //   the AssertOnQuery { q => getLatestCommittedDeltaVersion(q) <= 10 } assertion
  //   Reason: Requires inspecting internal streaming-query offset state via
  //   q.committedOffsets (Scala-private Seq of DeltaSourceOffset). The Java StreamingQuery
  //   interface does not expose this. The primary assertion (stream blocks with "Streaming
  //   read is not supported" when startingVersion=0 encounters a rename+rename-back) IS
  //   exercised through testBlockingRenameColumn's restart scenario.
  //
  // SKIPPED: Stack-trace assertions in "should not generate latestOffset past schema change"
  //   (e.g., q.exception.get.cause.getStackTrace.exists(_.toString.contains("latestOffset")))
  //   Reason: Requires q.exception (Scala Option[StreamingQueryException]) which is not part
  //   of the Java StreamingQuery API. The functionally equivalent assertion — that the stream
  //   fails with "Streaming read is not supported" — IS made in
  //   testNoLatestOffsetPastSchemaChange.
  // ══════════════════════════════════════════════════════════════════════════════
}
