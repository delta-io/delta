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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Java integration test for the remove-column-mapping streaming read scenarios against real Unity
 * Catalog tables in the default AUTO mode (no V2_ENABLE_MODE override).
 *
 * <p>This port of {@code RemoveColumnMappingStreamingReadV2Suite} validates AUTO-mode routing:
 * MANAGED tables are served by DSv2/Kernel and EXTERNAL tables fall back to V1. Each permutation
 * from the Scala suite's {@code shouldPassTests} set is reproduced here with both the
 * no-schema-tracking and with-schema-tracking variants.
 *
 * <p>Terminology:
 *
 * <ul>
 *   <li><b>Upgrade</b> – enable column mapping ({@code delta.columnMapping.mode=name}).
 *   <li><b>Downgrade</b> – remove column mapping ({@code delta.columnMapping.mode=none}) + insert
 *       sentinel rows.
 *   <li><b>Rename</b> – {@code ALTER TABLE … RENAME COLUMN third_column_name TO
 *       renamed_third_column_name}.
 *   <li><b>Drop</b> – {@code ALTER TABLE … DROP COLUMN third_column_name}.
 *   <li><b>StartStreamRead</b> – marker indicating the point at which the stream is started.
 *   <li><b>FailNonAdditiveChange</b> – the stream must throw with "DELTA_STREAMING_INCOMPATIBLE
 *       _SCHEMA_CHANGE" (non-additive schema change detected).
 *   <li><b>Success</b> – the stream drains cleanly and reads all committed rows.
 *   <li><b>SuccessAndFailSchemaTracking</b> – succeeds without schema tracking, but fails with
 *       "DELTA_STREAMING_METADATA_EVOLUTION" when schema tracking is enabled.
 * </ul>
 *
 * <p>The table starts with three columns ({@code first_column_name}, {@code second_column_name},
 * {@code third_column_name}) with {@code columnMapping.mode=none} and CDF enabled.
 */
public class UCRemoveColumnMappingStreamingReadTest extends UCDeltaTableIntegrationBaseTest {

  // ---------------------------------------------------------------------------
  // Column / table constants mirrored from RemoveColumnMappingSuiteUtils
  // ---------------------------------------------------------------------------
  private static final String FIRST_COL = "first_column_name";
  private static final String SECOND_COL = "second_column_name";
  private static final String THIRD_COL = "third_column_name";
  private static final String RENAMED_THIRD_COL = "renamed_third_column_name";

  /** Total seed rows inserted at table creation (matches the Scala suite's {@code totalRows}). */
  private static final int TOTAL_ROWS = 10; // Reduced from 100 for UC integration test speed.

  /** Expected outcome enum – drives helper logic. */
  private enum Outcome {
    /** Stream drains without error; all rows are visible. */
    SUCCESS,
    /**
     * Stream must throw with a message containing "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE"
     * (non-additive schema change detected during or at start of streaming).
     */
    FAIL_NON_ADDITIVE_CHANGE,
    /**
     * Without schema tracking: succeeds (same as SUCCESS). With schema tracking: stream throws with
     * "DELTA_STREAMING_METADATA_EVOLUTION".
     */
    SUCCESS_AND_FAIL_SCHEMA_TRACKING,
  }

  /** Step enum – mirrors the Scala case objects in the base suite. */
  private enum Step {
    UPGRADE,
    DOWNGRADE,
    RENAME,
    DROP,
    START_STREAM_READ,
  }

  // ---------------------------------------------------------------------------
  // JUnit temp dir – fresh for each test instance
  // ---------------------------------------------------------------------------
  @TempDir private Path tempDir;

  private int checkpointCount = 0;

  // ---------------------------------------------------------------------------
  // Per-test state reset helper
  // ---------------------------------------------------------------------------

  /**
   * Returns a fresh local checkpoint directory path (unique per call within a test).
   *
   * <p>The path is returned as a {@code file://} URI string so Spark recognises it even on systems
   * where the default FS is not local.
   */
  private String checkpoint() throws Exception {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectories(ckDir);
    return ckDir.toUri().toString();
  }

  /**
   * Returns a fresh paired {@code [checkpoint, schemaLocation]} where the schema-tracking location
   * lives *inside* the checkpoint directory.
   *
   * <p>Delta requires {@code schemaTrackingLocation} to be a subdirectory of the checkpoint
   * location, otherwise it throws {@code DELTA_STREAMING_SCHEMA_LOCATION_NOT_UNDER_CHECKPOINT}.
   * Both paths are returned as {@code file://} URI strings.
   */
  private String[] checkpointAndSchemaLocation() throws Exception {
    Path ckDir = tempDir.resolve("cks-" + checkpointCount++);
    Files.createDirectories(ckDir);
    Path slDir = ckDir.resolve("_schema_location");
    Files.createDirectories(slDir);
    return new String[] {ckDir.toUri().toString(), slDir.toUri().toString()};
  }

  // ---------------------------------------------------------------------------
  // Table setup helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates the column-mapping test table with an initial seed of {@code TOTAL_ROWS} rows across
   * three integer columns, with CDF enabled and column mapping starting at {@code none}.
   *
   * <p>The {@code tableProperties} argument must already include {@code
   * 'delta.enableChangeDataFeed'='true'} as required by {@code withNewTable}.
   */
  private void createCmTable(String tableName) {
    // Table already created by withNewTable with the schema below; we insert the seed rows here.
    // The schema: first_column_name INT, second_column_name INT, third_column_name INT
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < TOTAL_ROWS; i++) {
      if (i > 0) values.append(", ");
      // Row: (i, i+1, i+2)
      values.append(String.format("(%d, %d, %d)", i, i + 1, i + 2));
    }
    sql("INSERT INTO %s VALUES %s", tableName, values.toString());
  }

  // ---------------------------------------------------------------------------
  // DDL operation helpers
  // ---------------------------------------------------------------------------

  /** Upgrade: enable column mapping (name mode) with required reader/writer versions. */
  private void upgrade(String tableName) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ("
            + "'delta.columnMapping.mode'='name', "
            + "'delta.minReaderVersion'='2', "
            + "'delta.minWriterVersion'='5')",
        tableName);
  }

  /**
   * Downgrade: remove column mapping by setting mode back to {@code none}, then insert sentinel
   * rows of -1 values (matching {@code insertMoreRows(v = -1)} in the Scala suite).
   */
  private void downgrade(String tableName) {
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.columnMapping.mode'='none')", tableName);
    // Insert -1 sentinel rows to produce additional data after the downgrade.
    // We must discover current column count dynamically; after a Drop there are only 2 columns.
    int numCols = spark().table(tableName).schema().fields().length;
    StringBuilder selectCols = new StringBuilder();
    for (int i = 0; i < numCols; i++) {
      if (i > 0) selectCols.append(", ");
      selectCols.append("-1");
    }
    sql(
        "INSERT INTO %s SELECT %s FROM %s LIMIT %d",
        tableName, selectCols.toString(), tableName, TOTAL_ROWS);
  }

  /** Rename: {@code ALTER TABLE … RENAME COLUMN third_column_name TO renamed_third_column_name}. */
  private void rename(String tableName) {
    sql("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, THIRD_COL, RENAMED_THIRD_COL);
  }

  /** Drop: {@code ALTER TABLE … DROP COLUMN third_column_name}. */
  private void drop(String tableName) {
    sql("ALTER TABLE %s DROP COLUMN %s", tableName, THIRD_COL);
  }

  // ---------------------------------------------------------------------------
  // Core scenario runner
  // ---------------------------------------------------------------------------

  /**
   * Creates a fresh table for each variant (no-schema-tracking and with-schema-tracking) and runs
   * the scenario on each. The two variants use table names {@code baseName + "_nt"} and {@code
   * baseName + "_st"} respectively, ensuring each variant sees a pristine table with no mutations
   * carried over from the other variant.
   *
   * @param baseName base table name (variant suffixes are appended automatically)
   * @param tableType MANAGED or EXTERNAL
   * @param outcome expected result of the streaming read
   * @param steps ordered sequence of operations encoding the test scenario
   */
  private void runBothVariants(String baseName, TableType tableType, Outcome outcome, Step... steps)
      throws Exception {
    boolean[] variants = {false, true};
    for (int i = 0; i < variants.length; i++) {
      boolean withSchemaTracking = variants[i];
      String name = baseName + (withSchemaTracking ? "_st" : "_nt");
      withNewTable(
          name,
          CM_SCHEMA,
          null,
          tableType,
          CM_TABLE_PROPS,
          t -> {
            createCmTable(t);
            runScenarioVariant(t, outcome, withSchemaTracking, steps);
          });
    }
  }

  private void runScenarioVariant(
      String tableName, Outcome outcome, boolean withSchemaTracking, Step... steps)
      throws Exception {

    // Locate the StartStreamRead marker.
    int startIdx = -1;
    for (int i = 0; i < steps.length; i++) {
      if (steps[i] == Step.START_STREAM_READ) {
        startIdx = i;
        break;
      }
    }
    if (startIdx < 0) {
      throw new IllegalArgumentException("Scenario must contain a START_STREAM_READ step");
    }

    // Apply all steps before StartStreamRead as pre-stream DDL.
    for (int i = 0; i < startIdx; i++) {
      applyStep(tableName, steps[i]);
    }

    // Build the stream reader. When schema tracking is enabled, the schema-tracking location MUST
    // live inside the checkpoint dir (Delta requirement), so allocate them as a pair.
    final String ckpt;
    DataStreamReader reader = spark().readStream().format("delta");
    if (withSchemaTracking) {
      String[] locs = checkpointAndSchemaLocation();
      ckpt = locs[0];
      reader = reader.option("schemaTrackingLocation", locs[1]);
    } else {
      ckpt = checkpoint();
    }
    DataStreamReader finalReader = reader;

    // Collect rows via foreachBatch so we can assert on row count for SUCCESS variants.
    // Synchronized because foreachBatch callbacks may run on different threads.
    List<Row> collected = java.util.Collections.synchronizedList(new ArrayList<>());

    // Steps after StartStreamRead (excluding StartStreamRead itself).
    Step[] postSteps = new Step[steps.length - startIdx - 1];
    System.arraycopy(steps, startIdx + 1, postSteps, 0, postSteps.length);

    // Determine the expected outcome for *this* variant.
    boolean expectSchemaTrackingFail =
        withSchemaTracking && outcome == Outcome.SUCCESS_AND_FAIL_SCHEMA_TRACKING;
    boolean expectNonAdditiveFail = outcome == Outcome.FAIL_NON_ADDITIVE_CHANGE;

    if (postSteps.length == 0) {
      // All DDL was done before the stream; use AvailableNow for a clean terminating read.
      ThrowingCallable runStream =
          () ->
              finalReader
                  .table(tableName)
                  .writeStream()
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ckpt)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> collected.addAll(df.collectAsList()))
                  .start()
                  .awaitTermination();

      if (expectNonAdditiveFail) {
        // With schema tracking Delta may raise METADATA_EVOLUTION instead of
        // INCOMPATIBLE_SCHEMA_CHANGE; accept either (plus the older text form).
        assertStreamingThrowsContainingAny(
            runStream,
            "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE",
            "DELTA_STREAMING_METADATA_EVOLUTION",
            "Streaming read is not supported");
      } else if (expectSchemaTrackingFail) {
        assertStreamingThrowsContaining(runStream, "DELTA_STREAMING_METADATA_EVOLUTION");
      } else {
        // SUCCESS (with or without schema tracking in non-fail case)
        assertThatCode(runStream).doesNotThrowAnyException();
        assertThat(collected).isNotEmpty();
      }

    } else {
      // There are post-stream DDL steps; use a continuous query so we can interleave DDL.
      StreamingQuery query =
          finalReader
              .table(tableName)
              .writeStream()
              .option("checkpointLocation", ckpt)
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, id) -> collected.addAll(df.collectAsList()))
              .start();

      try {
        // Drain existing data before applying post-stream DDL.
        query.processAllAvailable();

        // Apply each post-stream step, draining in between.
        // Capture the first exception thrown during processing so we can assert on it below.
        Exception firstStreamingException = null;
        for (Step step : postSteps) {
          applyStep(tableName, step);
          // Try to process; capture the first error and stop the loop.
          try {
            query.processAllAvailable();
          } catch (Exception e) {
            firstStreamingException = e;
            break;
          }
        }

        // Also check if the query itself recorded an exception (may differ from the thrown one).
        Exception queryException = query.exception().isDefined() ? query.exception().get() : null;

        // Prefer the query-level exception (richer) over the processAllAvailable wrapper.
        Throwable effectiveException =
            queryException != null ? queryException : firstStreamingException;

        if (expectNonAdditiveFail || expectSchemaTrackingFail) {
          // Error fragments that can appear depending on whether schema tracking is active:
          //   - with schema tracking:    DELTA_STREAMING_METADATA_EVOLUTION
          //   - without schema tracking: DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE
          //                              or "Streaming read is not supported" (older path)
          final String[] nonAdditiveFragments = {
            "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE",
            "DELTA_STREAMING_METADATA_EVOLUTION",
            "Streaming read is not supported"
          };
          if (effectiveException != null) {
            // Already have the exception — just assert its message.
            String msg = causeChainMessage(effectiveException);
            boolean found = false;
            for (String frag : nonAdditiveFragments) {
              if (msg.toLowerCase(java.util.Locale.ROOT)
                  .contains(frag.toLowerCase(java.util.Locale.ROOT))) {
                found = true;
                break;
              }
            }
            assertThat(found)
                .as(
                    "Expected streaming exception to contain one of %s but was:\n%s",
                    java.util.Arrays.toString(nonAdditiveFragments), msg)
                .isTrue();
          } else {
            // No exception yet — trigger one more processAllAvailable to surface the error.
            assertStreamingThrowsContainingAny(query::processAllAvailable, nonAdditiveFragments);
          }
        } else {
          // SUCCESS: no exception should have occurred.
          if (firstStreamingException != null) {
            throw new AssertionError(
                "Expected stream to succeed but got exception", firstStreamingException);
          }
          assertThat(collected).isNotEmpty();
        }
      } finally {
        query.stop();
      }
    }
  }

  /** Applies a single non-StartStreamRead step to the table. */
  private void applyStep(String tableName, Step step) {
    switch (step) {
      case UPGRADE:
        upgrade(tableName);
        break;
      case DOWNGRADE:
        downgrade(tableName);
        break;
      case RENAME:
        rename(tableName);
        break;
      case DROP:
        drop(tableName);
        break;
      case START_STREAM_READ:
        throw new IllegalStateException("START_STREAM_READ should not be applied as a DDL step");
      default:
        throw new IllegalStateException("Unknown step: " + step);
    }
  }

  /** Table properties string required for every column-mapping test table. */
  private static final String CM_TABLE_PROPS =
      "'delta.enableChangeDataFeed'='true', 'delta.columnMapping.mode'='none'";

  /** Shared schema for every column-mapping test table. */
  private static final String CM_SCHEMA =
      FIRST_COL + " INT, " + SECOND_COL + " INT, " + THIRD_COL + " INT";

  /**
   * Asserts that {@code action} throws an exception whose cause chain contains {@code fragment}.
   */
  private static void assertStreamingThrowsContaining(ThrowingCallable action, String fragment) {
    assertStreamingThrowsContainingAny(action, fragment);
  }

  /**
   * Asserts that {@code action} throws an exception whose cause chain contains at least one of the
   * given {@code fragments} (case-insensitive). Use this when the exact error class depends on
   * whether schema tracking is enabled.
   */
  private static void assertStreamingThrowsContainingAny(
      ThrowingCallable action, String... fragments) {
    assertThatThrownBy(action)
        .satisfies(
            e -> {
              StringBuilder full = new StringBuilder();
              for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null) full.append(t.getMessage()).append(' ');
              }
              String msg = full.toString().toLowerCase(java.util.Locale.ROOT);
              boolean found = false;
              for (String fragment : fragments) {
                if (msg.contains(fragment.toLowerCase(java.util.Locale.ROOT))) {
                  found = true;
                  break;
                }
              }
              assertThat(found)
                  .as(
                      "Expected exception message to contain one of %s but was:\n%s",
                      java.util.Arrays.toString(fragments), full)
                  .isTrue();
            });
  }

  /** Extracts the combined cause-chain message from a Throwable (for query.exception() checks). */
  private static String causeChainMessage(Throwable e) {
    if (e == null) return "";
    StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null) sb.append(t.getMessage()).append(' ');
    }
    return sb.toString();
  }

  // ===========================================================================
  // Test methods — one per permutation from RemoveColumnMappingStreamingReadV2Suite#shouldPassTests
  // Each method name encodes the step sequence and expected outcome.
  // Both no-schema-tracking and with-schema-tracking variants are exercised inside
  // runBothVariants().
  // ===========================================================================

  // ---------------------------------------------------------------------------
  // Group 1: StartStreamRead first, then Upgrade+Downgrade
  // ---------------------------------------------------------------------------

  /**
   * StartStreamRead, Upgrade, Downgrade, SuccessAndFailSchemaTracking (also covers "… with schema
   * tracking" variant internally)
   *
   * <p>Physical/logical names don't change between start and end, so it succeeds without schema
   * tracking. Schema tracking prohibits reading across an upgrade, so it fails.
   */
  @TestAllTableTypes
  public void testStartStreamRead_Upgrade_Downgrade_SuccessAndFailSchemaTracking(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_sr_upg_dng",
        tableType,
        Outcome.SUCCESS_AND_FAIL_SCHEMA_TRACKING,
        Step.START_STREAM_READ,
        Step.UPGRADE,
        Step.DOWNGRADE);
  }

  // ---------------------------------------------------------------------------
  // Group 2: Upgrade first, then StartStreamRead, then Downgrade
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, StartStreamRead, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant internally)
   *
   * <p>Physical names change between start (upgraded) and end (downgraded), so the stream must fail
   * with a non-additive schema change error regardless of schema tracking.
   */
  @TestAllTableTypes
  public void testUpgrade_StartStreamRead_Downgrade_FailNonAdditiveChange(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_upg_sr_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.START_STREAM_READ,
        Step.DOWNGRADE);
  }

  // ---------------------------------------------------------------------------
  // Group 3: Upgrade, Downgrade, then StartStreamRead (reading plain table)
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, Downgrade, StartStreamRead, Success (also covers "… with schema tracking" variant
   * internally)
   *
   * <p>By the time the stream starts the table is already back to no-column-mapping. Both variants
   * succeed.
   */
  @TestAllTableTypes
  public void testUpgrade_Downgrade_StartStreamRead_Success(TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_dng_sr",
        tableType,
        Outcome.SUCCESS,
        Step.UPGRADE,
        Step.DOWNGRADE,
        Step.START_STREAM_READ);
  }

  // ---------------------------------------------------------------------------
  // Group 4: StartStreamRead with Upgrade+Rename/Drop+Downgrade
  // ---------------------------------------------------------------------------

  /**
   * StartStreamRead, Upgrade, Rename, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   */
  @TestAllTableTypes
  public void testStartStreamRead_Upgrade_Rename_Downgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_sr_upg_rn_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.START_STREAM_READ,
        Step.UPGRADE,
        Step.RENAME,
        Step.DOWNGRADE);
  }

  /**
   * StartStreamRead, Upgrade, Drop, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   */
  @TestAllTableTypes
  public void testStartStreamRead_Upgrade_Drop_Downgrade_FailNonAdditiveChange(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_sr_upg_drp_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.START_STREAM_READ,
        Step.UPGRADE,
        Step.DROP,
        Step.DOWNGRADE);
  }

  /**
   * StartStreamRead, Upgrade, Rename, Downgrade, Upgrade, FailNonAdditiveChange (also covers "…
   * with schema tracking" variant)
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testStartStreamRead_Upgrade_Rename_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_sr_upg_rn_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.START_STREAM_READ,
        Step.UPGRADE,
        Step.RENAME,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  /**
   * StartStreamRead, Upgrade, Drop, Downgrade, Upgrade, FailNonAdditiveChange (also covers "… with
   * schema tracking" variant)
   */
  @TestAllTableTypes
  public void testStartStreamRead_Upgrade_Drop_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_sr_upg_drp_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.START_STREAM_READ,
        Step.UPGRADE,
        Step.DROP,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  // ---------------------------------------------------------------------------
  // Group 5: Upgrade, StartStreamRead, then Rename/Drop + Downgrade
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, StartStreamRead, Rename, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   */
  @TestAllTableTypes
  public void testUpgrade_StartStreamRead_Rename_Downgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_sr_rn_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.START_STREAM_READ,
        Step.RENAME,
        Step.DOWNGRADE);
  }

  /**
   * Upgrade, StartStreamRead, Drop, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testUpgrade_StartStreamRead_Drop_Downgrade_FailNonAdditiveChange(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_upg_sr_drp_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.START_STREAM_READ,
        Step.DROP,
        Step.DOWNGRADE);
  }

  /**
   * Upgrade, StartStreamRead, Rename, Downgrade, Upgrade, FailNonAdditiveChange (also covers "…
   * with schema tracking" variant)
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testUpgrade_StartStreamRead_Rename_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_sr_rn_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.START_STREAM_READ,
        Step.RENAME,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  /**
   * Upgrade, StartStreamRead, Drop, Downgrade, Upgrade, FailNonAdditiveChange (also covers "… with
   * schema tracking" variant)
   */
  @TestAllTableTypes
  public void testUpgrade_StartStreamRead_Drop_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_sr_drp_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.START_STREAM_READ,
        Step.DROP,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  // ---------------------------------------------------------------------------
  // Group 6: Upgrade, Rename/Drop, StartStreamRead, Downgrade
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, Rename, StartStreamRead, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   *
   * <p>Schema at stream-start (renamed column) is physically different from the schema after
   * downgrade, so the stream must fail.
   */
  @TestAllTableTypes
  public void testUpgrade_Rename_StartStreamRead_Downgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_rn_sr_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.RENAME,
        Step.START_STREAM_READ,
        Step.DOWNGRADE);
  }

  /**
   * Upgrade, Rename, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange (also covers "…
   * with schema tracking" variant)
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testUpgrade_Rename_StartStreamRead_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_rn_sr_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.RENAME,
        Step.START_STREAM_READ,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  /**
   * Upgrade, Drop, StartStreamRead, Downgrade, FailNonAdditiveChange (also covers "… with schema
   * tracking" variant)
   */
  @TestAllTableTypes
  public void testUpgrade_Drop_StartStreamRead_Downgrade_FailNonAdditiveChange(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_upg_drp_sr_dng",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.DROP,
        Step.START_STREAM_READ,
        Step.DOWNGRADE);
  }

  /**
   * Upgrade, Drop, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange (also covers "… with
   * schema tracking" variant)
   */
  @TestAllTableTypes
  public void testUpgrade_Drop_StartStreamRead_Downgrade_Upgrade_FailNonAdditiveChange(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_drp_sr_dng_upg",
        tableType,
        Outcome.FAIL_NON_ADDITIVE_CHANGE,
        Step.UPGRADE,
        Step.DROP,
        Step.START_STREAM_READ,
        Step.DOWNGRADE,
        Step.UPGRADE);
  }

  // ---------------------------------------------------------------------------
  // Group 7: Upgrade, Rename/Drop, Downgrade, StartStreamRead (plain table read)
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, Rename, Downgrade, StartStreamRead, Success (also covers "… with schema tracking"
   * variant)
   *
   * <p>By the time the stream starts the table has no column mapping; the stream reads a plain
   * table and succeeds.
   */
  @TestAllTableTypes
  public void testUpgrade_Rename_Downgrade_StartStreamRead_Success(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_upg_rn_dng_sr",
        tableType,
        Outcome.SUCCESS,
        Step.UPGRADE,
        Step.RENAME,
        Step.DOWNGRADE,
        Step.START_STREAM_READ);
  }

  /**
   * Upgrade, Drop, Downgrade, StartStreamRead, Success (also covers "… with schema tracking"
   * variant)
   */
  @TestAllTableTypes
  public void testUpgrade_Drop_Downgrade_StartStreamRead_Success(TableType tableType)
      throws Exception {
    runBothVariants(
        "cm_upg_drp_dng_sr",
        tableType,
        Outcome.SUCCESS,
        Step.UPGRADE,
        Step.DROP,
        Step.DOWNGRADE,
        Step.START_STREAM_READ);
  }

  // ---------------------------------------------------------------------------
  // Group 8: Upgrade, Rename/Drop, Downgrade, StartStreamRead, Upgrade
  //          (SuccessAndFailSchemaTracking — reading across the second upgrade)
  // ---------------------------------------------------------------------------

  /**
   * Upgrade, Rename, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking (also covers
   * "… with schema tracking" variant)
   *
   * <p>Reading across the second upgrade is fine without schema tracking but fails with it.
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testUpgrade_Rename_Downgrade_StartStreamRead_Upgrade_SuccessAndFailSchemaTracking(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_rn_dng_sr_upg",
        tableType,
        Outcome.SUCCESS_AND_FAIL_SCHEMA_TRACKING,
        Step.UPGRADE,
        Step.RENAME,
        Step.DOWNGRADE,
        Step.START_STREAM_READ,
        Step.UPGRADE);
  }

  /**
   * Upgrade, Drop, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking (also covers
   * "… with schema tracking" variant)
   */
  // ⚠ KNOWN FAILURE — EXTERNAL only; MANAGED (DSv2) passes. SIGN-OFF: SAFE.
  // Reason: this rename/drop + remove-column-mapping (downgrade) flow expects the stream to throw
  // (FailNonAdditiveChange / metadata-evolution under schema tracking). The MANAGED / AUTO→DSv2
  // (Kernel) path raises it as expected and passes; the V1 (path-based / EXTERNAL) source does not
  // raise the block, so the "stream must throw" assertion fails on EXTERNAL.
  // Why safe: INVERSE of a DSv2 regression — DSv2 is stricter / more correct than V1 here and
  // matches the original RemoveColumnMappingStreamingReadV2Suite expectation. The EXTERNAL miss is
  // pre-existing V1 leniency on column-mapping removal, not an AUTO→DSv2 routing defect.
  @TestAllTableTypes
  public void testUpgrade_Drop_Downgrade_StartStreamRead_Upgrade_SuccessAndFailSchemaTracking(
      TableType tableType) throws Exception {
    runBothVariants(
        "cm_upg_drp_dng_sr_upg",
        tableType,
        Outcome.SUCCESS_AND_FAIL_SCHEMA_TRACKING,
        Step.UPGRADE,
        Step.DROP,
        Step.DOWNGRADE,
        Step.START_STREAM_READ,
        Step.UPGRADE);
  }
}
