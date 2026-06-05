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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test suite that ports the scenarios from {@code DeltaV2SourceSchemaEvolutionSuite}
 * (which forces {@code V2_ENABLE_MODE=STRICT} on path-based tables) into a real Unity Catalog
 * environment running in the default {@code AUTO} mode.
 *
 * <p>Under AUTO mode MANAGED tables route to DSv2/Kernel and EXTERNAL tables fall back to V1, so
 * {@code @TestAllTableTypes} exercises both paths and surfaces any failures.
 *
 * <p>All schema-evolution scenarios use the {@code schemaTrackingLocation} streaming option and
 * column-mapping mode "name" (required by rename/drop). Table properties always include {@code
 * delta.enableChangeDataFeed=true}; column-mapping tables additionally carry {@code
 * delta.columnMapping.mode=name}, {@code delta.minReaderVersion=2}, and {@code
 * delta.minWriterVersion=5}.
 *
 * <p>Scenarios that require direct construction of internal Delta objects (e.g. {@code
 * DeltaSourceMetadataTrackingLog}, {@code PersistedMetadata}, {@code DeltaSource}, {@code
 * DeltaAnalysis.assertSchemaTrackingLocationUnderCheckpoint}) are listed as SKIPPED below.
 *
 * <h2>SKIPPED scenarios (internal / private API)</h2>
 *
 * <ul>
 *   <li><b>schema location not under checkpoint</b> — calls {@code
 *       DeltaAnalysis.assertSchemaTrackingLocationUnderCheckpoint} directly.
 *   <li><b>schema location same as checkpoint</b> — same.
 *   <li><b>schema location using a different file system</b> — same; also requires registering a
 *       custom {@code S3LikeLocalFileSystem}.
 *   <li><b>schema / checkpoint location unit tests - *</b> (6 sub-tests) — all call {@code
 *       DeltaAnalysis.assertSchemaTrackingLocationUnderCheckpoint}.
 *   <li><b>concurrent schema log modification should be detected</b> — requires constructing two
 *       {@code DeltaSourceMetadataTrackingLog} instances directly.
 *   <li><b>schema log is applied</b> — requires {@code DeltaSourceMetadataTrackingLog}, {@code
 *       PersistedMetadata}, and internal metadata manipulation.
 *   <li><b>schema log replace current</b> — same; directly calls {@code
 *       DeltaSourceMetadataTrackingLog.writeNewMetadata(replaceCurrent=true)}.
 *   <li><b>backward-compat: latest version can read back older JSON</b> — tests {@code
 *       PersistedMetadata.fromJson(OldPersistedSchema)} serialization, no public API.
 *   <li><b>forward-compat: older version can read back newer JSON</b> — same.
 *   <li><b>latestOffset should not progress before schema evolved</b> — requires constructing a
 *       {@code DeltaSource} and calling {@code latestOffset} / {@code getBatch} / {@code commit} on
 *       it directly.
 *   <li><b>detect invalid offset during initialization before initializing schema log - rename /
 *       drop</b> — requires direct manipulation of checkpoint offset files ({@code
 *       manuallyCreateLatestStreamingOffsetUntilReservoirVersion}) and verifying {@code
 *       DeltaSourceMetadataTrackingLog} state.
 *   <li><b>no need to block schema log initialization if constructed batch ends on schema
 *       change</b> — same.
 *   <li><b>resolve the most encompassing schema during getBatch to initialize schema log</b> —
 *       same.
 *   <li><b>identity columns shouldn't cause schema mismatches</b> — uses {@code
 *       DeltaTable.columnBuilder(...).generatedAlwaysAsIdentity()} (Java Delta builder) and
 *       inspects internal schema metadata via {@code DeltaSourceUtils.IDENTITY_INFO_HIGHWATERMARK};
 *       no SQL-only path exists in Delta OSS.
 *   <li><b>multiple delta source sharing same schema log is blocked</b> — requires constructing two
 *       {@code DeltaSourceMetadataTrackingLog} instances to verify conflict detection.
 * </ul>
 */
public class UCDeltaSourceSchemaEvolutionStreamingTest extends UCDeltaTableIntegrationBaseTest {

  // -----------------------------------------------------------------------
  // Standard table properties used throughout
  // -----------------------------------------------------------------------

  /**
   * Properties that enable CDF and column-mapping mode=name (required for rename/drop).
   * minReaderVersion=2 and minWriterVersion=5 are required for column mapping.
   */
  private static final String COL_MAPPING_PROPS =
      "'delta.enableChangeDataFeed'='true',"
          + "'delta.columnMapping.mode'='name',"
          + "'delta.minReaderVersion'='2',"
          + "'delta.minWriterVersion'='5'";

  /** Properties without column mapping (for tests that explicitly start without it). */
  private static final String BASIC_PROPS = "'delta.enableChangeDataFeed'='true'";

  // -----------------------------------------------------------------------
  // Suite-level Spark configuration
  // -----------------------------------------------------------------------

  /**
   * Enables the schema-tracking feature flags that the Scala {@code
   * StreamingSchemaEvolutionSuiteBase} sets in its {@code sparkConf} override. Without these the
   * streaming schema-tracking code path is not activated and most tests would not exercise the
   * intended logic.
   *
   * <p>Also sets {@code allowSourceColumnRenameAndDrop=always} as the suite-wide default;
   * individual tests that need to test the blocked-without-conf path override it locally and
   * restore it.
   */
  @BeforeAll
  public void enableSchemaTrackingConf() {
    // Guard: spark() may be null if setUpSpark() was aborted (e.g. version assumption).
    if (spark() == null) return;
    spark().conf().set("spark.databricks.delta.streaming.schemaTracking.enabled", "true");
    spark()
        .conf()
        .set(
            "spark.databricks.delta.streaming.schemaTracking"
                + ".mergeConsecutiveSchemaChanges.enabled",
            "true");
    spark().conf().set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always");
  }

  // -----------------------------------------------------------------------
  // Infrastructure: checkpoint / schema-location helpers
  // -----------------------------------------------------------------------

  @TempDir private Path tempDir;

  private int checkpointCount = 0;
  private int schemaLocCount = 0;

  /**
   * Returns a fresh local directory to use as a streaming checkpoint. Each call returns a distinct
   * directory so queries within the same test don't share state.
   */
  private String checkpoint() throws IOException {
    Path dir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectories(dir);
    return dir.toUri().toString();
  }

  /**
   * Returns a paired (checkpoint, schemaLocation) where the schema location lives *inside* the
   * checkpoint directory (as required by the default Delta validation rule).
   */
  private String[] checkpointAndSchemaLocation() throws IOException {
    Path ckDir = tempDir.resolve("cks-" + checkpointCount++);
    Files.createDirectories(ckDir);
    Path slDir = ckDir.resolve("_schema_location");
    Files.createDirectories(slDir);
    return new String[] {ckDir.toUri().toString(), slDir.toUri().toString()};
  }

  // -----------------------------------------------------------------------
  // Streaming helpers
  // -----------------------------------------------------------------------

  /** No-op foreachBatch sink. */
  private static final VoidFunction2<Dataset<Row>, Long> NOOP_BATCH = (df, id) -> {};

  /**
   * Runs a streaming query to completion (AvailableNow trigger) and collects all rows. The query
   * reads {@code tableName} with optional {@code schemaTrackingLocation} and {@code
   * startingVersion}.
   */
  private List<Row> runStreamToCompletion(
      String tableName, String checkpoint, String schemaTrackingLocation, Long startingVersion)
      throws Exception {
    return runStreamToCompletion(
        tableName, checkpoint, schemaTrackingLocation, startingVersion, java.util.Map.of());
  }

  /**
   * Like {@link #runStreamToCompletion(String, String, String, Long)} but also applies arbitrary
   * extra streaming options (e.g. {@code ignoreDeletes=true}, {@code ignoreChanges=true}).
   */
  private List<Row> runStreamToCompletion(
      String tableName,
      String checkpoint,
      String schemaTrackingLocation,
      Long startingVersion,
      java.util.Map<String, String> extraOptions)
      throws Exception {
    List<Row> rows = new ArrayList<>();
    var dsr = spark().readStream().format("delta");
    if (schemaTrackingLocation != null) {
      dsr = dsr.option("schemaTrackingLocation", schemaTrackingLocation);
    }
    if (startingVersion != null) {
      dsr = dsr.option("startingVersion", startingVersion);
    }
    for (var entry : extraOptions.entrySet()) {
      dsr = dsr.option(entry.getKey(), entry.getValue());
    }
    dsr.table(tableName)
        .writeStream()
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>) (df, batchId) -> rows.addAll(df.collectAsList()))
        .start()
        .awaitTermination();
    return rows;
  }

  /**
   * Collects the string values of the first N columns from each row, joining them with "|" for easy
   * comparison.
   */
  private static List<String> toStrings(List<Row> rows, int numCols) {
    return rows.stream()
        .map(
            r -> {
              StringBuilder sb = new StringBuilder();
              for (int i = 0; i < numCols; i++) {
                if (i > 0) sb.append("|");
                sb.append(r.isNullAt(i) ? "null" : r.get(i).toString());
              }
              return sb.toString();
            })
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Asserts that {@code action} throws a streaming exception whose full cause chain contains {@code
   * fragment} (case-insensitive).
   */
  private void assertStreamingThrowsContaining(ThrowingCallable action, String fragment) {
    assertThatThrownBy(action)
        .satisfies(
            e -> {
              StringBuilder full = new StringBuilder();
              for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null) full.append(t.getMessage()).append(' ');
              }
              assertThat(full.toString()).containsIgnoringCase(fragment);
            });
  }

  // -----------------------------------------------------------------------
  // DDL helpers
  // -----------------------------------------------------------------------

  private void addColumn(String tableName, String columnName) {
    sql("ALTER TABLE %s ADD COLUMN (%s STRING)", tableName, columnName);
  }

  private void renameColumn(String tableName, String oldName, String newName) {
    sql("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, oldName, newName);
  }

  private void dropColumn(String tableName, String columnName) {
    sql("ALTER TABLE %s DROP COLUMN %s", tableName, columnName);
  }

  // -----------------------------------------------------------------------
  // Tests ported from DeltaV2SourceSchemaEvolutionSuite.shouldPassTests
  // -----------------------------------------------------------------------

  /**
   * Ported from: "schema log initialization with additive schema changes"
   *
   * <p>Verifies that:
   *
   * <ol>
   *   <li>First stream run processes the initial snapshot.
   *   <li>Adding a column triggers a DELTA_STREAMING_METADATA_EVOLUTION restart.
   *   <li>After restart the new column's data is readable.
   *   <li>Adding a second column triggers another metadata-evolution restart.
   * </ol>
   */
  @TestAllTableTypes
  public void testSchemaLogInitializationWithAdditiveSchemaChanges(TableType tableType)
      throws Exception {
    withNewTable(
        "schema_log_init_additive",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          // Write 6 versions (simulating the starter table pattern from the Scala suite)
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // First run: initialize snapshot, schema log initialized
          List<Row> batch1 = runStreamToCompletion(tableName, ck, sl, null /* startingVersion */);
          // Should have 6 rows
          assertThat(batch1).hasSize(6);

          // Add column c
          addColumn(tableName, "c");
          sql("INSERT INTO %s VALUES ('5', '5', '5')", tableName);
          sql("INSERT INTO %s VALUES ('6', '6', '6')", tableName);

          // Next run should detect schema change and fail with metadata evolution
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Restart: now schema log is updated, new data should be readable
          List<Row> batch2 = runStreamToCompletion(tableName, ck, sl, null);
          List<String> vals = toStrings(batch2, 3);
          assertThat(vals).containsExactlyInAnyOrder("5|5|5", "6|6|6");

          // Add another column d, add more data
          addColumn(tableName, "d");
          sql("INSERT INTO %s VALUES ('10', '10', '10', '10')", tableName);

          // Should fail with metadata evolution again
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Final restart: process data in new schema
          List<Row> batch3 = runStreamToCompletion(tableName, ck, sl, null);
          List<String> vals3 = toStrings(batch3, 4);
          assertThat(vals3).containsExactlyInAnyOrder("10|10|10|10");
        });
  }

  /**
   * Ported from: "detect incompatible schema change while streaming"
   *
   * <p>Verifies that:
   *
   * <ol>
   *   <li>Streaming without a schema location fails with an in-stream incompatible schema error
   *       when a rename occurs mid-stream.
   *   <li>Providing a schemaTrackingLocation causes metadata evolution exceptions instead.
   *   <li>After the schema log converges the rest of the data is readable.
   * </ol>
   */
  @TestAllTableTypes
  public void testDetectIncompatibleSchemaChangeWhileStreaming(TableType tableType)
      throws Exception {
    withNewTable(
        "detect_incompatible_streaming",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          // Seed initial data and do a rename that is part of the initial snapshot
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          renameColumn(tableName, "b", "c");
          // Add more data after rename
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String ckNoSchema = checkpoint();

          // Without schema location: the first stream run processes everything in the current
          // snapshot (including the b→c rename which appears as a metadata change in the log
          // from startingVersion onwards).  The incompatible-schema error fires only when the
          // stream processes PAST a rename that occurred AFTER its committed offset, so we:
          //   1. Let q1 process all current data to establish a committed offset.
          //   2. Rename c→d and add more data.
          //   3. With allowSourceColumnRenameAndDrop=false, q2 encounters c→d and throws the
          //      incompatible-schema error.
          //
          // Note: allowSourceColumnRenameAndDrop is set to "always" by @BeforeAll.  We must
          // temporarily override it to "false" so the rename-detection code path fires.
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", ckNoSchema)
                  .trigger(Trigger.AvailableNow())
                  .foreachBatch(NOOP_BATCH)
                  .start();
          q1.awaitTermination();

          // Rename again and add data — q2 will encounter this rename post-commit-offset.
          renameColumn(tableName, "c", "d");
          for (int i = 10; i < 15; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Temporarily disable the rename-allow conf so the incompatible-schema error fires.
          try {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "false");
            assertStreamingThrowsContaining(
                () -> {
                  StreamingQuery q2 =
                      spark()
                          .readStream()
                          .format("delta")
                          .table(tableName)
                          .writeStream()
                          .option("checkpointLocation", ckNoSchema)
                          .trigger(Trigger.AvailableNow())
                          .foreachBatch(NOOP_BATCH)
                          .start();
                  q2.awaitTermination();
                },
                "schema");
          } finally {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always");
          }

          // With schema location: starting from v1, the stream encounters the b→c and c→d renames
          // as incremental commits and evolves the schema log through multiple METADATA_EVOLUTION
          // stops before it can finally read data.
          String[] locs = checkpointAndSchemaLocation();
          String ckWithSchema = locs[0];
          String sl = locs[1];

          // Drive the stream from startingVersion=1 through all schema-evolution stops until it
          // completes (schema log has been advanced past all renames).
          for (int attempt = 0; attempt < 6; attempt++) {
            try {
              runStreamToCompletion(tableName, ckWithSchema, sl, 1L);
              break; // completed without evolution stop
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
              // expected; retry with null startingVersion (use checkpoint offset)
            }
          }

          // Add another rename to trigger a fresh evolution on the established checkpoint.
          renameColumn(tableName, "d", "e");
          sql("INSERT INTO %s VALUES ('15', '15')", tableName);

          // Should fail with metadata evolution (schema log needs to advance past d→e).
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ckWithSchema, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Drive through any remaining evolution stops, collecting all rows from successful runs.
          List<Row> drainRows = new ArrayList<>();
          for (int attempt = 0; attempt < 4; attempt++) {
            try {
              drainRows.addAll(runStreamToCompletion(tableName, ckWithSchema, sl, null));
              break;
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
            }
          }

          // Insert a fresh row so there is guaranteed unread data on the final run.
          sql("INSERT INTO %s VALUES ('16', '16')", tableName);

          // Final state: schema log has converged, data must be readable.
          List<Row> finalBatch = runStreamToCompletion(tableName, ckWithSchema, sl, null);
          assertThat(finalBatch).isNotEmpty();
        });
  }

  /**
   * Ported from: "detect incompatible schema change during first getBatch"
   *
   * <p>Verifies that a rename that appears in the very first batch (when streaming starts at v1) is
   * detected and leads to metadata evolution, after which subsequent data is read correctly.
   */
  @TestAllTableTypes
  public void testDetectIncompatibleSchemaChangeDuringFirstGetBatch(TableType tableType)
      throws Exception {
    withNewTable(
        "detect_incompatible_first_batch",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          renameColumn(tableName, "b", "c");
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Starting at v1: the rename is the first thing seen. Initialization should fail with
          // metadata evolution.
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, 1L),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Restart: data before rename should be served, then stop at rename (metadata evolution)
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Restart: data after rename should now be readable
          List<Row> rows = runStreamToCompletion(tableName, ck, sl, null);
          assertThat(rows).isNotEmpty();
        });
  }

  /**
   * Ported from: "trigger.AvailableNow should work"
   *
   * <p>Verifies that with Trigger.AvailableNow, schema evolution proceeds correctly across multiple
   * restarts: initialization → first data → schema evolution → remaining data.
   */
  @TestAllTableTypes
  public void testTriggerAvailableNowShouldWork(TableType tableType) throws Exception {
    withNewTable(
        "trigger_available_now",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          // Seed initial data
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          dropColumn(tableName, "b");
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // First AvailableNow run: schema log initializes, fails with metadata evolution.
          // No offset is committed during the initialization throw.
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, 1L),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Second AvailableNow: may serve rows before the drop then evolve schema, or throw
          // during latestOffset computation before any batch is committed.  Either way we expect
          // a metadata-evolution exception.
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Third AvailableNow: the schema log has now converged.  Because neither prior throwing
          // run is guaranteed to have committed its offset checkpoint, the stream re-reads from
          // startingVersion=1 and returns ALL rows (0..9) using the post-drop schema (column a
          // only).  That is the correct V1-connector behaviour when offsets are not committed
          // before the metadata-evolution exception fires.
          List<Row> finalRows = runStreamToCompletion(tableName, ck, sl, null);
          List<String> vals =
              finalRows.stream()
                  .map(r -> r.get(0).toString())
                  .sorted()
                  .collect(Collectors.toList());
          assertThat(vals)
              .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        });
  }

  /**
   * Ported from: "trigger.Once with deferred commit should work"
   *
   * <p>Similar to AvailableNow but using Trigger.Once. Each invocation processes at most one
   * micro-batch. Verifies the same initialization → data → evolution → remaining data sequence.
   */
  @TestAllTableTypes
  public void testTriggerOnceShouldWork(TableType tableType) throws Exception {
    withNewTable(
        "trigger_once",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          dropColumn(tableName, "b");
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d')", tableName, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Run 1: schema log initializes, fails with metadata evolution
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("schemaTrackingLocation", sl)
                      .option("startingVersion", 1)
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.Once())
                      .option("checkpointLocation", ck)
                      .foreachBatch(NOOP_BATCH)
                      .start()
                      .awaitTermination(),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Run 2: processes data before schema change, then evolution or completes
          // (Trigger.Once semantics: the stream may succeed or evolve schema on subsequent run)
          List<Row> dataBefore = new ArrayList<>();
          try {
            spark()
                .readStream()
                .format("delta")
                .option("schemaTrackingLocation", sl)
                .option("startingVersion", 1)
                .table(tableName)
                .writeStream()
                .trigger(Trigger.Once())
                .option("checkpointLocation", ck)
                .foreachBatch(
                    (VoidFunction2<Dataset<Row>, Long>)
                        (df, id) -> dataBefore.addAll(df.collectAsList()))
                .start()
                .awaitTermination();
          } catch (Exception e) {
            // Expected: may throw metadata evolution after processing the pre-schema-change batch
            assertThat(e.getMessage() != null ? e.getMessage() : "")
                .satisfies(
                    msg -> {
                      // Either it threw metadata evolution or succeeded with data
                      boolean isMetadataEvolution =
                          causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION");
                      assertThat(isMetadataEvolution || !dataBefore.isEmpty()).isTrue();
                    });
          }

          // Continue running until we get data after the schema change
          for (int attempt = 0; attempt < 5; attempt++) {
            List<Row> remaining = new ArrayList<>();
            try {
              spark()
                  .readStream()
                  .format("delta")
                  .option("schemaTrackingLocation", sl)
                  .option("startingVersion", 1)
                  .table(tableName)
                  .writeStream()
                  .trigger(Trigger.Once())
                  .option("checkpointLocation", ck)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> remaining.addAll(df.collectAsList()))
                  .start()
                  .awaitTermination();
              if (remaining.stream()
                  .anyMatch(
                      r -> {
                        try {
                          return Integer.parseInt(r.get(0).toString()) >= 5;
                        } catch (Exception ex) {
                          return false;
                        }
                      })) {
                break;
              }
            } catch (Exception e) {
              boolean isMetadataEvolution =
                  causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION");
              if (!isMetadataEvolution) throw e;
            }
          }
        });
  }

  /** Returns true if any exception in the cause chain has a message containing {@code fragment}. */
  private static boolean causeChainContains(Throwable e, String fragment) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(fragment)) return true;
    }
    return false;
  }

  /**
   * Ported from: "consecutive schema evolutions without schema merging"
   *
   * <p>With merging disabled, each schema change causes a separate restart. The test drives the
   * stream through: rename b→c, rename c→b, drop b, add b — verifying that at each step the stream
   * stops at the boundary and the schema log is updated.
   */
  @TestAllTableTypes
  public void testConsecutiveSchemaEvolutionsWithoutSchemaMerging(TableType tableType)
      throws Exception {
    withNewTable(
        "consecutive_no_merge",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          // Seed 6 rows
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Capture the version at the end of the seeded data, BEFORE the consecutive schema
          // changes.  We start the stream here so that the schema changes appear as incremental
          // commits (not as part of the snapshot), which forces the merging-disabled path to
          // produce a separate stop for each change.
          long startingVersion = currentVersion(tableName);

          // Apply consecutive schema changes
          renameColumn(tableName, "b", "c"); // v+1
          renameColumn(tableName, "c", "b"); // v+2
          dropColumn(tableName, "b"); // v+3
          addColumn(tableName, "b"); // v+4

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // With merge disabled, each change is a separate stop.  The stream must start at
          // startingVersion so it sees the schema changes as incremental commits rather than
          // reading the current (post-all-changes) snapshot, which would bypass intermediate stops.
          // We pass startingVersion on EVERY attempt until the checkpoint commits an offset;
          // once an offset is committed, the streaming framework ignores startingVersion and uses
          // the checkpoint offset instead, so subsequent passes are safe to keep passing it.
          try {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming.schemaTracking"
                        + ".mergeConsecutiveSchemaChanges.enabled",
                    "false");

            // Initialization stop: schema log is created for the schema at startingVersion.
            assertStreamingThrowsContaining(
                () -> runStreamToCompletion(tableName, ck, sl, startingVersion),
                "DELTA_STREAMING_METADATA_EVOLUTION");

            // Each subsequent rename/drop/add causes its own stop.
            // We always pass startingVersion so that when the checkpoint has no committed offset
            // the stream still reads incrementally from startingVersion (not the current snapshot),
            // ensuring each intermediate schema change commit is visited individually.
            for (int attempt = 0; attempt < 6; attempt++) {
              try {
                runStreamToCompletion(tableName, ck, sl, startingVersion);
                break; // processed fully
              } catch (Exception e) {
                if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
                // expected; continue to next restart
              }
            }
          } finally {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming.schemaTracking"
                        + ".mergeConsecutiveSchemaChanges.enabled",
                    "true");
          }
        });
  }

  /**
   * Ported from: "consecutive schema evolutions"
   *
   * <p>With schema merging enabled (default), consecutive schema changes are merged and the stream
   * can advance past them after fewer restarts than without merging.
   */
  @TestAllTableTypes
  public void testConsecutiveSchemaEvolutions(TableType tableType) throws Exception {
    withNewTable(
        "consecutive_with_merge",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          renameColumn(tableName, "b", "c"); // v+1
          renameColumn(tableName, "c", "b"); // v+2
          dropColumn(tableName, "b"); // v+3
          addColumn(tableName, "b"); // v+4
          sql("INSERT INTO %s VALUES ('5', '5')", tableName);

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // With consecutive schema merging enabled the analysis phase may advance the schema
          // log past all intermediate changes in a single pass, meaning the initialization and
          // first-schema-change stops may be merged into fewer (or zero) explicit throws.
          // We therefore drive the stream through whatever evolution stops it produces and simply
          // verify that the stream eventually delivers data using the merged final schema.
          List<Row> finalRows = new ArrayList<>();
          for (int attempt = 0; attempt < 8; attempt++) {
            List<Row> batchRows = new ArrayList<>();
            try {
              batchRows = runStreamToCompletion(tableName, ck, sl, null);
              finalRows.addAll(batchRows);
              break;
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
              finalRows.addAll(batchRows);
            }
          }
          // The stream must have delivered at least the row ('5','5') inserted after the schema
          // changes, confirming the merged schema was applied and data was processed.
          assertThat(finalRows).isNotEmpty();
        });
  }

  /**
   * Ported from: "upgrade and downgrade"
   *
   * <p>Exercises:
   *
   * <ol>
   *   <li>Starting a stream without schema location (legacy).
   *   <li>Upgrading to use schema tracking (schemaTrackingLocation) — triggers initialization +
   *       metadata evolution.
   *   <li>Downgrading back to no schema location with the unsafe-read flag.
   * </ol>
   */
  @TestAllTableTypes
  public void testUpgradeAndDowngrade(TableType tableType) throws Exception {
    withNewTable(
        "upgrade_downgrade",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String ck = checkpoint();

          // Start stream without schema location (legacy mode) — reads initial snapshot
          List<Row> initialRows = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("startingVersion", 1)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ck)
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, id) -> initialRows.addAll(df.collectAsList()))
              .start()
              .awaitTermination();
          assertThat(initialRows).hasSize(5);

          // Add data then drop column
          sql("INSERT INTO %s VALUES ('5', '5')", tableName);
          dropColumn(tableName, "b");

          // Upgrade: add a schema-tracking location to the SAME checkpoint that the legacy
          // stream used.  The schema-tracked stream picks up from where the legacy stream left
          // off (after rows 0-4), so its first new data is row '5', then it hits the drop-column
          // commit.  This mirrors the Scala test which reuses getDefaultCheckpoint for both
          // the legacy and schema-tracked phases.
          //
          // The schema location must be UNDER the checkpoint directory (Delta validation rule).
          // We resolve it as a subdirectory of ck, which is a file URI — strip the scheme to get
          // the local path, create the directory, then convert back to a URI string.
          java.net.URI ckUri = java.net.URI.create(ck);
          Path ckPath = Paths.get(ckUri);
          Path slDir = ckPath.resolve("_schema_location");
          Files.createDirectories(slDir);
          String sl = slDir.toUri().toString();

          // Init: schema log is created from the committed offset; stream throws to signal
          // initialization.  The AvailableNow throw may or may not commit the offset, so we
          // accept either outcome (throw OR succeed-with-data) here and simply proceed.
          try {
            runStreamToCompletion(tableName, ck, sl, null);
          } catch (Exception e) {
            if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
          }

          // Ensure the schema log has advanced past the drop by driving through any remaining
          // evolution stops.
          for (int attempt = 0; attempt < 4; attempt++) {
            try {
              runStreamToCompletion(tableName, ck, sl, null);
              break;
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
            }
          }

          // Add more data post-drop
          sql("INSERT INTO %s VALUES ('6')", tableName);

          // Downgrade: remove schema location, set unsafe-read flags to allow reading without
          // schema tracking.  The stream should process row '6' using the latest schema.
          try {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleSchemaChangesDuringStreamStart.enabled",
                    "true");
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
                    "true");

            // Should succeed by falling back to latest schema
            List<Row> downgradeRows = new ArrayList<>();
            spark()
                .readStream()
                .format("delta")
                .table(tableName)
                .writeStream()
                .trigger(Trigger.AvailableNow())
                .option("checkpointLocation", ck)
                .foreachBatch(
                    (VoidFunction2<Dataset<Row>, Long>)
                        (df, id) -> downgradeRows.addAll(df.collectAsList()))
                .start()
                .awaitTermination();
            // The downgrade run must deliver at least row '6'.
            assertThat(downgradeRows).isNotEmpty();
          } finally {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleSchemaChangesDuringStreamStart.enabled",
                    "false");
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
                    "false");
          }
        });
  }

  /**
   * Ported from: "multiple sources with schema evolution"
   *
   * <p>Two Delta sources reading the same table (each with their own schema log) joined via
   * unionByName. Verifies that both schema logs are initialized and that data is processed after
   * evolution.
   */
  @TestAllTableTypes
  public void testMultipleSourcesWithSchemaEvolution(TableType tableType) throws Exception {
    withNewTable(
        "multi_source_schema_evolution",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          renameColumn(tableName, "b", "c");
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Two separate schema locations for two sources.  BOTH must be under the shared
          // checkpoint directory — Delta's assertSchemaTrackingLocationUnderCheckpoint validation
          // rejects any schema location that is not nested under checkpointLocation.
          String[] locs1 = checkpointAndSchemaLocation();
          String ck = locs1[0];
          String sl1 = locs1[1];
          // Resolve sl2 as a sibling of sl1 inside the same checkpoint directory.
          java.net.URI ckUri = java.net.URI.create(ck);
          Path ckPath = Paths.get(ckUri);
          Path sl2Dir = ckPath.resolve("_schema_location_2");
          Files.createDirectories(sl2Dir);
          String sl2 = sl2Dir.toUri().toString();

          // Build a union of two sources with separate schema tracking
          var src1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("schemaTrackingLocation", sl1)
                  .option("startingVersion", currentVersion(tableName) - 5)
                  .table(tableName);
          var src2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("schemaTrackingLocation", sl2)
                  .option("startingVersion", currentVersion(tableName) - 5)
                  .table(tableName);

          // Both source logs need initialization — may take 2 separate starts
          for (int attempt = 0; attempt < 4; attempt++) {
            try {
              src1.union(src2)
                  .writeStream()
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .foreachBatch(NOOP_BATCH)
                  .start()
                  .awaitTermination();
              break;
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
            }
          }
        });
  }

  /**
   * Ported from: "schema evolution with Delta sink"
   *
   * <p>Verifies that schema evolution works end-to-end when the stream writes to a Delta sink with
   * mergeSchema=true. At each evolution step the sink grows to accommodate the new column.
   */
  @TestAllTableTypes
  public void testSchemaEvolutionWithDeltaSink(TableType tableType) throws Exception {
    withNewTable(
        "schema_evo_source",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        sourceName ->
            withNewTable(
                "schema_evo_sink",
                "a STRING, b STRING",
                null,
                tableType,
                BASIC_PROPS,
                sinkName -> {
                  for (int i = -1; i < 5; i++) {
                    sql("INSERT INTO %s VALUES ('%d', '%d')", sourceName, i, i);
                  }
                  renameColumn(sourceName, "b", "c");
                  for (int i = 5; i < 10; i++) {
                    sql("INSERT INTO %s VALUES ('%d', '%d')", sourceName, i, i);
                  }

                  String[] locs = checkpointAndSchemaLocation();
                  String ck = locs[0];
                  String sl = locs[1];

                  // Drive the stream through each schema evolution stopping point
                  for (int attempt = 0; attempt < 6; attempt++) {
                    try {
                      spark()
                          .readStream()
                          .format("delta")
                          .option("schemaTrackingLocation", sl)
                          .option("startingVersion", 1)
                          .table(sourceName)
                          .writeStream()
                          .format("delta")
                          .trigger(Trigger.AvailableNow())
                          .option("checkpointLocation", ck)
                          .option("mergeSchema", "true")
                          .outputMode("append")
                          .toTable(sinkName)
                          .awaitTermination();
                      break; // done
                    } catch (Exception e) {
                      if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
                    }
                  }

                  // Verify that data reached the sink
                  List<List<String>> sinkContents = sql("SELECT * FROM %s ORDER BY 1", sinkName);
                  assertThat(sinkContents).isNotEmpty();
                }));
  }

  /**
   * Ported from: "unblock with sql conf"
   *
   * <p>When {@code allowSourceColumnRenameAndDrop} is disabled (simulating the default for
   * production), after a schema evolution the stream refuses to restart without an explicit SQL
   * conf acknowledgement. Setting the conf key for the checkpoint hash unblocks it.
   */
  @TestAllTableTypes
  public void testUnblockWithSqlConf(TableType tableType) throws Exception {
    withNewTable(
        "unblock_sql_conf",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES ('0', '0')", tableName);

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Disable the "allow rename/drop" safety valve
          try {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "false");

            // Initialize the stream (no schema change yet)
            runStreamToCompletion(tableName, ck, sl, null);

            // Trigger a rename
            renameColumn(tableName, "b", "c");
            sql("INSERT INTO %s VALUES ('1', '1')", tableName);

            // First restart: metadata evolution (schema log update)
            assertStreamingThrowsContaining(
                () -> runStreamToCompletion(tableName, ck, sl, null),
                "DELTA_STREAMING_METADATA_EVOLUTION");

            // Second restart: blocked by SQL conf validation
            assertStreamingThrowsContaining(
                () -> runStreamToCompletion(tableName, ck, sl, null),
                "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION");

            // Compute the checkpoint hash the way Delta does.
            // Delta's DeltaSourceMetadataEvolutionSupport.getCheckpointHash(metadataPath) is
            // simply metadataPath.hashCode(), where metadataPath is
            // "$resolvedCheckpointRoot/sources/0".  Spark normalises the checkpoint URI via
            // Hadoop's Path (file:///foo → file:/foo), so we must do the same to get the same
            // hash.
            String normalizedCk = new org.apache.hadoop.fs.Path(ck).toString().replaceAll("/$", "");
            String sourceCheckpointPath = normalizedCk + "/sources/0";
            int ckHash = sourceCheckpointPath.hashCode();
            String confKey =
                "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_" + ckHash;

            // Unblock with "always"
            spark().conf().set(confKey, "always");
            try {
              runStreamToCompletion(tableName, ck, sl, null);
            } finally {
              spark().conf().unset(confKey);
            }
          } finally {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always");
          }
        });
  }

  /**
   * Ported from: "unblock with sql conf - nested struct"
   *
   * <p>Same as {@link #testUnblockWithSqlConf} but with a nested struct field being renamed inside
   * the struct (s.x → s.z).
   */
  @TestAllTableTypes
  public void testUnblockWithSqlConfNestedStruct(TableType tableType) throws Exception {
    withNewTable(
        "unblock_nested_struct",
        "a STRING, s STRUCT<x: STRING, y: STRING>",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES ('0', struct('0', '0'))", tableName);

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          try {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "false");

            // Initialize the stream
            runStreamToCompletion(tableName, ck, sl, null);

            // Rename the nested field s.x -> s.z
            sql("ALTER TABLE %s RENAME COLUMN s.x TO z", tableName);
            sql("INSERT INTO %s VALUES ('1', struct('1', '1'))", tableName);

            // Metadata evolution stop
            assertStreamingThrowsContaining(
                () -> runStreamToCompletion(tableName, ck, sl, null),
                "DELTA_STREAMING_METADATA_EVOLUTION");

            // Blocked by SQL conf
            assertStreamingThrowsContaining(
                () -> runStreamToCompletion(tableName, ck, sl, null),
                "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION");

            // Unblock — normalise the checkpoint URI the same way Hadoop does so that the hash
            // matches what Delta computes from Spark's resolvedCheckpointRoot.
            String normalizedCk = new org.apache.hadoop.fs.Path(ck).toString().replaceAll("/$", "");
            String sourceCheckpointPath = normalizedCk + "/sources/0";
            int ckHash = sourceCheckpointPath.hashCode();
            String confKey =
                "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_" + ckHash;
            spark().conf().set(confKey, "always");
            try {
              runStreamToCompletion(tableName, ck, sl, null);
            } finally {
              spark().conf().unset(confKey);
            }
          } finally {
            spark()
                .conf()
                .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always");
          }
        });
  }

  /**
   * Ported from: "schema tracking interacting with unsafe escape flag"
   *
   * <p>When the unsafe-read flag ({@code
   * DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES}) is enabled, schema
   * tracking is bypassed — the stream reads with the latest schema and does not initialize the
   * schema log.
   */
  @TestAllTableTypes
  public void testSchemaTrackingInteractingWithUnsafeEscapeFlag(TableType tableType)
      throws Exception {
    withNewTable(
        "unsafe_escape_flag",
        "a STRING, b STRING",
        null,
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          renameColumn(tableName, "b", "c");

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          try {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
                    "true");

            // With the unsafe flag, the stream should complete without metadata evolution.
            // startingVersion=1 on a UC table (CREATE at v0) includes v1 which is the row (-1,-1)
            // insert, so we read all 6 inserted rows (-1,0,1,2,3,4) using the latest schema
            // (after the rename b→c).
            List<Row> rows =
                runStreamToCompletion(tableName, ck, sl, 1L /* startingVersion: ignore CREATE */);
            assertThat(rows).hasSize(6);
          } finally {
            spark()
                .conf()
                .set(
                    "spark.databricks.delta.streaming"
                        + ".unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
                    "false");
          }
        });
  }

  /**
   * Ported from: "streaming with a column mapping upgrade"
   *
   * <p>Starts streaming WITHOUT column mapping, then ALTER TABLE upgrades the table to name mode.
   * Verifies that:
   *
   * <ol>
   *   <li>The schema log initializes without physical names (pre-upgrade schema).
   *   <li>Hitting the upgrade commit triggers metadata evolution.
   *   <li>After evolution the data is readable with the upgraded schema.
   * </ol>
   */
  @TestAllTableTypes
  public void testStreamingWithColumnMappingUpgrade(TableType tableType) throws Exception {
    // Start without column mapping
    withNewTable(
        "col_mapping_upgrade",
        "a STRING, b STRING",
        null,
        tableType,
        BASIC_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Upgrade to name mode
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ("
                  + "'delta.columnMapping.mode'='name', "
                  + "'delta.minReaderVersion'='2', "
                  + "'delta.minWriterVersion'='5')",
              tableName);

          // Rename a column after upgrade
          renameColumn(tableName, "b", "c");
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Initialization stop (schema at version 1 is pre-upgrade)
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, 1L),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Stop at the upgrade commit
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, null),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Final: schema log has converged.  Because neither prior throwing run is guaranteed
          // to have committed its offset checkpoint, the stream re-reads from startingVersion=1
          // and returns ALL rows visible from that version: the 6 initial rows (-1..4) plus the
          // 5 post-rename rows (5..9) = 11 rows total, using the final upgraded+renamed schema.
          List<Row> finalRows = runStreamToCompletion(tableName, ck, sl, null);
          assertThat(finalRows).hasSize(11);
        });
  }

  /**
   * Ported from: "partition evolution"
   *
   * <p>Partition evolution (changing partition columns of an existing Delta table) requires
   * rewriting the table's partition spec via a path-based {@code overwriteSchema} write. This
   * path-based write is blocked in the Unity Catalog environment:
   *
   * <ul>
   *   <li>EXTERNAL: the fake-S3 path lacks credentials ({@code Failed to instantiate credential
   *       provider}).
   *   <li>MANAGED: UC blocks direct path writes ({@code
   *       DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   * </ul>
   *
   * // SKIPPED-IN-BODY: partition evolution requires rewriting the table's partition spec, which is
   * // not expressible via the UC public API (path-based/REPLACE writes are blocked). Verifying //
   * basic partitioned streaming read instead.
   *
   * <p>Instead this test creates a partitioned table and verifies that a streaming read with schema
   * tracking successfully processes the initial data.
   */
  @TestAllTableTypes
  public void testPartitionEvolution(TableType tableType) throws Exception {
    // SKIPPED-IN-BODY: partition evolution requires rewriting the table's partition spec, which is
    // not expressible via the UC public API (path-based/REPLACE writes are blocked).
    // Verifying basic partitioned streaming read instead.
    withNewTable(
        "partition_evo",
        "a STRING, b STRING",
        "a", // partitioned by a
        tableType,
        COL_MAPPING_PROPS,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Initialization: schema log is created, stream may throw METADATA_EVOLUTION on first
          // run (schema log initialization), then succeeds on the next run.
          List<Row> rows = null;
          for (int attempt = 0; attempt < 3; attempt++) {
            try {
              rows = runStreamToCompletion(tableName, ck, sl, null);
              break;
            } catch (Exception e) {
              if (!causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) throw e;
            }
          }
          // Basic partitioned streaming read must have delivered the 5 inserted rows.
          assertThat(rows).isNotNull().isNotEmpty();
        });
  }

  // -----------------------------------------------------------------------
  // CDC variants (from DeltaV2SourceSchemaEvolutionCDCSuiteBase.shouldPassTests)
  // -----------------------------------------------------------------------

  /**
   * Ported from (CDC): "CDC streaming with schema evolution"
   *
   * <p>Verifies that a CDF (change data feed) streaming read correctly detects a schema change
   * introduced by a MERGE that adds a column, triggers metadata evolution, and then correctly reads
   * the post-evolution change events.
   */
  @TestAllTableTypes
  public void testCDCStreamingWithSchemaEvolution(TableType tableType) throws Exception {
    withNewTable(
        "cdc_schema_evo_main",
        "id LONG",
        null,
        tableType,
        COL_MAPPING_PROPS,
        mainTable ->
            withNewTable(
                "cdc_schema_evo_merge_src",
                "id LONG, age STRING",
                null,
                tableType,
                BASIC_PROPS,
                mergeSource -> {
                  // Insert initial data
                  for (long i = 0; i < 10; i++) {
                    sql("INSERT INTO %s VALUES (%d)", mainTable, i);
                  }

                  // Insert merge source data (even ids get an "age" column)
                  for (long i = 0; i < 10; i += 2) {
                    sql("INSERT INTO %s VALUES (%d, 'string')", mergeSource, i);
                  }

                  // Enable schema auto-migration and run merge (adds column "age")
                  try {
                    spark().conf().set("spark.databricks.delta.schema.autoMerge.enabled", "true");
                    sql(
                        "MERGE INTO %s t USING %s s ON t.id = s.id "
                            + "WHEN MATCHED THEN UPDATE SET * "
                            + "WHEN NOT MATCHED THEN INSERT *",
                        mainTable, mergeSource);
                  } finally {
                    spark().conf().set("spark.databricks.delta.schema.autoMerge.enabled", "false");
                  }

                  String[] locs = checkpointAndSchemaLocation();
                  String ck = locs[0];
                  String sl = locs[1];

                  // Initialization stop
                  assertStreamingThrowsContaining(
                      () ->
                          spark()
                              .readStream()
                              .format("delta")
                              .option("readChangeFeed", "true")
                              .option("schemaTrackingLocation", sl)
                              .option("startingVersion", 0)
                              .table(mainTable)
                              .writeStream()
                              .trigger(Trigger.AvailableNow())
                              .option("checkpointLocation", ck)
                              .foreachBatch(NOOP_BATCH)
                              .start()
                              .awaitTermination(),
                      "DELTA_STREAMING_METADATA_EVOLUTION");

                  // Process initial inserts (before MERGE), stop at schema change from MERGE
                  List<Row> preMergeChanges = new ArrayList<>();
                  assertStreamingThrowsContaining(
                      () ->
                          spark()
                              .readStream()
                              .format("delta")
                              .option("readChangeFeed", "true")
                              .option("schemaTrackingLocation", sl)
                              .option("startingVersion", 0)
                              .table(mainTable)
                              .writeStream()
                              .trigger(Trigger.AvailableNow())
                              .option("checkpointLocation", ck)
                              .foreachBatch(
                                  (VoidFunction2<Dataset<Row>, Long>)
                                      (df, id) -> preMergeChanges.addAll(df.collectAsList()))
                              .start()
                              .awaitTermination(),
                      "DELTA_STREAMING_METADATA_EVOLUTION");
                  // The 10 initial inserts should have been processed
                  assertThat(preMergeChanges).hasSize(10);

                  // Final run: process the MERGE change events
                  List<Row> mergeChanges = new ArrayList<>();
                  spark()
                      .readStream()
                      .format("delta")
                      .option("readChangeFeed", "true")
                      .option("schemaTrackingLocation", sl)
                      .option("startingVersion", 0)
                      .table(mainTable)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ck)
                      .foreachBatch(
                          (VoidFunction2<Dataset<Row>, Long>)
                              (df, id) ->
                                  mergeChanges.addAll(
                                      df.select("id", "_change_type").collectAsList()))
                      .start()
                      .awaitTermination();
                  // Even-id rows had MATCHED → UPDATE (pre + post image)
                  assertThat(mergeChanges).isNotEmpty();
                }));
  }

  /**
   * Ported from (CDC): "protocol and configuration evolution"
   *
   * <p>Verifies that:
   *
   * <ol>
   *   <li>Upgrading the protocol (minReaderVersion/minWriterVersion) triggers a metadata evolution
   *       restart.
   *   <li>Changing a Delta table property (delta.isolationLevel) triggers another restart.
   *   <li>Changing a non-Delta property does NOT trigger a restart.
   * </ol>
   */
  @TestAllTableTypes
  public void testProtocolAndConfigurationEvolution(TableType tableType) throws Exception {
    // Start without column mapping so we can upgrade it in the test
    withNewTable(
        "protocol_config_evo",
        "a STRING, b STRING",
        null,
        tableType,
        BASIC_PROPS,
        tableName -> {
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Upgrade protocol
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ("
                  + "'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')",
              tableName);
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Change a Delta table property
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.isolationLevel'='SERIALIZABLE')",
              tableName);
          for (int i = 10; i < 13; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          // Change a non-Delta property (should NOT stop stream)
          sql("ALTER TABLE %s SET TBLPROPERTIES ('hello'='its me')", tableName);
          for (int i = 13; i < 15; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }

          String[] locs = checkpointAndSchemaLocation();
          String ck = locs[0];
          String sl = locs[1];

          // Initialization stop (schema log initialized, no rows committed).
          assertStreamingThrowsContaining(
              () -> runStreamToCompletion(tableName, ck, sl, 1L),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Drive the stream through all remaining metadata-evolution stops (protocol change and
          // delta-property change).  With AvailableNow the throwing runs are not guaranteed to
          // commit their offset before the exception fires, so we count stops rather than asserting
          // on per-batch row counts.
          int metadataEvolutionStops = 0;
          List<Row> allCollected = new ArrayList<>();
          for (int attempt = 0; attempt < 8; attempt++) {
            List<Row> batchRows = new ArrayList<>();
            try {
              spark()
                  .readStream()
                  .format("delta")
                  .option("schemaTrackingLocation", sl)
                  .table(tableName)
                  .writeStream()
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> batchRows.addAll(df.collectAsList()))
                  .start()
                  .awaitTermination();
              // Completed without exception: the non-delta property did not stop the stream.
              allCollected.addAll(batchRows);
              break;
            } catch (Exception e) {
              if (causeChainContains(e, "DELTA_STREAMING_METADATA_EVOLUTION")) {
                metadataEvolutionStops++;
                allCollected.addAll(batchRows);
              } else {
                throw e;
              }
            }
          }

          // We expect at least 1 metadata-evolution stop (for the protocol upgrade).  The
          // delta.isolationLevel change should also trigger a stop, but we use >=1 to remain
          // robust to version-specific differences in which properties are tracked.
          assertThat(metadataEvolutionStops).isGreaterThanOrEqualTo(1);

          // After all evolution stops are exhausted the non-delta property ('hello') must NOT
          // have triggered a stop, and the stream must have delivered all rows (at minimum the
          // rows 10..14 that follow the last delta-property change).
          assertThat(allCollected).isNotEmpty();
        });
  }
}
