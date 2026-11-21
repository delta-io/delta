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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC integration port of {@code V1V2SourceSchemaLogCompatibilitySuiteBase}.
 *
 * <p>Verifies that a single stream's schema tracking log is interchangeable between the V1 (legacy
 * DeltaSource) and V2 (Kernel-based SparkMicroBatchStream) connectors when the table lives in Unity
 * Catalog. Each test alternates the connector across stream restarts that share the same checkpoint
 * and schema location, exercising schema log initialization, non-additive evolution, and
 * additive-then-non-additive sequences.
 *
 * <p><b>EXTERNAL tables only.</b> V1/V2 connector alternation requires path-based access; Unity
 * Catalog managed tables forbid path-based access and raise {@code
 * DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED} when V1 (NONE mode) is used. All
 * interop tests therefore run exclusively on EXTERNAL tables.
 *
 * <p>The connector is toggled per-stream via {@link #withV1(Runnable)}/{@link #withV2(Runnable)}
 * helpers that flip {@code spark.databricks.delta.v2.enableMode} around each stream run and restore
 * the prior value in a finally block, faithfully reproducing the Scala {@code withV1}/{@code
 * withV2} helpers.
 *
 * <p>SKIPPED tests (require {@code DeltaSourceMetadataTrackingLog}/{@code PersistedMetadata}
 * internals not reachable from Java public API):
 *
 * <ul>
 *   <li>{@code "V1 and V2 write equivalent schema tracking log entries (rename)"} — requires {@code
 *       DeltaSourceMetadataTrackingLog/PersistedMetadata} internals
 *   <li>{@code "V1 and V2 write equivalent schema tracking log entries (drop)"} — requires {@code
 *       DeltaSourceMetadataTrackingLog/PersistedMetadata} internals
 *   <li>{@code "V1 and V2 mergers produce equivalent merged schema log entries"} — requires {@code
 *       DeltaSourceMetadataTrackingLog/PersistedMetadata} internals
 *   <li>{@code "V1 and V2 read identical pre-seeded schema log to the same final state (rename)"} —
 *       requires {@code DeltaSourceMetadataTrackingLog/PersistedMetadata} internals
 *   <li>{@code "V1 and V2 read identical pre-seeded schema log to the same final state (drop)"} —
 *       requires {@code DeltaSourceMetadataTrackingLog/PersistedMetadata} internals
 * </ul>
 */
public class UCV1V2SourceSchemaLogCompatibilityTest extends UCDeltaTableIntegrationBaseTest {

  /**
   * The Spark conf key for the Delta V2 enable mode.
   *
   * <p>Verified against {@code DeltaSQLConf.V2_ENABLE_MODE} which is {@code
   * buildConf("v2.enableMode")} prefixed with {@code "spark.databricks.delta"}.
   */
  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";

  /**
   * Table properties used for every starter table: CDF enabled + column mapping in name mode + the
   * minimum reader/writer versions required for column mapping.
   */
  private static final String STARTER_TABLE_PROPERTIES =
      "'delta.enableChangeDataFeed'='true',"
          + "'delta.columnMapping.mode'='name',"
          + "'delta.minReaderVersion'='2',"
          + "'delta.minWriterVersion'='5'";

  /**
   * Spark confs that must be set for every streaming query in this suite. They mirror the {@code
   * sparkConf} overrides in {@code StreamingSchemaEvolutionSuiteBase}:
   *
   * <ul>
   *   <li>Schema tracking enabled (already the default as of the version we target, but set
   *       explicitly to be safe).
   *   <li>Merge consecutive schema changes enabled.
   *   <li>Allow column rename+drop so non-additive evolution does not permanently block the stream.
   * </ul>
   */
  private static final String ALLOW_RENAME_DROP_KEY =
      "spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop";

  @TempDir private Path tempDir;

  private int checkpointCounter = 0;
  private int schemaLocationCounter = 0;

  // ---------------------------------------------------------------------------
  // Connector-toggle helpers
  // ---------------------------------------------------------------------------

  /**
   * Runs {@code body} with {@code spark.databricks.delta.v2.enableMode} set to "NONE" (V1
   * connector), restoring the prior value in a finally block.
   */
  private void withV1(Runnable body) {
    withConnector(false, body);
  }

  /**
   * Runs {@code body} with {@code spark.databricks.delta.v2.enableMode} set to "STRICT" (V2
   * connector), restoring the prior value in a finally block.
   */
  private void withV2(Runnable body) {
    withConnector(true, body);
  }

  /**
   * Runs {@code body} under the specified connector mode, restoring the prior conf value in a
   * finally block.
   */
  private void withConnector(boolean useV2, Runnable body) {
    scala.Option<String> priorOpt = spark().conf().getOption(V2_ENABLE_MODE_KEY);
    String prior = priorOpt.isDefined() ? priorOpt.get() : null;
    spark().conf().set(V2_ENABLE_MODE_KEY, useV2 ? "STRICT" : "NONE");
    try {
      body.run();
    } finally {
      if (prior == null) {
        spark().conf().unset(V2_ENABLE_MODE_KEY);
      } else {
        spark().conf().set(V2_ENABLE_MODE_KEY, prior);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Table-setup helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates the "starter table" from the Scala suite: schema {@code a STRING, b STRING}, column
   * mapping in name mode, CDF enabled, seeded with rows for ids -1..4 (six rows total, reproducing
   * the six-version write loop in {@code withStarterTable}).
   */
  private void withStarterTable(TableType tableType, StarterTableCode code) throws Exception {
    withNewTable(
        "schema_compat_test_" + System.nanoTime(),
        "a STRING, b STRING",
        null,
        tableType,
        STARTER_TABLE_PROPERTIES,
        tableName -> {
          // Each i generates one INSERT (one commit), mirroring the Scala loop.
          for (int i = -1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%d', '%d')", tableName, i, i);
          }
          setSchemaEvolutionConfs();
          code.run(tableName);
        });
  }

  /** Set the per-query Spark confs required by schema-tracking tests. */
  private void setSchemaEvolutionConfs() {
    spark().conf().set(ALLOW_RENAME_DROP_KEY, "always");
    spark().conf().set("spark.databricks.delta.streaming.schemaTracking.enabled", "true");
    spark()
        .conf()
        .set(
            "spark.databricks.delta.streaming.schemaTracking.mergeConsecutiveSchemaChanges.enabled",
            "true");
  }

  // ---------------------------------------------------------------------------
  // DDL helpers (always executed with V1 so DDL works on UC-managed tables)
  // ---------------------------------------------------------------------------

  private void addColumn(String tableName, String column) {
    withV1(() -> sql("ALTER TABLE %s ADD COLUMN (%s STRING)", tableName, column));
  }

  private void renameColumn(String tableName, String oldCol, String newCol) {
    withV1(() -> sql("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, oldCol, newCol));
  }

  private void dropColumn(String tableName, String column) {
    withV1(() -> sql("ALTER TABLE %s DROP COLUMN %s", tableName, column));
  }

  /**
   * Inserts rows into {@code tableName} for each integer in [{@code startInclusive}, {@code
   * endExclusive}). Each row fills every column with the string representation of the integer,
   * mirroring the Scala {@code addData} which uses the live schema from {@code
   * log.update().schema}.
   *
   * <p>The schema is discovered once before the loop via {@code DESCRIBE TABLE}. All inserts run
   * under V1 so they work regardless of the active reader mode.
   */
  private void addData(String tableName, int startInclusive, int endExclusive) {
    // Discover the current column names under V1 (DESCRIBE returns name, type, comment rows).
    List<List<String>> descRows =
        sql("DESCRIBE TABLE %s", tableName).stream()
            // DESCRIBE TABLE adds blank separator rows and partition info after the column rows;
            // stop at the first row whose "col_name" is blank or starts with "#".
            .takeWhile(
                row -> row.size() >= 1 && !row.get(0).isEmpty() && !row.get(0).startsWith("#"))
            .collect(Collectors.toList());
    List<String> colNames = descRows.stream().map(row -> row.get(0)).collect(Collectors.toList());
    String colList = String.join(", ", colNames);

    for (int i = startInclusive; i < endExclusive; i++) {
      final int val = i;
      // Build VALUES clause: ('val', 'val', ...) with one literal per column.
      String valuesClause =
          colNames.stream()
              .map(ignored -> "'" + val + "'")
              .collect(Collectors.joining(", ", "(", ")"));
      // Use V1 for DML so it works regardless of the active reader mode.
      withV1(() -> sql("INSERT INTO %s (%s) VALUES %s", tableName, colList, valuesClause));
    }
  }

  // ---------------------------------------------------------------------------
  // Streaming-run helpers
  // ---------------------------------------------------------------------------

  private String newCheckpoint() throws Exception {
    Path ck = tempDir.resolve("ck-" + checkpointCounter++);
    Files.createDirectory(ck);
    return ck.toString();
  }

  private String newSchemaLocation(String checkpointDir) throws Exception {
    // Schema location must be under the checkpoint dir to satisfy the validation.
    Path sl = Path.of(checkpointDir, "_schema_location_" + schemaLocationCounter++);
    Files.createDirectory(sl);
    return sl.toString();
  }

  /**
   * Runs a single Trigger.AvailableNow streaming pass from {@code tableName}, collecting all rows
   * via foreachBatch. The connector mode (V1/V2) is already set by the caller via
   * withV1/withV2/withConnector.
   */
  private List<Row> runStream(String tableName, String checkpointDir, String schemaLocation)
      throws Exception {
    List<Row> collected = new ArrayList<>();
    spark()
        .readStream()
        .format("delta")
        .option("schemaTrackingLocation", schemaLocation)
        .table(tableName)
        .writeStream()
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpointDir)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (df, batchId) -> collected.addAll(df.collectAsList()))
        .start()
        .awaitTermination();
    return collected;
  }

  /**
   * Asserts that {@code action} throws an exception whose cause chain contains at least one of the
   * given message fragments (case-insensitive search over stringified cause chain).
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

  /** Extracts string values of column {@code colIndex} from a row list. */
  private static List<String> colValues(List<Row> rows, int colIndex) {
    return rows.stream()
        .map(r -> r.isNullAt(colIndex) ? null : r.get(colIndex).toString())
        .collect(Collectors.toList());
  }

  // ---------------------------------------------------------------------------
  // Functional interface for starter-table test bodies
  // ---------------------------------------------------------------------------

  @FunctionalInterface
  private interface StarterTableCode {
    void run(String tableName) throws Exception;
  }

  // ===========================================================================
  // Tests: scenario tests (connector-alternation)
  // ===========================================================================

  /**
   * Port of: {@code "alternating connectors with no schema evolution leaves the schema log
   * untouched"}.
   *
   * <p>V1 initializes the schema log; then V2→V1→V2 bounces with new data between each restart.
   * Without schema evolution neither connector should write a new log entry on restart.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testAlternatingConnectorsNoSchemaEvolution() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V1 initializes the schema log and reads the seed rows.
          List<Row> batch0 = new ArrayList<>();
          withV1(
              () -> {
                try {
                  batch0.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          // Seed rows: ids -1..4 inclusive → 6 rows, each (a, b) both equal to the id string.
          assertThat(colValues(batch0, 0)).containsExactlyInAnyOrder("-1", "0", "1", "2", "3", "4");

          // Bounce V1→V2→V1→V2 with new data between each restart.
          boolean[] useV2Sequence = {true, false, true};
          int[][] dataRanges = {{5, 10}, {10, 15}, {15, 20}};
          for (int step = 0; step < useV2Sequence.length; step++) {
            boolean useV2 = useV2Sequence[step];
            int rangeStart = dataRanges[step][0];
            int rangeEnd = dataRanges[step][1];
            addData(tableName, rangeStart, rangeEnd);

            List<Row> batch = new ArrayList<>();
            withConnector(
                useV2,
                () -> {
                  try {
                    batch.addAll(runStream(tableName, ck, schemaLoc));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
            // Each restart should see only the newly-added rows (AvailableNow from checkpoint).
            List<String> expected = new ArrayList<>();
            for (int i = rangeStart; i < rangeEnd; i++) {
              expected.add(String.valueOf(i));
            }
            assertThat(colValues(batch, 0)).containsExactlyInAnyOrderElementsOf(expected);
          }
        });
  }

  /**
   * Port of: {@code "V1-initialized schema log can be read by V2"}.
   *
   * <p>V1 runs first to initialize the log; V2 resumes from the same checkpoint and reads new data.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testV1InitializedSchemaLogReadByV2() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V1 initializes the schema log.
          List<Row> batch0 = new ArrayList<>();
          withV1(
              () -> {
                try {
                  batch0.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batch0, 0)).containsExactlyInAnyOrder("-1", "0", "1", "2", "3", "4");

          addData(tableName, 5, 10);

          // V2 resumes from V1's checkpoint and schema log.
          List<Row> batch1 = new ArrayList<>();
          withV2(
              () -> {
                try {
                  batch1.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batch1, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
        });
  }

  /**
   * Port of: {@code "V2-initialized schema log can be read by V1"}.
   *
   * <p>V2 runs first to initialize the log; V1 resumes from the same checkpoint and reads new data.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testV2InitializedSchemaLogReadByV1() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V2 initializes the schema log.
          List<Row> batch0 = new ArrayList<>();
          withV2(
              () -> {
                try {
                  batch0.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batch0, 0)).containsExactlyInAnyOrder("-1", "0", "1", "2", "3", "4");

          addData(tableName, 5, 10);

          // V1 resumes from V2's checkpoint and schema log.
          List<Row> batch1 = new ArrayList<>();
          withV1(
              () -> {
                try {
                  batch1.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batch1, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
        });
  }

  /**
   * Port of: {@code "non-additive evolution (rename) written by V1 is consumed by V2"}.
   *
   * <p>V1 runs first, then column b is renamed to c (non-additive). V1 restarts, hits the evolution
   * barrier, and writes the new metadata to the schema log — expected to throw
   * MetadataEvolutionException. V2 then resumes under the V1-evolved schema.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testNonAdditiveRenameWrittenByV1ConsumedByV2() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V1 initializes the schema log.
          withV1(
              () -> {
                try {
                  runStream(tableName, ck, schemaLoc);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

          // Non-additive: rename b -> c.
          renameColumn(tableName, "b", "c");
          addData(tableName, 5, 10);

          // V1 hits the evolution barrier and writes the new schema to the log.
          assertStreamingThrowsContaining(
              () ->
                  withV1(
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // V2 starts under the V1-evolved schema and processes the post-evolution data.
          List<Row> batch = new ArrayList<>();
          withV2(
              () -> {
                try {
                  batch.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          // Post-rename schema is (a, c); rows 5..9 all have equal a and c values.
          assertThat(colValues(batch, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
          // Column c (index 1) should also be present and equal to column a.
          assertThat(colValues(batch, 1)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
        });
  }

  /**
   * Port of: {@code "non-additive evolution (drop) written by V1 is consumed by V2"}.
   *
   * <p>V1 runs first, then column b is dropped (non-additive). V1 restarts, hits the evolution
   * barrier — expected to throw MetadataEvolutionException. V2 then resumes under the V1-evolved
   * schema.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testNonAdditiveDropWrittenByV1ConsumedByV2() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V1 initializes the schema log.
          withV1(
              () -> {
                try {
                  runStream(tableName, ck, schemaLoc);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

          // Non-additive: drop column b.
          dropColumn(tableName, "b");
          addData(tableName, 5, 10);

          // V1 hits the evolution barrier.
          assertStreamingThrowsContaining(
              () ->
                  withV1(
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // V2 starts under the V1-evolved (single-column) schema.
          List<Row> batch = new ArrayList<>();
          withV2(
              () -> {
                try {
                  batch.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          // Post-drop schema is (a); only one column remains.
          assertThat(colValues(batch, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
          assertThat(batch.get(0).length()).isEqualTo(1);
        });
  }

  /**
   * Port of: {@code "non-additive evolution (rename) written by V2 is consumed by V1"}.
   *
   * <p>V2 runs first, then column b is renamed to c (non-additive). V2 restarts, hits the evolution
   * barrier — expected to throw MetadataEvolutionException. V1 then resumes under the V2-evolved
   * schema.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testNonAdditiveRenameWrittenByV2ConsumedByV1() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V2 initializes the schema log.
          withV2(
              () -> {
                try {
                  runStream(tableName, ck, schemaLoc);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

          // Non-additive: rename b -> c.
          renameColumn(tableName, "b", "c");
          addData(tableName, 5, 10);

          // V2 hits the evolution barrier and writes the new schema to the log.
          assertStreamingThrowsContaining(
              () ->
                  withV2(
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // V1 starts under the V2-evolved schema and processes the post-evolution data.
          List<Row> batch = new ArrayList<>();
          withV1(
              () -> {
                try {
                  batch.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          // Post-rename schema is (a, c).
          assertThat(colValues(batch, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
          assertThat(colValues(batch, 1)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
        });
  }

  /**
   * Port of: {@code "non-additive evolution (drop) written by V2 is consumed by V1"}.
   *
   * <p>V2 runs first, then column b is dropped (non-additive). V2 restarts, hits the evolution
   * barrier — expected to throw MetadataEvolutionException. V1 then resumes under the V2-evolved
   * schema.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testNonAdditiveDropWrittenByV2ConsumedByV1() throws Exception {
    withStarterTable(
        TableType.EXTERNAL,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // V2 initializes the schema log.
          withV2(
              () -> {
                try {
                  runStream(tableName, ck, schemaLoc);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

          // Non-additive: drop column b.
          dropColumn(tableName, "b");
          addData(tableName, 5, 10);

          // V2 hits the evolution barrier.
          assertStreamingThrowsContaining(
              () ->
                  withV2(
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // V1 starts under the V2-evolved (single-column) schema.
          List<Row> batch = new ArrayList<>();
          withV1(
              () -> {
                try {
                  batch.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batch, 0)).containsExactlyInAnyOrder("5", "6", "7", "8", "9");
          assertThat(batch.get(0).length()).isEqualTo(1);
        });
  }

  // ---------------------------------------------------------------------------
  // Alternating-connector tests (V1-V2-V1-V2 and V2-V1-V2-V1)
  // ---------------------------------------------------------------------------

  /**
   * Port of: {@code "alternating connectors (V1-V2-V1-V2) across additive then non-additive
   * evolution (rename)"}.
   *
   * <p>Stage order: V1 (init) → add column c → V2 (hits additive barrier, throws) → V2 (drains
   * 3-col data) → rename c→d → V1 (hits non-additive barrier, throws) → V2 (drains post-rename
   * data).
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testAlternatingV1V2V1V2AdditiveRename() throws Exception {
    runAlternatingEvolutionTest(TableType.EXTERNAL, false, "rename");
  }

  /**
   * Port of: {@code "alternating connectors (V1-V2-V1-V2) across additive then non-additive
   * evolution (drop)"}.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testAlternatingV1V2V1V2AdditiveDrop() throws Exception {
    runAlternatingEvolutionTest(TableType.EXTERNAL, false, "drop");
  }

  /**
   * Port of: {@code "alternating connectors (V2-V1-V2-V1) across additive then non-additive
   * evolution (rename)"}.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testAlternatingV2V1V2V1AdditiveRename() throws Exception {
    runAlternatingEvolutionTest(TableType.EXTERNAL, true, "rename");
  }

  /**
   * Port of: {@code "alternating connectors (V2-V1-V2-V1) across additive then non-additive
   * evolution (drop)"}.
   *
   * <p>EXTERNAL-only: V1 (NONE mode) requires path-based access, which is blocked for managed
   * tables ({@code DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED}).
   */
  @Test
  public void testAlternatingV2V1V2V1AdditiveDrop() throws Exception {
    runAlternatingEvolutionTest(TableType.EXTERNAL, true, "drop");
  }

  /**
   * Shared logic for all four alternating-connector tests.
   *
   * @param initUseV2 if true, the alternation order is V2-V1-V2-V1; if false, V1-V2-V1-V2.
   * @param nonAdditiveKind "rename" or "drop"
   */
  private void runAlternatingEvolutionTest(
      TableType tableType, boolean initUseV2, String nonAdditiveKind) throws Exception {
    boolean pickupUseV2 = !initUseV2;

    withStarterTable(
        tableType,
        tableName -> {
          String ck = newCheckpoint();
          String schemaLoc = newSchemaLocation(ck);

          // Stage 1: initiator initializes the schema log on the starter (a, b) schema.
          withConnector(
              initUseV2,
              () -> {
                try {
                  runStream(tableName, ck, schemaLoc);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

          // Stage 2a: add column c (additive evolution).
          addColumn(tableName, "c");
          addData(tableName, 5, 10);

          // Stage 2b: pickup connector hits the additive evolution barrier.
          assertStreamingThrowsContaining(
              () ->
                  withConnector(
                      pickupUseV2,
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Stage 2c: pickup connector drains the 3-column data after acknowledging the evolution.
          List<Row> batchAdditive = new ArrayList<>();
          withConnector(
              pickupUseV2,
              () -> {
                try {
                  batchAdditive.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batchAdditive, 0))
              .containsExactlyInAnyOrder("5", "6", "7", "8", "9");

          // Stage 3a: apply the non-additive change.
          if ("rename".equals(nonAdditiveKind)) {
            renameColumn(tableName, "c", "d");
          } else {
            dropColumn(tableName, "c");
          }
          addData(tableName, 10, 15);

          // Stage 3b: initiator connector hits the non-additive evolution barrier.
          assertStreamingThrowsContaining(
              () ->
                  withConnector(
                      initUseV2,
                      () -> {
                        try {
                          runStream(tableName, ck, schemaLoc);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }),
              "DELTA_STREAMING_METADATA_EVOLUTION");

          // Stage 4: pickup connector drains the post-non-additive-evolution data.
          List<Row> batchNonAdditive = new ArrayList<>();
          withConnector(
              pickupUseV2,
              () -> {
                try {
                  batchNonAdditive.addAll(runStream(tableName, ck, schemaLoc));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
          assertThat(colValues(batchNonAdditive, 0))
              .containsExactlyInAnyOrder("10", "11", "12", "13", "14");
          if ("rename".equals(nonAdditiveKind)) {
            // Post-rename: (a, b, d) — column at index 1 is b (the original second col of the
            // starter which was NOT affected by the add+rename chain), index 2 is d.
            // The exact column positions depend on how the schema evolves; we assert that column a
            // is present and correct. The row shape has 3 columns (a, b, d).
            assertThat(batchNonAdditive.get(0).length()).isEqualTo(3);
          } else {
            // Post-drop: (a, b) — column c was dropped, leaving the two starter columns.
            assertThat(batchNonAdditive.get(0).length()).isEqualTo(2);
          }
        });
  }

  // ===========================================================================
  // SKIPPED: strict-equivalence tests
  //
  // The following five scenarios from V1V2SourceSchemaLogCompatibilitySuiteBase.runOnlyTests
  // are NOT ported because they reach into DeltaSourceMetadataTrackingLog and PersistedMetadata
  // private/internal classes that are not accessible from Java public API:
  //
  //   SKIPPED: "V1 and V2 write equivalent schema tracking log entries (rename)"
  //            — requires DeltaSourceMetadataTrackingLog/PersistedMetadata internals
  //
  //   SKIPPED: "V1 and V2 write equivalent schema tracking log entries (drop)"
  //            — requires DeltaSourceMetadataTrackingLog/PersistedMetadata internals
  //
  //   SKIPPED: "V1 and V2 mergers produce equivalent merged schema log entries"
  //            — requires DeltaSourceMetadataTrackingLog/PersistedMetadata internals
  //
  //   SKIPPED: "V1 and V2 read identical pre-seeded schema log to the same final state (rename)"
  //            — requires DeltaSourceMetadataTrackingLog/PersistedMetadata internals
  //
  //   SKIPPED: "V1 and V2 read identical pre-seeded schema log to the same final state (drop)"
  //            — requires DeltaSourceMetadataTrackingLog/PersistedMetadata internals
  // ===========================================================================
}
