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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Combinatorial test framework for verifying DSv2 streaming reads against batch reads.
 *
 * <p>Subclasses declare which {@link TableSetup} implementations to run via {@link #tableSetups()}.
 * The framework generates a test for each {@code TableSetup x AssertionMode} combination using
 * JUnit 5 {@code @TestFactory}. INCREMENTAL mode is skipped for setups with {@code
 * incrementalRounds() == 0}.
 *
 * <p>All tables are created as MANAGED (catalog-managed Delta tables).
 */
public abstract class DSv2StreamingTestBase extends UCDeltaTableIntegrationBaseTest {

  private static final long STREAMING_TIMEOUT_MS = 60_000L;

  @TempDir private Path tempDir;

  private int checkpointCount;

  /** Subclasses return the list of table setups to test. */
  protected abstract List<TableSetup> tableSetups();

  /**
   * Generates dynamic tests: one per {@code TableSetup x AssertionMode} combination. INCREMENTAL
   * mode is skipped for setups where {@code incrementalRounds() == 0}.
   */
  @TestFactory
  Stream<DynamicNode> streamingTests() {
    return tableSetups().stream()
        .flatMap(
            setup -> {
              Stream.Builder<DynamicNode> tests = Stream.builder();
              // SNAPSHOT always runs
              tests.add(
                  DynamicTest.dynamicTest(
                      setup.name() + " / SNAPSHOT",
                      () -> runStreamingTest(setup, AssertionMode.SNAPSHOT)));
              // INCREMENTAL only when the setup provides incremental data
              if (setup.incrementalRounds() > 0) {
                tests.add(
                    DynamicTest.dynamicTest(
                        setup.name() + " / INCREMENTAL",
                        () -> runStreamingTest(setup, AssertionMode.INCREMENTAL)));
              }
              return tests.build();
            });
  }

  private void runStreamingTest(TableSetup setup, AssertionMode mode) throws Exception {
    String tableName = "dsv2_stream_" + UUID.randomUUID().toString().replace("-", "");
    withNewTable(
        tableName,
        setup.schema(),
        setup.partitionColumns(),
        TableType.MANAGED,
        setup.tableProperties(),
        fullTableName -> {
          setup.setUp(spark(), fullTableName);
          if (mode == AssertionMode.SNAPSHOT) {
            runSnapshotTest(setup, fullTableName);
          } else {
            runIncrementalTest(setup, fullTableName);
          }
        });
  }

  /** Snapshot mode: stream all existing data with AvailableNow, then compare to batch read. */
  private void runSnapshotTest(TableSetup setup, String tableName) throws Exception {
    String queryName = "snapshot_" + UUID.randomUUID().toString().replace("-", "");
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
        .awaitTermination(STREAMING_TIMEOUT_MS);

    assertStreamingEqualsBatch(queryName, tableName);
  }

  /**
   * Incremental mode: start continuous stream, verify after initial data, then add data in rounds
   * and verify at each step.
   */
  private void runIncrementalTest(TableSetup setup, String tableName) throws Exception {
    String queryName = "incremental_" + UUID.randomUUID().toString().replace("-", "");
    StreamingQuery query =
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
    try {
      // Verify initial data
      query.processAllAvailable();
      assertStreamingEqualsBatch(queryName, tableName);

      // Incremental rounds
      for (int round = 1; round <= setup.incrementalRounds(); round++) {
        setup.addIncrementalData(spark(), tableName, round);
        query.processAllAvailable();
        assertStreamingEqualsBatch(queryName, tableName);
      }
    } finally {
      query.stop();
    }
  }

  /**
   * Asserts that the streaming memory sink contains the same rows as a batch read of the table.
   * Comparison is order-independent (both sides sorted by first column).
   */
  private void assertStreamingEqualsBatch(String queryName, String tableName) {
    List<List<String>> streamingRows =
        toSortedStringRows(spark().sql("SELECT * FROM " + queryName));
    List<List<String>> batchRows = toSortedStringRows(spark().read().table(tableName));
    assertThat(streamingRows)
        .as("Streaming output for %s should match batch read of %s", queryName, tableName)
        .isEqualTo(batchRows);
  }

  /**
   * Converts a DataFrame to a sorted list of string rows for deterministic comparison. Rows are
   * sorted lexicographically by all columns concatenated.
   */
  private static List<List<String>> toSortedStringRows(Dataset<Row> df) {
    Row[] rows = (Row[]) df.collect();
    return java.util.Arrays.stream(rows)
        .map(
            row -> {
              java.util.List<String> cells = new java.util.ArrayList<>();
              for (int i = 0; i < row.length(); i++) {
                cells.add(row.isNullAt(i) ? "null" : row.get(i).toString());
              }
              return cells;
            })
        .sorted(
            (a, b) -> {
              for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
                int cmp = a.get(i).compareTo(b.get(i));
                if (cmp != 0) return cmp;
              }
              return Integer.compare(a.size(), b.size());
            })
        .collect(Collectors.toList());
  }

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }
}
