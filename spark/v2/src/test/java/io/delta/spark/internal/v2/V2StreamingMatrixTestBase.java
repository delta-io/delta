/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Reusable harness for testing DSv2 streaming reads over Delta tables in different configurations.
 *
 * <p>This base class provides three layers that subclasses compose via {@code @ParameterizedTest}:
 *
 * <ol>
 *   <li><b>Core types</b> &mdash; {@link TableFeature}, {@link TableContext}, {@link
 *       ThrowingRunnable}
 *   <li><b>Stream lifecycle</b> &mdash; {@link #startStream}, {@link #stopAndCleanup}
 *   <li><b>Verification strategies</b>:
 *       <ul>
 *         <li>{@link #verifyPostMutationRead} &mdash; mutation then stream; compare to batch
 *         <li>{@link #verifyStreamThroughMutation} &mdash; stream first, then mutation during
 *         <li>{@link #verifyMultiBatchRead} &mdash; rate-limited to force multi-batch offsets
 *         <li>{@link #verifyStreamError} &mdash; assert a specific error is thrown
 *       </ul>
 * </ol>
 *
 * <p>Table creation helpers and mutation helpers are also provided so that stacked PRs can plug in
 * new scenarios with minimal boilerplate.
 */
public abstract class V2StreamingMatrixTestBase extends V2TestBase {

  // ---------------------------------------------------------------------------
  // Core types
  // ---------------------------------------------------------------------------

  /** Feature flags describing table capabilities. Used for applicability filtering in subclasses. */
  protected enum TableFeature {
    PARTITIONED,
    CLUSTERED,
    COLUMN_MAPPING,
    DELETION_VECTORS,
    GENERATED_COLUMNS,
    IDENTITY_COLUMNS,
    CHECK_CONSTRAINTS,
    DEFAULT_COLUMNS,
    TYPE_WIDENING
  }

  /** Immutable descriptor of a table under test. */
  protected static final class TableContext {
    final String path;
    final String dataColumn;
    final EnumSet<TableFeature> features;

    TableContext(String path, String dataColumn, EnumSet<TableFeature> features) {
      this.path = path;
      this.dataColumn = dataColumn;
      this.features = EnumSet.copyOf(features);
    }

    /** V1 SQL identifier: {@code delta.`path`}. Used for writes and DDL. */
    String sqlIdent() {
      return str("delta.`%s`", escapePath(path));
    }

    /** V2 SQL identifier: {@code dsv2.delta.`path`}. Used for streaming reads. */
    String dsv2Ident() {
      return str("dsv2.delta.`%s`", escapePath(path));
    }

    boolean has(TableFeature feature) {
      return features.contains(feature);
    }

    TableContext withPath(String newPath) {
      return new TableContext(newPath, dataColumn, features);
    }

    TableContext withDataColumn(String newDataColumn) {
      return new TableContext(path, newDataColumn, features);
    }
  }

  /** Runnable that may throw checked exceptions. Used for mutation lambdas. */
  @FunctionalInterface
  protected interface ThrowingRunnable {
    void run() throws Exception;
  }

  // ---------------------------------------------------------------------------
  // Verification strategies
  // ---------------------------------------------------------------------------

  /**
   * <b>Strategy 1: Post-mutation read.</b>
   *
   * <p>The mutation has already been applied. Start a fresh V2 stream, verify it matches the batch
   * snapshot, append more rows, and verify the stream catches up.
   *
   * @param table the table (already created and mutated)
   * @param workDir temp directory for checkpoints
   * @param label unique label for test naming
   */
  protected void verifyPostMutationRead(TableContext table, Path workDir, String label)
      throws Exception {
    String qName = queryName(label);
    Path cpDir = Files.createDirectories(workDir.resolve("cp").resolve(qName));

    StreamingQuery query = null;
    try {
      query = startStream(table, qName, cpDir, Collections.emptyMap());
      query.processAllAvailable();
      assertSnapshotEquals(qName, table);

      insertAppend(table, 10_000, 5);
      query.processAllAvailable();
      assertSnapshotEquals(qName, table);

      assertFalse(
          spark.sql(str("SELECT * FROM %s", qName)).isEmpty(),
          "stream result should not be empty");
    } finally {
      stopAndCleanup(query, qName);
    }
  }

  /**
   * <b>Strategy 2: Stream through mutation.</b>
   *
   * <p>Start the stream first so initial data is consumed, then apply the mutation while the stream
   * is running, then append clean rows. Verify the stream survives and picks up subsequent appends.
   *
   * <p>For mutations that produce {@code RemoveFile} with {@code dataChange=true}, callers should
   * pass {@code skipChangeCommits=true} (via {@link #skipChangeCommitsOptions()}) so the stream
   * skips those commits rather than erroring.
   *
   * @param table the table (already created with seed data, NOT yet mutated)
   * @param mutation the mutation to apply while the stream is running
   * @param workDir temp directory for checkpoints
   * @param label unique label for test naming
   * @param streamOptions stream options (e.g., skipChangeCommits, ignoreChanges)
   */
  protected void verifyStreamThroughMutation(
      TableContext table,
      ThrowingRunnable mutation,
      Path workDir,
      String label,
      Map<String, String> streamOptions)
      throws Exception {
    String qName = queryName(label);
    Path cpDir = Files.createDirectories(workDir.resolve("cp").resolve(qName));

    StreamingQuery query = null;
    try {
      query = startStream(table, qName, cpDir, streamOptions);

      // Consume initial snapshot
      query.processAllAvailable();
      long initialCount = spark.sql(str("SELECT * FROM %s", qName)).count();
      assertTrue(initialCount > 0, "stream should have consumed seed data");

      // Apply mutation while stream is active
      mutation.run();

      // Process the mutation's commit(s)
      query.processAllAvailable();

      // Append clean insert-only rows and verify the stream picks them up
      insertAppend(table, 10_000, 5);
      query.processAllAvailable();

      long finalCount = spark.sql(str("SELECT * FROM %s", qName)).count();
      assertTrue(
          finalCount > initialCount,
          str("stream should have picked up appended rows: initial=%d final=%d",
              initialCount, finalCount));

      // No exception should have been captured
      if (query.exception().isDefined()) {
        fail("stream error: " + query.exception().get().toString());
      }
    } finally {
      stopAndCleanup(query, qName);
    }
  }

  /**
   * <b>Strategy 3: Multi-batch read.</b>
   *
   * <p>Like {@link #verifyPostMutationRead} but with {@code maxFilesPerTrigger=1}, forcing the
   * stream to advance through multiple micro-batches. This exercises the offset advancement path
   * ({@code getNextOffsetFromPreviousOffset}) that single-batch {@code processAllAvailable()} tests
   * never hit.
   *
   * @param table the table (already created and mutated)
   * @param workDir temp directory for checkpoints
   * @param label unique label for test naming
   */
  protected void verifyMultiBatchRead(TableContext table, Path workDir, String label)
      throws Exception {
    String qName = queryName(label);
    Path cpDir = Files.createDirectories(workDir.resolve("cp").resolve(qName));

    StreamingQuery query = null;
    try {
      query =
          startStream(table, qName, cpDir, Collections.singletonMap("maxFilesPerTrigger", "1"));
      query.processAllAvailable();
      assertSnapshotEquals(qName, table);

      insertAppend(table, 10_000, 3);
      query.processAllAvailable();
      assertSnapshotEquals(qName, table);

      assertFalse(
          spark.sql(str("SELECT * FROM %s", qName)).isEmpty(),
          "stream result should not be empty");
    } finally {
      stopAndCleanup(query, qName);
    }
  }

  /**
   * <b>Strategy 4: Expected error.</b>
   *
   * <p>Start a stream, execute a trigger action (e.g., a mutation that produces data-change removes
   * without the required stream option), and assert that a {@link StreamingQueryException} is thrown
   * whose message contains the expected substring.
   *
   * @param table the table (already created with seed data)
   * @param triggerAction action that causes the stream to fail
   * @param workDir temp directory for checkpoints
   * @param label unique label for test naming
   * @param expectedErrorSubstring substring expected in the error message
   */
  protected void verifyStreamError(
      TableContext table,
      ThrowingRunnable triggerAction,
      Path workDir,
      String label,
      String expectedErrorSubstring)
      throws Exception {
    String qName = queryName(label);
    Path cpDir = Files.createDirectories(workDir.resolve("cp").resolve(qName));

    StreamingQuery query = null;
    try {
      query = startStream(table, qName, cpDir, Collections.emptyMap());
      query.processAllAvailable();

      triggerAction.run();

      StreamingQueryException ex =
          assertThrows(StreamingQueryException.class, query::processAllAvailable);
      assertTrue(
          ex.getMessage().contains(expectedErrorSubstring),
          str(
              "Expected error containing '%s' but got: %s",
              expectedErrorSubstring, ex.getMessage()));
    } finally {
      stopAndCleanup(query, qName);
    }
  }

  // ---------------------------------------------------------------------------
  // Stream lifecycle
  // ---------------------------------------------------------------------------

  /** Start a V2 streaming query writing to a memory sink. */
  protected StreamingQuery startStream(
      TableContext table, String queryName, Path checkpointDir, Map<String, String> options)
      throws Exception {
    DataStreamReader reader = spark.readStream();
    for (Map.Entry<String, String> opt : options.entrySet()) {
      reader = reader.option(opt.getKey(), opt.getValue());
    }
    return reader
        .table(table.dsv2Ident())
        .writeStream()
        .format("memory")
        .queryName(queryName)
        .outputMode("append")
        .option("checkpointLocation", checkpointDir.toString())
        .start();
  }

  /** Stop a query, drop the memory view, and clear the DeltaLog cache to release file handles. */
  protected void stopAndCleanup(StreamingQuery query, String queryName) {
    if (query != null) {
      try {
        query.stop();
      } catch (Exception ignored) {
        // Best-effort cleanup
      }
    }
    try {
      spark.sql(str("DROP VIEW IF EXISTS %s", queryName));
    } catch (Exception ignored) {
      // Best-effort cleanup
    }
    DeltaLog.clearCache();
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  /**
   * Assert that the rows in the memory sink match the current Delta table snapshot as a multiset
   * (order-independent, duplicate-sensitive). Uses {@code exceptAll} in both directions.
   */
  protected void assertSnapshotEquals(String queryName, TableContext table) {
    Dataset<Row> actual = canonicalize(spark.sql(str("SELECT * FROM %s", queryName)));
    Dataset<Row> expected = canonicalize(spark.read().format("delta").load(table.path));

    long extraInActual = actual.exceptAll(expected).count();
    long missingFromActual = expected.exceptAll(actual).count();

    assertEquals(
        0L,
        extraInActual,
        () ->
            str(
                "stream has %d unexpected extra rows",
                extraInActual));
    assertEquals(
        0L,
        missingFromActual,
        () ->
            str(
                "stream is missing %d expected rows",
                missingFromActual));
  }

  /** Project columns in sorted order so {@code exceptAll} works regardless of column ordering. */
  private Dataset<Row> canonicalize(Dataset<Row> df) {
    String[] cols = df.columns().clone();
    Arrays.sort(cols);
    Column[] projection = Arrays.stream(cols).map(functions::col).toArray(Column[]::new);
    return df.select(projection);
  }

  // ---------------------------------------------------------------------------
  // Table / SQL helpers (for use by subclass scenarios)
  // ---------------------------------------------------------------------------

  /** Create a {@link TableContext} pointing at a {@code table} subdirectory under {@code dir}. */
  protected TableContext tableAt(Path dir, EnumSet<TableFeature> features) {
    return new TableContext(dir.resolve("table").toString(), "data", features);
  }

  /**
   * Insert rows via {@code INSERT INTO ... VALUES}. Handles partitioned tables automatically.
   *
   * @return the same table context (for chaining)
   */
  protected TableContext insertAppend(TableContext table, int startId, int count) {
    if (count <= 0) return table;
    List<String> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int id = startId + i;
      rows.add(
          table.has(TableFeature.PARTITIONED)
              ? str("(%d, 'v%d', %d)", id, id, id % 2)
              : str("(%d, 'v%d')", id, id));
    }
    String cols =
        table.has(TableFeature.PARTITIONED)
            ? str("id, %s, part", table.dataColumn)
            : str("id, %s", table.dataColumn);
    spark.sql(
        str("INSERT INTO %s (%s) VALUES %s", table.sqlIdent(), cols, String.join(", ", rows)));
    return table;
  }

  /** Write N single-row commits. Useful for creating small files for OPTIMIZE tests. */
  protected void writeSmallFileCommits(TableContext table, int commits) {
    for (int i = 0; i < commits; i++) {
      insertAppend(table, 20_000 + i, 1);
    }
  }

  /** Options map: {@code skipChangeCommits=true}. For mutations that produce data-change removes. */
  protected static Map<String, String> skipChangeCommitsOptions() {
    return Collections.singletonMap("skipChangeCommits", "true");
  }

  /** Options map: {@code ignoreChanges=true}. For streams that should tolerate data-change removes. */
  protected static Map<String, String> ignoreChangesOptions() {
    return Collections.singletonMap("ignoreChanges", "true");
  }

  // ---------------------------------------------------------------------------
  // Name / path helpers
  // ---------------------------------------------------------------------------

  protected static String escapePath(String path) {
    return path.replace("`", "``");
  }

  /** Generate a unique query name for a memory sink. */
  protected static String queryName(String label) {
    return "dsv2_"
        + label.replaceAll("[^A-Za-z0-9_]", "_")
        + "_"
        + UUID.randomUUID().toString().replace('-', '_');
  }

  /** Generate a unique temp view name. */
  protected static String tempViewName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace('-', '_');
  }
}
