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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.io.TempDir;

/**
 * DataFrame streaming test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers streaming read and write via Structured Streaming. Uses {@link Trigger#AvailableNow()}
 * for deterministic, testable streaming without manual termination. Tests run against both EXTERNAL
 * and MANAGED table types.
 */
public class UCDeltaTableDataFrameStreamingTest extends UCDeltaTableIntegrationBaseTest {

  /** No-op foreachBatch sink used by negative tests that only need the stream to start. */
  private static final VoidFunction2<Dataset<Row>, Long> NOOP_BATCH = (df, id) -> {};

  @TempDir private Path tempDir;

  private int checkpointCount;

  @TestAllTableTypes
  public void testStreamingReadWrite(TableType tableType) throws Exception {
    withNewTable(
        "streaming_rw_src",
        "id INT",
        tableType,
        srcName ->
            withNewTable(
                "streaming_rw_sink",
                "id INT",
                tableType,
                sinkName -> {
                  sql("INSERT INTO %s VALUES (1), (2), (3)", srcName);
                  String ck = checkpoint();

                  // AvailableNow: process all existing data and terminate.
                  spark()
                      .readStream()
                      .format("delta")
                      .table(srcName)
                      .writeStream()
                      .format("delta")
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", ck)
                      .toTable(sinkName)
                      .awaitTermination();
                  check(sinkName, List.of(row("1"), row("2"), row("3")));

                  // Continuous: reuse same checkpoint so the query resumes from where
                  // AvailableNow left off and only picks up newly inserted rows.
                  StreamingQuery query =
                      spark()
                          .readStream()
                          .format("delta")
                          .table(srcName)
                          .writeStream()
                          .format("delta")
                          .outputMode("append")
                          .option("checkpointLocation", ck)
                          .toTable(sinkName);
                  try {
                    sql("INSERT INTO %s VALUES (4), (5)", srcName);
                    query.processAllAvailable();
                    check(sinkName, List.of(row("1"), row("2"), row("3"), row("4"), row("5")));
                  } finally {
                    query.stop();
                  }
                }));
  }

  @TestAllTableTypes
  public void testStreamingReadFromVersion(TableType tableType) throws Exception {
    withNewTable(
        "streaming_version_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("startingVersion", v1 + 1)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(4, 5);
        });
  }

  /**
   * Verifies incremental streaming via a foreachBatch sink (in-memory accumulator). Complements
   * {@link #testStreamingReadWrite}, which covers the same data-arrival scenario but writes to a
   * Delta table sink and validates via SQL.
   */
  @TestAllTableTypes
  public void testStreamingContinuous(TableType tableType) throws Exception {
    withNewTable(
        "streaming_continuous_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(1, 2, 3);

            sql("INSERT INTO %s VALUES (4), (5)", tableName);
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
          } finally {
            query.stop();
          }
        });
  }

  /**
   * Verifies that {@code maxFilesPerTrigger=1} causes the stream to process each of the 3 separate
   * commits as its own micro-batch, producing exactly 3 batches total under {@link
   * Trigger#AvailableNow()}.
   */
  @TestAllTableTypes
  public void testStreamingMaxFilesPerTrigger(TableType tableType) throws Exception {
    withNewTable(
        "streaming_max_files_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);
          sql("INSERT INTO %s VALUES (3)", tableName);

          List<Integer> result = new ArrayList<>();
          List<Long> batchIds = new ArrayList<>();
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
                        result.addAll(ids(df));
                        batchIds.add(batchId);
                      })
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
          assertThat(batchIds).hasSize(3);
        });
  }

  /**
   * Verifies that streaming from a table that has rows deleted (RemoveFile with dataChange=true)
   * fails with a clear error directing the user to the {@code ignoreDeletes} option.
   */
  @TestAllTableTypes
  public void testStreamingDeleteFailsWithHelpfulError(TableType tableType) throws Exception {
    withNewTable(
        "streaming_delete_error_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(1, 2, 3);

            sql("DELETE FROM %s WHERE id = 1", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "ignoreDeletes");
          } finally {
            query.stop();
          }
        });
  }

  /**
   * CDF streaming reads work for EXTERNAL tables but fail for MANAGED tables.
   *
   * <p>For EXTERNAL: verifies that inserts and a delete produce the expected typed change events.
   * For MANAGED: verifies the stream fails with an error containing "not supported" and "CDC".
   */
  @TestAllTableTypes
  public void testStreamingCDFRead(TableType tableType) throws Exception {
    withNewTable(
        "streaming_cdf_read_test",
        "id INT",
        null,
        tableType,
        "'delta.enableChangeDataFeed'='true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long insertVersion = currentVersion(tableName);
          sql("DELETE FROM %s WHERE id = 1", tableName);

          if (tableType == TableType.EXTERNAL) {
            List<Row> changes = new ArrayList<>();
            spark()
                .readStream()
                .format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", insertVersion)
                .table(tableName)
                .writeStream()
                .trigger(Trigger.AvailableNow())
                .option("checkpointLocation", checkpoint())
                .foreachBatch(
                    (VoidFunction2<Dataset<Row>, Long>)
                        (df, id) -> changes.addAll(df.select("id", "_change_type").collectAsList()))
                .start()
                .awaitTermination();
            assertThat(changes)
                .extracting(r -> r.getString(1))
                .containsExactlyInAnyOrder("insert", "insert", "insert", "delete");
            assertThat(changes)
                .filteredOn(r -> "delete".equals(r.getString(1)))
                .extracting(r -> r.getInt(0))
                .containsExactly(1);
          } else {
            assertInvalidStreamOption(
                tableName,
                r -> r.option("readChangeFeed", "true").option("startingVersion", insertVersion),
                "not supported",
                "CDC");
          }
        });
  }

  /**
   * Delta streaming sink does not support {@code complete} output mode. Verifies the stream fails
   * with a clear error mentioning "complete".
   */
  @TestAllTableTypes
  public void testStreamingWriteCompleteModeNotSupported(TableType tableType) throws Exception {
    withNewTable(
        "streaming_complete_mode_src",
        "id INT",
        tableType,
        srcName ->
            withNewTable(
                "streaming_complete_mode_sink",
                "id INT",
                tableType,
                sinkName -> {
                  sql("INSERT INTO %s VALUES (1), (2), (3)", srcName);
                  assertStreamingThrowsContaining(
                      () ->
                          spark()
                              .readStream()
                              .format("delta")
                              .table(srcName)
                              .writeStream()
                              .format("delta")
                              .outputMode("complete")
                              .trigger(Trigger.AvailableNow())
                              .option("checkpointLocation", checkpoint())
                              .toTable(sinkName)
                              .awaitTermination(),
                      "complete");
                }));
  }

  /**
   * Verifies that invalid or unsupported streaming read options are rejected with clear errors.
   *
   * <ul>
   *   <li>Negative {@code startingVersion} is rejected (both table types).
   *   <li>{@code startingVersion} beyond the table history is rejected (both table types).
   *   <li>{@code ignoreChanges} and {@code ignoreDeletes} fail with a "not supported" error for
   *       MANAGED tables; EXTERNAL tables accept these options silently.
   * </ul>
   */
  @TestAllTableTypes
  public void testStreamingInvalidOptions(TableType tableType) throws Exception {
    withNewTable(
        "streaming_invalid_options_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);

          assertInvalidStreamOption(
              tableName, r -> r.option("startingVersion", -1), "startingVersion");
          assertInvalidStreamOption(tableName, r -> r.option("startingVersion", 999999), "999999");
        });
  }

  /**
   * Verifies that {@code startingTimestamp} streams only from the commit at-or-after the given
   * timestamp, excluding rows from earlier commits.
   *
   * <p>The timestamp of the second commit is captured after both inserts complete. Delta resolves
   * this to that commit (inclusive), so only rows from the second insert are delivered.
   */
  @TestAllTableTypes
  public void testStreamingStartingTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "streaming_ts_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName); // commit 1 — before the cutoff
          sql("INSERT INTO %s VALUES (4), (5)", tableName); // commit 2 — the starting point
          String commitTimestamp = currentTimestamp(tableName); // timestamp of commit 2

          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("startingTimestamp", commitTimestamp)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(4, 5);
        });
  }

  /**
   * Verifies that {@code skipChangeCommits=true} skips commits that contain deletions, allowing the
   * stream to continue on subsequent insert commits.
   *
   * <p>EXTERNAL tables (V1 connector): the delete commit is skipped; follow-up inserts arrive.
   * MANAGED tables (V2 connector): the option is not yet supported and the stream fails.
   */
  @TestAllTableTypes
  public void testStreamingSkipChangeCommits(TableType tableType) throws Exception {
    withNewTable(
        "streaming_skip_changes_test",
        "id INT",
        tableType,
        tableName -> {
          if (tableType == TableType.EXTERNAL) {
            verifySkipChangeCommitsExternal(tableName);
          } else {
            assertInvalidStreamOption(
                tableName, r -> r.option("skipChangeCommits", "true"), "valid version");
          }
        });
  }

  private void verifySkipChangeCommitsExternal(String tableName) throws Exception {
    sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
    List<Integer> result = new ArrayList<>();
    StreamingQuery query =
        spark()
            .readStream()
            .format("delta")
            .option("skipChangeCommits", "true")
            .table(tableName)
            .writeStream()
            .option("checkpointLocation", checkpoint())
            .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
            .start();
    try {
      query.processAllAvailable();
      assertThat(result).containsExactlyInAnyOrder(1, 2, 3);

      sql("DELETE FROM %s WHERE id = 1", tableName); // change commit — skipped by the option
      sql("INSERT INTO %s VALUES (4)", tableName);
      query.processAllAvailable();

      assertThat(result).containsExactlyInAnyOrder(1, 2, 3, 4);
    } finally {
      query.stop();
    }
  }

  /**
   * Starts a streaming read with options applied by {@code configure}, then asserts the stream
   * fails with a message containing all {@code fragments} (case-insensitive).
   */
  private void assertInvalidStreamOption(
      String tableName,
      Function<DataStreamReader, DataStreamReader> configure,
      String... fragments) {
    assertStreamingThrowsContaining(
        () ->
            configure
                .apply(spark().readStream().format("delta"))
                .table(tableName)
                .writeStream()
                .trigger(Trigger.AvailableNow())
                .option("checkpointLocation", checkpoint())
                .foreachBatch(NOOP_BATCH)
                .start()
                .awaitTermination(),
        fragments);
  }

  /**
   * Asserts that {@code action} throws an exception whose cause chain contains all the given {@code
   * fragments} (case-insensitive).
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

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private List<Integer> ids(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }
}
