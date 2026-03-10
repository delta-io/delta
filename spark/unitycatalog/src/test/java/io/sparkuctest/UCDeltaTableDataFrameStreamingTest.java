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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

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
   * Verifies incremental (continuous) streaming where new data arriving after the query starts is
   * processed in subsequent micro-batches via {@code processAllAvailable()}.
   */
  @TestAllTableTypes
  public void testStreamingContinuous(TableType tableType) throws Exception {
    withNewTable(
        "streaming_continuous_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          List<Integer> result = Collections.synchronizedList(new ArrayList<>());
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
          assertThat(batchIds.size()).isEqualTo(3);
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
          List<Integer> result = Collections.synchronizedList(new ArrayList<>());
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
   *   <li>{@code readChangeFeed}, {@code ignoreChanges}, and {@code ignoreDeletes} are blocked by
   *       the V2 connector (MANAGED tables only — EXTERNAL tables route through V1 which silently
   *       accepts these options).
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

          if (tableType == TableType.MANAGED) {
            assertInvalidStreamOption(
                tableName,
                r -> r.option("readChangeFeed", "true").option("startingVersion", 0),
                "not supported",
                "CDC");
            assertInvalidStreamOption(
                tableName,
                r -> r.option("ignoreChanges", "true"),
                "not supported",
                "ignoreChanges");
            assertInvalidStreamOption(
                tableName,
                r -> r.option("ignoreDeletes", "true"),
                "not supported",
                "ignoreDeletes");
          }
        });
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
   * Asserts that {@code action} throws an exception whose combined message (exception + cause)
   * contains all the given {@code fragments} (case-insensitive).
   */
  private static void assertStreamingThrowsContaining(
      ThrowingCallable action, String... fragments) {
    assertThatThrownBy(action)
        .satisfies(
            e -> {
              String full =
                  Optional.ofNullable(e.getMessage()).orElse("")
                      + " "
                      + Optional.ofNullable(e.getCause()).map(Throwable::getMessage).orElse("");
              for (String fragment : fragments) {
                assertThat(full).containsIgnoringCase(fragment);
              }
            });
  }

  private String checkpoint() throws IOException {
    return Files.createTempDirectory("delta-streaming-ck-").toString();
  }

  private List<Integer> ids(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }
}
