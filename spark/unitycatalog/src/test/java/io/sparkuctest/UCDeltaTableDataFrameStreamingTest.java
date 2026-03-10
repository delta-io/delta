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
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
  public void testStreamingRead(TableType tableType) throws Exception {
    withNewTable(
        "streaming_read_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testStreamingWrite(TableType tableType) throws Exception {
    withNewTable(
        "streaming_write_src",
        "id INT",
        tableType,
        srcName ->
            withNewTable(
                "streaming_write_sink",
                "id INT",
                tableType,
                sinkName -> {
                  sql("INSERT INTO %s VALUES (1), (2), (3)", srcName);
                  spark()
                      .readStream()
                      .format("delta")
                      .table(srcName)
                      .writeStream()
                      .format("delta")
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .toTable(sinkName)
                      .awaitTermination();
                  check(sinkName, List.of(row("1"), row("2"), row("3")));
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
            sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
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
            sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
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
   * Verifies that a negative {@code startingVersion} is rejected with a clear error mentioning
   * "startingVersion".
   */
  @TestAllTableTypes
  public void testStreamingReadNegativeStartingVersion(TableType tableType) throws Exception {
    withNewTable(
        "streaming_neg_version_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", -1)
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .foreachBatch(NOOP_BATCH)
                      .start()
                      .awaitTermination(),
              "startingVersion");
        });
  }

  /**
   * Verifies that a {@code startingVersion} beyond the table's history is rejected with a clear
   * error mentioning the requested version number.
   */
  @TestAllTableTypes
  public void testStreamingReadNonExistentStartingVersion(TableType tableType) throws Exception {
    withNewTable(
        "streaming_future_version_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          assertStreamingThrowsContaining(
              () ->
                  spark()
                      .readStream()
                      .format("delta")
                      .option("startingVersion", 999999)
                      .table(tableName)
                      .writeStream()
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .foreachBatch(NOOP_BATCH)
                      .start()
                      .awaitTermination(),
              "999999");
        });
  }

  /**
   * V2 connector blocks {@code readChangeFeed} in UNSUPPORTED_STREAMING_OPTIONS. Verifies the
   * stream fails immediately with a clear "not supported" error.
   */
  @TestAllTableTypes
  public void testStreamingReadChangeFeedNotSupported(TableType tableType) throws Exception {
    withNewTable(
        "streaming_cdf_not_supported_test",
        "id INT",
        tableType,
        tableName ->
            // Validation fires before any reading — no data needed, version 0 suffices.
            assertStreamingThrowsContaining(
                () ->
                    spark()
                        .readStream()
                        .format("delta")
                        .option("readChangeFeed", "true")
                        .option("startingVersion", 0)
                        .table(tableName)
                        .writeStream()
                        .trigger(Trigger.AvailableNow())
                        .option("checkpointLocation", checkpoint())
                        .foreachBatch(NOOP_BATCH)
                        .start(),
                "not supported",
                "readChangeFeed"));
  }

  /**
   * V2 connector blocks {@code ignoreChanges} in UNSUPPORTED_STREAMING_OPTIONS. Verifies the stream
   * fails immediately with a clear "not supported" error.
   */
  @TestAllTableTypes
  public void testStreamingIgnoreChangesNotSupported(TableType tableType) throws Exception {
    withNewTable(
        "streaming_ignore_changes_test",
        "id INT",
        tableType,
        tableName ->
            assertStreamingThrowsContaining(
                () ->
                    spark()
                        .readStream()
                        .format("delta")
                        .option("ignoreChanges", "true")
                        .table(tableName)
                        .writeStream()
                        .trigger(Trigger.AvailableNow())
                        .option("checkpointLocation", checkpoint())
                        .foreachBatch(NOOP_BATCH)
                        .start(),
                "not supported",
                "ignoreChanges"));
  }

  /**
   * V2 connector blocks {@code ignoreDeletes} in UNSUPPORTED_STREAMING_OPTIONS. Verifies the stream
   * fails immediately with a clear "not supported" error.
   */
  @TestAllTableTypes
  public void testStreamingIgnoreDeletesNotSupported(TableType tableType) throws Exception {
    withNewTable(
        "streaming_ignore_deletes_test",
        "id INT",
        tableType,
        tableName ->
            assertStreamingThrowsContaining(
                () ->
                    spark()
                        .readStream()
                        .format("delta")
                        .option("ignoreDeletes", "true")
                        .table(tableName)
                        .writeStream()
                        .trigger(Trigger.AvailableNow())
                        .option("checkpointLocation", checkpoint())
                        .foreachBatch(NOOP_BATCH)
                        .start(),
                "not supported",
                "ignoreDeletes"));
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
