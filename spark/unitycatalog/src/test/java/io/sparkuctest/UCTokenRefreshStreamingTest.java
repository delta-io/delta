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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.test.shims.StreamingTestShims;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Long-running streaming test to validate UC token/credential refresh.
 *
 * <p>This test runs streaming writes and reads for 65+ minutes to ensure that credential refresh
 * works correctly for long-running streaming jobs. It validates correctness with row-by-row
 * verification.
 *
 * <p>Run with: build/sbt "sparkUnityCatalog/testOnly io.sparkuctest.UCTokenRefreshStreamingTest"
 *
 * <p>Required environment variables:
 *
 * <ul>
 *   <li>UC_REMOTE=true
 *   <li>UC_URI - Unity Catalog server URI
 *   <li>UC_TOKEN - Authentication token
 *   <li>UC_CATALOG_NAME - Catalog name (e.g., "main")
 *   <li>UC_SCHEMA_NAME - Schema name (e.g., "demo_zh")
 *   <li>UC_BASE_TABLE_LOCATION - S3 base location for tables
 * </ul>
 */
@Tag("long-running")
public class UCTokenRefreshStreamingTest extends UCDeltaTableIntegrationBaseTest {

  // Test duration and intervals
  private static final long TEST_DURATION_MINUTES = 65; // Run past 1hr credential expiry
  private static final long BATCH_INTERVAL_SECONDS = 30; // Write a batch every 30 seconds
  private static final int ROWS_PER_BATCH = 100; // 100 rows per batch
  private static final long VALIDATE_INTERVAL_SECONDS = 60; // Validate every 60 seconds

  // Schema for test data
  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("batch_num", DataTypes.LongType, false, Metadata.empty()),
            new StructField("row_num", DataTypes.LongType, false, Metadata.empty()),
            new StructField("write_ts", DataTypes.LongType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, false, Metadata.empty())
          });

  /** Creates a local temporary directory for checkpoint location. */
  private String createTempCheckpointDir() {
    try {
      return Files.createTempDirectory("spark-checkpoint-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Main test: runs streaming writes for 65+ minutes with periodic validation.
   *
   * <p>This test validates:
   *
   * <ol>
   *   <li>Token/credential refresh works (job doesn't die after 1hr)
   *   <li>Data correctness with row-by-row verification
   *   <li>No data loss or corruption during long-running streaming
   * </ol>
   */
  @Test
  public void testTokenRefreshWithStreaming() throws Exception {
    withNewTable(
        "token_refresh_streaming_test",
        "batch_num BIGINT, row_num BIGINT, write_ts BIGINT, value STRING",
        TableType.MANAGED,
        this::runLongRunningStreamingTest);
  }

  private void runLongRunningStreamingTest(String tableName) throws Exception {
    SparkSession spark = spark();
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(Duration.ofMinutes(TEST_DURATION_MINUTES));

    // Track expected rows for validation
    Map<String, ExpectedRow> expectedRows = new HashMap<>();
    long batchNum = 0;
    long lastValidationTime = System.currentTimeMillis();
    long totalRowsWritten = 0;

    // Create MemoryStream for generating test data
    var memoryStream =
        StreamingTestShims.MemoryStream().apply(Encoders.row(SCHEMA), spark.sqlContext());

    // Start streaming query writing to the table
    StreamingQuery query =
        memoryStream
            .toDF()
            .writeStream()
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", createTempCheckpointDir())
            .toTable(tableName);

    System.out.println(
        String.format(
            "=== Starting UC Token Refresh Streaming Test ===\n"
                + "Table: %s\n"
                + "Duration: %d minutes\n"
                + "Batch interval: %d seconds\n"
                + "Rows per batch: %d\n"
                + "Validation interval: %d seconds",
            tableName,
            TEST_DURATION_MINUTES,
            BATCH_INTERVAL_SECONDS,
            ROWS_PER_BATCH,
            VALIDATE_INTERVAL_SECONDS));

    try {
      assertTrue(query.isActive(), "Streaming query should be active");

      while (Instant.now().isBefore(endTime)) {
        // Generate and write a batch of rows
        long writeTs = System.currentTimeMillis();
        List<Row> batchRows = new ArrayList<>();

        for (int rowNum = 0; rowNum < ROWS_PER_BATCH; rowNum++) {
          String value = String.format("batch_%d_row_%d", batchNum, rowNum);
          Row row = RowFactory.create(batchNum, (long) rowNum, writeTs, value);
          batchRows.add(row);

          // Track expected row
          String rowKey = String.format("%d:%d", batchNum, rowNum);
          expectedRows.put(rowKey, new ExpectedRow(batchNum, rowNum, writeTs, value));
        }

        // Add batch to stream
        Seq<Row> batchSeq = createRowsAsSeq(batchRows.toArray(new Row[0]));
        memoryStream.addData(batchSeq);
        query.processAllAvailable();

        totalRowsWritten += ROWS_PER_BATCH;
        batchNum++;

        // Log progress
        Duration elapsed = Duration.between(startTime, Instant.now());
        System.out.println(
            String.format(
                "Batch %d written: %d rows | Total rows: %d | Elapsed: %d min",
                batchNum, ROWS_PER_BATCH, totalRowsWritten, elapsed.toMinutes()));

        // Periodic validation
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastValidationTime
            >= TimeUnit.SECONDS.toMillis(VALIDATE_INTERVAL_SECONDS)) {
          validateRows(tableName, expectedRows, spark);
          lastValidationTime = currentTime;
        }

        // Check query is still active
        assertTrue(
            query.isActive(),
            String.format(
                "Streaming query died after %d minutes! Token refresh may have failed.",
                elapsed.toMinutes()));

        // Sleep until next batch
        Thread.sleep(TimeUnit.SECONDS.toMillis(BATCH_INTERVAL_SECONDS));
      }

      // Final validation
      System.out.println("=== Running final validation ===");
      validateRows(tableName, expectedRows, spark);

      Duration totalElapsed = Duration.between(startTime, Instant.now());
      System.out.println(
          String.format(
              "=== TEST PASSED ===\n"
                  + "Total duration: %d minutes\n"
                  + "Total batches: %d\n"
                  + "Total rows written and validated: %d",
              totalElapsed.toMinutes(), batchNum, totalRowsWritten));

    } finally {
      if (query != null) {
        query.stop();
        query.awaitTermination();
        System.out.println("Streaming query stopped");
      }
    }
  }

  /** Validates all rows in the table against expected rows. */
  private void validateRows(
      String tableName, Map<String, ExpectedRow> expectedRows, SparkSession spark) {
    System.out.println(String.format("Validating %d expected rows...", expectedRows.size()));

    Dataset<Row> df =
        spark.sql(
            String.format(
                "SELECT batch_num, row_num, write_ts, value FROM %s ORDER BY batch_num, row_num",
                tableName));

    Row[] actualRows = (Row[]) df.collect();
    Set<String> actualRowKeys = new HashSet<>();
    List<String> mismatches = new ArrayList<>();

    for (Row row : actualRows) {
      long batchNum = row.getLong(0);
      long rowNum = row.getLong(1);
      long writeTs = row.getLong(2);
      String value = row.getString(3);

      String rowKey = String.format("%d:%d", batchNum, rowNum);
      actualRowKeys.add(rowKey);

      ExpectedRow expected = expectedRows.get(rowKey);
      if (expected == null) {
        mismatches.add(String.format("EXTRA ROW: %s (not in expected set)", rowKey));
      } else if (!expected.matches(batchNum, rowNum, writeTs, value)) {
        mismatches.add(
            String.format(
                "MISMATCH at %s: expected=%s, actual=(batch=%d, row=%d, ts=%d, value=%s)",
                rowKey, expected, batchNum, rowNum, writeTs, value));
      }
    }

    // Check for missing rows
    for (String expectedKey : expectedRows.keySet()) {
      if (!actualRowKeys.contains(expectedKey)) {
        mismatches.add(String.format("MISSING ROW: %s", expectedKey));
      }
    }

    if (!mismatches.isEmpty()) {
      String errorMsg =
          String.format(
              "Validation failed with %d issues:\n%s",
              mismatches.size(),
              String.join("\n", mismatches.subList(0, Math.min(20, mismatches.size()))));
      if (mismatches.size() > 20) {
        errorMsg += String.format("\n... and %d more issues", mismatches.size() - 20);
      }
      fail(errorMsg);
    }

    System.out.println(
        String.format(
            "Validation passed: %d rows verified (expected=%d, actual=%d)",
            actualRows.length, expectedRows.size(), actualRows.length));
  }

  /** Helper class to track expected row data. */
  private static class ExpectedRow {
    final long batchNum;
    final long rowNum;
    final long writeTs;
    final String value;

    ExpectedRow(long batchNum, long rowNum, long writeTs, String value) {
      this.batchNum = batchNum;
      this.rowNum = rowNum;
      this.writeTs = writeTs;
      this.value = value;
    }

    boolean matches(long batchNum, long rowNum, long writeTs, String value) {
      return this.batchNum == batchNum
          && this.rowNum == rowNum
          && this.writeTs == writeTs
          && this.value.equals(value);
    }

    @Override
    public String toString() {
      return String.format("(batch=%d, row=%d, ts=%d, value=%s)", batchNum, rowNum, writeTs, value);
    }
  }

  private static Seq<Row> createRowsAsSeq(Row... rows) {
    return JavaConverters.asScalaIteratorConverter(Arrays.asList(rows).iterator())
        .asScala()
        .toSeq();
  }
}
