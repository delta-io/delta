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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import scala.Option;

public class UCDeltaTableStreamingTest extends UCDeltaTableIntegrationBaseTest {

  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";
  private static final String V2_ENABLE_MODE_STRICT = "STRICT";
  private static final int STREAM_INSERTS = 100;
  private static final long STREAM_TRIGGER_MS = 1_500L;
  private static final long WRITER_INSERT_PAUSE_MS = 100L;
  private static final long WRITER_INSERT_JITTER_MS = 50L;
  private static final long SINK_POLL_SLEEP_MS = 200L;
  private static final long SINK_POLL_TIMEOUT_MS = 120_000L;

  @Test
  public void testStreamingReadsUcManagedTableProductionLike() throws Exception {
    SparkSession spark = getSparkSession();
    Option<String> originalMode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
    String tableName = "streaming_uc_managed_prod";
    String fullTableName = getCatalogName() + ".default." + tableName;

    try {
      // Step 1: Seed the UC-managed table with an initial commit.
      sql(
          "CREATE TABLE %s USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
              + "AS SELECT CAST(0 AS INT) AS id, CAST('seed' AS STRING) AS value",
          fullTableName);

      withTempDir(
          checkpointDir -> {
            String queryName = "uc_streaming_prod_" + UUID.randomUUID().toString().replace("-", "");
            StreamingQuery query = null;
            Thread writerThread = null;
            AtomicReference<Throwable> writerError = new AtomicReference<>();

            try {
              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);
              assertEquals(V2_ENABLE_MODE_STRICT, spark.conf().get(V2_ENABLE_MODE_KEY));

              // Use a separate session with the original mode for batch inserts.
              final SparkSession writerSpark = spark.newSession();
              restoreV2Mode(writerSpark, originalMode);

              // Step 2: Start the streaming reader (DSv2 STRICT) with a processing-time trigger.
              Dataset<Row> input =
                  spark
                      .readStream()
                      .format("delta")
                      .option("startingVersion", "0")
                      .table(fullTableName);

              query =
                  input
                      .writeStream()
                      .format("memory")
                      .queryName(queryName)
                      .option("checkpointLocation", checkpointDir.getAbsolutePath())
                      .outputMode("append")
                      .trigger(Trigger.ProcessingTime(STREAM_TRIGGER_MS))
                      .start();

              // Step 3: Start a background writer that appends commits.
              writerThread =
                  new Thread(
                      () -> {
                        try {
                          System.out.println("[UCDeltaTableStreamingTestV2] Writer thread started");
                          for (int i = 1; i <= STREAM_INSERTS; i++) {
                            String value = "value_" + i;
                            writerSpark
                                .sql(
                                    String.format(
                                        "INSERT INTO %s VALUES (%d, '%s')",
                                        fullTableName, i, value))
                                .collect();
                            System.out.println(
                                "[UCDeltaTableStreamingTestV2] Inserted row id="
                                    + i
                                    + " value="
                                    + value);
                            long jitterMs =
                                ThreadLocalRandom.current()
                                    .nextLong(
                                        -WRITER_INSERT_JITTER_MS, WRITER_INSERT_JITTER_MS + 1);
                            long sleepMs = Math.max(0L, WRITER_INSERT_PAUSE_MS + jitterMs);
                            Thread.sleep(sleepMs);
                          }
                          System.out.println(
                              "[UCDeltaTableStreamingTestV2] Writer thread completed");
                        } catch (Throwable t) {
                          writerError.set(t);
                        }
                      },
                      "uc-streaming-writer");
              writerThread.start();

              // Step 4: Wait until the reader observes all expected rows in the memory sink.
              int expectedRows = 1 + STREAM_INSERTS;
              awaitMemorySinkRows(spark, queryName, expectedRows, SINK_POLL_TIMEOUT_MS);

              if (writerThread != null) {
                writerThread.join(SINK_POLL_TIMEOUT_MS);
                if (writerThread.isAlive()) {
                  throw new IllegalStateException("Writer thread did not finish within timeout");
                }
              }
              if (writerError.get() != null) {
                throw new RuntimeException("Writer thread failed", writerError.get());
              }

              // Step 5: Validate the complete contents of the memory sink.
              List<List<String>> expected = new ArrayList<>();
              expected.add(List.of("0", "seed"));
              for (int i = 1; i <= STREAM_INSERTS; i++) {
                expected.add(List.of(String.valueOf(i), "value_" + i));
              }
              check(queryName, expected);
            } finally {
              if (query != null) {
                query.stop();
              }
              if (writerThread != null && writerThread.isAlive()) {
                writerThread.interrupt();
              }
              spark.sql("DROP VIEW IF EXISTS " + queryName);
            }
          });
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
      restoreV2Mode(spark, originalMode);
    }
  }

  private static void awaitMemorySinkRows(
      SparkSession spark, String queryName, int expectedRows, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    long lastCount = -1L;
    while (System.currentTimeMillis() < deadline) {
      if (spark.catalog().tableExists(queryName)) {
        long rowCount = spark.sql("SELECT * FROM " + queryName).count();
        if (rowCount != lastCount) {
          System.out.println(
              "[UCDeltaTableStreamingTestV2] Memory sink row count for "
                  + queryName
                  + ": "
                  + rowCount
                  + "/"
                  + expectedRows);
          lastCount = rowCount;
        }
        if (rowCount >= expectedRows) {
          return;
        }
      }
      Thread.sleep(SINK_POLL_SLEEP_MS);
    }
    throw new IllegalStateException(
        "Timed out waiting for memory sink to reach " + expectedRows + " rows");
  }

  private static void restoreV2Mode(SparkSession spark, Option<String> originalMode) {
    if (originalMode.isDefined()) {
      spark.conf().set(V2_ENABLE_MODE_KEY, originalMode.get());
    } else {
      spark.conf().unset(V2_ENABLE_MODE_KEY);
    }
  }
}
