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

import static org.junit.jupiter.api.Assertions.*;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.DeltaGetCommitsResponse;
import io.unitycatalog.client.model.TableInfo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Streaming test suite for Delta Lake operations through Unity Catalog.
 *
 * <p>Tests structured streaming write and read operations with Delta format tables managed by Unity
 * Catalog.
 */
public class UCDeltaStreamingTest extends UCDeltaTableIntegrationBaseTest {
  private static final class DebugBuffer {
    private final java.util.ArrayDeque<String> events = new java.util.ArrayDeque<>();
    private final int maxEvents;

    private DebugBuffer(int maxEvents) {
      this.maxEvents = maxEvents;
    }

    private void record(String format, Object... args) {
      String prefix =
          "[UCDeltaStreamingTest]"
              + "[t="
              + System.currentTimeMillis()
              + "]"
              + "[thread="
              + Thread.currentThread().getName()
              + "] ";
      String line = prefix + String.format(format, args);
      if (events.size() >= maxEvents) {
        events.removeFirst();
      }
      events.addLast(line);
    }

    private void dumpToStdout(String reason, Throwable t) {
      System.out.println("========== UCDeltaStreamingTest DEBUG DUMP BEGIN ==========");
      System.out.println("reason=" + reason);
      if (t != null) {
        System.out.println("throwable=" + t);
      }
      for (String e : events) {
        System.out.println(e);
      }
      System.out.println("========== UCDeltaStreamingTest DEBUG DUMP END ==========");
    }
  }

  private static void dbgQuery(DebugBuffer dbg, StreamingQuery query) {
    if (query == null) {
      return;
    }
    try {
      dbg.record(
          "query.id=%s runId=%s name=%s isActive=%s",
          query.id(), query.runId(), query.name(), query.isActive());
      dbg.record("query.status=%s", query.status());
      if (query.lastProgress() != null) {
        dbg.record("query.lastProgress=%s", query.lastProgress());
      }
      try {
        org.apache.spark.sql.streaming.StreamingQueryProgress[] recentProgress =
            query.recentProgress();
        if (recentProgress != null) {
          dbg.record("query.recentProgress.length=%d", recentProgress.length);
          int max = Math.min(recentProgress.length, 10);
          for (int i = 0; i < max; i += 1) {
            dbg.record("query.recentProgress[%d]=%s", i, recentProgress[i]);
          }
        }
      } catch (Exception e) {
        dbg.record("query.recentProgress failed: %s", e);
      }
    } catch (Exception e) {
      dbg.record("dbgQuery failed: %s", e);
    }
  }

  private static void dbgDeltaHistoryLatestVersion(
      DebugBuffer dbg, SparkSession spark, String tableName) {
    try {
      List<Row> rows = spark.sql("DESCRIBE HISTORY " + tableName + " LIMIT 5").collectAsList();
      dbg.record("DESCRIBE HISTORY %s (top %d): %s", tableName, rows.size(), rows);
    } catch (Exception e) {
      dbg.record("DESCRIBE HISTORY failed for %s: %s", tableName, e);
    }
  }

  private static void dbgUCLatestRatifiedVersion(
      DebugBuffer dbg, ApiClient client, String tableName) {
    try {
      TablesApi tablesApi = new TablesApi(client);
      TableInfo tableInfo = tablesApi.getTable(tableName, false, false);
      DeltaCommitsApi deltaCommitsApi = new DeltaCommitsApi(client);
      DeltaGetCommitsResponse resp =
          deltaCommitsApi.getCommits(
              new DeltaGetCommits().tableId(tableInfo.getTableId()).startVersion(0L));
      dbg.record(
          "UC ratified latestTableVersion=%s (tableId=%s, table=%s)",
          resp.getLatestTableVersion(), tableInfo.getTableId(), tableName);
    } catch (Exception e) {
      dbg.record("UC getCommits failed for %s: %s", tableName, e);
    }
  }

  private static void awaitMemorySinkUpToId(
      DebugBuffer dbg,
      SparkSession spark,
      StreamingQuery query,
      String queryName,
      String sourceTableName,
      ApiClient client,
      long expectedMaxId,
      String expectedValue,
      long timeoutMs)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    long nextProcessAllAvailableMs = 0L;
    while (true) {
      long nowMs = System.currentTimeMillis();
      if (query != null && !query.isActive()) {
        dbg.record("awaitMemorySinkUpToId: query is not active");
      }

      // Try to make progress.
      if (query != null && nowMs >= nextProcessAllAvailableMs) {
        query.processAllAvailable();
        // Don't hammer UC / streaming planning loops too hard; ratification is asynchronous.
        nextProcessAllAvailableMs = nowMs + 200L;
      }

      try {
        List<Row> stats =
            spark.sql("SELECT count(*) AS c, max(id) AS m FROM " + queryName).collectAsList();
        if (!stats.isEmpty()) {
          Row r = stats.get(0);
          long count = ((Number) r.get(0)).longValue();
          long maxId = r.isNullAt(1) ? -1L : ((Number) r.get(1)).longValue();
          if (count == expectedMaxId + 1 && maxId == expectedMaxId) {
            List<Row> v =
                spark
                    .sql(
                        String.format(
                            "SELECT value FROM %s WHERE id = %d", queryName, expectedMaxId))
                    .collectAsList();
            if (!v.isEmpty() && expectedValue.equals(String.valueOf(v.get(0).get(0)))) {
              return;
            }
          }
        }
      } catch (Exception e) {
        dbg.record("awaitMemorySinkUpToId: failed to query memory sink: %s", e);
      }

      if (System.currentTimeMillis() >= deadlineMs) {
        dbg.record(
            "awaitMemorySinkUpToId TIMEOUT: expectedMaxId=%d expectedValue=%s",
            expectedMaxId, expectedValue);
        dbgQuery(dbg, query);
        dbgDeltaHistoryLatestVersion(dbg, spark, sourceTableName);
        dbgUCLatestRatifiedVersion(dbg, client, sourceTableName);
        throw new AssertionError(
            "Timed out waiting for memory sink "
                + queryName
                + " to reach max(id)="
                + expectedMaxId);
      }
      Thread.sleep(50);
    }
  }

  /**
   * Creates a local temporary directory for checkpoint location. Checkpoint must be on local
   * filesystem since Spark doesn't have direct cloud storage credentials (credentials are managed
   * by UC server for catalog-managed tables only).
   */
  public String createTempCheckpointDir() {
    try {
      return Files.createTempDirectory("spark-checkpoint-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  /*
  @TestAllTableTypes
  public void testStreamingWriteToManagedTable(TableType tableType) throws Exception {
    withNewTable(
        "streaming_write_test",
        "id BIGINT, value STRING",
        tableType,
        (tableName) -> {
          // Define schema for the stream
          StructType schema =
              new StructType(
                  new StructField[] {
                    new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("value", DataTypes.StringType, false, Metadata.empty())
                  });

          // Create MemoryStream - using Scala companion object with proper encoder via shims
          var memoryStream =
              StreamingTestShims.MemoryStream().apply(Encoders.row(schema), spark().sqlContext());

          // Start streaming query writing to the Unity Catalog managed table
          StreamingQuery query =
              memoryStream
                  .toDF()
                  .writeStream()
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", createTempCheckpointDir())
                  .toTable(tableName);

          // Assert that the query is active
          assertTrue(query.isActive(), "Streaming query should be active");

          // Let's do 3 rounds testing, and for every round, adding 1 row and waiting to be
          // available, and finally verify the results and unity catalog latest version are
          // expected.
          ApiClient client = unityCatalogInfo().createApiClient();
          for (long i = 1; i <= 3; i += 1) {
            Seq<Row> batchRow = createRowsAsSeq(RowFactory.create(i, String.valueOf(i)));
            memoryStream.addData(batchRow);

            // Process all available data
            query.processAllAvailable();

            // Verify the content
            check(
                tableName,
                LongStream.range(1, i + 1)
                    .mapToObj(idx -> List.of(String.valueOf(idx), String.valueOf(idx)))
                    .collect(Collectors.toUnmodifiableList()));

            // The UC server should have the latest version, for managed table.
            if (TableType.MANAGED == tableType) {
              assertUCManagedTableVersion(i, tableName, client);
            }
          }

          // Stop the stream.
          query.stop();
          query.awaitTermination();

          // Assert that the query has stopped
          assertFalse(query.isActive(), "Streaming query should have stopped");
        });
  }
  */

  @TestAllTableTypes
  public void testStreamingReadFromTable(TableType tableType) throws Exception {
    for (int _repeats = 0; _repeats < 5; _repeats++) {
      DebugBuffer dbg = new DebugBuffer(/* maxEvents= */ 2000);
      String uniqueTableName =
          "streaming_read_test_" + UUID.randomUUID().toString().replace("-", "");
      dbg.record(
          "BEGIN repeat=%d tableType=%s uniqueTableName=%s", _repeats, tableType, uniqueTableName);
      withNewTable(
          uniqueTableName,
          "id BIGINT, value STRING",
          tableType,
          (tableName) -> {
            SparkSession spark = spark();
            ApiClient client = unityCatalogInfo().createApiClient();
            String queryName =
                "uc_streaming_read_"
                    + tableType.name().toLowerCase()
                    + "_"
                    + UUID.randomUUID().toString().replace("-", "");
            StreamingQuery query = null;
            String checkpointLocation = null;
            boolean failed = false;

            try {
              dbg.record("withNewTable: tableName=%s queryName=%s", tableName, queryName);

              // Seed an initial commit (required for managed tables, harmless for external).
              dbg.record("SQL: INSERT seed into %s", tableName);
              spark.sql(String.format("INSERT INTO %s VALUES (0, 'seed')", tableName)).collect();

              Dataset<Row> input = spark.readStream().table(tableName);
              // Start the streaming query into a memory sink
              checkpointLocation = createTempCheckpointDir();
              dbg.record("Starting query: name=%s checkpoint=%s", queryName, checkpointLocation);
              query =
                  input
                      .writeStream()
                      .format("memory")
                      .queryName(queryName)
                      .option("checkpointLocation", checkpointLocation)
                      .outputMode("append")
                      .start();

              dbg.record("Started query. isActive=%s", query.isActive());
              assertTrue(query.isActive(), "Streaming query should be active");

              // Write a batch of commits first, then wait for the stream to catch up. UC
              // ratification can lag Delta commits; streaming should not fail, and should
              // eventually become consistent.
              final long maxIdToWrite = 500L;
              for (long i = 1; i <= maxIdToWrite; i += 1) {
                String value = "value_" + i;
                dbg.record("SQL: INSERT (%d, '%s') into %s", i, value, tableName);
                spark
                    .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", tableName, i, value))
                    .collect();
              }

              awaitMemorySinkUpToId(
                  dbg,
                  spark,
                  query,
                  queryName,
                  tableName,
                  client,
                  /* expectedMaxId= */ maxIdToWrite,
                  /* expectedValue= */ "value_" + maxIdToWrite,
                  /* timeoutMs= */ 60_000L);
            } catch (Throwable t) {
              failed = true;
              dbg.record("Top-level failure: %s", t);
              dbgQuery(dbg, query);
              dbgDeltaHistoryLatestVersion(dbg, spark, tableName);
              dbgUCLatestRatifiedVersion(dbg, client, tableName);
              dbg.dumpToStdout(
                  "top-level failure tableName="
                      + tableName
                      + " queryName="
                      + queryName
                      + " checkpoint="
                      + checkpointLocation,
                  t);
              throw t;
            } finally {
              dbg.record("FINALLY: query is %snull", (query == null ? "" : "non-"));
              if (query != null) {
                if (!failed) {
                  dbg.record("FINALLY: extra processAllAvailable()");
                  try {
                    // TODO: remove additional processAllAvailable once interrupt is handled
                    // gracefully
                    query.processAllAvailable();
                  } catch (Exception e) {
                    dbg.record("FINALLY: processAllAvailable threw: %s", e);
                  }
                }
                dbg.record("FINALLY: awaitTermination(10000)");
                try {
                  query.awaitTermination(10000);
                } catch (Exception e) {
                  dbg.record("FINALLY: awaitTermination threw: %s", e);
                }
                dbg.record("FINALLY: stop()");
                try {
                  query.stop();
                } catch (Exception e) {
                  dbg.record("FINALLY: stop threw: %s", e);
                }
                dbg.record("FINALLY: post-stop isActive=%s", query.isActive());
                dbgQuery(dbg, query);
                assertFalse(query.isActive(), "Streaming query should have stopped");
              }
              dbg.record("SQL: DROP VIEW IF EXISTS %s", queryName);
              spark.sql("DROP VIEW IF EXISTS " + queryName);
            }
          });
      dbg.record(
          "END repeat=%d tableType=%s uniqueTableName=%s", _repeats, tableType, uniqueTableName);
    }
  }

  private void assertUCManagedTableVersion(long expectedVersion, String tableName, ApiClient client)
      throws ApiException {
    // Get the table info.
    TablesApi tablesApi = new TablesApi(client);
    TableInfo tableInfo = tablesApi.getTable(tableName, false, false);

    // Get the latest UC commit version.
    DeltaCommitsApi deltaCommitsApi = new DeltaCommitsApi(client);
    DeltaGetCommitsResponse resp =
        deltaCommitsApi.getCommits(
            new DeltaGetCommits().tableId(tableInfo.getTableId()).startVersion(0L));
    assertNotNull(resp, "DeltaGetCommits response should not be null");
    assertNotNull(resp.getLatestTableVersion(), "Latest table version should not be null");

    // The UC server should have the latest version.
    assertEquals(expectedVersion, resp.getLatestTableVersion());
  }

  private static Seq<Row> createRowsAsSeq(Row... rows) {
    return JavaConverters.asScalaIteratorConverter(Arrays.asList(rows).iterator())
        .asScala()
        .toSeq();
  }
}
