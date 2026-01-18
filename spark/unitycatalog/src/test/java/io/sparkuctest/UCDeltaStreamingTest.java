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
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Streaming test suite for Delta Lake operations through Unity Catalog.
 *
 * <p>Tests structured streaming write and read operations with Delta format tables managed by Unity
 * Catalog.
 */
public class UCDeltaStreamingTest extends UCDeltaTableIntegrationBaseTest {

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
          for (long i = 1; i < 3; i += 1) {
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

  @TestAllTableTypes
  public void testStreamingReadFromTable(TableType tableType) throws Exception {
    withNewTable(
        "streaming_read_test",
        "id BIGINT, value STRING",
        tableType,
        (tableName) -> {
          SparkSession spark = spark();
          Option<String> originalMode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
          String queryName =
              "uc_streaming_read_"
                  + tableType.name().toLowerCase()
                  + "_"
                  + UUID.randomUUID().toString().replace("-", "");
          StreamingQuery query = null;
          List<List<String>> expected = new ArrayList<>();

          try {
            // Seed an initial commit (required for managed tables, harmless for external).
            spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
            spark.sql(String.format("INSERT INTO %s VALUES (0, 'seed')", tableName)).collect();
            expected.add(List.of("0", "seed"));
            // For managed tables, wait for UC to backfill staged commits before switching to V2.
            // This avoids a race condition where V2 streaming reads try to access staged commit
            // files that have already been backfilled/moved by UC.
            if (tableType == TableType.MANAGED) {
              Thread.sleep(500);
            }
            // Enable V2 for streaming reads.
            spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);
            Dataset<Row> input = spark.readStream().table(tableName);
            // Start the streaming query into a memory sink
            query =
                input
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .option("checkpointLocation", createTempCheckpointDir())
                    .outputMode("append")
                    .start();

            assertTrue(query.isActive(), "Streaming query should be active");

            // Write a few batches and verify the stream consumes them.
            // For writing, we disable V2 mode, write, then re-enable it for reading
            for (long i = 1; i < 3; i += 1) {
              String value = "value_" + i;

              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
              spark
                  .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", tableName, i, value))
                  .collect();
              // For managed tables, wait for UC to backfill staged commits before switching to V2.
              if (tableType == TableType.MANAGED) {
                Thread.sleep(500);
              }
              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);

              query.processAllAvailable();
              // Validate by checking if query and expected match.
              expected.add(List.of(String.valueOf(i), value));
              check(queryName, expected);
            }
          } finally {
            if (query != null) {
              query.stop();
              query.awaitTermination();
              assertFalse(query.isActive(), "Streaming query should have stopped");
            }
            spark.sql("DROP VIEW IF EXISTS " + queryName);
            restoreV2Mode(spark, originalMode);
          }
        });
  }

  @TestAllTableTypes
  public void testStreamingReadBackfillCleanupStress(TableType tableType) throws Exception {
    if (tableType != TableType.MANAGED) {
      return;
    }

    withNewTable(
        "streaming_read_backfill_cleanup_stress",
        "id BIGINT, value STRING",
        tableType,
        (tableName) -> {
          SparkSession spark = spark();
          Option<String> originalMode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
          String queryName =
              "uc_streaming_read_backfill_stress_" + UUID.randomUUID().toString().replace("-", "");
          StreamingQuery query = null;
          List<List<String>> expected = new ArrayList<>();

          AtomicBoolean stopCleanup = new AtomicBoolean(false);
          AtomicBoolean pauseCleanup = new AtomicBoolean(false);
          // Track the minimum version that can be safely deleted
          // (versions below this have been fully processed by streaming)
          AtomicLong safeToDeleteBelow = new AtomicLong(0);
          AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
          Thread cleanupThread = null;

          try {
            spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
            spark.sql(String.format("INSERT INTO %s VALUES (0, 'seed')", tableName)).collect();
            expected.add(List.of("0", "seed"));

            // Resolve log dir path before switching to STRICT mode
            // (DESCRIBE DETAIL doesn't work in V2 strict mode)
            Path logDir = resolveLogDirPath(spark, tableName);
            Path stagedDir = logDir.resolve("_staged_commits");

            spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);
            Dataset<Row> input = spark.readStream().table(tableName);
            query =
                input
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .option("checkpointLocation", createTempCheckpointDir())
                    .outputMode("append")
                    .start();
            assertTrue(query.isActive(), "Streaming query should be active");
            cleanupThread =
                new Thread(
                    () ->
                        aggressivelyDeleteStagedCommits(
                            logDir,
                            stagedDir,
                            stopCleanup,
                            pauseCleanup,
                            safeToDeleteBelow,
                            cleanupFailure),
                    "uc-staged-commit-cleaner");
            cleanupThread.setDaemon(true);
            cleanupThread.start();

            for (long i = 1; i <= 50; i += 1) {
              String value = "value_" + i;
              // Pause cleanup during V1 write and streaming read operations.
              pauseCleanup.set(true);
              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
              spark
                  .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", tableName, i, value))
                  .collect();
              expected.add(List.of(String.valueOf(i), value));
              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);

              // Streaming read - still paused to avoid race with concurrent deletion
              query.processAllAvailable();

              // After streaming read completes, we can delete older staged commits.
              // Keep a large buffer (10 versions) because streaming execution might
              // read historical log entries during delta log replay.
              if (i > 10) {
                safeToDeleteBelow.set(i - 10);
              }

              // Allow cleanup between iterations - cleanup will only delete
              // versions below safeToDeleteBelow (tests the Kernel fallback logic
              // for older versions that get cleaned up during subsequent reads)
              pauseCleanup.set(false);
              Thread.sleep(20); // Give cleanup thread a chance to run
            }

            check(queryName, expected);
          } finally {
            stopCleanup.set(true);
            if (cleanupThread != null) {
              cleanupThread.join(2000);
            }
            if (cleanupFailure.get() != null) {
              throw new RuntimeException("Staged commit cleanup failed", cleanupFailure.get());
            }
            if (query != null) {
              query.stop();
              query.awaitTermination();
              assertFalse(query.isActive(), "Streaming query should have stopped");
            }
            spark.sql("DROP VIEW IF EXISTS " + queryName);
            restoreV2Mode(spark, originalMode);
          }
        });
  }

  private static Path resolveLogDirPath(SparkSession spark, String tableName) {
    String quotedTable =
        Arrays.stream(tableName.split("\\."))
            .map(part -> "`" + part + "`")
            .collect(Collectors.joining("."));
    System.out.println(
        "DEBUG UCDeltaStreamingTest.resolveLogDirPath: tableName="
            + tableName
            + " quoted="
            + quotedTable);
    Row row = spark.sql("DESCRIBE DETAIL " + quotedTable).select("location").head();
    String location = row.getString(0);
    Path tablePath =
        location.startsWith("file:") ? Paths.get(URI.create(location)) : Paths.get(location);
    return tablePath.resolve("_delta_log");
  }

  private static void aggressivelyDeleteStagedCommits(
      Path logDir,
      Path stagedDir,
      AtomicBoolean stopCleanup,
      AtomicBoolean pauseCleanup,
      AtomicLong safeToDeleteBelow,
      AtomicReference<Throwable> cleanupFailure) {
    while (!stopCleanup.get()) {
      try {
        // Skip this iteration if cleanup is paused
        if (pauseCleanup.get()) {
          Thread.sleep(10);
          continue;
        }

        if (Files.isDirectory(stagedDir)) {
          try (DirectoryStream<Path> stream = Files.newDirectoryStream(stagedDir, "*.json")) {
            for (Path stagedPath : stream) {
              if (stopCleanup.get() || pauseCleanup.get()) {
                break;
              }
              String fileName = stagedPath.getFileName().toString();
              int dotIndex = fileName.indexOf('.');
              if (dotIndex <= 0) {
                continue;
              }
              long version;
              try {
                version = Long.parseLong(fileName.substring(0, dotIndex));
              } catch (NumberFormatException e) {
                continue;
              }
              // Only delete versions that have been fully processed by streaming
              // (version < safeToDeleteBelow) to avoid race conditions
              if (version >= safeToDeleteBelow.get()) {
                continue;
              }
              Path publishedPath = logDir.resolve(String.format("%020d.json", version));
              // Only delete if published version exists (UC has backfilled)
              if (Files.exists(publishedPath) && !pauseCleanup.get()) {
                try {
                  Files.deleteIfExists(stagedPath);
                } catch (java.nio.file.NoSuchFileException e) {
                  // File already deleted, ignore
                }
              }
            }
          }
        }
        Thread.sleep(10);
      } catch (Throwable t) {
        cleanupFailure.compareAndSet(null, t);
        stopCleanup.set(true);
      }
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

  private static void restoreV2Mode(SparkSession spark, Option<String> originalMode) {
    if (originalMode.isDefined()) {
      spark.conf().set(V2_ENABLE_MODE_KEY, originalMode.get());
    } else {
      spark.conf().unset(V2_ENABLE_MODE_KEY);
    }
  }
}
