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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
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

  @Test
  public void testStreamingReadWithStagedCommitDeletion() throws Exception {
    withNewTable(
        "streaming_read_staged_commit_deletion",
        "id BIGINT, value STRING",
        TableType.MANAGED,
        tableName -> {
          SparkSession spark = spark();
          Option<String> originalMode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
          String queryName =
              "uc_streaming_read_staged_commit_deletion_"
                  + UUID.randomUUID().toString().replace("-", "");
          StreamingQuery query = null;
          List<List<String>> expected = new ArrayList<>();

          ApiClient client = unityCatalogInfo().createApiClient();
          TablesApi tablesApi = new TablesApi(client);
          TableInfo tableInfo = tablesApi.getTable(tableName, false, false);
          Path logPath = resolveDeltaLogPath(tableInfo, spark, tableName);
          Path stagedCommitsDir = logPath.resolve("_staged_commits");
          Assumptions.assumeTrue(
              Files.isDirectory(stagedCommitsDir),
              "Missing staged commits directory at " + stagedCommitsDir);

          try {
            // Seed an initial commit before starting the streaming query.
            spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
            spark.sql(String.format("INSERT INTO %s VALUES (0, 'seed')", tableName)).collect();
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

            query.processAllAvailable();
            expected.add(List.of("0", "seed"));
            check(queryName, expected);

            long lastDeletedVersion = -1L;
            int deletions = 0;
            int iterations = 20;

            for (long i = 1; i <= iterations; i += 1) {
              String value = "value_" + i;

              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
              spark
                  .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", tableName, i, value))
                  .collect();
              spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);

              Path stagedCommit =
                  waitForNextStagedCommitFile(stagedCommitsDir, lastDeletedVersion, 5000);
              if (stagedCommit != null) {
                long version = parseStagedCommitVersion(stagedCommit.getFileName().toString());
                waitForPublishedCommit(logPath, version, 5000);

                deleteStagedCommitFile(stagedCommit);
                lastDeletedVersion = version;
                deletions += 1;
              }

              query.processAllAvailable();
              expected.add(List.of(String.valueOf(i), value));
              check(queryName, expected);
            }

            assertTrue(deletions > 0, "Expected staged commit deletions");
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

  private static Path resolveDeltaLogPath(TableInfo tableInfo, SparkSession spark, String tableName)
      throws Exception {
    String location = tableInfo.getStorageLocation();
    if (location == null || location.isEmpty()) {
      Row row = spark.sql("DESCRIBE DETAIL " + tableName).collectAsList().get(0);
      int locationIndex = row.fieldIndex("location");
      location = row.getString(locationIndex);
    }

    Assumptions.assumeTrue(
        location != null && !location.isEmpty(), "Table location is not available");

    URI locationUri = URI.create(location);
    if (locationUri.getScheme() != null) {
      Assumptions.assumeTrue(
          "file".equals(locationUri.getScheme()),
          "Test requires file-based tables, found: " + location);
      return Paths.get(locationUri).resolve("_delta_log");
    }

    return Paths.get(location).resolve("_delta_log");
  }

  private static Path waitForNextStagedCommitFile(
      Path stagedCommitsDir, long lastVersion, long timeoutMs) throws Exception {
    long deadlineNs = System.nanoTime() + timeoutMs * 1_000_000L;
    while (System.nanoTime() < deadlineNs) {
      Path candidate = null;
      long candidateVersion = Long.MAX_VALUE;
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(stagedCommitsDir, "*.json")) {
        for (Path path : stream) {
          long version = parseStagedCommitVersion(path.getFileName().toString());
          if (version > lastVersion && version < candidateVersion) {
            candidate = path;
            candidateVersion = version;
          }
        }
      }
      if (candidate != null) {
        return candidate;
      }
      Thread.sleep(50);
    }
    return null;
  }

  private static void waitForPublishedCommit(Path logPath, long version, long timeoutMs)
      throws Exception {
    Path commitPath = logPath.resolve(String.format("%020d.json", version));
    long deadlineNs = System.nanoTime() + timeoutMs * 1_000_000L;
    while (System.nanoTime() < deadlineNs) {
      if (Files.exists(commitPath)) {
        return;
      }
      Thread.sleep(50);
    }
    assertTrue(Files.exists(commitPath), "Published commit not found for version " + version);
  }

  private static void deleteStagedCommitFile(Path stagedCommit) throws Exception {
    Files.deleteIfExists(stagedCommit);
    assertFalse(
        Files.exists(stagedCommit), "Expected staged commit to be deleted: " + stagedCommit);
  }

  private static long parseStagedCommitVersion(String fileName) {
    int dotIndex = fileName.indexOf('.');
    if (dotIndex <= 0) {
      return -1L;
    }
    try {
      return Long.parseLong(fileName.substring(0, dotIndex));
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
