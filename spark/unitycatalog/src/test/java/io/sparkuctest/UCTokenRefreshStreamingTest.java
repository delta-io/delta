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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

/**
 * Long-running streaming READ test to validate UC CCv2 token/credential refresh.
 *
 * <p>This test runs streaming reads for 65+ minutes to ensure that credential refresh works
 * correctly for long-running streaming jobs. Data is written via batch INSERTs and read via
 * streaming queries.
 *
 * <p>Run with: build/sbt "sparkUnityCatalog/testOnly io.sparkuctest.UCTokenRefreshStreamingTest"
 *
 * <p>Required environment variables:
 *
 * <ul>
 *   <li>UC_REMOTE=true
 *   <li>UC_URI - Unity Catalog server URI
 *   <li>UC_TOKEN - Authentication token (for PAT auth)
 *   <li>UC_CATALOG_NAME - Catalog name (e.g., "main")
 *   <li>UC_SCHEMA_NAME - Schema name (e.g., "demo_zh")
 *   <li>UC_BASE_TABLE_LOCATION - S3 base location for external tables
 * </ul>
 *
 * <p>Optional environment variables for OAuth:
 *
 * <ul>
 *   <li>CATALOG_OAUTH_URI - OAuth token endpoint
 *   <li>CATALOG_OAUTH_CLIENT_ID - OAuth client ID
 *   <li>CATALOG_OAUTH_CLIENT_SECRET - OAuth client secret
 * </ul>
 *
 * <p>Optional test control:
 *
 * <ul>
 *   <li>CREDENTIAL_RENEWAL_TEST_DURATION_SECONDS - Test duration (default: 3900 = 65 min)
 *   <li>CREDENTIAL_RENEWAL_TEST_RENEWAL_ENABLED - Enable credential renewal (default: true)
 * </ul>
 */
@Tag("long-running")
public class UCTokenRefreshStreamingTest {

  // Environment variable keys
  private static final String UC_URI = "UC_URI";
  private static final String UC_TOKEN = "UC_TOKEN";
  private static final String UC_CATALOG_NAME = "UC_CATALOG_NAME";
  private static final String UC_SCHEMA_NAME = "UC_SCHEMA_NAME";
  private static final String UC_BASE_TABLE_LOCATION = "UC_BASE_TABLE_LOCATION";

  // OAuth environment variables
  private static final String OAUTH_URI = envAsString("CATALOG_OAUTH_URI", "");
  private static final String OAUTH_CLIENT_ID = envAsString("CATALOG_OAUTH_CLIENT_ID", "");
  private static final String OAUTH_CLIENT_SECRET = envAsString("CATALOG_OAUTH_CLIENT_SECRET", "");

  // Test control
  private static final String PREFIX = "CREDENTIAL_RENEWAL_TEST_";
  private static final boolean RENEW_CRED_ENABLED = envAsBoolean(PREFIX + "RENEWAL_ENABLED", true);
  private static final long DURATION_SECONDS = envAsLong(PREFIX + "DURATION_SECONDS", 3900L);

  // Test intervals
  private static final long BATCH_INTERVAL_SECONDS = 30;
  private static final int ROWS_PER_BATCH = 100;
  private static final long VALIDATE_INTERVAL_SECONDS = 60;

  // Spark config keys
  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";
  private static final String V2_ENABLE_MODE_STRICT = "STRICT";

  // Table types
  private static final String MANAGED_TABLE_TYPE = "ManagedTable";
  private static final String EXTERNAL_TABLE_TYPE = "ExternalTable";

  // CCv2 markers
  private static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  private static final String DELTA_CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
  private static final String SUPPORTED = "supported";

  // Catalog config keys
  private static final String CATALOG_URI = "uri";
  private static final String CATALOG_TOKEN = "token";
  private static final String CATALOG_AUTH_TYPE = "auth.type";
  private static final String CATALOG_OAUTH_URI = "auth.oauth.uri";
  private static final String CATALOG_OAUTH_CLIENT_ID = "auth.oauth.clientId";
  private static final String CATALOG_OAUTH_CLIENT_SECRET = "auth.oauth.clientSecret";
  private static final String CATALOG_RENEW_CREDENTIAL_ENABLED = "renewCredential.enabled";

  // Runtime state
  private SparkSession spark;
  private String currentTableName;

  /** Creates a local temporary directory for checkpoint location. */
  private String createTempCheckpointDir() {
    try {
      return Files.createTempDirectory("spark-checkpoint-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @AfterEach
  public void afterEach() {
    if (currentTableName != null && spark != null) {
      try {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", currentTableName));
        System.out.println("Dropped table: " + currentTableName);
      } catch (Exception e) {
        System.out.println("Warning: Failed to drop table " + currentTableName + ": " + e);
      }
    }

    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /**
   * Main test: runs streaming READS for 65+ minutes with batch writes feeding data.
   *
   * <p>Parameterized across:
   *
   * <ul>
   *   <li>Table types: MANAGED, EXTERNAL
   *   <li>Auth types: PAT (static token), OAuth
   * </ul>
   */
  @ParameterizedTest
  @MethodSource("streamingReadTestArguments")
  public void testStreamingReadTokenRefresh(
      String tableType, String baseLocation, Map<String, String> catalogProps) throws Exception {

    if (!RENEW_CRED_ENABLED) {
      fail(
          "Credential renewal is disabled. "
              + "Set CREDENTIAL_RENEWAL_TEST_RENEWAL_ENABLED=true to run this test.");
    }

    String catalogName = envAsString(UC_CATALOG_NAME, "main");
    String schemaName = envAsString(UC_SCHEMA_NAME, "default");

    // Set up Spark with the given catalog props
    setupSparkSession(catalogName, catalogProps);

    // Create table
    String tableSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String tableName =
        String.format("streaming_read_test_%s_%s", tableType.toLowerCase(), tableSuffix);
    currentTableName = String.format("%s.%s.%s", catalogName, schemaName, tableName);

    createTable(currentTableName, tableType, baseLocation);
    assertCcv2Enabled(currentTableName, tableType);

    // Run the long-running streaming read test
    runStreamingReadTest(currentTableName, tableType, catalogProps);
  }

  private void setupSparkSession(String catalogName, Map<String, String> catalogProps) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("UC-Token-Refresh-Streaming-Read-Test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            // Delta Lake required configurations
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // AWS S3 configuration
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            // Disable AQE to avoid partition coalescing
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false");

    // Configure Unity Catalog
    String catalogKey = "spark.sql.catalog." + catalogName;
    builder.config(catalogKey, "io.unitycatalog.spark.UCSingleCatalog");

    // Set all catalog properties
    for (Map.Entry<String, String> entry : catalogProps.entrySet()) {
      builder.config(catalogKey + "." + entry.getKey(), entry.getValue());
    }

    spark = builder.getOrCreate();
    System.out.println("SparkSession created with catalog: " + catalogName);
  }

  private void createTable(String fullTableName, String tableType, String baseLocation) {
    String schemaName = fullTableName.substring(0, fullTableName.lastIndexOf('.'));

    // Ensure schema exists
    spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));

    if (MANAGED_TABLE_TYPE.equals(tableType)) {
      // Managed table - requires catalogManaged feature
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, batch_num BIGINT, value STRING) "
                  + "USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
              fullTableName));
    } else {
      // External table - requires LOCATION
      String tableLocation = baseLocation + "/" + UUID.randomUUID();
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, batch_num BIGINT, value STRING) "
                  + "USING DELTA LOCATION '%s'",
              fullTableName, tableLocation));
    }

    System.out.println("Created " + tableType + " table: " + fullTableName);
  }

  private void runStreamingReadTest(
      String tableName, String tableType, Map<String, String> catalogProps) throws Exception {

    Instant startTime = Instant.now();

    // Thread-safe data structures for background writer
    Set<String> expectedRowKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());
    AtomicLong batchCounter = new AtomicLong(0);
    AtomicLong totalRowsWritten = new AtomicLong(0);
    AtomicBoolean stopWriter = new AtomicBoolean(false);
    AtomicBoolean writerError = new AtomicBoolean(false);
    List<String> writerErrors = Collections.synchronizedList(new ArrayList<>());

    Option<String> originalV2Mode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
    SparkSession writerSpark = spark.newSession();
    restoreV2Mode(writerSpark, originalV2Mode);

    // Insert seed data before starting streaming read
    System.out.println("Inserting seed data...");
    insertBatch(writerSpark, tableName, batchCounter.getAndIncrement(), expectedRowKeys);
    totalRowsWritten.addAndGet(ROWS_PER_BATCH);

    // Set STRICT for streaming reads
    spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);

    // Start streaming read query with Trigger.ProcessingTime (realistic customer pattern)
    String queryName = "streaming_read_" + UUID.randomUUID().toString().replace("-", "");
    Dataset<Row> streamingInput =
        spark.readStream().format("delta").option("startingVersion", "0").table(tableName);

    StreamingQuery query =
        streamingInput
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .option("checkpointLocation", createTempCheckpointDir())
            .trigger(Trigger.ProcessingTime(BATCH_INTERVAL_SECONDS * 1000)) // Realistic!
            .outputMode("append")
            .start();

    System.out.println(
        String.format(
            "=== Starting UC Token Refresh Streaming READ Test (Realistic Simulation) ===\n"
                + "Table: %s\n"
                + "Table Type: %s\n"
                + "Auth Type: %s\n"
                + "Duration: %d seconds (%d minutes)\n"
                + "Credential Renewal Enabled: %s\n"
                + "Trigger interval: %d seconds (Trigger.ProcessingTime)\n"
                + "Rows per batch: %d\n"
                + "Pattern: Background writer + automatic streaming trigger",
            tableName,
            tableType,
            catalogProps.containsKey(CATALOG_AUTH_TYPE) ? "OAuth" : "PAT",
            DURATION_SECONDS,
            DURATION_SECONDS / 60,
            RENEW_CRED_ENABLED,
            BATCH_INTERVAL_SECONDS,
            ROWS_PER_BATCH));

    // Background writer thread - simulates realistic data ingestion
    ExecutorService writerExecutor = Executors.newSingleThreadExecutor();

    try {
      assertTrue(query.isActive(), "Streaming query should be active");

      // Start background writer thread
      writerExecutor.submit(
          () -> {
            // Use a dedicated SparkSession so writer config does not affect the reader
            while (!stopWriter.get() && !Thread.currentThread().isInterrupted()) {
              try {
                long batchNum = batchCounter.getAndIncrement();

                insertBatch(writerSpark, tableName, batchNum, expectedRowKeys);
                totalRowsWritten.addAndGet(ROWS_PER_BATCH);

                // Log progress
                Duration elapsed = Duration.between(startTime, Instant.now());
                System.out.println(
                    String.format(
                        "[Writer] Batch %d: %d rows written | Total: %d rows | Elapsed: %d min",
                        batchNum, ROWS_PER_BATCH, totalRowsWritten.get(), elapsed.toMinutes()));

                // Sleep until next batch
                Thread.sleep(TimeUnit.SECONDS.toMillis(BATCH_INTERVAL_SECONDS));

              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[Writer] Interrupted, stopping...");
                break;
              } catch (Exception e) {
                writerError.set(true);
                writerErrors.add(e.getMessage());
                System.out.println("[Writer] ERROR: " + e.getMessage());
                e.printStackTrace();
                break;
              }
            }
            System.out.println("[Writer] Background writer thread stopped");
          });

      // Main thread: wait for test duration while monitoring
      long lastLogTime = System.currentTimeMillis();
      long logIntervalMs = TimeUnit.SECONDS.toMillis(VALIDATE_INTERVAL_SECONDS);
      long lastLoggedBatchId = -1;

      while (Duration.between(startTime, Instant.now()).getSeconds() < DURATION_SECONDS) {
        // Check for writer errors
        if (writerError.get()) {
          fail("Background writer encountered an error: " + String.join(", ", writerErrors));
        }

        // Check query is still active
        if (!query.isActive()) {
          Duration elapsed = Duration.between(startTime, Instant.now());
          fail(
              String.format(
                  "Streaming read query died after %d minutes! Token refresh may have failed.",
                  elapsed.toMinutes()));
        }

        StreamingQueryProgress progress = query.lastProgress();
        if (progress != null && progress.batchId() != lastLoggedBatchId) {
          lastLoggedBatchId = progress.batchId();
          System.out.println(
              String.format(
                  "[Reader] Batch %d: %d rows read | inputRowsPerSecond=%.2f | "
                      + "processedRowsPerSecond=%.2f",
                  progress.batchId(),
                  progress.numInputRows(),
                  progress.inputRowsPerSecond(),
                  progress.processedRowsPerSecond()));
        }

        // Periodic status log
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime >= logIntervalMs) {
          Duration elapsed = Duration.between(startTime, Instant.now());
          System.out.println(
              String.format(
                  "[Monitor] Status: %d min elapsed | %d batches | %d rows | Query active: %s",
                  elapsed.toMinutes(),
                  batchCounter.get(),
                  totalRowsWritten.get(),
                  query.isActive()));
          lastLogTime = currentTime;
        }

        // Sleep for a short interval to avoid busy-waiting
        Thread.sleep(5000);
      }

      // Stop the writer thread
      System.out.println("=== Stopping background writer ===");
      stopWriter.set(true);
      writerExecutor.shutdown();
      if (!writerExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        writerExecutor.shutdownNow();
      }

      // Wait for streaming to catch up with all written data
      System.out.println("=== Waiting for streaming to catch up ===");
      query.processAllAvailable(); // Only used at the end for final validation

      // Final validation
      System.out.println("=== Running final validation ===");
      validateStreamingRead(queryName, expectedRowKeys);

      Duration totalElapsed = Duration.between(startTime, Instant.now());
      System.out.println(
          String.format(
              "=== TEST PASSED ===\n"
                  + "Total duration: %d minutes\n"
                  + "Total batches: %d\n"
                  + "Total rows written and read: %d\n"
                  + "Pattern: Trigger.ProcessingTime(%d seconds) + background writer",
              totalElapsed.toMinutes(),
              batchCounter.get(),
              totalRowsWritten.get(),
              BATCH_INTERVAL_SECONDS));

    } finally {
      // Ensure writer is stopped
      stopWriter.set(true);
      writerExecutor.shutdownNow();

      if (query != null) {
        query.stop();
        query.awaitTermination();
        System.out.println("Streaming query stopped");
      }
      spark.sql("DROP VIEW IF EXISTS " + queryName);
      restoreV2Mode(spark, originalV2Mode);
    }
  }

  private void insertBatch(String tableName, long batchNum, Set<String> expectedRowKeys) {
    insertBatch(spark, tableName, batchNum, expectedRowKeys);
  }

  private void insertBatch(
      SparkSession session, String tableName, long batchNum, Set<String> expectedRowKeys) {
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      long id = batchNum * ROWS_PER_BATCH + i;
      String value = String.format("batch_%d_row_%d", batchNum, i);
      if (i > 0) values.append(", ");
      values.append(String.format("(%d, %d, '%s')", id, batchNum, value));
      expectedRowKeys.add(String.format("%d:%d", id, batchNum));
    }
    session.sql(String.format("INSERT INTO %s VALUES %s", tableName, values));
  }

  private void validateStreamingRead(String queryName, Set<String> expectedRowKeys) {
    System.out.println(
        String.format("Validating streaming read: expecting %d rows...", expectedRowKeys.size()));

    Dataset<Row> df = spark.sql(String.format("SELECT id, batch_num, value FROM %s", queryName));
    Row[] actualRows = (Row[]) df.collect();

    Set<String> actualRowKeys = new HashSet<>();
    List<String> issues = new ArrayList<>();

    for (Row row : actualRows) {
      long id = row.getLong(0);
      long batchNum = row.getLong(1);
      String rowKey = String.format("%d:%d", id, batchNum);
      actualRowKeys.add(rowKey);

      if (!expectedRowKeys.contains(rowKey)) {
        issues.add(String.format("EXTRA ROW: %s", rowKey));
      }
    }

    for (String expectedKey : expectedRowKeys) {
      if (!actualRowKeys.contains(expectedKey)) {
        issues.add(String.format("MISSING ROW: %s", expectedKey));
      }
    }

    if (!issues.isEmpty()) {
      String errorMsg =
          String.format(
              "Streaming read validation failed with %d issues:\n%s",
              issues.size(), String.join("\n", issues.subList(0, Math.min(20, issues.size()))));
      if (issues.size() > 20) {
        errorMsg += String.format("\n... and %d more issues", issues.size() - 20);
      }
      fail(errorMsg);
    }

    System.out.println(
        String.format(
            "Validation passed: %d rows read (expected=%d)",
            actualRows.length, expectedRowKeys.size()));
  }

  private void restoreV2Mode(SparkSession session, Option<String> originalMode) {
    if (originalMode.isDefined()) {
      session.conf().set(V2_ENABLE_MODE_KEY, originalMode.get());
    } else {
      session.conf().unset(V2_ENABLE_MODE_KEY);
    }
  }

  private void assertCcv2Enabled(String tableName, String tableType) {
    if (!MANAGED_TABLE_TYPE.equals(tableType)) {
      return;
    }

    Dataset<Row> rows = spark.sql("DESC EXTENDED " + tableName);
    String tableProperties = null;
    for (Row row : rows.collectAsList()) {
      if (!row.isNullAt(0) && "Table Properties".equals(row.getString(0))) {
        tableProperties = row.isNullAt(1) ? null : row.getString(1);
        break;
      }
    }

    assertNotNull(tableProperties, "DESC EXTENDED did not return Table Properties");
    assertTrue(
        tableProperties.contains(UC_TABLE_ID_KEY),
        "Expected UC table ID in Table Properties to confirm CCv2");
    assertTrue(
        tableProperties.contains(String.format("%s=%s", DELTA_CATALOG_MANAGED_KEY, SUPPORTED)),
        "Expected catalogManaged feature enabled in Table Properties");
  }

  /** Generates test arguments: combinations of table types, locations, and auth configs. */
  public static Stream<Arguments> streamingReadTestArguments() {
    List<Arguments> args = new ArrayList<>();

    String serverUri = envAsString(UC_URI, "");
    String authToken = envAsString(UC_TOKEN, "");
    String baseLocation = envAsString(UC_BASE_TABLE_LOCATION, "");

    if (isEmptyOrNull(serverUri) || isEmptyOrNull(baseLocation)) {
      System.out.println(
          "WARNING: UC_URI or UC_BASE_TABLE_LOCATION not set. Skipping streaming read tests.");
      return Stream.empty();
    }

    List<String> tableTypes = List.of(MANAGED_TABLE_TYPE, EXTERNAL_TABLE_TYPE);

    for (String tableType : tableTypes) {
      // PAT authentication (if token is provided)
      if (!isEmptyOrNull(authToken)) {
        args.add(
            Arguments.of(
                tableType,
                baseLocation,
                Map.of(
                    CATALOG_URI, serverUri,
                    CATALOG_TOKEN, authToken,
                    CATALOG_RENEW_CREDENTIAL_ENABLED, String.valueOf(RENEW_CRED_ENABLED))));
      }

      // OAuth authentication (if OAuth config is provided)
      if (!isEmptyOrNull(OAUTH_URI)) {
        args.add(
            Arguments.of(
                tableType,
                baseLocation,
                Map.of(
                    CATALOG_URI, serverUri,
                    CATALOG_AUTH_TYPE, "oauth",
                    CATALOG_OAUTH_URI, OAUTH_URI,
                    CATALOG_OAUTH_CLIENT_ID, OAUTH_CLIENT_ID,
                    CATALOG_OAUTH_CLIENT_SECRET, OAUTH_CLIENT_SECRET,
                    CATALOG_RENEW_CREDENTIAL_ENABLED, String.valueOf(RENEW_CRED_ENABLED))));
      }
    }

    System.out.println("Generated " + args.size() + " test configurations");
    return args.stream();
  }

  // Utility methods
  private static String envAsString(String key, String defaultValue) {
    return System.getenv().getOrDefault(key, defaultValue);
  }

  private static long envAsLong(String key, long defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  private static boolean envAsBoolean(String key, boolean defaultValue) {
    String value = System.getenv().get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  private static boolean isEmptyOrNull(String value) {
    return value == null || value.isEmpty();
  }
}
