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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.spark.UCSingleCatalog;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import scala.Option;

/**
 * Long-running streaming read test intended to exercise UC credential refresh.
 *
 * <p>This test is opt-in and requires a remote UC endpoint. It runs a mostly idle streaming read
 * and periodically writes single rows to keep the stream active across the expected credential
 * expiration window. Progress is logged via {@code System.out.println}.
 */
public class UCCredentialRefreshStreamingTest {
  private static final String PREFIX = "CREDENTIAL_RENEWAL_TEST_";
  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";
  private static final String V2_ENABLE_MODE_STRICT = "STRICT";
  private static final String V2_ENABLE_MODE_NONE = "NONE";

  private static final long DEFAULT_DURATION_SECONDS = 5400L; // 90 minutes
  private static final long DEFAULT_WRITE_INTERVAL_SECONDS = 300L; // 5 minutes
  private static final long DEFAULT_EXPECT_REFRESH_AFTER_SECONDS = 3600L; // 1 hour

  @Test
  public void testLongRunningStreamingReadCredentialRefresh() throws Exception {
    assumeTrue(envAsBoolean(PREFIX + "ENABLED", false), "Credential refresh test not enabled");

    String catalogName = firstNonEmptyEnv("UC_CATALOG_NAME", "CATALOG_NAME");
    String schemaName = firstNonEmptyEnv("UC_SCHEMA_NAME", "SCHEMA_NAME");
    String catalogUri = firstNonEmptyEnv("UC_URI", "CATALOG_URI");
    String authToken = firstNonEmptyEnv("UC_TOKEN", "CATALOG_AUTH_TOKEN");
    String oauthUri = firstNonEmptyEnv("CATALOG_OAUTH_URI");
    String oauthClientId = firstNonEmptyEnv("CATALOG_OAUTH_CLIENT_ID");
    String oauthClientSecret = firstNonEmptyEnv("CATALOG_OAUTH_CLIENT_SECRET");

    assumeTrue(
        catalogName != null && schemaName != null && catalogUri != null,
        "Remote UC env not configured (catalog, schema, uri)");

    boolean useOauth = oauthUri != null && oauthClientId != null && oauthClientSecret != null;
    assumeTrue(useOauth || authToken != null, "Missing UC auth (token or OAuth)");

    long durationSeconds = envAsLong(PREFIX + "DURATION_SECONDS", DEFAULT_DURATION_SECONDS);
    long writeIntervalSeconds =
        envAsLong(PREFIX + "WRITE_INTERVAL_SECONDS", DEFAULT_WRITE_INTERVAL_SECONDS);
    long expectRefreshAfterSeconds =
        envAsLong(PREFIX + "EXPECT_REFRESH_AFTER_SECONDS", DEFAULT_EXPECT_REFRESH_AFTER_SECONDS);
    boolean renewEnabled = envAsBoolean(PREFIX + "RENEWAL_ENABLED", true);

    System.out.println(
        String.format(
            "CRED_REFRESH_TEST_START ts=%s durationSeconds=%d writeIntervalSeconds=%d expectRefreshAfterSeconds=%d renewEnabled=%s",
            Instant.now(),
            durationSeconds,
            writeIntervalSeconds,
            expectRefreshAfterSeconds,
            renewEnabled));

    SparkSession spark =
        SparkSession.builder()
            .appName("uc-credential-refresh-streaming-read")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog." + catalogName, UCSingleCatalog.class.getName())
            .config("spark.sql.catalog." + catalogName + ".uri", catalogUri)
            .getOrCreate();

    if (useOauth) {
      spark.conf().set("spark.sql.catalog." + catalogName + ".auth.type", "oauth");
      spark.conf().set("spark.sql.catalog." + catalogName + ".auth.oauth.uri", oauthUri);
      spark.conf().set("spark.sql.catalog." + catalogName + ".auth.oauth.clientId", oauthClientId);
      spark.conf().set(
          "spark.sql.catalog." + catalogName + ".auth.oauth.clientSecret", oauthClientSecret);
    } else {
      spark.conf().set("spark.sql.catalog." + catalogName + ".token", authToken);
    }
    spark.conf().set(
        "spark.sql.catalog." + catalogName + ".renewCredential.enabled",
        String.valueOf(renewEnabled));

    String tableName =
        firstNonEmptyEnv(
            PREFIX + "MANAGED_TABLE",
            catalogName + "." + schemaName + ".managedStreamingCredRenewal");
    String queryName =
        "uc_cred_refresh_streaming_"
            + UUID.randomUUID().toString().replace("-", "");

    StreamingQuery query = null;
    try {
      ensureManagedTableExists(spark, tableName);
      spark.sql("DELETE FROM " + tableName);

      // Seed an initial commit.
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

      long startMillis = System.currentTimeMillis();
      long endMillis = startMillis + durationSeconds * 1000L;
      boolean refreshWindowLogged = false;
      long expectedCount = 1L;

      while (System.currentTimeMillis() < endMillis) {
        long elapsedSeconds = (System.currentTimeMillis() - startMillis) / 1000L;
        if (!refreshWindowLogged && elapsedSeconds >= expectRefreshAfterSeconds) {
          System.out.println(
              String.format(
                  "CRED_REFRESH_WINDOW_REACHED elapsedSeconds=%d ts=%s",
                  elapsedSeconds, Instant.now()));
          refreshWindowLogged = true;
        }

        spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_NONE);
        spark
            .sql(
                String.format(
                    "INSERT INTO %s VALUES (%d, '%s')",
                    tableName, expectedCount, "value_" + expectedCount))
            .collect();
        spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);

        query.processAllAvailable();
        assertNoQueryException(query);

        expectedCount += 1;
        assertRowCount(spark, queryName, expectedCount);

        System.out.println(
            String.format(
                "CRED_REFRESH_TICK elapsedSeconds=%d rows=%d ts=%s",
                elapsedSeconds, expectedCount, Instant.now()));

        long sleepMillis = writeIntervalSeconds * 1000L;
        if (sleepMillis > 0 && System.currentTimeMillis() + sleepMillis < endMillis) {
          Thread.sleep(sleepMillis);
        }
      }

      System.out.println(
          String.format(
              "CRED_REFRESH_TEST_END elapsedSeconds=%d rows=%d ts=%s",
              (System.currentTimeMillis() - startMillis) / 1000L,
              expectedCount,
              Instant.now()));
    } finally {
      if (query != null) {
        try {
          query.stop();
          query.awaitTermination();
        } catch (Exception e) {
          System.out.println("CRED_REFRESH_STOP_EXCEPTION " + e);
        }
      }
      spark.sql("DROP VIEW IF EXISTS " + queryName);
      spark.stop();
    }
  }

  private static void ensureManagedTableExists(SparkSession spark, String tableName) {
    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + tableName
              + " (id BIGINT, value STRING) USING delta "
              + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    } catch (Exception e) {
      throw new IllegalStateException(
          "Managed table creation failed. Pre-create the table and set "
              + PREFIX
              + "MANAGED_TABLE to its name.",
          e);
    }
  }

  private static void assertNoQueryException(StreamingQuery query) {
    Option<?> exception = query.exception();
    if (exception != null && exception.isDefined()) {
      Object value = exception.get();
      if (value instanceof Throwable) {
        throw new IllegalStateException("Streaming query failed", (Throwable) value);
      }
      throw new IllegalStateException("Streaming query failed: " + value);
    }
  }

  private static void assertRowCount(SparkSession spark, String queryName, long expected) {
    List<Row> results = spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList();
    long actual = results.get(0).getLong(0);
    if (actual != expected) {
      throw new IllegalStateException(
          String.format(Locale.ROOT, "Expected %d rows but got %d", expected, actual));
    }
  }

  private static String createTempCheckpointDir() {
    try {
      return java.nio.file.Files.createTempDirectory("spark-checkpoint-")
          .toFile()
          .getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create checkpoint dir", e);
    }
  }

  private static boolean envAsBoolean(String name, boolean defaultValue) {
    String value = System.getenv(name);
    if (value == null) {
      return defaultValue;
    }
    return "true".equalsIgnoreCase(value.trim());
  }

  private static long envAsLong(String name, long defaultValue) {
    String value = System.getenv(name);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static String firstNonEmptyEnv(String... names) {
    for (String name : names) {
      if (name == null) {
        continue;
      }
      String value = System.getenv(name);
      if (value != null && !value.trim().isEmpty()) {
        return value.trim();
      }
    }
    return null;
  }
}
