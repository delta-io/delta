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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.TableInfo;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import scala.Option;

public class UCDeltaTableStreamingTest extends UCDeltaTableIntegrationBaseTest {

  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";
  private static final String V2_ENABLE_MODE_STRICT = "STRICT";
  private static final int UC_COMMIT_POLL_ATTEMPTS = 20;
  private static final long UC_COMMIT_POLL_SLEEP_MS = 250L;

  @Test
  public void testStreamingReadsUcManagedTable() throws Exception {
    SparkSession spark = getSparkSession();
    Option<String> originalMode = spark.conf().getOption(V2_ENABLE_MODE_KEY);
    String tableName = "streaming_uc_managed";
    String fullTableName = getCatalogName() + ".default." + tableName;

    try {
      sql(
          "CREATE TABLE %s USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported') "
              + "AS SELECT CAST(0 AS INT) AS id, CAST('seed' AS STRING) AS value",
          fullTableName);
      long expectedVersion = waitForUcCommitVisibility(fullTableName, 0L);

      File checkpointDir = Files.createTempDirectory("uc-streaming-checkpoint-").toFile();
      String queryName = "uc_streaming_" + UUID.randomUUID().toString().replace("-", "");
      StreamingQuery query = null;
      SparkSession writerSpark = null;

      try {
        spark.conf().set(V2_ENABLE_MODE_KEY, V2_ENABLE_MODE_STRICT);
        assertEquals(V2_ENABLE_MODE_STRICT, spark.conf().get(V2_ENABLE_MODE_KEY));

        // Use a separate session with the original mode for batch inserts; do not stop it since
        // sessions share the SparkContext.
        writerSpark = spark.newSession();
        restoreV2Mode(writerSpark, originalMode);

        Dataset<Row> input =
            spark.readStream().format("delta").option("startingVersion", "0").table(fullTableName);

        query =
            input
                .writeStream()
                .format("memory")
                .queryName(queryName)
                .option("checkpointLocation", checkpointDir.getAbsolutePath())
                .outputMode("append")
                .start();

        expectedVersion = waitForUcCommitVisibility(fullTableName, expectedVersion);

        query.processAllAvailable();

        List<List<String>> expected = new ArrayList<>();
        expected.add(List.of("0", "seed"));
        check(queryName, expected);

        assertTrue(
            query.lastProgress() != null, "Expected initial streaming progress to be available");
        long lastBatchId = query.lastProgress().batchId();

        for (int i = 1; i <= 3; i++) {
          String value = "value_" + i;
          writerSpark
              .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", fullTableName, i, value))
              .collect();
          expectedVersion = waitForUcCommitVisibility(fullTableName, expectedVersion + 1);

          query.processAllAvailable();

          assertTrue(query.lastProgress() != null, "Expected streaming progress after insert " + i);
          long batchId = query.lastProgress().batchId();
          assertTrue(batchId > lastBatchId, "Expected a new micro-batch after insert " + i);
          lastBatchId = batchId;

          expected.add(List.of(String.valueOf(i), value));
          check(queryName, expected);
        }
      } finally {
        if (query != null) {
          query.stop();
        }
        spark.sql("DROP VIEW IF EXISTS " + queryName);
        deleteRecursively(checkpointDir);
      }
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
      restoreV2Mode(spark, originalMode);
    }
  }

  /**
   * Polls UC commit state until the UC-visible version is at least the expected version.
   *
   * <ul>
   *   <li>Look up the UC table to get its table ID and storage location.
   *   <li>Call UC {@code getCommits} to find the latest table version and ratified commits.
   *   <li>Compute the UC-visible version as max(latest, max ratified commit version).
   *   <li>Retry until UC reports a version &gt;= expectedVersion or the poll budget is exhausted.
   * </ul>
   *
   * <p>This is test-only waiting to avoid flakiness in streaming offset resolution when UC commit
   * metadata is slightly behind. It does not mutate UC state, which matches production behavior.
   */
  private long waitForUcCommitVisibility(String fullTableName, long expectedVersion)
      throws Exception {
    ApiClient apiClient = createClient();
    TablesApi tablesApi = new TablesApi(apiClient);
    TableInfo tableInfo = tablesApi.getTable(fullTableName, true, true);
    if (tableInfo.getStorageLocation() == null) {
      throw new IllegalStateException("Missing storage location for table " + fullTableName);
    }

    Map<String, String> authConfig = new HashMap<>();
    authConfig.put("type", "static");
    authConfig.put("token", getServerToken());
    TokenProvider tokenProvider = TokenProvider.create(authConfig);
    URI tableUri = URI.create(tableInfo.getStorageLocation());

    try (UCClient ucClient = new UCTokenBasedRestClient(getServerUri(), tokenProvider)) {
      long lastLatest = -1L;
      long lastMaxCommit = -1L;
      for (int attempt = 0; attempt < UC_COMMIT_POLL_ATTEMPTS; attempt++) {
        GetCommitsResponse response =
            ucClient.getCommits(
                tableInfo.getTableId(), tableUri, Optional.empty(), Optional.empty());
        long latest = response.getLatestTableVersion();
        long maxCommitVersion =
            response.getCommits().stream().mapToLong(Commit::getVersion).max().orElse(latest);
        long visibleVersion = Math.max(latest, maxCommitVersion);
        lastLatest = latest;
        lastMaxCommit = maxCommitVersion;
        if (visibleVersion >= expectedVersion) {
          return visibleVersion;
        }
        Thread.sleep(UC_COMMIT_POLL_SLEEP_MS);
      }
      throw new IllegalStateException(
          "UC commit coordinator did not report version >= "
              + expectedVersion
              + " for "
              + fullTableName
              + " (latest="
              + lastLatest
              + ", maxCommit="
              + lastMaxCommit
              + ")");
    }
  }

  private static void restoreV2Mode(SparkSession spark, Option<String> originalMode) {
    if (originalMode.isDefined()) {
      spark.conf().set(V2_ENABLE_MODE_KEY, originalMode.get());
    } else {
      spark.conf().unset(V2_ENABLE_MODE_KEY);
    }
  }

  private static void deleteRecursively(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File child : files) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }
}
