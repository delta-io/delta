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
import java.util.ArrayList;
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
    String uniqueTableName = "streaming_read_test_" + UUID.randomUUID().toString().replace("-", "");
    withNewTable(
        uniqueTableName,
        "id BIGINT, value STRING",
        tableType,
        (tableName) -> {
          SparkSession spark = spark();
          String queryName =
              "uc_streaming_read_"
                  + tableType.name().toLowerCase()
                  + "_"
                  + UUID.randomUUID().toString().replace("-", "");
          StreamingQuery query = null;

          try {
            List<List<String>> expected = new ArrayList<>();
            // Seed an initial commit (required for managed tables, harmless for external).
            spark.sql(String.format("INSERT INTO %s VALUES (0, 'seed')", tableName)).collect();
            expected.add(List.of("0", "seed"));
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
            for (long i = 1; i <= 3; i += 1) {
              String value = "value_" + i;
              spark
                  .sql(String.format("INSERT INTO %s VALUES (%d, '%s')", tableName, i, value))
                  .collect();

              query.processAllAvailable();
              // Validate by checking if query and expected match.
              expected.add(List.of(String.valueOf(i), value));
              check(queryName, expected);
            }
          } finally {
            if (query != null) {
              // TODO: remove additional processAllAvailable once interrupt is handled gracefully
              query.processAllAvailable();
              query.awaitTermination(10000);
              query.stop();
              assertFalse(query.isActive(), "Streaming query should have stopped");
            }
            spark.sql("DROP VIEW IF EXISTS " + queryName);
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
}
