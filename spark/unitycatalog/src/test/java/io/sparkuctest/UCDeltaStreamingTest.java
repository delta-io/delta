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
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.test.shims.StreamingTestShims;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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

  @ParameterizedTest
  @MethodSource("allTableTypes")
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

          // Let's do 10 rounds testing, and for every round, adding 1 row and waiting to be
          // available, and finally verify the results and unity catalog latest version are
          // expected.
          ApiClient client = unityCatalogInfo().createApiClient();
          for (long i = 1; i < 10; i += 1) {
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
