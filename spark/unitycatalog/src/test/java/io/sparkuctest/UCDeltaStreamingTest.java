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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.execution.streaming.MemoryStream;
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

          // Create MemoryStream - using Scala companion object with proper encoder
          MemoryStream<Row> memoryStream =
              MemoryStream.apply(Encoders.row(schema), spark().sqlContext());

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

          // Add test data - convert to Rows
          Seq<Row> batchRow =
              JavaConverters.asScalaIteratorConverter(
                      Arrays.asList(
                              RowFactory.create(1L, "Alice"),
                              RowFactory.create(2L, "Bob"),
                              RowFactory.create(3L, "Charlie"))
                          .iterator())
                  .asScala()
                  .toSeq();
          memoryStream.addData(batchRow);

          // Process all available data
          query.processAllAvailable();

          // Stop the stream.
          query.stop();
          query.awaitTermination();

          // Assert that the query has stopped
          assertFalse(query.isActive(), "Streaming query should have stopped");

          // Verify the content
          check(
              tableName,
              List.of(List.of("1", "Alice"), List.of("2", "Bob"), List.of("3", "Charlie")));
        });
  }
}
