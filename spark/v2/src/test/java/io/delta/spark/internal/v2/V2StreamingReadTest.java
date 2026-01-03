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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Tests for V2 streaming read operations. */
public class V2StreamingReadTest extends V2TestBase {

  @Test
  public void testStreamingReadMultipleVersions(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write version 0
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(1, "Alice", 100.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    // Write version 1
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(2, "Bob", 200.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Write version 2
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(3, "Charlie", 300.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Start streaming from version 0 - should read all three versions
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "0").table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    // Process all batches - should have all data from versions 0, 1, and 2
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_multiple_versions");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "Alice", 100.0),
            RowFactory.create(2, "Bob", 200.0),
            RowFactory.create(3, "Charlie", 300.0));

    assertDataEquals(actualRows, expectedRows);
  }

  @Test
  public void testStreamingReadWithoutStartingVersionThrowsException(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write initial data
    List<Row> initialRows =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    spark.createDataFrame(initialRows, TEST_SCHEMA).write().format("delta").save(tablePath);

    // Try to create streaming DataFrame without startingVersion using V2 path
    // Using dsv2.delta.`path` syntax to force V2 (SparkMicroBatchStream) instead of V1
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Should throw UnsupportedOperationException when trying to process
    org.apache.spark.sql.streaming.StreamingQueryException exception =
        assertThrows(
            org.apache.spark.sql.streaming.StreamingQueryException.class,
            () -> {
              StreamingQuery query =
                  spark
                      .readStream()
                      .table(dsv2TableRef)
                      .writeStream()
                      .format("memory")
                      .queryName("test_no_starting_version")
                      .outputMode("append")
                      .start();
              query.processAllAvailable();
              query.stop();
            });

    // Verify the root cause is UnsupportedOperationException
    Throwable rootCause = exception.getCause();
    assertTrue(
        rootCause instanceof UnsupportedOperationException,
        "Root cause should be UnsupportedOperationException, but was: "
            + (rootCause != null ? rootCause.getClass().getName() : "null"));
    assertTrue(
        rootCause.getMessage().contains("is not supported"),
        "Exception message should indicate operation is not supported: " + rootCause.getMessage());
  }
}
