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
package io.delta.kernel.spark;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Tests for V2 streaming read operations. */
public class V2StreamingReadTest extends V2TestBase {

  @Test
  public void testStreamingRead(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Create test data using standard Delta Lake
    Dataset<Row> testData =
        spark.createDataFrame(
            Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0)),
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false))));
    testData.write().format("delta").save(tablePath);

    // Test streaming read using path-based table
    Dataset<Row> streamingDF = spark.readStream().table(format("dsv2.delta.`%s`", tablePath));

    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");
    StreamingQueryException exception =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery query =
                  streamingDF
                      .writeStream()
                      .format("memory")
                      .queryName("test_streaming_query")
                      .outputMode("append")
                      .start();
              query.processAllAvailable();
              query.stop();
            });
    Throwable rootCause = exception.getCause();
    assertTrue(
        rootCause instanceof UnsupportedOperationException,
        "Root cause should be UnsupportedOperationException");
    assertTrue(
        rootCause.getMessage().contains("is not supported"),
        "Root cause message should indicate that streaming operation is not supported: "
            + rootCause.getMessage());
  }
}

