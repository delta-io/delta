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
import java.util.List;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Dsv2BasicTest {

  private SparkSession spark;
  private String nameSpace;

  private static final StructType TEST_SCHEMA =
      DataTypes.createStructType(
          Arrays.asList(
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false),
              DataTypes.createStructField("value", DataTypes.DoubleType, false)));

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .setMaster("local[*]")
            .setAppName("Dsv2BasicTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTable() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.create_table_test (id INT, name STRING, value DOUBLE)",
            nameSpace));

    Dataset<Row> actual =
        spark.sql(String.format("DESCRIBE TABLE dsv2.%s.create_table_test", nameSpace));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));
    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testBatchRead() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.batch_read_test (id INT, name STRING, value DOUBLE)", nameSpace));

    // Select and validate the data
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.%s.batch_read_test", nameSpace));

    List<Row> expectedRows = Arrays.asList();

    assertDatasetEquals(result, expectedRows);
  }

  @Test
  public void testQueryTableNotExist() {
    AnalysisException e =
        org.junit.jupiter.api.Assertions.assertThrows(
            AnalysisException.class,
            () -> spark.sql(String.format("SELECT * FROM dsv2.%s.not_found_test", nameSpace)));
    assertEquals(
        "TABLE_OR_VIEW_NOT_FOUND",
        e.getErrorClass(),
        "Missing table should raise TABLE_OR_VIEW_NOT_FOUND");
  }

  @Test
  public void testPathBasedTable(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Create test data and write as Delta table
    Dataset<Row> testData =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(1, "Alice", 100.0),
                RowFactory.create(2, "Bob", 200.0),
                RowFactory.create(3, "Charlie", 300.0)),
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false))));

    testData.write().format("delta").save(tablePath);

    // TODO: [delta-io/delta#5001] change to select query after batch read is supported for dsv2
    // path.
    Dataset<Row> actual = spark.sql(String.format("DESCRIBE TABLE dsv2.delta.`%s`", tablePath));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));

    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testStreamingReadMultipleVersions(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write version 0
    writeInitialData(tablePath, Arrays.asList(RowFactory.create(1, "Alice", 100.0)));

    // Write version 1
    appendTestData(tablePath, Arrays.asList(RowFactory.create(2, "Bob", 200.0)));

    // Write version 2
    appendTestData(tablePath, Arrays.asList(RowFactory.create(3, "Charlie", 300.0)));

    // Start streaming from version 0 - should read all three versions
    Dataset<Row> streamingDF = createStreamingDF(tablePath, "0");
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    // Process all batches - should have all data from versions 0, 1, and 2
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_multiple_versions");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "Alice", 100.0),
            RowFactory.create(2, "Bob", 200.0),
            RowFactory.create(3, "Charlie", 300.0));

    assertStreamingDataEquals(actualRows, expectedRows);
  }

  @Test
  public void testStreamingReadWithStartingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write initial data (version 0)
    List<Row> version0Rows =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    writeInitialData(tablePath, version0Rows);

    // Start streaming from "latest" (should start reading from version 1 onwards)
    Dataset<Row> streamingDF = createStreamingDF(tablePath, "latest");
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName("test_latest_initial")
              .outputMode("append")
              .start();

      query.processAllAvailable();

      // Should have no data initially since we're starting after the current version
      Dataset<Row> results = spark.sql("SELECT * FROM test_latest_initial");
      List<Row> initialRows = results.collectAsList();
      assertTrue(
          initialRows.isEmpty(),
          "Should have no data when starting from 'latest' before new data is added");

      // Add more data (version 1)
      List<Row> version1Rows =
          Arrays.asList(
              RowFactory.create(3, "Charlie", 300.0), RowFactory.create(4, "David", 400.0));
      appendTestData(tablePath, version1Rows);

      // Process the next batch
      query.processAllAvailable();

      // Now should only have the new data (version 1)
      Dataset<Row> finalResults = spark.sql("SELECT * FROM test_latest_initial");
      List<Row> finalRows = finalResults.collectAsList();

      assertStreamingDataEquals(finalRows, version1Rows);
    } finally {
      if (query != null) {
        query.stop();
      }
    }
  }

  @Test
  public void testStreamingReadWithRateLimit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write initial data with 3 rows (likely to be in a single file)
    List<Row> initialRows =
        Arrays.asList(
            RowFactory.create(1, "Alice", 100.0),
            RowFactory.create(2, "Bob", 200.0),
            RowFactory.create(3, "Charlie", 300.0));
    writeInitialData(tablePath, initialRows);

    // Start streaming with maxFilesPerTrigger=1 to test rate limiting
    Dataset<Row> streamingDF = createStreamingDF(tablePath, "0", "1");
    assertTrue(streamingDF.isStreaming(), "Dataset should be streaming");

    // Process with rate limiting - should still get all data, just potentially in multiple batches
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_rate_limit");
    assertStreamingDataEquals(actualRows, initialRows);
  }

  @Test
  public void testStreamingReadWithoutStartingVersionThrowsException(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Write initial data
    List<Row> initialRows =
        Arrays.asList(RowFactory.create(1, "Alice", 100.0), RowFactory.create(2, "Bob", 200.0));
    writeInitialData(tablePath, initialRows);

    // Try to create streaming DataFrame without startingVersion using DSv2 path
    // Using dsv2.delta.`path` syntax to force DSv2 (SparkMicroBatchStream) instead of DSv1
    String dsv2TableRef = String.format("dsv2.delta.`%s`", tablePath);

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

  //////////////////////
  // Private helpers //
  /////////////////////
  private void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }

  private void appendData(String tablePath, Dataset<Row> data) {
    data.write().format("delta").mode("append").save(tablePath);
  }

  private List<Row> processStreamingQuery(Dataset<Row> streamingDF, String queryName)
      throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .start();

      query.processAllAvailable();

      // Query the memory sink to get results
      Dataset<Row> results = spark.sql("SELECT * FROM " + queryName);
      return results.collectAsList();
    } finally {
      if (query != null) {
        query.stop();
      }
    }
  }

  private void assertStreamingDataEquals(List<Row> actualRows, List<Row> expectedRows) {
    assertEquals(
        expectedRows.size(),
        actualRows.size(),
        () ->
            "Row count differs: expected="
                + expectedRows.size()
                + " actual="
                + actualRows.size()
                + "\nExpected rows: "
                + expectedRows
                + "\nActual rows: "
                + actualRows);

    // Compare rows (order-independent for robustness)
    assertTrue(
        actualRows.containsAll(expectedRows) && expectedRows.containsAll(actualRows),
        () -> "Streaming data differs:\nExpected: " + expectedRows + "\nActual: " + actualRows);
  }

  private Dataset<Row> createTestData(List<Row> rows) {
    return spark.createDataFrame(rows, TEST_SCHEMA);
  }

  private void writeInitialData(String tablePath, List<Row> rows) {
    createTestData(rows).write().format("delta").save(tablePath);
  }

  private void appendTestData(String tablePath, List<Row> rows) {
    appendData(tablePath, createTestData(rows));
  }

  private Dataset<Row> createStreamingDF(String tablePath, String startingVersion) {
    // Use dsv2.delta.`path` syntax to force DSv2 (SparkMicroBatchStream) instead of DSv1
    String dsv2TableRef = String.format("dsv2.delta.`%s`", tablePath);
    return spark.readStream().option("startingVersion", startingVersion).table(dsv2TableRef);
  }

  private Dataset<Row> createStreamingDF(
      String tablePath, String startingVersion, String maxFilesPerTrigger) {
    // Use dsv2.delta.`path` syntax to force DSv2 (SparkMicroBatchStream) instead of DSv1
    String dsv2TableRef = String.format("dsv2.delta.`%s`", tablePath);
    if (maxFilesPerTrigger != null) {
      return spark
          .readStream()
          .option("startingVersion", startingVersion)
          .option("maxFilesPerTrigger", maxFilesPerTrigger)
          .table(dsv2TableRef);
    }
    return createStreamingDF(tablePath, startingVersion);
  }
}
