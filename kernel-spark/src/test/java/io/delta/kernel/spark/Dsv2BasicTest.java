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
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Dsv2BasicTest {

  private SparkSession spark;
  private String nameSpace;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    Dataset<Row> streamingDF =
        spark.readStream().table(String.format("dsv2.delta.`%s`", tablePath));

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
}
