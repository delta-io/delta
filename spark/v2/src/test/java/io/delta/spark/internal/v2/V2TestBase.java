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
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base class for V2 tests with common SparkSession setup and helper methods.
 *
 * <p>This base class configures the Spark session with the V2 catalog and provides utility methods
 * for assertions. The V1 catalog is still used for write operations because this is currently
 * necessary until V2 supports write operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class V2TestBase {

  protected SparkSession spark;
  protected String nameSpace;

  protected static final StructType TEST_SCHEMA =
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
            .set("spark.sql.catalog.dsv2", "io.delta.spark.internal.v2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .setMaster("local[*]")
            .setAppName(getClass().getSimpleName());
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Builds a formatted string by substituting placeholders with the provided arguments. Useful for
   * constructing SQL queries and table identifiers.
   */
  protected static String str(String template, Object... args) {
    return String.format(template, args);
  }

  /**
   * Executes a SQL query and verifies the result matches the expected rows.
   *
   * @param sql the SQL query to execute
   * @param expectedRows the expected rows as a list of lists (each inner list is a row)
   */
  protected void check(String sql, List<List<Object>> expectedRows) {
    Dataset<Row> result = spark.sql(sql);
    List<Row> actualRows = result.collectAsList();
    List<Row> expected =
        expectedRows.stream()
            .map(row -> RowFactory.create(row.toArray()))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(
        expected,
        actualRows,
        () -> "Query: " + sql + "\nExpected: " + expected + "\nActual: " + actualRows);
  }

  /** Creates a row representation as a list of values. */
  protected static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }

  /** Asserts that a dataset equals the expected rows. */
  protected void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }

  /**
   * Processes a streaming query and returns the collected rows.
   *
   * @param streamingDF the streaming DataFrame to process
   * @param queryName the name for the memory sink query
   * @return the list of rows collected from the stream
   * @throws Exception if the streaming query fails
   */
  protected List<Row> processStreamingQuery(Dataset<Row> streamingDF, String queryName)
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

  /**
   * Asserts that rows equal the expected rows (order-independent).
   *
   * @param actualRows the actual rows
   * @param expectedRows the expected rows
   */
  protected void assertDataEquals(List<Row> actualRows, List<Row> expectedRows) {
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
        () -> "Data differs:\nExpected: " + expectedRows + "\nActual: " + actualRows);
  }
}
