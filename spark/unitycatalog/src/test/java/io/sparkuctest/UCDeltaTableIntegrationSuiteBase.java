/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract base class for Unity Catalog + Delta Table integration tests.
 *
 * This class provides a pluggable SQL execution framework via the SQLExecutor interface,
 * allowing tests to be written once and executed via different execution engines
 * (e.g., Spark SQL, JDBC, REST API, etc.).
 *
 * Subclasses must provide an executor by implementing the getSqlExecutor method.
 */
public abstract class UCDeltaTableIntegrationSuiteBase extends UnityCatalogSupport {

  private SparkSession sparkSession;

  /**
   * Create the SparkSession before each test class.
   */
  @Before
  public void setUpSpark() throws Exception {
    super.setupUnityCatalog(); // Start UC server first
    
    SparkConf conf = new SparkConf()
        .setAppName("UnityCatalog Integration Tests")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.shuffle.partitions", "5")
        .set("spark.databricks.delta.snapshotPartitions", "2")
        .set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        // Delta Lake required configurations
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    
    // Configure with Unity Catalog
    conf = configureSparkWithUnityCatalog(conf);
    
    sparkSession = SparkSession.builder().config(conf).getOrCreate();
  }

  /**
   * Stop the SparkSession after each test class.
   */
  @After
  public void tearDownSpark() throws Exception {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
    super.tearDownUnityCatalog(); // Stop UC server last
  }

  /**
   * Get the SparkSession for tests.
   */
  protected SparkSession spark() {
    return sparkSession;
  }

  /**
   * The SQL executor used to run queries and verify results.
   * Must be implemented by subclasses to provide the execution engine.
   */
  protected abstract SQLExecutor getSqlExecutor();

  /**
   * Convenience method for getSqlExecutor().runSQL - executes SQL and returns results.
   */
  protected List<List<String>> sql(String sqlQuery) {
    return getSqlExecutor().runSQL(sqlQuery);
  }

  /**
   * Convenience method for getSqlExecutor().checkTable - verifies table contents.
   */
  protected void check(String tableName, List<List<String>> expected) {
    getSqlExecutor().checkTable(tableName, expected);
  }

  /**
   * Helper method to run code with a temporary directory that gets cleaned up.
   */
  protected void withTempDir(TempDirCode code) throws Exception {
    File tempDir = Files.createTempDirectory("spark-test-").toFile();
    try {
      code.run(tempDir);
    } finally {
      deleteRecursively(tempDir);
    }
  }

  /**
   * Recursively delete a directory.
   */
  private void deleteRecursively(File file) {
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

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param schema The table schema (e.g., "id INT, name STRING")
   * @param testCode The test function that receives the full table name
   */
  protected void withNewTable(String tableName, String schema, TestCode testCode) throws Exception {
    withTempDir((File dir) -> {
      File tablePath = new File(dir, tableName);
      String fullTableName = getUnityCatalogName() + ".default." + tableName;

      // Create the table
      sql(
          "CREATE TABLE " + fullTableName + " (" +
          schema +
          ") USING DELTA " +
          "LOCATION '" + tablePath.getAbsolutePath() + "'"
      );

      try {
        // Run the test code with the full table name
        testCode.run(fullTableName);
      } finally {
        // Clean up the table
        spark().sql("DROP TABLE IF EXISTS " + fullTableName);
      }
    });
  }

  /**
   * Functional interface for test code that takes a temporary directory.
   */
  @FunctionalInterface
  protected interface TempDirCode {
    void run(File dir) throws Exception;
  }

  /**
   * Functional interface for test code that takes a table name parameter.
   */
  @FunctionalInterface
  protected interface TestCode {
    void run(String tableName) throws Exception;
  }

  /**
   * Interface defining the interface for executing SQL and verifying results.
   *
   * This abstraction allows tests to be independent of the execution engine,
   * making it easy to test the same logic via different interfaces (Spark SQL, JDBC, etc.).
   */
  public interface SQLExecutor {
    /**
     * Execute a SQL statement and return the results.
     *
     * @param sql The SQL statement to execute
     * @return The query results as a list of rows, where each row is a list of strings
     */
    List<List<String>> runSQL(String sql);

    /**
     * Read all data from a table and verify it matches the expected results.
     *
     * @param tableName The fully qualified table name
     * @param expected The expected results as a list of rows
     */
    void checkTable(String tableName, List<List<String>> expected);

    /**
     * Execute a SQL query and verify the results match the expected output.
     *
     * @param sql The SQL query to execute
     * @param expected The expected results as a list of rows
     */
    void checkWithSQL(String sql, List<List<String>> expected);
  }

  /**
   * Default SQL executor implementation using SparkSession.
   *
   * This executor runs all SQL queries through Spark SQL and converts
   * results to string lists for easy comparison.
   */
  public static class SparkSQLExecutor implements SQLExecutor {
    private final SparkSession spark;

    public SparkSQLExecutor(SparkSession spark) {
      this.spark = spark;
    }

    @Override
    public List<List<String>> runSQL(String sql) {
      Dataset<Row> df = spark.sql(sql);
      Row[] rows = (Row[]) df.collect();
      return Arrays.stream(rows)
          .map(row -> {
            List<String> cells = new java.util.ArrayList<>();
            for (int i = 0; i < row.length(); i++) {
              cells.add(row.isNullAt(i) ? "null" : row.get(i).toString());
            }
            return cells;
          })
          .collect(Collectors.toList());
    }

    @Override
    public void checkTable(String tableName, List<List<String>> expected) {
      List<List<String>> actual = runSQL("SELECT * FROM " + tableName + " ORDER BY 1");
      if (!actual.equals(expected)) {
        throw new AssertionError(
            "Table " + tableName + " contents do not match.\n" +
            "Expected: " + expected + "\n" +
            "Actual: " + actual);
      }
    }

    @Override
    public void checkWithSQL(String sql, List<List<String>> expected) {
      List<List<String>> actual = runSQL(sql);
      if (!actual.equals(expected)) {
        throw new AssertionError(
            "Query results do not match.\n" +
            "SQL: " + sql + "\n" +
            "Expected: " + expected + "\n" +
            "Actual: " + actual);
      }
    }
  }
}

