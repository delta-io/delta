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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract base class for Unity Catalog + Delta Table integration tests.
 *
 * This class provides a pluggable SQL execution framework via the SQLExecutor interface,
 * allowing tests to be written once and executed via different execution engines
 * (e.g., Spark SQL, JDBC, REST API, etc.).
 *
 * Subclasses must provide an executor by implementing the getSqlExecutor method.
 */
public abstract class UCDeltaTableIntegrationBaseTest extends UnityCatalogSupport {

  /**
   * Provides all table types for parameterized tests.
   * Tests can use this as a @MethodSource to test different table types.
   */
  protected static Stream<TableType> allTableTypes() {
    return Stream.of(TableType.EXTERNAL, TableType.MANAGED);
  }

  private SparkSession sparkSession;

  /**
   * Create the SparkSession before all tests.
   */
  @BeforeAll
  public void setUpSpark() {
    // UC server is started by UnityCatalogSupport.setupServer()
    // And the BeforeAll of parent class UnityCatalogSupport will be called before this method.
    
    SparkConf conf = new SparkConf()
        .setAppName("UnityCatalog Integration Tests")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        // Delta Lake required configurations
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    
    // Configure with Unity Catalog
    conf = configureSparkWithUnityCatalog(conf);
    
    sparkSession = SparkSession.builder().config(conf).getOrCreate();

    // Enable testing so that catalogManaged UC tables can be created.
    // This is checked by CreateDeltaTableCommand which calls org.apache.spark.util.Utils.isTesting.
    // TODO: clean up once it's not required.
    System.setProperty("spark.testing", "true");
  }

  /**
   * Stop the SparkSession after all tests.
   */
  @AfterAll
  public void tearDownSpark() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
    // UC server is stopped by UnityCatalogSupport.tearDownServer()
  }

  /**
   * Get the SQL executor. Private to force subclasses to use sql() and check() methods.
   */
  private SQLExecutor getSqlExecutor() {
    return new SparkSQLExecutor(sparkSession);
  }

  /**
   * Execute SQL through the SQL executor and return results.
   * 
   * When called with arguments, formats the SQL query using String.format:
   * <pre>
   * sql("INSERT INTO %s VALUES (%d, '%s')", tableName, 1, "value")
   * </pre>
   * 
   * When called without arguments, executes the SQL as-is:
   * <pre>
   * sql("CREATE TABLE test (id INT)")
   * </pre>
   * 
   * @param sqlQuery SQL query with optional format specifiers (e.g., "SELECT * FROM %s WHERE id = %d")
   * @param args Arguments to be formatted into the SQL query
   * @return List of result rows, each row is a list of string values
   */
  protected List<List<String>> sql(String sqlQuery, Object... args) {
    String formattedQuery = args.length > 0 ? String.format(sqlQuery, args) : sqlQuery;
    return getSqlExecutor().runSQL(formattedQuery);
  }

  /**
   * Verify table contents by selecting all rows ordered by the first column.
   * 
   * @param tableName The fully qualified table name
   * @param expected The expected results as a list of rows
   */
  protected void check(String tableName, List<List<String>> expected) {
    getSqlExecutor().checkWithSQL("SELECT * FROM " + tableName + " ORDER BY 1", expected);
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
   * Table types for parameterized testing.
   */
  public enum TableType {
    EXTERNAL,  // Requires LOCATION clause
    MANAGED    // No LOCATION clause (Spark manages the data)
  }

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param tableSchema The table schema (e.g., "id INT, name STRING")
   * @param tableType The type of table (EXTERNAL or MANAGED)
   * @param testCode The test function that receives the full table name
   */
  protected void withNewTable(String tableName, String tableSchema, TableType tableType, TestCode testCode) throws Exception {
    String fullTableName = getCatalogName() + ".default." + tableName;

    if (tableType == TableType.EXTERNAL) {
      // External table requires a location
      withTempDir((File dir) -> {
        File tablePath = new File(dir, tableName);
        sql("CREATE TABLE %s (%s) USING DELTA LOCATION '%s'", 
            fullTableName, tableSchema, tablePath.getAbsolutePath());

        try {
          testCode.run(fullTableName);
        } finally {
          sql("DROP TABLE IF EXISTS %s", fullTableName);
        }
      });
    } else {
      // Managed table - Spark manages the location
      // Unity Catalog requires 'delta.feature.catalogManaged'='supported' for managed tables
      sql("CREATE TABLE %s (%s) USING DELTA " +
          "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')", 
          fullTableName, tableSchema);

      try {
        testCode.run(fullTableName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullTableName);
      }
    }
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

