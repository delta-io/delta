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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Abstract base class for Unity Catalog + Delta Table integration tests.
 *
 * <p>This class provides a pluggable SQL execution framework via the SQLExecutor interface,
 * allowing tests to be written once and executed via different execution engines (e.g., Spark SQL,
 * JDBC, REST API, etc.).
 *
 * <p>Subclasses must provide an executor by implementing the getSqlExecutor method.
 */
public abstract class UCDeltaTableIntegrationBaseTest extends UnityCatalogSupport {
  protected static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";
  protected static final String V2_ENABLE_MODE_STRICT = "STRICT";
  protected static final String V2_ENABLE_MODE_NONE = "NONE";

  public static final List<TableType> ALL_TABLE_TYPES =
      List.of(TableType.EXTERNAL, TableType.MANAGED);

  /**
   * Tests with this annotation will test against ALL_TABLE_TYPES. Example:
   *
   * <pre>{@code
   * @TestAllTableTypes
   * public void testAdvancedInsertOperations(TableType tableType)
   * }</pre>
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface TestAllTableTypes {}

  /** Generate dynamic tests for all methods with @TestAllTableTypes to run with ALL_TABLE_TYPES. */
  @TestFactory
  Stream<DynamicContainer> allTableTypesTestsFactory() {
    List<Method> methods =
        Stream.of(this.getClass().getDeclaredMethods())
            .filter(m -> m.isAnnotationPresent(TestAllTableTypes.class))
            .collect(Collectors.toList());
    List<DynamicContainer> containers = new ArrayList<>();
    for (Method method : methods) {
      List<DynamicTest> tests = new ArrayList<>();
      for (TableType tableType : ALL_TABLE_TYPES) {
        String testName = String.format("%s(%s)", method.getName(), tableType);
        tests.add(DynamicTest.dynamicTest(testName, () -> method.invoke(this, tableType)));
      }
      containers.add(DynamicContainer.dynamicContainer(method.getName(), tests));
    }
    return containers.stream();
  }

  private SparkSession sparkSession;

  /** Create the SparkSession before all tests. */
  @BeforeAll
  public void setUpSpark() {
    // UC server is started by UnityCatalogSupport.setupServer()
    // And the BeforeAll of parent class UnityCatalogSupport will be called before this method.

    SparkConf conf =
        new SparkConf()
            .setAppName("UnityCatalog Integration Tests")
            .setMaster("local[2]")
            .set("spark.ui.enabled", "false")
            // Delta Lake required configurations
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    // Configure with Unity Catalog
    conf = configureSparkWithUnityCatalog(conf);

    sparkSession = SparkSession.builder().config(conf).getOrCreate();
  }

  private SparkConf configureSparkWithUnityCatalog(SparkConf conf) {
    // Set the AWS S3 implementation for remote unity catalog server testing.
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // Set the catalog specific configs.
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();
    return conf.set("spark.sql.catalog." + catalogName, "io.unitycatalog.spark.UCSingleCatalog")
        .set("spark.sql.catalog." + catalogName + ".uri", uc.serverUri())
        .set("spark.sql.catalog." + catalogName + ".token", uc.serverToken());
  }

  /** Stop the SparkSession after all tests. */
  @AfterAll
  public void tearDownSpark() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
    // UC server is stopped by UnityCatalogSupport.tearDownServer()
  }

  /** Get the SparkSession for direct access (e.g., for streaming operations). */
  protected SparkSession spark() {
    return sparkSession;
  }

  /** Get the SQL executor. Private to force subclasses to use sql() and check() methods. */
  private SQLExecutor getSqlExecutor() {
    return new SparkSQLExecutor(sparkSession);
  }

  /**
   * Execute SQL through the SQL executor and return results.
   *
   * <p>When called with arguments, formats the SQL query using String.format:
   *
   * <pre>
   * sql("INSERT INTO %s VALUES (%d, '%s')", tableName, 1, "value")
   * </pre>
   *
   * <p>When called without arguments, executes the SQL as-is:
   *
   * <pre>
   * sql("CREATE TABLE test (id INT)")
   * </pre>
   *
   * @param sqlQuery SQL query with optional format specifiers (e.g., "SELECT * FROM %s WHERE id =
   *     %d")
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

  /** Helper method to run code with a temporary directory that gets cleaned up. */
  protected void withTempDir(TempDirCode code) throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    Path tempDir = new Path(uc.baseTableLocation(), "temp-" + UUID.randomUUID());
    code.run(tempDir);
  }

  /** Table types for parameterized testing. */
  public enum TableType {
    EXTERNAL, // Requires LOCATION clause
    MANAGED // No LOCATION clause (Spark manages the data)
  }

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param tableSchema The table schema (e.g., "id INT, name STRING")
   * @param partitionFields The partition fields (e.g., "id, name")
   * @param tableType The type of table (EXTERNAL or MANAGED)
   * @param tableProperties Additional table properties (e.g., "delta.enableChangeDataFeed"="true")
   * @param testCode The test function that receives the full table name
   */
  protected void withNewTable(
      String tableName,
      String tableSchema,
      String partitionFields,
      TableType tableType,
      String tableProperties,
      TestCode testCode)
      throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    TableSetup tableSetup =
        new TableSetup()
            .setCatalogName(uc.catalogName())
            .setSchemaName(uc.schemaName())
            .setTableName(tableName)
            .setTableSchema(tableSchema)
            .setTableType(tableType);
    if (partitionFields != null && !partitionFields.isEmpty()) {
      tableSetup.setPartitionFields(partitionFields);
    }

    String fullTableName = tableSetup.fullTableName();
    if (tableType == TableType.EXTERNAL) {
      // External table requires a location
      withTempDir(
          (Path dir) -> {
            tableSetup.setExternalLocation(new Path(dir, tableName).toString());
            sql(tableSetup.toSql());
            try {
              testCode.run(fullTableName);
            } finally {
              sql("DROP TABLE IF EXISTS %s", fullTableName);
            }
          });
    } else {
      // Managed table requires 'delta.feature.catalogManaged'='supported' table property.
      sql(tableSetup.toSql());
      try {
        testCode.run(fullTableName);
      } finally {
        sql("DROP TABLE IF EXISTS %s", fullTableName);
      }
    }
  }

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param tableSchema The table schema (e.g., "id INT, name STRING")
   * @param partitionFields The partition fields (e.g., "id, name")
   * @param tableType The type of table (EXTERNAL or MANAGED)
   * @param testCode The test function that receives the full table name
   */
  protected void withNewTable(
      String tableName,
      String tableSchema,
      String partitionFields,
      TableType tableType,
      TestCode testCode)
      throws Exception {
    withNewTable(tableName, tableSchema, partitionFields, tableType, null, testCode);
  }

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param tableSchema The table schema (e.g., "id INT, name STRING")
   * @param tableType The type of table (EXTERNAL or MANAGED)
   * @param testCode The test function that receives the full table name
   */
  protected void withNewTable(
      String tableName, String tableSchema, TableType tableType, TestCode testCode)
      throws Exception {
    withNewTable(tableName, tableSchema, null, tableType, testCode);
  }

  /** Functional interface for test code that takes a temporary directory. */
  @FunctionalInterface
  protected interface TempDirCode {

    void run(Path dir) throws Exception;
  }

  /** Functional interface for test code that takes a table name parameter. */
  @FunctionalInterface
  protected interface TestCode {

    void run(String tableName) throws Exception;
  }

  /**
   * Interface defining the interface for executing SQL and verifying results.
   *
   * <p>This abstraction allows tests to be independent of the execution engine, making it easy to
   * test the same logic via different interfaces (Spark SQL, JDBC, etc.).
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
   * <p>This executor runs all SQL queries through Spark SQL and converts results to string lists
   * for easy comparison.
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
          .map(
              row -> {
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
            String.format(
                "Query results do not match.\nSQL: %s\n Expected: %s\nActual: %s",
                sql, expected, actual));
      }
    }
  }
}
