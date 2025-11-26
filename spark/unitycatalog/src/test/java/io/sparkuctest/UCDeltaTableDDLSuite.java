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
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DDL test suite for Delta Table operations through Unity Catalog.
 *
 * Covers CREATE, ALTER, DROP, and other schema-related operations.
 */
public class UCDeltaTableDDLSuite extends UCDeltaTableIntegrationSuiteBase {

  @Override
  protected SQLExecutor getSqlExecutor() {
    return new SparkSQLExecutor(spark());
  }

  @Test
  public void testCreateTableWithDifferentDataTypes() throws Exception {
    withNewTable("create_types_test",
        "id BIGINT, name STRING, price DECIMAL(10,2), active BOOLEAN, created_at TIMESTAMP",
        tableName -> {
          // Insert data with various types
          sql("INSERT INTO " + tableName + " VALUES " +
              "(1L, 'Product A', 99.99, true, '2023-01-01 10:00:00'), " +
              "(2L, 'Product B', 149.50, false, '2023-01-02 15:30:00')");

          // Verify table creation and data types
          check(tableName, Arrays.asList(
              Arrays.asList("1", "Product A", "99.99", "true", "2023-01-01 10:00:00.0"),
              Arrays.asList("2", "Product B", "149.50", "false", "2023-01-02 15:30:00.0")
          ));
        });
  }

  @Test
  public void testCreateTableWithTblProperties() throws Exception {
    withTempDir((File dir) -> {
      File tablePath = new File(dir, "table_with_props");
      String tableName = getUnityCatalogName() + ".default.table_with_props";

      // Create table with table properties
      sql("CREATE TABLE " + tableName + " (id INT, value STRING) " +
          "USING DELTA LOCATION '" + tablePath.getAbsolutePath() + "' " +
          "TBLPROPERTIES (" +
          "'delta.autoOptimize.optimizeWrite' = 'true', " +
          "'delta.autoOptimize.autoCompact' = 'true')");

      try {
        // Insert data to verify table works
        sql("INSERT INTO " + tableName + " VALUES (1, 'test')");
        check(tableName, Arrays.asList(Arrays.asList("1", "test")));

        // Verify table properties can be queried
        List<List<String>> description = sql("DESCRIBE EXTENDED " + tableName);
        Assert.assertTrue("Should be able to describe the table", !description.isEmpty());
      } finally {
        spark().sql("DROP TABLE IF EXISTS " + tableName);
      }
    });
  }

  @Test
  public void testCreateTableAsSelect() throws Exception {
    withNewTable("ctas_source", "id INT, category STRING, value DOUBLE", sourceTable -> {
      // Setup source data
      sql("INSERT INTO " + sourceTable + " VALUES " +
          "(1, 'A', 10.5), (2, 'B', 20.0), (3, 'A', 15.75), (4, 'C', 30.25)");

      withTempDir((File dir) -> {
        File tablePath = new File(dir, "ctas_result");
        String resultTable = getUnityCatalogName() + ".default.ctas_result";

        // Create table using CTAS
        sql("CREATE TABLE " + resultTable + " USING DELTA " +
            "LOCATION '" + tablePath.getAbsolutePath() + "' " +
            "AS SELECT category, AVG(value) as avg_value " +
            "FROM " + sourceTable + " GROUP BY category ORDER BY category");

        try {
          // Verify CTAS result
          check(resultTable, Arrays.asList(
              Arrays.asList("A", "13.125"),
              Arrays.asList("B", "20.0"),
              Arrays.asList("C", "30.25")
          ));
        } finally {
          spark().sql("DROP TABLE IF EXISTS " + resultTable);
        }
      });
    });
  }

  @Test
  public void testDropTableOperation() throws Exception {
    withTempDir((File dir) -> {
      File tablePath = new File(dir, "drop_test");
      String tableName = getUnityCatalogName() + ".default.drop_test";

      // Create and populate table
      sql("CREATE TABLE " + tableName + " (id INT, name STRING) " +
          "USING DELTA LOCATION '" + tablePath.getAbsolutePath() + "'");
      sql("INSERT INTO " + tableName + " VALUES (1, 'test')");

      // Verify table exists and has data
      check(tableName, Arrays.asList(Arrays.asList("1", "test")));

      // Drop the table
      sql("DROP TABLE " + tableName);

      // Verify table no longer exists
      try {
        sql("SELECT * FROM " + tableName);
        Assert.fail("Expected exception when querying dropped table");
      } catch (Exception e) {
        // Expected
      }
    });
  }

  @Test
  public void testShowTablesInUnityCatalog() throws Exception {
    withNewTable("show_tables_test", "id INT", tableName -> {
      sql("INSERT INTO " + tableName + " VALUES (1)");

      // Show tables in the schema
      List<List<String>> tables = sql("SHOW TABLES IN " + getUnityCatalogName() + ".default");
      List<String> tableNames = tables.stream()
          .map(row -> row.get(1))
          .collect(Collectors.toList());

      // Verify our test table appears in the list
      Assert.assertTrue(
          "Table should appear in SHOW TABLES. Found tables: " + String.join(", ", tableNames),
          tableNames.contains("show_tables_test")
      );
    });
  }

  @Test
  public void testDescribeTableOperation() throws Exception {
    withNewTable("describe_test", "id BIGINT, name STRING, active BOOLEAN", tableName -> {
      sql("INSERT INTO " + tableName + " VALUES (1, 'test', true)");

      // Describe the table structure
      List<List<String>> description = sql("DESCRIBE " + tableName);

      // Verify expected columns are present
      Map<String, String> columnInfo = description.stream()
          .collect(Collectors.toMap(row -> row.get(0), row -> row.get(1)));

      Assert.assertTrue("Should have 'id' column", columnInfo.containsKey("id"));
      Assert.assertTrue("Should have 'name' column", columnInfo.containsKey("name"));
      Assert.assertTrue("Should have 'active' column", columnInfo.containsKey("active"));

      // Verify data types
      Assert.assertTrue(
          "ID should be bigint, got: " + columnInfo.get("id"),
          columnInfo.get("id").contains("bigint")
      );
      Assert.assertTrue(
          "Name should be string, got: " + columnInfo.get("name"),
          columnInfo.get("name").contains("string")
      );
      Assert.assertTrue(
          "Active should be boolean, got: " + columnInfo.get("active"),
          columnInfo.get("active").contains("boolean")
      );
    });
  }

  @Test
  public void testDescribeExtendedTableOperation() throws Exception {
    withNewTable("describe_extended_test", "id INT, data STRING", tableName -> {
      sql("INSERT INTO " + tableName + " VALUES (1, 'sample')");

      // Get extended description
      List<List<String>> extendedDesc = sql("DESCRIBE EXTENDED " + tableName);

      // Verify we get extended information (should be more than just column info)
      Assert.assertTrue(
          "Extended description should contain more than just column definitions",
          extendedDesc.size() > 3
      );

      // Look for key extended properties
      String descText = extendedDesc.stream()
          .flatMap(List::stream)
          .collect(Collectors.joining(" "))
          .toLowerCase();

      Assert.assertTrue(
          "Extended description should contain table metadata information",
          descText.contains("table") || descText.contains("location") || descText.contains("provider")
      );
    });
  }

  @Test
  public void testCreateTableIfNotExists() throws Exception {
    withTempDir((File dir) -> {
      File tablePath = new File(dir, "if_not_exists_test");
      String tableName = getUnityCatalogName() + ".default.if_not_exists_test";

      // First creation should succeed
      sql("CREATE TABLE IF NOT EXISTS " + tableName + " (id INT, name STRING) " +
          "USING DELTA LOCATION '" + tablePath.getAbsolutePath() + "'");

      sql("INSERT INTO " + tableName + " VALUES (1, 'first')");

      try {
        // Second creation should not fail and not affect data
        sql("CREATE TABLE IF NOT EXISTS " + tableName + " (id INT, name STRING, extra STRING) " +
            "USING DELTA LOCATION '" + tablePath.getAbsolutePath() + "'");

        // Verify original data is still there and schema unchanged
        check(tableName, Arrays.asList(Arrays.asList("1", "first")));

        // Verify schema wasn't changed (should not have 'extra' column)
        List<List<String>> description = sql("DESCRIBE " + tableName);
        List<String> columns = description.stream()
            .map(row -> row.get(0))
            .collect(Collectors.toList());
        Assert.assertFalse("Schema should not have been modified", columns.contains("extra"));

      } finally {
        spark().sql("DROP TABLE IF EXISTS " + tableName);
      }
    });
  }

  @Test
  public void testTableNamingWithSpecialCharacters() throws Exception {
    withTempDir((File dir) -> {
      File tablePath = new File(dir, "special_name_test");
      String tableName = getUnityCatalogName() + ".default.`test_table_with_underscores`";

      // Create table with special characters in name
      sql("CREATE TABLE " + tableName + " (id INT, value STRING) " +
          "USING DELTA LOCATION '" + tablePath.getAbsolutePath() + "'");

      try {
        // Insert and verify data works with special table name
        sql("INSERT INTO " + tableName + " VALUES (1, 'special')");
        check(tableName, Arrays.asList(Arrays.asList("1", "special")));

        // Verify table appears in SHOW TABLES
        List<List<String>> tables = sql("SHOW TABLES IN " + getUnityCatalogName() + ".default");
        List<String> tableNames = tables.stream()
            .map(row -> row.get(1))
            .collect(Collectors.toList());

        Assert.assertTrue(
            "Special table name should appear in SHOW TABLES. Found: " + String.join(", ", tableNames),
            tableNames.contains("test_table_with_underscores")
        );

      } finally {
        spark().sql("DROP TABLE IF EXISTS " + tableName);
      }
    });
  }
}

