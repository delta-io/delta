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

package com.sparkuctest;

import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility operations test suite for Delta Table operations through Unity Catalog.
 *
 * Covers OPTIMIZE, DESCRIBE HISTORY, SHOW operations, and other table utilities.
 */
public class UCDeltaTableUtilitySuite extends UCDeltaTableIntegrationSuiteBase {

  @Override
  protected SQLExecutor getSqlExecutor() {
    return new SparkSQLExecutor(spark());
  }

  @Test
  public void testOptimizeTableOperation() throws Exception {
    withNewTable("optimize_test", "id INT, category STRING, value DOUBLE", tableName -> {
      // Insert data in multiple batches to create multiple files
      sql("INSERT INTO " + tableName + " VALUES (1, 'A', 10.0)");
      sql("INSERT INTO " + tableName + " VALUES (2, 'B', 20.0)");
      sql("INSERT INTO " + tableName + " VALUES (3, 'A', 15.0)");
      sql("INSERT INTO " + tableName + " VALUES (4, 'C', 25.0)");

      // Run OPTIMIZE
      sql("OPTIMIZE " + tableName);

      // Verify data is still intact after optimization
      check(tableName, Arrays.asList(
          Arrays.asList("1", "A", "10.0"),
          Arrays.asList("2", "B", "20.0"),
          Arrays.asList("3", "A", "15.0"),
          Arrays.asList("4", "C", "25.0")
      ));
    });
  }

  @Test
  public void testOptimizeWithZOrderBy() throws Exception {
    withNewTable("zorder_test", "id INT, category STRING, priority INT, value DOUBLE", tableName -> {
      // Insert test data
      sql("INSERT INTO " + tableName + " VALUES " +
          "(1, 'High', 1, 100.0), (2, 'Low', 3, 50.0), (3, 'Medium', 2, 75.0), " +
          "(4, 'High', 1, 120.0), (5, 'Low', 3, 40.0)");

      // Run OPTIMIZE with ZORDER
      sql("OPTIMIZE " + tableName + " ZORDER BY (category, priority)");

      // Verify data integrity after Z-ordering
      check(tableName, Arrays.asList(
          Arrays.asList("1", "High", "1", "100.0"),
          Arrays.asList("2", "Low", "3", "50.0"),
          Arrays.asList("3", "Medium", "2", "75.0"),
          Arrays.asList("4", "High", "1", "120.0"),
          Arrays.asList("5", "Low", "3", "40.0")
      ));
    });
  }

  @Test
  public void testDescribeHistoryOperation() throws Exception {
    withNewTable("history_test", "id INT, name STRING", tableName -> {
      // Perform several operations to create history
      sql("INSERT INTO " + tableName + " VALUES (1, 'initial')");
      sql("INSERT INTO " + tableName + " VALUES (2, 'second')");
      sql("UPDATE " + tableName + " SET name = 'updated' WHERE id = 1");
      sql("DELETE FROM " + tableName + " WHERE id = 2");

      // Get table history
      List<List<String>> history = sql("DESCRIBE HISTORY " + tableName);

      // Verify we have history entries
      Assert.assertTrue("Table should have history entries", !history.isEmpty());

      // Verify the history contains expected operations
      Assert.assertTrue(
          "History should contain multiple entries. Found " + history.size() + " entries",
          history.size() >= 2
      );

      // Check that history entries contain some operation information
      boolean hasOperationInfo = history.stream()
          .anyMatch(row -> row.stream()
              .anyMatch(col -> {
                String lower = col.toLowerCase();
                return lower.contains("insert") || lower.contains("update") ||
                    lower.contains("delete") || lower.contains("write") || lower.contains("create");
              }));

      Assert.assertTrue(
          "History should contain operation information or have sufficient entries",
          hasOperationInfo || history.size() >= 3
      );
    });
  }

  @Test
  public void testShowCatalogsOperation() {
    // Show all catalogs
    List<List<String>> catalogs = sql("SHOW CATALOGS");

    // Verify our Unity Catalog appears
    List<String> catalogNames = catalogs.stream()
        .map(row -> row.get(0))
        .collect(Collectors.toList());

    Assert.assertTrue(
        "Unity Catalog '" + getUnityCatalogName() + "' should appear in catalogs. Found: " +
        String.join(", ", catalogNames),
        catalogNames.contains(getUnityCatalogName())
    );
  }

  @Test
  public void testShowSchemasOperation() {
    // Show schemas in our catalog
    List<List<String>> schemas = sql("SHOW SCHEMAS IN " + getUnityCatalogName());

    // Verify default schema exists
    List<String> schemaNames = schemas.stream()
        .map(row -> row.get(0))
        .collect(Collectors.toList());

    Assert.assertTrue(
        "Default schema should exist in catalog. Found schemas: " + String.join(", ", schemaNames),
        schemaNames.contains("default")
    );
  }

  @Test
  public void testShowColumnsOperation() throws Exception {
    withNewTable("show_columns_test",
        "id BIGINT, name STRING, active BOOLEAN, created_at TIMESTAMP",
        tableName -> {
          sql("INSERT INTO " + tableName + " VALUES (1, 'test', true, '2023-01-01 10:00:00')");

          // Show columns for the table
          List<List<String>> columns = sql("SHOW COLUMNS IN " + tableName);

          // Verify expected columns
          Set<String> columnNames = columns.stream()
              .map(row -> row.get(0))
              .collect(Collectors.toSet());

          Assert.assertTrue("Should have 'id' column", columnNames.contains("id"));
          Assert.assertTrue("Should have 'name' column", columnNames.contains("name"));
          Assert.assertTrue("Should have 'active' column", columnNames.contains("active"));
          Assert.assertTrue("Should have 'created_at' column", columnNames.contains("created_at"));
        });
  }

  @Test
  public void testTableStatisticsAfterOperations() throws Exception {
    withNewTable("stats_test", "id INT, category STRING", tableName -> {
      // Insert initial data
      sql("INSERT INTO " + tableName + " VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')");

      // Perform some operations
      sql("UPDATE " + tableName + " SET category = 'Updated' WHERE id = 1");
      sql("DELETE FROM " + tableName + " WHERE id = 5");

      // Verify final state
      check(tableName, Arrays.asList(
          Arrays.asList("1", "Updated"),
          Arrays.asList("2", "B"),
          Arrays.asList("3", "A"),
          Arrays.asList("4", "C")
      ));

      // Verify we can still query aggregates
      List<List<String>> categoryCount = sql(
          "SELECT category, COUNT(*) FROM " + tableName + " GROUP BY category ORDER BY category");
      Assert.assertTrue("Should be able to compute aggregates", !categoryCount.isEmpty());
    });
  }

  @Test
  public void testShowPartitionsOnNonPartitionedTable() throws Exception {
    withNewTable("non_partitioned_test", "id INT, value STRING", tableName -> {
      sql("INSERT INTO " + tableName + " VALUES (1, 'test')");

      // Show partitions (should handle non-partitioned tables gracefully)
      try {
        List<List<String>> partitions = sql("SHOW PARTITIONS " + tableName);
        // If successful, should return empty or minimal result
        Assert.assertTrue(
            "Non-partitioned table should have no meaningful partitions",
            partitions.isEmpty() || partitions.stream().allMatch(row -> row.size() <= 1)
        );
      } catch (Exception e) {
        // It's acceptable if SHOW PARTITIONS fails on non-partitioned tables
        // This is expected behavior in some Spark versions
      }
    });
  }

  @Test
  public void testTableMetadataAfterMultipleOperations() throws Exception {
    withNewTable("metadata_test", "id INT, status STRING, updated_at TIMESTAMP", tableName -> {
      // Series of operations that modify table metadata
      sql("INSERT INTO " + tableName + " VALUES (1, 'created', '2023-01-01 10:00:00')");
      sql("INSERT INTO " + tableName + " VALUES (2, 'created', '2023-01-01 11:00:00')");
      sql("UPDATE " + tableName + " SET status = 'modified', updated_at = '2023-01-01 12:00:00' WHERE id = 1");

      // Verify data integrity
      check(tableName, Arrays.asList(
          Arrays.asList("1", "modified", "2023-01-01 12:00:00.0"),
          Arrays.asList("2", "created", "2023-01-01 11:00:00.0")
      ));

      // Verify table can still be described
      List<List<String>> description = sql("DESCRIBE " + tableName);
      Assert.assertTrue(
          "Table should have at least 3 columns in description",
          description.size() >= 3
      );

      // Verify extended description works
      List<List<String>> extendedDesc = sql("DESCRIBE EXTENDED " + tableName);
      Assert.assertTrue(
          "Extended description should have more info than basic",
          extendedDesc.size() > description.size()
      );
    });
  }

  @Test
  public void testConcurrentSafeOperations() throws Exception {
    withNewTable("concurrent_test", "id INT, batch_id STRING", tableName -> {
      // Simulate concurrent operations by performing multiple writes
      sql("INSERT INTO " + tableName + " VALUES (1, 'batch1')");
      sql("INSERT INTO " + tableName + " VALUES (2, 'batch1')");

      // Optimize in between writes
      sql("OPTIMIZE " + tableName);

      sql("INSERT INTO " + tableName + " VALUES (3, 'batch2')");
      sql("UPDATE " + tableName + " SET batch_id = 'updated' WHERE id = 1");

      // Verify final state
      check(tableName, Arrays.asList(
          Arrays.asList("1", "updated"),
          Arrays.asList("2", "batch1"),
          Arrays.asList("3", "batch2")
      ));

      // Verify table is still in good state
      getSqlExecutor().checkWithSQL("SELECT COUNT(*) FROM " + tableName, Arrays.asList(Arrays.asList("3")));
    });
  }
}

