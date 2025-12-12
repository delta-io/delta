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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * DML test suite for Delta Table operations through Unity Catalog.
 *
 * Covers INSERT, UPDATE, DELETE, and MERGE operations with various conditions and scenarios.
 */
public class UCDeltaTableDMLTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testUpdateWithSimpleCondition() throws Exception {
    withNewTable("update_simple_test", "id INT, status STRING", tableName -> {
      // Setup initial data
      sql("INSERT INTO %s VALUES (1, 'pending'), (2, 'pending'), (3, 'completed')", tableName);

      // Update specific rows
      sql("UPDATE %s SET status = 'processed' WHERE id = 1", tableName);

      // Verify update
      check(tableName, Arrays.asList(
          Arrays.asList("1", "processed"),
          Arrays.asList("2", "pending"),
          Arrays.asList("3", "completed")
      ));
    });
  }

  @Test
  public void testUpdateWithComplexCondition() throws Exception {
    withNewTable("update_complex_test", "id INT, priority INT, status STRING", tableName -> {
      // Setup data
      sql("INSERT INTO %s VALUES " +
          "(1, 1, 'low'), (2, 5, 'medium'), (3, 10, 'high'), (4, 2, 'low')", tableName);

      // Update based on priority
      sql("UPDATE %s SET status = 'urgent' WHERE priority >= 5", tableName);

      // Verify update
      check(tableName, Arrays.asList(
          Arrays.asList("1", "1", "low"),
          Arrays.asList("2", "5", "urgent"),
          Arrays.asList("3", "10", "urgent"),
          Arrays.asList("4", "2", "low")
      ));
    });
  }

  @Test
  public void testDeleteWithSimpleCondition() throws Exception {
    withNewTable("delete_simple_test", "id INT, active BOOLEAN", tableName -> {
      // Setup data
      sql("INSERT INTO %s VALUES (1, true), (2, false), (3, true), (4, false)", tableName);

      // Delete inactive records
      sql("DELETE FROM %s WHERE active = false", tableName);

      // Verify deletion
      check(tableName, Arrays.asList(
          Arrays.asList("1", "true"),
          Arrays.asList("3", "true")
      ));
    });
  }

  @Test
  public void testDeleteWithComplexCondition() throws Exception {
    withNewTable("delete_complex_test", "id INT, category STRING, value INT", tableName -> {
      // Setup data
      sql("INSERT INTO %s VALUES " +
          "(1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'C', 5), (5, 'B', 15)", tableName);

      // Delete records with specific conditions
      sql("DELETE FROM %s WHERE category = 'A' OR value < 10", tableName);

      // Verify deletion
      check(tableName, Arrays.asList(
          Arrays.asList("2", "B", "20"),
          Arrays.asList("5", "B", "15")
      ));
    });
  }

  @Test
  public void testMergeInsertOnly() throws Exception {
    withNewTable("merge_insert_test", "id INT, value STRING", tableName -> {
      // Setup target table with initial data
      sql("INSERT INTO %s VALUES (1, 'existing1'), (2, 'existing2')", tableName);

      // Create source data and perform merge
      withNewTable("merge_source", "id INT, value STRING", sourceTable -> {
        sql("INSERT INTO %s VALUES (3, 'new3'), (4, 'new4')", sourceTable);

        sql("MERGE INTO %s AS target " +
            "USING %s AS source " +
            "ON target.id = source.id " +
            "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)", tableName, sourceTable);
      });

      // Verify merge result
      check(tableName, Arrays.asList(
          Arrays.asList("1", "existing1"),
          Arrays.asList("2", "existing2"),
          Arrays.asList("3", "new3"),
          Arrays.asList("4", "new4")
      ));
    });
  }

  @Test
  public void testMergeUpdateOnly() throws Exception {
    withNewTable("merge_update_test", "id INT, value STRING", tableName -> {
      // Setup target table
      sql("INSERT INTO %s VALUES (1, 'old1'), (2, 'old2'), (3, 'old3')", tableName);

      // Perform merge to update existing records
      withNewTable("merge_update_source", "id INT, value STRING", sourceTable -> {
        sql("INSERT INTO %s VALUES (2, 'updated2'), (3, 'updated3')", sourceTable);

        sql("MERGE INTO %s AS target " +
            "USING %s AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED THEN UPDATE SET value = source.value", tableName, sourceTable);
      });

      // Verify merge result
      check(tableName, Arrays.asList(
          Arrays.asList("1", "old1"),
          Arrays.asList("2", "updated2"),
          Arrays.asList("3", "updated3")
      ));
    });
  }

  @Test
  public void testMergeCombinedInsertAndUpdate() throws Exception {
    withNewTable("merge_combined_test", "id INT, name STRING, status STRING", tableName -> {
      // Setup target table
      sql("INSERT INTO %s VALUES (1, 'Alice', 'active'), (2, 'Bob', 'inactive')", tableName);

      // Perform merge with both insert and update
      withNewTable("merge_combined_source", "id INT, name STRING, status STRING", sourceTable -> {
        sql("INSERT INTO %s VALUES " +
            "(2, 'Bob', 'active'), (3, 'Charlie', 'active'), (4, 'Diana', 'pending')", sourceTable);

        sql("MERGE INTO %s AS target " +
            "USING %s AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED THEN UPDATE SET status = source.status " +
            "WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (source.id, source.name, source.status)", tableName, sourceTable);
      });

      // Verify merge result
      check(tableName, Arrays.asList(
          Arrays.asList("1", "Alice", "active"),
          Arrays.asList("2", "Bob", "active"),
          Arrays.asList("3", "Charlie", "active"),
          Arrays.asList("4", "Diana", "pending")
      ));
    });
  }

  @Test
  public void testMergeWithDeleteAction() throws Exception {
    withNewTable("merge_delete_test", "id INT, active BOOLEAN", tableName -> {
      // Setup target table
      sql("INSERT INTO %s VALUES (1, true), (2, true), (3, false), (4, true)", tableName);

      // Perform merge with delete action
      withNewTable("merge_delete_source", "id INT, active BOOLEAN", sourceTable -> {
        sql("INSERT INTO %s VALUES (2, false), (3, false), (5, true)", sourceTable);

        sql("MERGE INTO %s AS target " +
            "USING %s AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND source.active = false THEN DELETE " +
            "WHEN MATCHED THEN UPDATE SET active = source.active " +
            "WHEN NOT MATCHED THEN INSERT (id, active) VALUES (source.id, source.active)", tableName, sourceTable);
      });

      // Verify merge result - records 2 and 3 should be deleted
      check(tableName, Arrays.asList(
          Arrays.asList("1", "true"),
          Arrays.asList("4", "true"),
          Arrays.asList("5", "true")
      ));
    });
  }

  @Test
  public void testInsertWithSelect() throws Exception {
    withNewTable("insert_select_target", "id INT, category STRING", targetTable -> {
      withNewTable("insert_select_source", "id INT, name STRING", sourceTable -> {
        // Setup source data
        sql("INSERT INTO %s VALUES (1, 'TypeA'), (2, 'TypeB'), (3, 'TypeA')", sourceTable);

        // Insert from SELECT
        sql("INSERT INTO %s " +
            "SELECT id, name FROM %s WHERE name = 'TypeA'", targetTable, sourceTable);

        // Verify result
        check(targetTable, Arrays.asList(
            Arrays.asList("1", "TypeA"),
            Arrays.asList("3", "TypeA")
        ));
      });
    });
  }

  @Test
  public void testInsertOperationsBasicAppend() throws Exception {
    withNewTable("insert_append_test", "id INT, value STRING", tableName -> {
      // Initial data
      sql("INSERT INTO %s VALUES (1, 'initial')", tableName);

      // Append more data
      sql("INSERT INTO %s VALUES (2, 'appended1'), (3, 'appended2')", tableName);

      // Verify appended data
      check(tableName, Arrays.asList(
          Arrays.asList("1", "initial"),
          Arrays.asList("2", "appended1"),
          Arrays.asList("3", "appended2")
      ));
    });
  }

  @Test
  public void testInsertOverwriteOperation() throws Exception {
    withNewTable("insert_overwrite_test", "id INT, status STRING", tableName -> {
      // Initial data
      sql("INSERT INTO %s VALUES (1, 'old'), (2, 'old'), (3, 'old')", tableName);

      // Overwrite with new data
      sql("INSERT OVERWRITE %s VALUES (4, 'new'), (5, 'new')", tableName);

      // Verify data was overwritten
      check(tableName, Arrays.asList(
          Arrays.asList("4", "new"),
          Arrays.asList("5", "new")
      ));
    });
  }

  @Test
  public void testInsertReplaceWhereOperation() throws Exception {
    withNewTable("insert_replace_test", "id INT, status STRING", tableName -> {
      // Initial data
      sql("INSERT INTO %s VALUES (1, 'pending'), (2, 'pending'), (3, 'completed')", tableName);

      // Replace specific rows
      sql("INSERT INTO %s REPLACE WHERE id <= 2 " +
          "VALUES (1, 'replaced'), (2, 'replaced')", tableName);

      // Verify replacement
      check(tableName, Arrays.asList(
          Arrays.asList("1", "replaced"),
          Arrays.asList("2", "replaced"),
          Arrays.asList("3", "completed")
      ));
    });
  }

  @Test
  public void testInsertWithValuesMultiplePatterns() throws Exception {
    withNewTable("insert_patterns_test", "id INT, name STRING, active BOOLEAN", tableName -> {
      // Single INSERT with multiple rows
      sql("INSERT INTO %s VALUES (1, 'User1', true), (2, 'User2', false)", tableName);

      // Multiple separate INSERT operations
      sql("INSERT INTO %s VALUES (3, 'User3', true)", tableName);
      sql("INSERT INTO %s VALUES (4, 'User4', false)", tableName);

      // Verify all inserts worked
      check(tableName, Arrays.asList(
          Arrays.asList("1", "User1", "true"),
          Arrays.asList("2", "User2", "false"),
          Arrays.asList("3", "User3", "true"),
          Arrays.asList("4", "User4", "false")
      ));
    });
  }
}

