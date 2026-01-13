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

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * DML test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers INSERT, UPDATE, DELETE, and MERGE operations with various conditions and scenarios.
 * Tests are parameterized to support different table types (currently EXTERNAL only, as Delta does
 * not support MANAGED catalog-owned tables).
 */
public class UCDeltaTableDMLTest extends UCDeltaTableIntegrationBaseTest {

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testBasicInsertOperations(TableType tableType) throws Exception {
    withNewTable(
        "insert_basic_test",
        "id INT, name STRING, active BOOLEAN",
        tableType,
        tableName -> {
          // Single row INSERT
          sql("INSERT INTO %s VALUES (1, 'initial', true)", tableName);

          // Verify single row
          check(tableName, List.of(List.of("1", "initial", "true")));

          // Multiple rows in single INSERT
          sql("INSERT INTO %s VALUES (2, 'User2', false), (3, 'User3', true)", tableName);

          // Multiple separate INSERT operations
          sql("INSERT INTO %s VALUES (4, 'User4', false)", tableName);

          // Verify all inserts (appended data)
          check(
              tableName,
              List.of(
                  List.of("1", "initial", "true"),
                  List.of("2", "User2", "false"),
                  List.of("3", "User3", "true"),
                  List.of("4", "User4", "false")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testAdvancedInsertOperations(TableType tableType) throws Exception {
    // Test INSERT ... SELECT
    withNewTable(
        "insert_select_target",
        "id INT, category STRING",
        tableType,
        targetTable -> {
          withNewTable(
              "insert_select_source",
              "id INT, name STRING",
              tableType,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (1, 'TypeA'), (2, 'TypeB'), (3, 'TypeA')", sourceTable);
                sql(
                    "INSERT INTO %s SELECT id, name FROM %s WHERE name = 'TypeA'",
                    targetTable, sourceTable);

                check(targetTable, List.of(List.of("1", "TypeA"), List.of("3", "TypeA")));
              });
        });

    // Test INSERT OVERWRITE
    withNewTable(
        "insert_overwrite_test",
        "id INT, status STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'old'), (2, 'old'), (3, 'old')", tableName);
          sql("INSERT OVERWRITE %s VALUES (4, 'new'), (5, 'new')", tableName);

          check(tableName, List.of(List.of("4", "new"), List.of("5", "new")));
        });

    // Test INSERT ... REPLACE WHERE
    withNewTable(
        "insert_replace_test",
        "id INT, status STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'pending'), (2, 'pending'), (3, 'completed')", tableName);
          sql(
              "INSERT INTO %s REPLACE WHERE id <= 2 VALUES (1, 'replaced'), (2, 'replaced')",
              tableName);

          check(
              tableName,
              List.of(
                  List.of("1", "replaced"), List.of("2", "replaced"), List.of("3", "completed")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testInsertWithDynamicPartitionOverwrite(TableType tableType) throws Exception {
    withNewTable(
        "insert_dynamic_partition_overwrite_test",
        "id INT, name STRING, date STRING",
        "date",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s PARTITION (date='2025-11-01') VALUES (1, 'AAA')", tableName);
          sql("INSERT INTO %s PARTITION (date='2025-11-01') VALUES (2, 'BBB')", tableName);

          // Verify the result before dynamic partition overwrite.
          check(
              tableName,
              List.of(List.of("1", "AAA", "2025-11-01"), List.of("2", "BBB", "2025-11-01")));

          // Enable dynamic partition overwrite
          sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");

          try {
            // Insert with dynamic partition overwrite
            sql("INSERT OVERWRITE %s VALUES (3, 'CCC', '2025-11-01')", tableName);

            // Verify the result - should have replaced with the new value
            check(tableName, List.of(List.of("3", "CCC", "2025-11-01")));
          } finally {
            // Disable dynamic partition overwrite
            sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");
          }
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testUpdateOperations(TableType tableType) throws Exception {
    withNewTable(
        "update_test",
        "id INT, priority INT, status STRING",
        tableType,
        tableName -> {
          // Setup data
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, 1, 'pending'), (2, 5, 'pending'), (3, 10, 'pending'), (4, 2, 'completed')",
              tableName);

          // Simple update: update specific row by id
          sql("UPDATE %s SET status = 'processed' WHERE id = 1", tableName);

          // Verify simple update
          check(
              tableName,
              List.of(
                  List.of("1", "1", "processed"),
                  List.of("2", "5", "pending"),
                  List.of("3", "10", "pending"),
                  List.of("4", "2", "completed")));

          // Complex update: update based on priority condition
          sql("UPDATE %s SET status = 'urgent' WHERE priority >= 5", tableName);

          // Verify complex update
          check(
              tableName,
              List.of(
                  List.of("1", "1", "processed"),
                  List.of("2", "5", "urgent"),
                  List.of("3", "10", "urgent"),
                  List.of("4", "2", "completed")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testDeleteOperations(TableType tableType) throws Exception {
    withNewTable(
        "delete_test",
        "id INT, category STRING, value INT, active BOOLEAN",
        tableType,
        tableName -> {
          // Setup data
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, 'A', 10, true), (2, 'B', 20, false), (3, 'A', 30, true), "
                  + "(4, 'C', 5, false), (5, 'B', 15, true)",
              tableName);

          // Simple delete: single condition
          sql("DELETE FROM %s WHERE active = false", tableName);

          // Verify simple delete
          check(
              tableName,
              List.of(
                  List.of("1", "A", "10", "true"),
                  List.of("3", "A", "30", "true"),
                  List.of("5", "B", "15", "true")));

          // Complex delete: multiple conditions with OR
          sql("DELETE FROM %s WHERE category = 'A' OR value < 10", tableName);

          // Verify complex delete
          check(tableName, List.of(List.of("5", "B", "15", "true")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testMergeInsertOnly(TableType tableType) throws Exception {
    withNewTable(
        "merge_insert_test",
        "id INT, value STRING",
        tableType,
        tableName -> {
          // Setup target table with initial data
          sql("INSERT INTO %s VALUES (1, 'existing1'), (2, 'existing2')", tableName);

          // Create source data and perform merge
          withNewTable(
              "merge_source",
              "id INT, value STRING",
              tableType,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (3, 'new3'), (4, 'new4')", sourceTable);

                sql(
                    "MERGE INTO %s AS target "
                        + "USING %s AS source "
                        + "ON target.id = source.id "
                        + "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)",
                    tableName, sourceTable);
              });

          // Verify merge result
          check(
              tableName,
              List.of(
                  List.of("1", "existing1"),
                  List.of("2", "existing2"),
                  List.of("3", "new3"),
                  List.of("4", "new4")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testMergeUpdateOnly(TableType tableType) throws Exception {
    withNewTable(
        "merge_update_test",
        "id INT, value STRING",
        tableType,
        tableName -> {
          // Setup target table
          sql("INSERT INTO %s VALUES (1, 'old1'), (2, 'old2'), (3, 'old3')", tableName);

          // Perform merge to update existing records
          withNewTable(
              "merge_update_source",
              "id INT, value STRING",
              tableType,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, 'updated2'), (3, 'updated3')", sourceTable);

                sql(
                    "MERGE INTO %s AS target "
                        + "USING %s AS source "
                        + "ON target.id = source.id "
                        + "WHEN MATCHED THEN UPDATE SET value = source.value",
                    tableName, sourceTable);
              });

          // Verify merge result
          check(
              tableName,
              List.of(List.of("1", "old1"), List.of("2", "updated2"), List.of("3", "updated3")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testMergeCombinedInsertAndUpdate(TableType tableType) throws Exception {
    withNewTable(
        "merge_combined_test",
        "id INT, name STRING, status STRING",
        tableType,
        tableName -> {
          // Setup target table
          sql("INSERT INTO %s VALUES (1, 'Alice', 'active'), (2, 'Bob', 'inactive')", tableName);

          // Perform merge with both insert and update
          withNewTable(
              "merge_combined_source",
              "id INT, name STRING, status STRING",
              tableType,
              sourceTable -> {
                sql(
                    "INSERT INTO %s VALUES "
                        + "(2, 'Bob', 'active'), (3, 'Charlie', 'active'), (4, 'Diana', 'pending')",
                    sourceTable);

                sql(
                    "MERGE INTO %s AS target "
                        + "USING %s AS source "
                        + "ON target.id = source.id "
                        + "WHEN MATCHED THEN UPDATE SET status = source.status "
                        + "WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (source.id, source.name, source.status)",
                    tableName, sourceTable);
              });

          // Verify merge result
          check(
              tableName,
              List.of(
                  List.of("1", "Alice", "active"),
                  List.of("2", "Bob", "active"),
                  List.of("3", "Charlie", "active"),
                  List.of("4", "Diana", "pending")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testMergeWithDeleteAction(TableType tableType) throws Exception {
    withNewTable(
        "merge_delete_test",
        "id INT, active BOOLEAN",
        tableType,
        tableName -> {
          // Setup target table
          sql("INSERT INTO %s VALUES (1, true), (2, true), (3, false), (4, true)", tableName);

          // Perform merge with delete action
          withNewTable(
              "merge_delete_source",
              "id INT, active BOOLEAN",
              tableType,
              sourceTable -> {
                sql("INSERT INTO %s VALUES (2, false), (3, true), (5, true)", sourceTable);

                sql(
                    "MERGE INTO %s AS target "
                        + "USING %s AS source "
                        + "ON target.id = source.id "
                        + "WHEN MATCHED AND source.active = false THEN DELETE "
                        + "WHEN MATCHED THEN UPDATE SET active = source.active "
                        + "WHEN NOT MATCHED THEN INSERT (id, active) VALUES (source.id, source.active)",
                    tableName, sourceTable);
              });

          // Verify merge result - record 2 should be deleted, record 3 should be updated
          check(
              tableName,
              List.of(
                  List.of("1", "true"), // not in source, no change
                  List.of("3", "true"), // matched and updated from false to true
                  List.of("4", "true"), // not in source, no change
                  List.of("5", "true") // not matched in target, inserted
                  ));
        });
  }
}
