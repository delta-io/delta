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

package com.sparkuctest

import org.apache.spark.SparkConf

/**
 * DML test suite for Delta Table operations through Unity Catalog.
 *
 * Covers INSERT, UPDATE, DELETE, and MERGE operations with various conditions and scenarios.
 * Based on UnityCatalogManagedTableDMLSuite from the Python integration tests.
 */
class UCDeltaTableDMLSuite extends UCDeltaTableIntegrationSuiteBase {

  override protected def sparkConf: SparkConf = configureSparkWithUnityCatalog(super.sparkConf)
  override protected def sqlExecutor: UCDeltaTableIntegrationSuiteBase.SQLExecutor = {
    new UCDeltaTableIntegrationSuiteBase.SparkSQLExecutor(spark)
  }

  test("UPDATE with simple condition") {
    withNewTable("update_simple_test", "id INT, status STRING") { tableName =>
      // Setup initial data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'pending'),
        (2, 'pending'),
        (3, 'completed')
      """)

      // Update specific rows
      sql(s"UPDATE $tableName SET status = 'processed' WHERE id = 1")

      // Verify update
      check(tableName, Seq(
        Seq("1", "processed"),
        Seq("2", "pending"),
        Seq("3", "completed")
      ))
    }
  }

  test("UPDATE with complex condition") {
    withNewTable("update_complex_test", "id INT, priority INT, status STRING") { tableName =>
      // Setup data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 1, 'low'),
        (2, 5, 'medium'),
        (3, 10, 'high'),
        (4, 2, 'low')
      """)

      // Update based on priority
      sql(s"UPDATE $tableName SET status = 'urgent' WHERE priority >= 5")

      // Verify update
      check(tableName, Seq(
        Seq("1", "1", "low"),
        Seq("2", "5", "urgent"),
        Seq("3", "10", "urgent"),
        Seq("4", "2", "low")
      ))
    }
  }

  test("DELETE with simple condition") {
    withNewTable("delete_simple_test", "id INT, active BOOLEAN") { tableName =>
      // Setup data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, true),
        (2, false),
        (3, true),
        (4, false)
      """)

      // Delete inactive records
      sql(s"DELETE FROM $tableName WHERE active = false")

      // Verify deletion
      check(tableName, Seq(
        Seq("1", "true"),
        Seq("3", "true")
      ))
    }
  }

  test("DELETE with complex condition") {
    withNewTable("delete_complex_test", "id INT, category STRING, value INT") { tableName =>
      // Setup data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'A', 10),
        (2, 'B', 20),
        (3, 'A', 30),
        (4, 'C', 5),
        (5, 'B', 15)
      """)

      // Delete records with specific conditions
      sql(s"DELETE FROM $tableName WHERE category = 'A' OR value < 10")

      // Verify deletion
      check(tableName, Seq(
        Seq("2", "B", "20"),
        Seq("5", "B", "15")
      ))
    }
  }

  test("MERGE - insert only") {
    withNewTable("merge_insert_test", "id INT, value STRING") { tableName =>
      // Setup target table with initial data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'existing1'),
        (2, 'existing2')
      """)

      // Create source data and perform merge
      withNewTable("merge_source", "id INT, value STRING") { sourceTable =>
        sql(s"""
          INSERT INTO $sourceTable VALUES
          (3, 'new3'),
          (4, 'new4')
        """)

        sql(s"""
          MERGE INTO $tableName AS target
          USING $sourceTable AS source
          ON target.id = source.id
          WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
        """)
      }

      // Verify merge result
      check(tableName, Seq(
        Seq("1", "existing1"),
        Seq("2", "existing2"),
        Seq("3", "new3"),
        Seq("4", "new4")
      ))
    }
  }

  test("MERGE - update only") {
    withNewTable("merge_update_test", "id INT, value STRING") { tableName =>
      // Setup target table
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'old1'),
        (2, 'old2'),
        (3, 'old3')
      """)

      // Perform merge to update existing records
      withNewTable("merge_update_source", "id INT, value STRING") { sourceTable =>
        sql(s"""
          INSERT INTO $sourceTable VALUES
          (2, 'updated2'),
          (3, 'updated3')
        """)

        sql(s"""
          MERGE INTO $tableName AS target
          USING $sourceTable AS source
          ON target.id = source.id
          WHEN MATCHED THEN UPDATE SET value = source.value
        """)
      }

      // Verify merge result
      check(tableName, Seq(
        Seq("1", "old1"),
        Seq("2", "updated2"),
        Seq("3", "updated3")
      ))
    }
  }

  test("MERGE - combined insert and update") {
    withNewTable("merge_combined_test", "id INT, name STRING, status STRING") { tableName =>
      // Setup target table
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'Alice', 'active'),
        (2, 'Bob', 'inactive')
      """)

      // Perform merge with both insert and update
      withNewTable("merge_combined_source", "id INT, name STRING, status STRING") { sourceTable =>
        sql(s"""
          INSERT INTO $sourceTable VALUES
          (2, 'Bob', 'active'),
          (3, 'Charlie', 'active'),
          (4, 'Diana', 'pending')
        """)

        sql(s"""
          MERGE INTO $tableName AS target
          USING $sourceTable AS source
          ON target.id = source.id
          WHEN MATCHED THEN UPDATE SET status = source.status
          WHEN NOT MATCHED THEN INSERT (id, name, status)
            VALUES (source.id, source.name, source.status)
        """)
      }

      // Verify merge result
      check(tableName, Seq(
        Seq("1", "Alice", "active"),
        Seq("2", "Bob", "active"),
        Seq("3", "Charlie", "active"),
        Seq("4", "Diana", "pending")
      ))
    }
  }

  test("MERGE with DELETE action") {
    withNewTable("merge_delete_test", "id INT, active BOOLEAN") { tableName =>
      // Setup target table
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, true),
        (2, true),
        (3, false),
        (4, true)
      """)

      // Perform merge with delete action
      withNewTable("merge_delete_source", "id INT, active BOOLEAN") { sourceTable =>
        sql(s"""
          INSERT INTO $sourceTable VALUES
          (2, false),
          (3, false),
          (5, true)
        """)

        sql(s"""
          MERGE INTO $tableName AS target
          USING $sourceTable AS source
          ON target.id = source.id
          WHEN MATCHED AND source.active = false THEN DELETE
          WHEN MATCHED THEN UPDATE SET active = source.active
          WHEN NOT MATCHED THEN INSERT (id, active) VALUES (source.id, source.active)
        """)
      }

      // Verify merge result - records 2 and 3 should be deleted
      check(tableName, Seq(
        Seq("1", "true"),
        Seq("4", "true"),
        Seq("5", "true")
      ))
    }
  }

  test("INSERT with SELECT") {
    withNewTable("insert_select_target", "id INT, category STRING") { targetTable =>
      withNewTable("insert_select_source", "id INT, name STRING") { sourceTable =>
        // Setup source data
        sql(s"""
          INSERT INTO $sourceTable VALUES
          (1, 'TypeA'),
          (2, 'TypeB'),
          (3, 'TypeA')
        """)

        // Insert from SELECT
        sql(s"""
          INSERT INTO $targetTable
          SELECT id, name FROM $sourceTable WHERE name = 'TypeA'
        """)

        // Verify result
        check(targetTable, Seq(
          Seq("1", "TypeA"),
          Seq("3", "TypeA")
        ))
      }
    }
  }

  test("INSERT operations - basic append") {
    withNewTable("insert_append_test", "id INT, value STRING") { tableName =>
      // Initial data
      sql(s"INSERT INTO $tableName VALUES (1, 'initial')")

      // Append more data
      sql(s"""
        INSERT INTO $tableName VALUES
        (2, 'appended1'),
        (3, 'appended2')
      """)

      // Verify appended data
      check(tableName, Seq(
        Seq("1", "initial"),
        Seq("2", "appended1"),
        Seq("3", "appended2")
      ))
    }
  }

  test("INSERT OVERWRITE operation") {
    withNewTable("insert_overwrite_test", "id INT, status STRING") { tableName =>
      // Initial data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'old'),
        (2, 'old'),
        (3, 'old')
      """)

      // Overwrite with new data
      sql(s"""
        INSERT OVERWRITE $tableName VALUES
        (4, 'new'),
        (5, 'new')
      """)

      // Verify data was overwritten
      check(tableName, Seq(
        Seq("4", "new"),
        Seq("5", "new")
      ))
    }
  }

  test("INSERT REPLACE WHERE operation") {
    withNewTable("insert_replace_test", "id INT, status STRING") { tableName =>
      // Initial data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'pending'),
        (2, 'pending'),
        (3, 'completed')
      """)

      // Replace specific rows
      sql(s"""
        INSERT INTO $tableName REPLACE WHERE id <= 2
        VALUES (1, 'replaced'), (2, 'replaced')
      """)

      // Verify replacement
      check(tableName, Seq(
        Seq("1", "replaced"),
        Seq("2", "replaced"),
        Seq("3", "completed")
      ))
    }
  }

  test("INSERT with VALUES - multiple patterns") {
    withNewTable("insert_patterns_test", "id INT, name STRING, active BOOLEAN") { tableName =>
      // Single INSERT with multiple rows
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'User1', true),
        (2, 'User2', false)
      """)

      // Multiple separate INSERT operations
      sql(s"INSERT INTO $tableName VALUES (3, 'User3', true)")
      sql(s"INSERT INTO $tableName VALUES (4, 'User4', false)")

      // Verify all inserts worked
      check(tableName, Seq(
        Seq("1", "User1", "true"),
        Seq("2", "User2", "false"),
        Seq("3", "User3", "true"),
        Seq("4", "User4", "false")
      ))
    }
  }
}
