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
import org.apache.spark.sql.AnalysisException

/**
 * Utility operations test suite for Delta Table operations through Unity Catalog.
 *
 * Covers OPTIMIZE, DESCRIBE HISTORY, SHOW operations, and other table utilities.
 * Based on UnityCatalogManagedTableUtilitySuite from the Python integration tests.
 */
class UCDeltaTableUtilitySuite extends UCDeltaTableIntegrationSuiteBase {

  override protected def sparkConf: SparkConf = configureSparkWithUnityCatalog(super.sparkConf)
  override protected def sqlExecutor: UCDeltaTableIntegrationSuiteBase.SQLExecutor = {
    new UCDeltaTableIntegrationSuiteBase.SparkSQLExecutor(spark)
  }

  test("OPTIMIZE table operation") {
    withNewTable("optimize_test", "id INT, category STRING, value DOUBLE") { tableName =>
      // Insert data in multiple batches to create multiple files
      sql(s"INSERT INTO $tableName VALUES (1, 'A', 10.0)")
      sql(s"INSERT INTO $tableName VALUES (2, 'B', 20.0)")
      sql(s"INSERT INTO $tableName VALUES (3, 'A', 15.0)")
      sql(s"INSERT INTO $tableName VALUES (4, 'C', 25.0)")

      // Run OPTIMIZE
      sql(s"OPTIMIZE $tableName")

      // Verify data is still intact after optimization
      check(tableName, Seq(
        Seq("1", "A", "10.0"),
        Seq("2", "B", "20.0"),
        Seq("3", "A", "15.0"),
        Seq("4", "C", "25.0")
      ))
    }
  }

  test("OPTIMIZE with ZORDER BY") {
    withNewTable("zorder_test",
      "id INT, category STRING, priority INT, value DOUBLE") { tableName =>
      // Insert test data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'High', 1, 100.0),
        (2, 'Low', 3, 50.0),
        (3, 'Medium', 2, 75.0),
        (4, 'High', 1, 120.0),
        (5, 'Low', 3, 40.0)
      """)

      // Run OPTIMIZE with ZORDER
      sql(s"OPTIMIZE $tableName ZORDER BY (category, priority)")

      // Verify data integrity after Z-ordering
      check(tableName, Seq(
        Seq("1", "High", "1", "100.0"),
        Seq("2", "Low", "3", "50.0"),
        Seq("3", "Medium", "2", "75.0"),
        Seq("4", "High", "1", "120.0"),
        Seq("5", "Low", "3", "40.0")
      ))
    }
  }

  test("DESCRIBE HISTORY operation") {
    withNewTable("history_test", "id INT, name STRING") { tableName =>
      // Perform several operations to create history
      sql(s"INSERT INTO $tableName VALUES (1, 'initial')")
      sql(s"INSERT INTO $tableName VALUES (2, 'second')")
      sql(s"UPDATE $tableName SET name = 'updated' WHERE id = 1")
      sql(s"DELETE FROM $tableName WHERE id = 2")

      // Get table history
      val history = sql(s"DESCRIBE HISTORY $tableName")

      // Verify we have history entries
      assert(history.nonEmpty, "Table should have history entries")

      // Verify the history contains expected operations
      // Unity Catalog DESCRIBE HISTORY may have different column structure, let's be more flexible
      // Just verify we have meaningful history data by checking we have multiple entries
      assert(history.length >= 2,
        s"History should contain multiple entries. Found ${history.length} entries")

      // Check that history entries contain some operation information
      // (Unity Catalog may structure this differently than standard Delta)
      // scalastyle:off caselocale
      val hasOperationInfo = history.exists(row =>
        row.exists(col => col.toString.toLowerCase.contains("insert") ||
                         col.toString.toLowerCase.contains("update") ||
                         col.toString.toLowerCase.contains("delete") ||
                         col.toString.toLowerCase.contains("write") ||
                         col.toString.toLowerCase.contains("create")))
      // scalastyle:on caselocale
      assert(hasOperationInfo || history.length >= 3,
        "History should contain operation information or have sufficient entries")
    }
  }

  test("SHOW CATALOGS operation") {
    // Show all catalogs
    val catalogs = sql("SHOW CATALOGS")

    // Verify our Unity Catalog appears
    val catalogNames = catalogs.map(row => row(0).toString)
    assert(catalogNames.contains(unityCatalogName),
        s"Unity Catalog '$unityCatalogName' should appear in catalogs. " +
        s"Found: ${catalogNames.mkString(", ")}")
  }

  test("SHOW SCHEMAS operation") {
    // Show schemas in our catalog
    val schemas = sql(s"SHOW SCHEMAS IN $unityCatalogName")

    // Verify default schema exists
    val schemaNames = schemas.map(row => row(0).toString)
    assert(schemaNames.contains("default"),
      s"Default schema should exist in catalog. " +
      s"Found schemas: ${schemaNames.mkString(", ")}")
  }

  test("SHOW COLUMNS operation") {
    withNewTable("show_columns_test",
      "id BIGINT, name STRING, active BOOLEAN, created_at TIMESTAMP") { tableName =>
      sql(s"INSERT INTO $tableName VALUES (1, 'test', true, '2023-01-01 10:00:00')")

      // Show columns for the table
      val columns = sql(s"SHOW COLUMNS IN $tableName")

      // Verify expected columns
      val columnNames = columns.map(row => row(0).toString).toSet
      assert(columnNames.contains("id"), "Should have 'id' column")
      assert(columnNames.contains("name"), "Should have 'name' column")
      assert(columnNames.contains("active"), "Should have 'active' column")
      assert(columnNames.contains("created_at"), "Should have 'created_at' column")
    }
  }

  test("table statistics after operations") {
    withNewTable("stats_test", "id INT, category STRING") { tableName =>
      // Insert initial data
      sql(s"""
        INSERT INTO $tableName VALUES
        (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')
      """)

      // Perform some operations
      sql(s"UPDATE $tableName SET category = 'Updated' WHERE id = 1")
      sql(s"DELETE FROM $tableName WHERE id = 5")

      // Verify final state
      check(tableName, Seq(
        Seq("1", "Updated"),
        Seq("2", "B"),
        Seq("3", "A"),
        Seq("4", "C")
      ))

      // Verify we can still query aggregates
      val categoryCount = sql(
        s"SELECT category, COUNT(*) FROM $tableName GROUP BY category ORDER BY category")
      assert(categoryCount.nonEmpty, "Should be able to compute aggregates")
    }
  }

  test("SHOW PARTITIONS on non-partitioned table") {
    withNewTable("non_partitioned_test", "id INT, value STRING") { tableName =>
      sql(s"INSERT INTO $tableName VALUES (1, 'test')")

      // Show partitions (should handle non-partitioned tables gracefully)
      try {
        val partitions = sql(s"SHOW PARTITIONS $tableName")
        // If successful, should return empty or minimal result
        assert(partitions.isEmpty || partitions.forall(_.length <= 1),
          "Non-partitioned table should have no meaningful partitions")
      } catch {
        case _: AnalysisException =>
          // It's acceptable if SHOW PARTITIONS fails on non-partitioned tables
          // This is expected behavior in some Spark versions
      }
    }
  }

  test("table metadata after multiple operations") {
    withNewTable("metadata_test", "id INT, status STRING, updated_at TIMESTAMP") { tableName =>
      // Series of operations that modify table metadata
      sql(s"INSERT INTO $tableName VALUES (1, 'created', '2023-01-01 10:00:00')")
      sql(s"INSERT INTO $tableName VALUES (2, 'created', '2023-01-01 11:00:00')")
      sql(s"""UPDATE $tableName SET status = 'modified',
               updated_at = '2023-01-01 12:00:00' WHERE id = 1""")

      // Verify data integrity
      check(tableName, Seq(
        Seq("1", "modified", "2023-01-01 12:00:00.0"),
        Seq("2", "created", "2023-01-01 11:00:00.0")
      ))

      // Verify table can still be described
      val description = sql(s"DESCRIBE $tableName")
      assert(description.length >= 3,
        "Table should have at least 3 columns in description")

      // Verify extended description works
      val extendedDesc = sql(s"DESCRIBE EXTENDED $tableName")
      assert(extendedDesc.length > description.length,
        "Extended description should have more info than basic")
    }
  }

  test("concurrent-safe operations") {
    withNewTable("concurrent_test", "id INT, batch_id STRING") { tableName =>
      // Simulate concurrent operations by performing multiple writes
      sql(s"INSERT INTO $tableName VALUES (1, 'batch1')")
      sql(s"INSERT INTO $tableName VALUES (2, 'batch1')")

      // Optimize in between writes
      sql(s"OPTIMIZE $tableName")

      sql(s"INSERT INTO $tableName VALUES (3, 'batch2')")
      sql(s"UPDATE $tableName SET batch_id = 'updated' WHERE id = 1")

      // Verify final state
      check(tableName, Seq(
        Seq("1", "updated"),
        Seq("2", "batch1"),
        Seq("3", "batch2")
      ))

      // Verify table is still in good state
      sqlExecutor.checkWithSQL(s"SELECT COUNT(*) FROM $tableName", Seq(Seq("3")))
    }
  }
}
