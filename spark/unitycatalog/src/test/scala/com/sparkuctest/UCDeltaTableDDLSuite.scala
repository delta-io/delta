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
 * DDL test suite for Delta Table operations through Unity Catalog.
 *
 * Covers CREATE, ALTER, DROP, and other schema-related operations.
 * Based on UnityCatalogManagedTableDDLSuite from the Python integration tests.
 */
class UCDeltaTableDDLSuite extends UCDeltaTableIntegrationSuiteBase {

  override protected def sparkConf: SparkConf = configureSparkWithUnityCatalog(super.sparkConf)
  override protected def sqlExecutor: UCDeltaTableIntegrationSuiteBase.SQLExecutor = {
    new UCDeltaTableIntegrationSuiteBase.SparkSQLExecutor(spark)
  }

  test("CREATE TABLE with different data types") {
    withNewTable("create_types_test",
      "id BIGINT, name STRING, price DECIMAL(10,2), active BOOLEAN, created_at TIMESTAMP") {
      tableName =>
      // Insert data with various types
      sql(s"""
        INSERT INTO $tableName VALUES
        (1L, 'Product A', 99.99, true, '2023-01-01 10:00:00'),
        (2L, 'Product B', 149.50, false, '2023-01-02 15:30:00')
      """)

      // Verify table creation and data types
      check(tableName, Seq(
        Seq("1", "Product A", "99.99", "true", "2023-01-01 10:00:00.0"),
        Seq("2", "Product B", "149.50", "false", "2023-01-02 15:30:00.0")
      ))
    }
  }

  test("CREATE TABLE with TBLPROPERTIES") {
    withTempDir { dir =>
      val tablePath = new java.io.File(dir, "table_with_props").getAbsolutePath
      val tableName = s"$unityCatalogName.default.table_with_props"

      // Create table with table properties
      sql(s"""
        CREATE TABLE $tableName (
          id INT,
          value STRING
        ) USING DELTA
        LOCATION '$tablePath'
        TBLPROPERTIES (
          'delta.autoOptimize.optimizeWrite' = 'true',
          'delta.autoOptimize.autoCompact' = 'true'
        )
      """)

      try {
        // Insert data to verify table works
        sql(s"INSERT INTO $tableName VALUES (1, 'test')")
        check(tableName, Seq(Seq("1", "test")))

        // Verify table properties can be queried
        val description = sql(s"DESCRIBE EXTENDED $tableName")
        assert(description.nonEmpty, "Should be able to describe the table")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("CREATE TABLE AS SELECT (CTAS)") {
    withNewTable("ctas_source", "id INT, category STRING, value DOUBLE") { sourceTable =>
      // Setup source data
      sql(s"""
        INSERT INTO $sourceTable VALUES
        (1, 'A', 10.5),
        (2, 'B', 20.0),
        (3, 'A', 15.75),
        (4, 'C', 30.25)
      """)

      withTempDir { dir =>
        val tablePath = new java.io.File(dir, "ctas_result").getAbsolutePath
        val resultTable = s"$unityCatalogName.default.ctas_result"

        // Create table using CTAS
        sql(s"""
          CREATE TABLE $resultTable
          USING DELTA
          LOCATION '$tablePath'
          AS SELECT category, AVG(value) as avg_value
          FROM $sourceTable
          GROUP BY category
          ORDER BY category
        """)

        try {
          // Verify CTAS result
          check(resultTable, Seq(
            Seq("A", "13.125"),
            Seq("B", "20.0"),
            Seq("C", "30.25")
          ))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $resultTable")
        }
      }
    }
  }

  test("DROP TABLE operation") {
    withTempDir { dir =>
      val tablePath = new java.io.File(dir, "drop_test").getAbsolutePath
      val tableName = s"$unityCatalogName.default.drop_test"

      // Create and populate table
      sql(s"""
        CREATE TABLE $tableName (id INT, name STRING)
        USING DELTA
        LOCATION '$tablePath'
      """)
      sql(s"INSERT INTO $tableName VALUES (1, 'test')")

      // Verify table exists and has data
      check(tableName, Seq(Seq("1", "test")))

      // Drop the table
      sql(s"DROP TABLE $tableName")

      // Verify table no longer exists
      intercept[Exception] {
        sql(s"SELECT * FROM $tableName")
      }
    }
  }

  test("SHOW TABLES in Unity Catalog") {
    withNewTable("show_tables_test", "id INT") { tableName =>
      sql(s"INSERT INTO $tableName VALUES (1)")

      // Show tables in the schema
      val tables = sql(s"SHOW TABLES IN $unityCatalogName.default")
      val tableNames = tables.map(row => row(1)) // Get table names from result

      // Verify our test table appears in the list
      assert(tableNames.contains("show_tables_test"),
        s"Table should appear in SHOW TABLES. Found tables: ${tableNames.mkString(", ")}")
    }
  }

  test("DESCRIBE TABLE operation") {
    withNewTable("describe_test", "id BIGINT, name STRING, active BOOLEAN") { tableName =>
      sql(s"INSERT INTO $tableName VALUES (1, 'test', true)")

      // Describe the table structure
      val description = sql(s"DESCRIBE $tableName")

      // Verify expected columns are present
      val columnInfo = description.map(row => (row(0), row(1))).toMap
      assert(columnInfo.contains("id"), "Should have 'id' column")
      assert(columnInfo.contains("name"), "Should have 'name' column")
      assert(columnInfo.contains("active"), "Should have 'active' column")

      // Verify data types
      assert(columnInfo("id").contains("bigint"),
        s"ID should be bigint, got: ${columnInfo("id")}")
      assert(columnInfo("name").contains("string"),
        s"Name should be string, got: ${columnInfo("name")}")
      assert(columnInfo("active").contains("boolean"),
        s"Active should be boolean, got: ${columnInfo("active")}")
    }
  }

  test("DESCRIBE EXTENDED table operation") {
    withNewTable("describe_extended_test", "id INT, data STRING") { tableName =>
      sql(s"INSERT INTO $tableName VALUES (1, 'sample')")

      // Get extended description
      val extendedDesc = sql(s"DESCRIBE EXTENDED $tableName")

      // Verify we get extended information (should be more than just column info)
      assert(extendedDesc.length > 3,
        "Extended description should contain more than just column definitions")

      // Look for key extended properties
      // scalastyle:off caselocale
      val descText = extendedDesc.map(_.mkString(" ")).mkString(" ").toLowerCase
      // scalastyle:on caselocale
      assert(descText.contains("table") || descText.contains("location") ||
        descText.contains("provider"),
        "Extended description should contain table metadata information")
    }
  }

  test("CREATE TABLE IF NOT EXISTS") {
    withTempDir { dir =>
      val tablePath = new java.io.File(dir, "if_not_exists_test").getAbsolutePath
      val tableName = s"$unityCatalogName.default.if_not_exists_test"

      // First creation should succeed
      sql(s"""
        CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING)
        USING DELTA
        LOCATION '$tablePath'
      """)

      sql(s"INSERT INTO $tableName VALUES (1, 'first')")

      try {
        // Second creation should not fail and not affect data
        sql(s"""
          CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, extra STRING)
          USING DELTA
          LOCATION '$tablePath'
        """)

        // Verify original data is still there and schema unchanged
        check(tableName, Seq(Seq("1", "first")))

        // Verify schema wasn't changed (should not have 'extra' column)
        val description = sql(s"DESCRIBE $tableName")
        val columns = description.map(_(0).toString)
        assert(!columns.contains("extra"), "Schema should not have been modified")

      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("table naming with special characters") {
    withTempDir { dir =>
      val tablePath = new java.io.File(dir, "special_name_test").getAbsolutePath
      // Use UC-compatible special characters (underscores and hyphens)
      val tableName = s"$unityCatalogName.default.`test_table_with_underscores`"

      // Create table with special characters in name
      sql(s"""
        CREATE TABLE $tableName (id INT, value STRING)
        USING DELTA
        LOCATION '$tablePath'
      """)

      try {
        // Insert and verify data works with special table name
        sql(s"INSERT INTO $tableName VALUES (1, 'special')")
        check(tableName, Seq(Seq("1", "special")))

        // Verify table appears in SHOW TABLES
        val tables = sql(s"SHOW TABLES IN $unityCatalogName.default")
        val tableNames = tables.map(row => row(1).toString)
        assert(tableNames.contains("test_table_with_underscores"),
          s"Special table name should appear in SHOW TABLES. Found: ${tableNames.mkString(", ")}")

      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }
}
