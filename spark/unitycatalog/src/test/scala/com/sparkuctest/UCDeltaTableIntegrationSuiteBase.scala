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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Abstract base class for Unity Catalog + Delta Table integration tests.
 *
 * This class provides a pluggable SQL execution framework via the SQLExecutor trait,
 * allowing tests to be written once and executed via different execution engines
 * (e.g., Spark SQL, JDBC, REST API, etc.).
 *
 * Subclasses must provide an executor by overriding the sqlExecutor method.
 */
abstract class UCDeltaTableIntegrationSuiteBase
    extends QueryTest
    with SharedSparkSession
    with UnityCatalogSupport
    with DeltaSQLCommandTest {

  /**
   * The SQL executor used to run queries and verify results.
   * Must be implemented by subclasses to provide the execution engine.
   */
  protected def sqlExecutor: UCDeltaTableIntegrationSuiteBase.SQLExecutor

  /**
   * Convenience method for sqlExecutor.runSQL - executes SQL and returns results.
   */
  protected def sql(sqlQuery: String): Seq[Seq[String]] = sqlExecutor.runSQL(sqlQuery)

  /**
   * Convenience method for sqlExecutor.checkTable - verifies table contents.
   */
  protected def check(tableName: String, expected: Seq[Seq[String]]): Unit = {
    sqlExecutor.checkTable(tableName, expected)
  }

  /**
   * Helper method to create a new Delta table, run test code, and clean up.
   *
   * @param tableName The simple table name (without catalog/schema prefix)
   * @param schema The table schema (e.g., "id INT, name STRING")
   * @param testCode The test function that receives the full table name
   */
  protected def withNewTable(tableName: String, schema: String)
                            (testCode: String => Unit): Unit = {
    withTempDir { dir =>
      val tablePath = new java.io.File(dir, tableName).getAbsolutePath
      val fullTableName = s"$unityCatalogName.default.$tableName"

      // Create the table
      sql(s"""
        CREATE TABLE $fullTableName (
          $schema
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      try {
        // Run the test code with the full table name
        testCode(fullTableName)
      } finally {
        // Clean up the table
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }
}

/**
 * Companion object containing the SQLExecutor trait and default implementation.
 */
object UCDeltaTableIntegrationSuiteBase {

  /**
   * Trait defining the interface for executing SQL and verifying results.
   *
   * This abstraction allows tests to be independent of the execution engine,
   * making it easy to test the same logic via different interfaces (Spark SQL, JDBC, etc.).
   */
  trait SQLExecutor {
    /**
     * Execute a SQL statement and return the results.
     *
     * @param sql The SQL statement to execute
     * @return The query results as a sequence of rows, where each row is a sequence of strings
     */
    def runSQL(sql: String): Seq[Seq[String]]

    /**
     * Read all data from a table and verify it matches the expected results.
     *
     * @param tableName The fully qualified table name
     * @param expected The expected results as a sequence of rows
     */
    def checkTable(tableName: String, expected: Seq[Seq[String]]): Unit

    /**
     * Execute a SQL query and verify the results match the expected output.
     *
     * @param sql The SQL query to execute
     * @param expected The expected results as a sequence of rows
     */
    def checkWithSQL(sql: String, expected: Seq[Seq[String]]): Unit
  }

  /**
   * Default SQL executor implementation using SparkSession.
   *
   * This executor runs all SQL queries through Spark SQL and converts
   * results to string sequences for easy comparison.
   *
   * @param spark The SparkSession to use for executing queries
   */
  class SparkSQLExecutor(spark: SparkSession) extends SQLExecutor {

    override def runSQL(sql: String): Seq[Seq[String]] = {
      val df = spark.sql(sql)
      df.collect().map { row =>
        (0 until row.length).map { i =>
          if (row.isNullAt(i)) "null" else row.get(i).toString
        }
      }.toSeq
    }

    override def checkTable(tableName: String, expected: Seq[Seq[String]]): Unit = {
      val actual = runSQL(s"SELECT * FROM $tableName ORDER BY 1")
      assert(actual == expected,
        s"Table $tableName contents do not match.\nExpected: $expected\nActual: $actual")
    }

    override def checkWithSQL(sql: String, expected: Seq[Seq[String]]): Unit = {
      val actual = runSQL(sql)
      assert(actual == expected,
        s"Query results do not match.\nSQL: $sql\nExpected: $expected\nActual: $actual")
    }
  }
}

