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

import java.io.File

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.TablesApi
import io.unitycatalog.client.model.ListTablesResponse

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite to verify that the UnityCatalogSupport trait correctly integrates
 * Delta Lake with Unity Catalog server.
 *
 * These tests validate that:
 * 1. The UC server is started and accessible
 * 2. Tables created via Spark are registered in UC
 * 3. Delta operations work correctly through UC
 *
 * The UC server is started automatically via UnityCatalogSupport trait.
 * We use the UC SDK to directly query the UC server to confirm operations.
 */
class UnityCatalogSupportSuite extends QueryTest
    with SharedSparkSession
    with UnityCatalogSupport
    with DeltaSQLCommandTest {

  /**
   * Configure Spark with Unity Catalog settings.
   * This integrates with DeltaSQLCommandTest to ensure all Delta configs are present.
   */
  override protected def sparkConf: SparkConf = {
    configureSparkWithUnityCatalog(super.sparkConf)
  }

  /**
   * Helper to create a UC API client to directly query the UC server.
   */
  private def createUCClient(): ApiClient = {
    val client = new ApiClient()
    // Extract port from unityCatalogUri
    val port = unityCatalogUri.split(":")(2).toInt
    client.setScheme("http")
    client.setHost("localhost")
    client.setPort(port)
    client
  }

  /**
   * Helper to list tables in UC server directly via SDK.
   */
  private def listTablesInUC(catalogName: String, schemaName: String): List[String] = {
    val client = createUCClient()
    val tablesApi = new TablesApi(client)
    val response: ListTablesResponse = tablesApi.listTables(catalogName, schemaName, null, null)

    import scala.jdk.CollectionConverters._
    if (response.getTables != null) {
      response.getTables.asScala.map(_.getName).toList
    } else {
      List.empty
    }
  }

  test("UnityCatalogSupport trait starts UC server and configures Spark correctly") {
    // 1. Verify UC server is accessible via URI
    assert(unityCatalogUri.startsWith("http://localhost:"),
      s"Unity Catalog URI should be localhost, got: $unityCatalogUri")

    // 2. Verify we can access schemas in the UC catalog via Spark
    // Note: SHOW CATALOGS may not list v2 catalogs, but we can access them directly
    val schemas = spark.sql(s"SHOW SCHEMAS IN $unityCatalogName").collect().map(_.getString(0))
    assert(schemas.contains("default"),
      s"Unity Catalog should have 'default' schema. Found: ${schemas.mkString(", ")}")

    // 3. Verify we can query UC server directly via SDK
    val ucTables = listTablesInUC(unityCatalogName, "default")
    // Should succeed even if empty - this confirms UC server is responding
    assert(ucTables != null, "Should be able to query UC server via SDK")

    // 4. Verify we can create a table in the UC catalog (will be cleaned up in next test)
    // This is the ultimate test that the catalog is properly configured
    withTempDir { dir =>
      val tablePath = new File(dir, "test_verify").getAbsolutePath
      val testTable = s"$unityCatalogName.default.test_verify_catalog"

      spark.sql(s"""
        CREATE TABLE $testTable (id INT) USING DELTA LOCATION '$tablePath'
      """)

      // If we got here, the catalog is working
      val tables = spark.sql(s"SHOW TABLES IN $unityCatalogName.default")
        .collect()
        .map(_.getString(1))
      assert(tables.contains("test_verify_catalog"),
        "Should be able to create tables in Unity Catalog")

      spark.sql(s"DROP TABLE $testTable")
    }
  }

  test("CREATE TABLE via Spark registers table in UC server") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_table").getAbsolutePath
      val tableName = "test_table_create"
      val fullTableName = s"$unityCatalogName.default.$tableName"

      // Create an external Delta table via Spark
      spark.sql(s"""
        CREATE TABLE $fullTableName (
          id INT,
          name STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      try {
        // Verify table is visible via Spark
        val sparkTables = spark.sql(s"SHOW TABLES IN $unityCatalogName.default")
          .collect()
          .map(_.getString(1))
        assert(sparkTables.contains(tableName),
          s"Table should be visible via Spark: ${sparkTables.mkString(", ")}")

        // Verify table is registered in UC server by querying directly via SDK
        val ucTables = listTablesInUC(unityCatalogName, "default")
        assert(ucTables.contains(tableName),
          s"Table '$tableName' should be registered in UC server. " +
          s"Found: ${ucTables.mkString(", ")}")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }

  test("INSERT and SELECT operations work through UC server") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_insert").getAbsolutePath
      val tableName = "test_table_insert"
      val fullTableName = s"$unityCatalogName.default.$tableName"

      // Create table
      spark.sql(s"""
        CREATE TABLE $fullTableName (
          id INT,
          value STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      try {
        // Verify table exists in UC server via SDK before insert
        val tablesBeforeInsert = listTablesInUC(unityCatalogName, "default")
        assert(tablesBeforeInsert.contains(tableName),
          s"Table should exist in UC before INSERT")

        // Insert data via Spark
        spark.sql(s"""
          INSERT INTO $fullTableName VALUES
          (1, 'row1'),
          (2, 'row2'),
          (3, 'row3')
        """)

        // Verify data via Spark
        val result = spark.sql(s"SELECT * FROM $fullTableName ORDER BY id")
        checkAnswer(
          result,
          Row(1, "row1") :: Row(2, "row2") :: Row(3, "row3") :: Nil
        )

        // Verify table still exists in UC server via SDK after insert
        val tablesAfterInsert = listTablesInUC(unityCatalogName, "default")
        assert(tablesAfterInsert.contains(tableName),
          s"Table should still exist in UC after INSERT")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }

  test("UPDATE and DELETE operations work through UC server") {
    withTempDir { dir =>
      val tablePath = new File(dir, "test_update_delete").getAbsolutePath
      val tableName = "test_table_update_delete"
      val fullTableName = s"$unityCatalogName.default.$tableName"

      // Create and populate table
      spark.sql(s"""
        CREATE TABLE $fullTableName (
          id INT,
          status STRING
        ) USING DELTA
        LOCATION '$tablePath'
      """)

      spark.sql(s"""
        INSERT INTO $fullTableName VALUES
        (1, 'pending'),
        (2, 'pending'),
        (3, 'completed'),
        (4, 'pending')
      """)

      try {
        // Verify table exists in UC server via SDK before UPDATE
        val tablesBeforeUpdate = listTablesInUC(unityCatalogName, "default")
        assert(tablesBeforeUpdate.contains(tableName),
          s"Table should exist in UC before UPDATE")

        // Perform UPDATE
        spark.sql(s"""
          UPDATE $fullTableName
          SET status = 'completed'
          WHERE id <= 2
        """)

        // Verify UPDATE results
        checkAnswer(
          spark.sql(s"SELECT * FROM $fullTableName WHERE status = 'completed' ORDER BY id"),
          Row(1, "completed") :: Row(2, "completed") :: Row(3, "completed") :: Nil
        )

        // Perform DELETE
        spark.sql(s"""
          DELETE FROM $fullTableName
          WHERE status = 'pending'
        """)

        // Verify DELETE results
        checkAnswer(
          spark.sql(s"SELECT * FROM $fullTableName ORDER BY id"),
          Row(1, "completed") :: Row(2, "completed") :: Row(3, "completed") :: Nil
        )

        // Verify table still exists in UC server via SDK after operations
        val tablesAfterOperations = listTablesInUC(unityCatalogName, "default")
        assert(tablesAfterOperations.contains(tableName),
          s"Table should still exist in UC after UPDATE and DELETE")
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }
}
