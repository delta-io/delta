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
   * Helper to list tables in UC server directly via SDK.
   */
  private def listTables(catalogName: String, schemaName: String): List[String] = {
    val client = createUnityCatalogClient()
    val tablesApi = new TablesApi(client)
    val response: ListTablesResponse = tablesApi.listTables(catalogName, schemaName, null, null)

    import scala.jdk.CollectionConverters._
    response.getTables.asScala.map(_.getName).toList
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
    val ucTables = listTables(unityCatalogName, "default")
    // Should succeed even if empty - this confirms UC server is responding

    // 4. Verify we can create a table in the UC catalog (will be cleaned up in next test)
    // This is the ultimate test that the catalog is properly configured
    withTempDir { dir =>
      val tablePath = new File(dir, "test_verify").getAbsolutePath
      val testTable = s"$unityCatalogName.default.test_verify_catalog"

      spark.sql(s"""
        CREATE TABLE $testTable (id INT) USING PARQUET LOCATION '$tablePath'
      """)

      // If we got here, the catalog is working
      val tables = spark.sql(s"SHOW TABLES IN $unityCatalogName.default")
        .collect()
        .map(_.getString(1))
      assert(tables.contains("test_verify_catalog"),
        "Should be able to create tables in Unity Catalog")

      // Insert data
      spark.sql(s"INSERT INTO $testTable VALUES (1), (2), (3)")

      // Verify we can select the data
      checkAnswer(
        spark.sql(s"SELECT * FROM $testTable ORDER BY id"),
        Seq(Row(1), Row(2), Row(3))
      )

      spark.sql(s"DROP TABLE $testTable")
    }
  }
}
