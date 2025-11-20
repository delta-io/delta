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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Tests for server-side planning with a mock client.
 */
class ServerSidePlannedTableSuite extends QueryTest with DeltaSQLCommandTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test database and shared table once for all tests
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("""
      CREATE TABLE test_db.shared_test (
        id INT,
        name STRING,
        value INT
      ) USING parquet
    """)
    sql("""
      INSERT INTO test_db.shared_test (id, name, value) VALUES
      (1, 'alpha', 10),
      (2, 'beta', 20),
      (3, 'gamma', 30)
    """)
  }

  /**
   * Helper method to run tests with server-side planning enabled.
   * Automatically sets up the test factory and config, then cleans up afterwards.
   * This prevents test pollution from leaked configuration.
   */
  private def withServerSidePlanningEnabled(f: => Unit): Unit = {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
    try {
      f
    } finally {
      // Reset factory
      ServerSidePlanningClientFactory.clearFactory()
      // Restore original config
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("full query through DeltaCatalog with server-side planning") {
    // This test verifies server-side planning works end-to-end by checking:
    // (1) DeltaCatalog returns ServerSidePlannedTable (not normal table)
    // (2) Query execution returns correct results
    // If both are true, the server-side planning client worked correctly - that's the only way
    // ServerSidePlannedTable can read data.

    withServerSidePlanningEnabled {
      // (1) Verify that DeltaCatalog actually returns ServerSidePlannedTable
      val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
        .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      val loadedTable = catalog.loadTable(
        org.apache.spark.sql.connector.catalog.Identifier.of(
          Array("test_db"), "shared_test"))
      assert(loadedTable.isInstanceOf[ServerSidePlannedTable],
        s"Expected ServerSidePlannedTable but got ${loadedTable.getClass.getName}")

      // (2) Execute query - should go through full server-side planning stack
      checkAnswer(
        sql("SELECT id, name, value FROM test_db.shared_test ORDER BY id"),
        Seq(
          Row(1, "alpha", 10),
          Row(2, "beta", 20),
          Row(3, "gamma", 30)
        )
      )
    }
  }

  test("verify normal path unchanged when feature disabled") {
    // Explicitly disable server-side planning
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "false")

    // Verify that DeltaCatalog returns normal table, not ServerSidePlannedTable
    val catalog = spark.sessionState.catalogManager.catalog("spark_catalog")
      .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
    val loadedTable = catalog.loadTable(
      org.apache.spark.sql.connector.catalog.Identifier.of(
        Array("test_db"), "shared_test"))
    assert(!loadedTable.isInstanceOf[ServerSidePlannedTable],
      s"Expected normal table but got ServerSidePlannedTable when config is disabled")
  }

  test("shouldUseServerSidePlanning() decision logic") {
    // Case 1: Force flag enabled -> should always use server-side planning
    assert(ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = true,
      forceServerSidePlanning = true),
      "Should use server-side planning when force flag is true")

    // Case 2: Unity Catalog without credentials -> should use server-side planning
    assert(ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = true,
      hasCredentials = false,
      forceServerSidePlanning = false),
      "Should use server-side planning for UC table without credentials")

    // Case 3: Unity Catalog with credentials -> should NOT use server-side planning
    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = true,
      hasCredentials = true,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for UC table with credentials")

    // Case 4: Non-UC catalog -> should NOT use server-side planning
    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = true,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for non-UC catalog")

    assert(!ServerSidePlannedTable.shouldUseServerSidePlanning(
      isUnityCatalog = false,
      hasCredentials = false,
      forceServerSidePlanning = false),
      "Should NOT use server-side planning for non-UC catalog (even without credentials)")
  }

  test("ServerSidePlannedTable is read-only") {
    withTable("readonly_test") {
      sql("""
        CREATE TABLE readonly_test (
          id INT,
          data STRING
        ) USING parquet
      """)

      // First insert WITHOUT server-side planning should succeed
      sql("INSERT INTO readonly_test VALUES (1, 'initial')")
      checkAnswer(
        sql("SELECT * FROM readonly_test"),
        Seq(Row(1, "initial"))
      )

      // Try to insert WITH server-side planning enabled - should fail
      withServerSidePlanningEnabled {
        val exception = intercept[AnalysisException] {
          sql("INSERT INTO readonly_test VALUES (2, 'should_fail')")
        }
        assert(exception.getMessage.contains("does not support append"))
      }

      // Verify data unchanged - second insert didn't happen
      checkAnswer(
        sql("SELECT * FROM readonly_test"),
        Seq(Row(1, "initial"))
      )
    }
  }

  test("UnityCatalogMetadata constructs IRC endpoint from UC URI") {
    val ucUri = "https://my-workspace.cloud.databricks.com"
    val metadata = UnityCatalogMetadata(
      catalogName = "test_catalog",
      ucUri = ucUri,
      ucToken = "test-token",
      tableProps = Map.empty
    )

    val expectedEndpoint =
      "https://my-workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg"
    assert(metadata.planningEndpointUri.get == expectedEndpoint)
  }
}
