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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, LessThan}

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
    withServerSidePlanningFactory(new TestServerSidePlanningClientFactory())(f)
  }

  /**
   * Helper method to run tests with pushdown capturing enabled.
   * TestServerSidePlanningClient captures pushdowns (filter, projection) passed to planScan().
   */
  private def withPushdownCapturingEnabled(f: => Unit): Unit = {
    withServerSidePlanningFactory(new TestServerSidePlanningClientFactory()) {
      try {
        f
      } finally {
        TestServerSidePlanningClient.clearCaptured()
      }
    }
  }

  /**
   * Common helper for setting up server-side planning with a specific factory.
   */
  private def withServerSidePlanningFactory(factory: ServerSidePlanningClientFactory)
      (f: => Unit): Unit = {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(factory)
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

  /**
   * Extract all leaf filters from a filter tree.
   * Spark may wrap filters with And and IsNotNull checks, so this flattens the tree.
   */
  private def collectLeafFilters(filter: Filter): Seq[Filter] = filter match {
    case And(left, right) => collectLeafFilters(left) ++ collectLeafFilters(right)
    case other => Seq(other)
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

  test("ServerSidePlanningMetadata.fromTable returns empty defaults for non-UC catalogs") {
    import org.apache.spark.sql.connector.catalog.Identifier

    // Create a simple identifier for testing
    val ident = Identifier.of(Array("my_catalog", "my_schema"), "my_table")

    // Call fromTable with a null table (we only use the identifier for catalog name extraction)
    val metadata = ServerSidePlanningMetadata.fromTable(
      table = null,
      spark = spark,
      ident = ident,
      isUnityCatalog = false
    )

    // Verify the metadata has expected defaults
    assert(metadata.catalogName == "my_catalog")
    assert(metadata.planningEndpointUri == "")
    assert(metadata.authToken.isEmpty)
    assert(metadata.tableProperties.isEmpty)
  }

  test("UnityCatalogMetadata constructs IRC endpoint from UC URI") {
    val ucUri = "https://unity-catalog-server.example.com"
    val metadata = UnityCatalogMetadata(
      catalogName = "test_catalog",
      ucUri = ucUri,
      ucToken = "test-token",
      tableProps = Map.empty
    )

    // This test validates the fallback case where /v1/config is unreachable.
    // The endpoint construction logic attempts to call /v1/config at the UC URI,
    // but since there's no server at this URL, it falls back to the simple path
    // without prefix. For tests of the prefix case with a real IRC server, see
    // IcebergRESTCatalogPlanningClientSuite.
    val expectedEndpoint =
      "https://unity-catalog-server.example.com/api/2.1/unity-catalog/iceberg-rest"
    assert(metadata.planningEndpointUri == expectedEndpoint)
  }

  test("simple EqualTo filter pushed to planning client") {
    withPushdownCapturingEnabled {
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id = 2").collect()

      val capturedFilter = TestServerSidePlanningClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Extract leaf filters and find the EqualTo filter
      val leafFilters = collectLeafFilters(capturedFilter.get)
      val eqFilter = leafFilters.collectFirst {
        case eq: EqualTo if eq.attribute == "id" => eq
      }
      assert(eqFilter.isDefined, "Expected EqualTo filter on 'id'")
      assert(eqFilter.get.value == 2, s"Expected EqualTo value 2, got ${eqFilter.get.value}")
    }
  }

  test("compound And filter pushed to planning client") {
    withPushdownCapturingEnabled {
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id > 1 AND value < 30").collect()

      val capturedFilter = TestServerSidePlanningClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      val filter = capturedFilter.get
      assert(filter.isInstanceOf[And], s"Expected And filter, got ${filter.getClass.getSimpleName}")

      // Extract all leaf filters from the And tree (Spark may add IsNotNull checks)
      val leafFilters = collectLeafFilters(filter)

      // Verify GreaterThan(id, 1) is present
      val gtFilter = leafFilters.collectFirst {
        case gt: GreaterThan if gt.attribute == "id" => gt
      }
      assert(gtFilter.isDefined, "Expected GreaterThan filter on 'id'")
      assert(gtFilter.get.value == 1, s"Expected GreaterThan value 1, got ${gtFilter.get.value}")

      // Verify LessThan(value, 30) is present
      val ltFilter = leafFilters.collectFirst {
        case lt: LessThan if lt.attribute == "value" => lt
      }
      assert(ltFilter.isDefined, "Expected LessThan filter on 'value'")
      assert(ltFilter.get.value == 30, s"Expected LessThan value 30, got ${ltFilter.get.value}")
    }
  }

  test("no filter pushed when no WHERE clause") {
    withPushdownCapturingEnabled {
      sql("SELECT id, name, value FROM test_db.shared_test").collect()

      val capturedFilter = TestServerSidePlanningClient.getCapturedFilter
      assert(capturedFilter.isEmpty, "No filter should be pushed when there's no WHERE clause")
    }
  }

  test("projection pushed when selecting specific columns") {
    withPushdownCapturingEnabled {
      sql("SELECT id, name FROM test_db.shared_test").collect()

      val capturedProjection = TestServerSidePlanningClient.getCapturedProjection
      assert(capturedProjection.isDefined, "Projection should be pushed down")
      assert(capturedProjection.get.toSet == Set("id", "name"),
        s"Expected {id, name}, got {${capturedProjection.get.mkString(", ")}}")
    }
  }

  test("no projection pushed when selecting all columns") {
    withPushdownCapturingEnabled {
      sql("SELECT * FROM test_db.shared_test").collect()

      val capturedProjection = TestServerSidePlanningClient.getCapturedProjection
      assert(capturedProjection.isEmpty,
        "No projection should be pushed when selecting all columns")
    }
  }

  test("projection and filter pushed together") {
    withPushdownCapturingEnabled {
      sql("SELECT id FROM test_db.shared_test WHERE value > 10").collect()

      // Verify projection was pushed with exactly the expected columns
      // Spark needs 'id' for SELECT and 'value' for WHERE clause
      val capturedProjection = TestServerSidePlanningClient.getCapturedProjection
      assert(capturedProjection.isDefined, "Projection should be pushed down")
      val projectedFields = capturedProjection.get.toSet
      assert(projectedFields == Set("id", "value"),
        s"Expected projection with exactly {id, value}, got {${projectedFields.mkString(", ")}}")

      // Verify filter was also pushed
      val capturedFilter = TestServerSidePlanningClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Verify GreaterThan(value, 10) is in the filter
      val leafFilters = collectLeafFilters(capturedFilter.get)
      val gtFilter = leafFilters.collectFirst {
        case gt: GreaterThan if gt.attribute == "value" => gt
      }
      assert(gtFilter.isDefined, "Expected GreaterThan filter on 'value'")
      assert(gtFilter.get.value == 10, s"Expected GreaterThan value 10, got ${gtFilter.get.value}")
    }
  }
}
