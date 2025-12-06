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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Test client that captures the filter passed to planScan() for verification.
 * Stores captured filter in thread-local variable accessible via companion object.
 */
class FilterCapturingTestClient(spark: SparkSession) extends ServerSidePlanningClient {
  override def planScan(
      database: String,
      table: String,
      filter: Option[org.apache.spark.sql.sources.Filter] = None): ScanPlan = {
    // Capture the filter for test verification
    FilterCapturingTestClient.capturedFilter.set(filter)

    // Delegate to TestServerSidePlanningClient for actual file discovery
    new TestServerSidePlanningClient(spark).planScan(database, table, filter)
  }
}

object FilterCapturingTestClient {
  private val capturedFilter =
    new ThreadLocal[Option[org.apache.spark.sql.sources.Filter]]()

  def getCapturedFilter: Option[org.apache.spark.sql.sources.Filter] = capturedFilter.get()
  def clearCapturedFilter(): Unit = capturedFilter.remove()
}

/**
 * Factory for creating FilterCapturingTestClient instances.
 */
class FilterCapturingTestClientFactory extends ServerSidePlanningClientFactory {
  override def buildForCatalog(
      spark: SparkSession,
      catalogName: String): ServerSidePlanningClient = {
    new FilterCapturingTestClient(spark)
  }
}

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

  test("filter pushdown - simple EqualTo filter") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query with WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id = 2").collect()

      // Verify filter was captured
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Spark may wrap EqualTo with IsNotNull check: And(IsNotNull("id"), EqualTo("id", 2))
      // We need to handle both cases
      val filter = capturedFilter.get
      val equalToFilter = filter match {
        case and: org.apache.spark.sql.sources.And =>
          // Wrapped case - extract the EqualTo from the And
          and.right match {
            case eq: org.apache.spark.sql.sources.EqualTo => eq
            case _ => and.left.asInstanceOf[org.apache.spark.sql.sources.EqualTo]
          }
        case eq: org.apache.spark.sql.sources.EqualTo =>
          // Unwrapped case
          eq
        case other =>
          fail(s"Expected EqualTo or And(IsNotNull, EqualTo) but got ${other.getClass.getName}")
      }

      assert(equalToFilter.attribute == "id",
        s"Expected attribute 'id' but got '${equalToFilter.attribute}'")
      assert(equalToFilter.value == 2, s"Expected value 2 but got ${equalToFilter.value}")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("filter pushdown - compound And filter") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query with compound WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test WHERE id > 1 AND value < 30").collect()

      // Verify filter was captured
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter.isDefined, "Filter should be pushed down")

      // Spark may wrap filters with IsNotNull checks in nested And structures:
      // And(And(IsNotNull("id"), GreaterThan("id", 1)), And(IsNotNull("value"), LessThan("value", 30)))
      // We just verify that the top-level is an And filter and contains the expected predicates
      val filter = capturedFilter.get
      assert(filter.isInstanceOf[org.apache.spark.sql.sources.And],
        s"Expected And filter but got ${filter.getClass.getName}")

      // Convert filter to string and verify it contains both predicates
      val filterStr = filter.toString
      assert(filterStr.contains("GreaterThan") && filterStr.contains("id"),
        s"Filter should contain GreaterThan on id: $filterStr")
      assert(filterStr.contains("LessThan") && filterStr.contains("value"),
        s"Filter should contain LessThan on value: $filterStr")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }

  test("filter pushdown - no filter when no WHERE clause") {
    val originalConfig = spark.conf.getOption(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
    ServerSidePlanningClientFactory.setFactory(new FilterCapturingTestClientFactory())
    spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")

    try {
      // Clear any previous captured filter
      FilterCapturingTestClient.clearCapturedFilter()

      // Execute query without WHERE clause
      sql("SELECT id, name, value FROM test_db.shared_test").collect()

      // Verify no filter was pushed
      // getCapturedFilter returns Option[Filter] - should be None when no WHERE clause
      val capturedFilter = FilterCapturingTestClient.getCapturedFilter
      assert(capturedFilter != null, "planScan should have been called")
      assert(capturedFilter.isEmpty, s"Expected no filter (None) but got ${capturedFilter}")
    } finally {
      FilterCapturingTestClient.clearCapturedFilter()
      ServerSidePlanningClientFactory.clearFactory()
      originalConfig match {
        case Some(value) => spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, value)
        case None => spark.conf.unset(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key)
      }
    }
  }
}
