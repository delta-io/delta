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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Schema, Table}
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.expressions.Binder
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer
import shadedForDelta.org.apache.iceberg.types.Types

class IcebergRESTCatalogPlanningClientSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = TestSchemas.testSchema
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Configure Spark to use the Iceberg REST catalog
    spark.conf.set(s"spark.sql.catalog.rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.type", "rest")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.uri", serverUri)

    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces].createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException("Catalog does not support namespaces")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.clearCaptured()
        server.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  // Tests that the REST /plan endpoint returns 0 files for an empty table.
  test("basic plan table scan via IcebergRESTCatalogPlanningClient") {
    withTempTable("testTable") { table =>
      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        val scanPlan = client.planScan(defaultNamespace.toString, "testTable")
        assert(scanPlan != null, "Scan plan should not be null")
        assert(scanPlan.files != null, "Scan plan files should not be null")
        assert(scanPlan.files.isEmpty,
          s"Empty table should have 0 files, got ${scanPlan.files.length}")
      } finally {
        client.close()
      }
    }
  }

  // Tests that the REST /plan endpoint returns the correct number of files for a non-empty table.
  // Creates a table, writes actual parquet files with data, then verifies the response includes
  // them.
  test("plan scan on non-empty table with data files") {
    withTempTable("tableWithData") { table =>
      val tableName = s"rest_catalog.${defaultNamespace}.tableWithData"
      populateTestData(tableName)

      // Get the actual data files from the table metadata to verify against scan plan
      val expectedFiles = spark.sql(
        s"SELECT file_path, file_size_in_bytes FROM ${tableName}.files")
        .collect()
        .map(row => (new Path(row.getString(0)).getName, row.getLong(1)))
        .toMap

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        val scanPlan = client.planScan(defaultNamespace.toString, "tableWithData")
        assert(scanPlan != null, "Scan plan should not be null")
        assert(scanPlan.files != null, "Scan plan files should not be null")
        assert(scanPlan.files.length == 2, s"Expected 2 files but got ${scanPlan.files.length}")

        // Get scanned files as map of filename -> size
        val scannedFiles = scanPlan.files.map { file =>
          (new Path(file.filePath).getName, file.fileSizeInBytes)
        }.toMap

        // Verify scan plan files match expected files
        assert(scannedFiles == expectedFiles,
          s"Scan plan files don't match expected files.\n" +
            s"Expected: $expectedFiles\n" +
            s"Got: $scannedFiles")
      } finally {
        client.close()
      }
    }
  }

  // TODO: Add test for partitioned table rejection
  // Once the test server (IcebergRESTCatalogAdapterWithPlanSupport) properly retains and serves
  // partition data through the commit/serialize/deserialize cycle, add a test that verifies:
  // 1. Creates a partitioned table with data files containing partition info
  // 2. Calls client.planScan() and expects UnsupportedOperationException
  // 3. Verifies exception message contains "partition data"
  // This will test the client's partition validation logic at
  // IcebergRESTCatalogPlanningClient:160-164

  test("UnityCatalogMetadata uses prefix from /v1/config endpoint") {
    import org.apache.spark.sql.delta.serverSidePlanning.UnityCatalogMetadata

    // Configure server to return prefix
    server.setCatalogPrefix("catalogs/test-catalog")

    val metadata = UnityCatalogMetadata(
      catalogName = "test_catalog",
      ucUri = serverUri,
      ucToken = "test-token",
      tableProps = Map.empty)

    // Verify endpoint includes prefix from /v1/config response
    val expectedEndpoint = s"$serverUri/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/test-catalog"
    assert(
      metadata.planningEndpointUri == expectedEndpoint,
      s"Expected endpoint to include prefix: ${metadata.planningEndpointUri}")
  }

  test("UnityCatalogMetadata falls back when /v1/config returns no prefix") {
    import org.apache.spark.sql.delta.serverSidePlanning.UnityCatalogMetadata

    // Configure server to return no prefix (fallback case)
    server.setCatalogPrefix(null)

    val metadata = UnityCatalogMetadata(
      catalogName = "test_catalog",
      ucUri = serverUri,
      ucToken = "test-token",
      tableProps = Map.empty)

    // Verify endpoint uses simple path without prefix
    val expectedEndpoint = s"$serverUri/api/2.1/unity-catalog/iceberg-rest"
    assert(
      metadata.planningEndpointUri == expectedEndpoint,
      s"Expected endpoint without prefix: ${metadata.planningEndpointUri}")
  }

  test("filter sent to IRC server over HTTP") {
    withTempTable("filterTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.filterTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        val testCases = Seq(
          (EqualTo("longCol", 2L), "EqualTo numeric (long)"),
          (EqualTo("intCol", 30), "EqualTo numeric (int)"),
          (EqualTo("stringCol", "bob"), "EqualTo string"),
          (EqualTo("boolCol", true), "EqualTo boolean"),
          (Not(EqualTo("longCol", 2L)), "NotEqualTo numeric (long)"),
          (Not(EqualTo("stringCol", "bob")), "NotEqualTo string"),
          (LessThan("longCol", 10L), "LessThan (long)"),
          (LessThan("floatCol", 4.5f), "LessThan (float)"),
          (GreaterThan("longCol", 5L), "GreaterThan (long)"),
          (GreaterThan("doubleCol", 100.0), "GreaterThan (double)"),
          (LessThanOrEqual("intCol", 30), "LessThanOrEqual (int)"),
          (GreaterThanOrEqual("doubleCol", 100.0), "GreaterThanOrEqual (double)"),
          (In("longCol", Array(1L, 2L, 3L)), "In numeric (long)"),
          (In("stringCol", Array("alice", "bob", "charlie")), "In string"),
          (IsNull("stringCol"), "IsNull"),
          (IsNotNull("stringCol"), "IsNotNull"),
          (StringStartsWith("stringCol", "ali"), "StringStartsWith"),
          (AlwaysTrue(), "AlwaysTrue"),
          (AlwaysFalse(), "AlwaysFalse"),
          (And(EqualTo("longCol", 2L), EqualTo("stringCol", "bob")), "And"),
          (Or(EqualTo("longCol", 1L), EqualTo("longCol", 3L)), "Or"),
          (EqualTo("address.intCol", 200), "EqualTo on nested numeric field"),
          (EqualTo("metadata.stringCol", "meta_bob"), "EqualTo on nested string field"),
          (GreaterThan("address.intCol", 500), "GreaterThan on nested numeric field"))

        testCases.foreach { case (filter, description) =>
          // Clear previous captured filter
          server.clearCaptured()

          // Convert Spark filter to expected Iceberg expression
          val expectedExpr = SparkToIcebergExpressionConverter.convert(filter)
          assert(
            expectedExpr.isDefined,
            s"[$description] Filter conversion should succeed for: $filter")

          // Call client with filter
          client.planScan(
            defaultNamespace.toString,
            "filterTest",
            sparkFilterOption = Some(filter))

          // Verify server captured the filter
          val capturedFilter = server.getCapturedFilter
          assert(capturedFilter != null, s"[$description] Server should have captured filter")

          // isEquivalentTo() only works on bound expressions, so bind both to schema for comparison
          // Binding resolves field references from names to schema-specific field IDs and types
          val boundExpected = Binder.bind(defaultSchema.asStruct(), expectedExpr.get, true)
          val boundCaptured = Binder.bind(defaultSchema.asStruct(), capturedFilter, true)

          assert(
            boundCaptured.isEquivalentTo(boundExpected),
            s"[$description] Expected expression: $boundExpected, got: $boundCaptured")
        }
      } finally {
        client.close()
      }
    }
  }

  // Test case classes for structured test data
  private case class ProjectionTestCase(
    description: String,
    projection: Seq[String],
    expected: Set[String])

  private case class FilterProjectionTestCase(
    description: String,
    filter: Filter,
    projection: Seq[String])

  test("projection sent to IRC server over HTTP") {
    withTempTable("projectionTest") { table =>
      // Populate test data using the shared helper method
      val tableName = s"rest_catalog.${defaultNamespace}.projectionTest"
      populateTestData(tableName)

      // Test cases covering different projection scenarios
      // Note: At this HTTP layer, we're only testing that column name strings are correctly
      // sent and received. Type serialization and data reading are tested end-to-end.
      val testCases = Seq(
        // Basic projections
        ProjectionTestCase(
          "single column",
          Seq("intCol"),
          Set("intCol")),
        ProjectionTestCase(
          "multiple columns",
          Seq("intCol", "stringCol"),
          Set("intCol", "stringCol")),

        // Nested field projections - test dot-notation string handling
        ProjectionTestCase(
          "individual nested field",
          Seq("address.intCol"),
          Set("address.intCol")),
        ProjectionTestCase(
          "multiple nested fields",
          Seq("address.intCol", "metadata.stringCol"),
          Set("address.intCol", "metadata.stringCol"))
      )

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        testCases.foreach { testCase =>
          // Clear previous captured projection
          server.clearCaptured()

          client.planScan(
            defaultNamespace.toString,
            "projectionTest",
            sparkProjectionOption = Some(testCase.projection))

          // Verify server captured the projection
          val capturedProjection = server.getCapturedProjection
          assert(capturedProjection != null,
            s"[${testCase.description}] Server should have captured projection")

          // Verify field names match expected
          val fieldNames = capturedProjection.asScala.toSet
          assert(fieldNames == testCase.expected,
            s"[${testCase.description}] Expected ${testCase.expected}, got: $fieldNames")
        }
      } finally {
        client.close()
      }
    }
  }

  test("filter and projection sent together to IRC server over HTTP") {
    withTempTable("filterProjectionTest") { table =>
      // Populate test data using the shared helper method
      val tableName = s"rest_catalog.${defaultNamespace}.filterProjectionTest"
      populateTestData(tableName)

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Note: Filter types are already tested in "filter sent to IRC server" test.
        // Here we only need to verify filter AND projection are sent together correctly.
        val testCases = Seq(
          FilterProjectionTestCase(
            "simple filter + projection",
            EqualTo("longCol", 2L),
            Seq("intCol", "stringCol")),
          FilterProjectionTestCase(
            "nested field in both filter and projection",
            EqualTo("address.intCol", 200),
            Seq("intCol", "address.intCol"))
        )

        testCases.foreach { testCase =>
          // Clear previous captured state
          server.clearCaptured()

          // Convert Spark filter to expected Iceberg expression
          val expectedExpr = SparkToIcebergExpressionConverter.convert(testCase.filter)
          assert(
            expectedExpr.isDefined,
            s"[${testCase.description}] Filter conversion should succeed for: ${testCase.filter}")

          // Call client with both filter and projection
          client.planScan(
            defaultNamespace.toString,
            "filterProjectionTest",
            sparkFilterOption = Some(testCase.filter),
            sparkProjectionOption = Some(testCase.projection))

          // Verify server captured both filter and projection
          val capturedFilter = server.getCapturedFilter
          val capturedProjection = server.getCapturedProjection

          assert(capturedFilter != null,
            s"[${testCase.description}] Server should have captured filter")
          assert(capturedProjection != null,
            s"[${testCase.description}] Server should have captured projection")

          // Verify filter is correct
          val boundExpected = Binder.bind(defaultSchema.asStruct(), expectedExpr.get, true)
          val boundCaptured = Binder.bind(defaultSchema.asStruct(), capturedFilter, true)
          assert(
            boundCaptured.isEquivalentTo(boundExpected),
            s"[${testCase.description}] Filter mismatch. Expected: $boundExpected, " +
            s"got: $boundCaptured")

          // Verify projection is correct
          val projectionFields = capturedProjection.asScala.toSet
          val expectedFields = testCase.projection.toSet
          assert(projectionFields == expectedFields,
            s"[${testCase.description}] Projection mismatch. Expected: $expectedFields, " +
            s"got: $projectionFields")
        }
      } finally {
        client.close()
      }
    }
  }

  private def startServer(): IcebergRESTServer = {
    val config = Map(IcebergRESTServer.REST_PORT -> "0").asJava
    val newServer = new IcebergRESTServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start IcebergRESTServer")
    }
    newServer
  }

  private def isServerReachable(server: IcebergRESTServer): Boolean = {
    val httpHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

    val httpClient = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .build()

    try {
      val httpGet = new HttpGet(s"http://localhost:${server.getPort}/v1/config")
      val httpResponse = httpClient.execute(httpGet)
      try {
        val statusCode = httpResponse.getStatusLine.getStatusCode
        statusCode == 200
      } finally {
        httpResponse.close()
      }
    } finally {
      httpClient.close()
    }
  }

  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    val tableId = TableIdentifier.of(defaultNamespace, tableName)
    val table = catalog.createTable(tableId, defaultSchema, defaultSpec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
      server.clearCaptured()
    }
  }

  /**
   * Populates a table with sample test data covering all schema types.
   * Uses parallelize with 2 partitions to create 2 data files.
   */
  private def populateTestData(tableName: String): Unit = {
    import org.apache.spark.sql.Row

    val data = spark.sparkContext.parallelize(0 until 250, numSlices = 2)
      .map(i => Row(
        i, // intCol
        i.toLong, // longCol
        i * 10.0, // doubleCol
        i.toFloat, // floatCol
        s"test_$i", // stringCol
        i % 2 == 0, // boolCol
        BigDecimal(i).bigDecimal, // decimalCol
        java.sql.Date.valueOf("2024-01-01"), // dateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // timestampCol
        Row(i * 100), // address.intCol
        Row(s"meta_$i") // metadata.stringCol
      ))


    spark.createDataFrame(data, TestSchemas.sparkSchema)
      .write
      .format("iceberg")
      .mode("append")
      .save(tableName)
  }

}
