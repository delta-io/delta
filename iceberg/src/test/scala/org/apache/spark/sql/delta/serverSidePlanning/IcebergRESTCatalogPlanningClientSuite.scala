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

  private case class PushdownTestCase(
    description: String,
    filter: Filter,
    projection: Seq[String],
    limit: Option[Int])

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
          "dotted field name inside struct with escaping",
          Seq("parent.`child.name`"),
          Set("parent.`child.name`")),
        ProjectionTestCase(
          "dotted column name with escaping",
          Seq("`address.city`"),
          Set("`address.city`"))
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

  test("limit sent to IRC server over HTTP") {
    withTempTable("limitTest") { table =>
      // Populate test data using the shared helper method
      val tableName = s"rest_catalog.${defaultNamespace}.limitTest"
      populateTestData(tableName)

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Test different limit values
        val testCases = Seq(
          (Some(10), Some(10L), "limit = 10"),
          (Some(100), Some(100L), "limit = 100"),
          (Some(1), Some(1L), "limit = 1"),
          (None, None, "no limit"))

        testCases.foreach { case (limitOption, expectedCaptured, description) =>
          // Clear previous captured state
          server.clearCaptured()

          client.planScan(
            defaultNamespace.toString,
            "limitTest",
            sparkLimitOption = limitOption)

          // Verify server captured the limit
          val capturedLimit = Option(server.getCapturedLimit)
          assert(capturedLimit == expectedCaptured,
            s"[$description] Expected $expectedCaptured, got: $capturedLimit")
        }
      } finally {
        client.close()
      }
    }
  }

  test("filter, projection, and limit sent together to IRC server over HTTP") {
    withTempTable("filterProjectionLimitTest") { table =>
      // Populate test data using the shared helper method
      val tableName = s"rest_catalog.${defaultNamespace}.filterProjectionLimitTest"
      populateTestData(tableName)

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Note: Filter types are already tested in "filter sent to IRC server" test.
        // Here we verify filter, projection, AND limit are sent together correctly.
        val testCases = Seq(
          PushdownTestCase(
            "filter + projection + limit",
            EqualTo("longCol", 2L),
            Seq("intCol", "stringCol"),
            Some(10)),
          PushdownTestCase(
            "nested field in both filter and projection + limit",
            EqualTo("address.intCol", 200),
            Seq("intCol", "address.intCol"),
            Some(5))
        )

        testCases.foreach { testCase =>
          // Clear previous captured state
          server.clearCaptured()

          // Convert Spark filter to expected Iceberg expression
          val expectedExpr = SparkToIcebergExpressionConverter.convert(testCase.filter)
          assert(
            expectedExpr.isDefined,
            s"[${testCase.description}] Filter conversion should succeed for: ${testCase.filter}")

          // Call client with filter, projection, and limit
          client.planScan(
            defaultNamespace.toString,
            "filterProjectionLimitTest",
            sparkFilterOption = Some(testCase.filter),
            sparkProjectionOption = Some(testCase.projection),
            sparkLimitOption = testCase.limit)

          // Verify server captured filter, projection, and limit
          val capturedFilter = server.getCapturedFilter
          val capturedProjection = server.getCapturedProjection
          val capturedLimit = server.getCapturedLimit

          assert(capturedFilter != null,
            s"[${testCase.description}] Server should have captured filter")
          assert(capturedProjection != null,
            s"[${testCase.description}] Server should have captured projection")
          assert(capturedLimit != null,
            s"[${testCase.description}] Server should have captured limit")

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

          // Verify limit is correct
          val expectedLimit = testCase.limit.map(_.toLong)
          assert(Option(capturedLimit) == expectedLimit,
            s"[${testCase.description}] Limit mismatch. Expected: $expectedLimit, " +
            s"got: ${Option(capturedLimit)}")
        }
      } finally {
        client.close()
      }
    }
  }

  test("caseSensitive=false sent to IRC server") {
    withTempTable("caseSensitiveTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.caseSensitiveTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        server.clearCaptured()

        // Call planScan - the client sets caseSensitive=false in the request
        val scanPlan = client.planScan(defaultNamespace.toString, "caseSensitiveTest")

        // Verify the scan succeeds and returns files
        assert(scanPlan.files.nonEmpty,
          "Expected planScan to return files for the test table")

        // Verify server captured caseSensitive=false
        val capturedCaseSensitive = server.getCapturedCaseSensitive()
        assert(capturedCaseSensitive == false,
          s"Expected server to capture caseSensitive=false, got $capturedCaseSensitive")
      } finally {
        client.close()
      }
    }
  }

  // Test case class for parameterized credential tests
  private case class CredentialTestCase(
    description: String,
    credentialConfig: Map[String, String],
    expectedCredentials: ScanPlanStorageCredentials)

  test("ScanPlan with cloud provider credentials") {
    withTempTable("credentialsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.credentialsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Test cases for all three cloud providers
        val testCases = Seq(
          CredentialTestCase(
            "S3",
            Map(
              "s3.access-key-id" -> "test-access-key",
              "s3.secret-access-key" -> "test-secret-key",
              "s3.session-token" -> "test-session-token"),
            S3Credentials(
              accessKeyId = "test-access-key",
              secretAccessKey = "test-secret-key",
              sessionToken = "test-session-token")),
          CredentialTestCase(
            "Azure",
            Map(
              "azure.account-name" -> "teststorageaccount",
              "azure.sas-token" -> "sp=r&st=2024-01-01T00:00:00Z&se=2024-12-31T23:59:59Z&sig=test",
              "azure.container-name" -> "testcontainer"),
            AzureCredentials(
              accountName = "teststorageaccount",
              sasToken = "sp=r&st=2024-01-01T00:00:00Z&se=2024-12-31T23:59:59Z&sig=test",
              containerName = "testcontainer")),
          CredentialTestCase(
            "GCS",
            Map("gcs.oauth2.token" -> "test-oauth2-token"),
            GcsCredentials(oauth2Token = "test-oauth2-token"))
        )

        testCases.foreach { testCase =>
          // Configure server to return test credentials
          server.setTestCredentials(testCase.credentialConfig.asJava)

          // Call planScan
          val scanPlan = client.planScan(defaultNamespace.toString, "credentialsTest")

          // Verify credentials are present and match expected type
          assert(scanPlan.credentials.isDefined,
            s"[${testCase.description}] Credentials should be present in ScanPlan")

          val actualCreds = scanPlan.credentials.get
          assert(actualCreds == testCase.expectedCredentials,
            s"[${testCase.description}] Expected credentials: ${testCase.expectedCredentials}, " +
            s"got: $actualCreds")

          // Clear for next test case
          server.clearCaptured()
        }
      } finally {
        client.close()
      }
    }
  }

  test("ScanPlan with no credentials") {
    withTempTable("noCredentialsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.noCredentialsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Don't configure any credentials (current default behavior)
        val scanPlan = client.planScan(defaultNamespace.toString, "noCredentialsTest")

        // Verify credentials are absent
        assert(scanPlan.credentials.isEmpty,
          "Credentials should be None when server doesn't return any")
      } finally {
        client.close()
      }
    }
  }

  test("incomplete credentials throw errors") {
    withTempTable("incompleteCredsTest") { table =>
      populateTestData(s"rest_catalog.${defaultNamespace}.incompleteCredsTest")

      val client = new IcebergRESTCatalogPlanningClient(serverUri, null)
      try {
        // Test cases for incomplete credentials that should throw errors
        val errorTestCases = Seq(
          ("Incomplete S3 (missing secret and token)",
            Map("s3.access-key-id" -> "test-key"),
            "s3.secret-access-key"),
          ("Incomplete Azure (missing SAS and container)",
            Map("azure.account-name" -> "testaccount"),
            "azure.sas-token")
        )

        errorTestCases.foreach { case (description, incompleteConfig, expectedMissingField) =>
          // Configure server with incomplete credentials
          server.setTestCredentials(incompleteConfig.asJava)

          // Verify that planScan throws IllegalStateException
          val exception = intercept[IllegalStateException] {
            client.planScan(defaultNamespace.toString, "incompleteCredsTest")
          }

          // Verify error message mentions the missing field
          assert(exception.getMessage.contains(expectedMissingField),
            s"[$description] Error message should mention missing field '$expectedMissingField'. " +
            s"Got: ${exception.getMessage}")

          // Clear for next test case
          server.clearCaptured()
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

  test("User-Agent header format") {
    val client = new IcebergRESTCatalogPlanningClient("http://localhost:8080", null)
    try {
      val userAgent = client.getUserAgent()

      // Verify the format follows RFC 7231: product/version [product/version ...]
      val parts = userAgent.split(" ")
      assert(parts.length == 4,
        s"User-Agent should have 4 space-separated components, got ${parts.length}: $userAgent")

      // First part should be Delta/version
      assert(parts(0).matches("Delta/.*"),
        s"First component should match 'Delta/<version>', got: ${parts(0)}")

      // Second part should be Spark/version
      assert(parts(1).matches("Spark/.*"),
        s"Second component should match 'Spark/<version>', got: ${parts(1)}")

      // Third part should be Java/version
      assert(parts(2).matches("Java/.*"),
        s"Third component should match 'Java/<version>', got: ${parts(2)}")

      // Fourth part should be Scala/version
      assert(parts(3).matches("Scala/.*"),
        s"Fourth component should match 'Scala/<version>', got: ${parts(3)}")

      // Verify versions are not "unknown" in test environment where all dependencies are available
      assert(!userAgent.contains("Spark/unknown"),
        s"Spark version should not be 'unknown' in test environment, got: $userAgent")
      assert(!userAgent.contains("Delta/unknown"),
        s"Delta version should not be 'unknown' in test environment, got: $userAgent")
      assert(!userAgent.contains("Java/unknown"),
        s"Java version should not be 'unknown' in test environment, got: $userAgent")
      assert(!userAgent.contains("Scala/unknown"),
        s"Scala version should not be 'unknown' in test environment, got: $userAgent")
    } finally {
      client.close()
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
        java.sql.Date.valueOf("2024-01-01"), // localDateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // localDateTimeCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // instantCol
        Row(i * 100), // address.intCol (nested struct)
        Row(s"meta_$i"), // metadata.stringCol (nested struct)
        Row(s"child_$i"), // parent.`child.name` (nested struct with dotted field name)
        s"city_$i", // address.city (literal top-level dotted column)
        s"abc_$i" // a.b.c (literal top-level dotted column)
      ))

    spark.createDataFrame(data, TestSchemas.sparkSchema)
      .write
      .format("iceberg")
      .mode("append")
      .save(tableName)
  }

}
