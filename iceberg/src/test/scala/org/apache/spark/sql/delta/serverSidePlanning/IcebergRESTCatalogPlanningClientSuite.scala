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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Schema, Table}
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer
import shadedForDelta.org.apache.iceberg.types.Types

class IcebergRESTCatalogPlanningClientSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get),
    Types.NestedField.required(2, "name", Types.StringType.get))
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
      // Write data to the table using Spark
      val tableName = s"rest_catalog.${defaultNamespace}.tableWithData"

      // Write data with 2 partitions to create 2 data files
      spark.sparkContext.parallelize(0 until 250, numSlices = 2)
        .map(id => (id.toLong, s"test_$id"))
        .toDF("id", "name")
        .write
        .format("iceberg")
        .mode("append")
        .save(tableName)

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

  // Tests credential extraction from a mock JSON response that includes storage credentials.
  // This simulates what a UC Iceberg REST server would return when vending credentials.
  test("credentials extracted from JSON response with storage-credentials") {
    // Create a mock JSON response similar to what UC would return
    // Using obviously fake credentials for testing
    val mockJsonWithCredentials =
      """{
        |  "plan-status": "completed",
        |  "file-scan-tasks": [],
        |  "storage-credentials": [
        |    {
        |      "type": "aws",
        |      "config": {
        |        "s3.access-key-id": "test-access-key-123",
        |        "s3.secret-access-key": "test-secret-key-456",
        |        "s3.session-token": "test-session-token-789",
        |        "s3.session-token-expires-at-ms": "1234567890000",
        |        "client.region": "us-west-2"
        |      }
        |    }
        |  ]
        |}""".stripMargin

    // Use reflection to call the private extractCredentials method
    val client = new IcebergRESTCatalogPlanningClient("http://dummy", "token")
    val extractMethod = client.getClass.getDeclaredMethod("extractCredentials", classOf[String])
    extractMethod.setAccessible(true)

    val credentials = extractMethod.invoke(client, mockJsonWithCredentials)
      .asInstanceOf[Option[StorageCredentials]]

    // Verify credentials were extracted correctly
    assert(credentials.isDefined, "Credentials should be present in mock response")
    val creds = credentials.get
    assert(creds.accessKeyId == "test-access-key-123")
    assert(creds.secretAccessKey == "test-secret-key-456")
    assert(creds.sessionToken == "test-session-token-789")

    client.close()
  }

  // Tests that credential extraction handles missing storage-credentials gracefully
  test("credentials extraction handles missing storage-credentials") {
    val mockJsonWithoutCredentials =
      """{
        |  "plan-status": "completed",
        |  "file-scan-tasks": []
        |}""".stripMargin

    val client = new IcebergRESTCatalogPlanningClient("http://dummy", "token")
    val extractMethod = client.getClass.getDeclaredMethod("extractCredentials", classOf[String])
    extractMethod.setAccessible(true)

    val credentials = extractMethod.invoke(client, mockJsonWithoutCredentials)
      .asInstanceOf[Option[StorageCredentials]]

    assert(credentials.isEmpty, "Credentials should be None when not present in response")

    client.close()
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

  // Tests end-to-end credential flow: IRC server returns credentials, client extracts them,
  // ServerSidePlannedTable injects into Hadoop config, and S3CredentialTestFileSystem validates.
  test("E2E credential injection from IRC server to S3CredentialTestFileSystem") {
    val originalFsImpl = spark.conf.getOption("fs.s3a.impl")

    try {
      // 1. Configure IRC server to return test credentials in /plan responses
      server.setTestCredentials(
        "test-access-key-irc",
        "test-secret-key-irc",
        "test-session-token-irc"
      )

      // 2. Create table with actual data files
      withTempTable("credTestTable") { table =>
        val tableName = s"rest_catalog.${defaultNamespace}.credTestTable"

        // Write data to create actual parquet files
        spark.sparkContext.parallelize(0 until 100, numSlices = 1)
          .map(id => (id.toLong, s"data_$id"))
          .toDF("id", "name")
          .write
          .format("iceberg")
          .mode("append")
          .save(tableName)

        // 3. Configure Spark to use S3CredentialTestFileSystem
        spark.conf.set("fs.s3a.impl",
          "org.apache.spark.sql.delta.serverSidePlanning.S3CredentialTestFileSystem")

        // 4. Set expected credentials that filesystem will validate
        S3CredentialTestFileSystem.setExpectedCredentials(
          "test-access-key-irc",
          "test-secret-key-irc",
          "test-session-token-irc"
        )

        // 5. Create IRC client pointing to local server
        val client = new IcebergRESTCatalogPlanningClient(serverUri, null)

        try {
          // 6. Create ServerSidePlannedTable with IRC client
          val plannedTable = ServerSidePlannedTable.forTesting(
            spark = spark,
            database = defaultNamespace.toString,
            tableName = "credTestTable",
            tableSchema = new org.apache.spark.sql.types.StructType()
              .add("id", org.apache.spark.sql.types.LongType, nullable = false)
              .add("name", org.apache.spark.sql.types.StringType, nullable = false),
            client = client
          )

          // 7. Execute scan via ServerSidePlannedTable
          // This will:
          // - Call IRC server /plan endpoint
          // - Extract credentials from JSON response
          // - Inject credentials into Hadoop config
          // - Read files via S3CredentialTestFileSystem
          // - Filesystem validates exact credential values
          val scanBuilder = plannedTable.newScanBuilder(
            new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
          val scan = scanBuilder.build()
          val batch = scan.toBatch
          val partitions = batch.planInputPartitions()

          // Verify we got partitions (means scan planning worked)
          assert(partitions.nonEmpty, "Should have at least one partition")

          // 8. Create readers and read data - this triggers filesystem credential validation
          val readerFactory = batch.createReaderFactory()
          val reader = readerFactory.createReader(partitions(0))

          var rowCount = 0
          while (reader.next()) {
            reader.get() // Access the row data
            rowCount += 1
          }
          reader.close()

          // Verify we read data successfully (means credentials were valid)
          assert(rowCount > 0, "Should have read at least one row")

        } finally {
          client.close()
          S3CredentialTestFileSystem.clearExpectedCredentials()
        }
      }
    } finally {
      // Cleanup
      originalFsImpl match {
        case Some(value) => spark.conf.set("fs.s3a.impl", value)
        case None => spark.conf.unset("fs.s3a.impl")
      }
    }
  }

  private def withTempTable[T](tableName: String)(func: Table => T): T = {
    val tableId = TableIdentifier.of(defaultNamespace, tableName)
    val table = catalog.createTable(tableId, defaultSchema, defaultSpec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
    }
  }


}
