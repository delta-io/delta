/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  Table,
  TableCatalog,
  TableChange,
  V1Table
}
import org.apache.spark.sql.delta.catalog.credentials.UCDeltaRestCatalogApiCredentialConf
import org.apache.spark.sql.delta.catalog.credentials.storage.AwsVendedTokenProvider
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaCatalogClientSuite
    extends QueryTest
    with DeltaSQLCommandTest
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var server: HttpServer = _
  private var serverUri: String = _
  private var configHandler: HttpExchange => Unit = _
  private var handler: HttpExchange => Unit = _
  private var credentialRequestCount: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/api/2.1/unity-catalog/delta/v1/config", exchange => {
      try {
        if (configHandler != null) {
          configHandler(exchange)
        } else {
          sendJson(exchange, 200,
            """{
              |  "endpoints": [
              |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
              |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
              |  ],
              |  "protocol-version": "1.0"
              |}""".stripMargin)
        }
      } finally {
        exchange.close()
      }
    })
    server.createContext("/api/2.1/unity-catalog/delta/v1/catalogs", exchange => {
      try {
        if (handler != null) handler(exchange) else sendJson(exchange, 404, "{}")
      } finally {
        exchange.close()
      }
    })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    configHandler = null
    handler = null
    credentialRequestCount = 0
  }

  test("loadTable skips credentials for local Delta locations") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson("file:/tmp/uc/table"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        credentialRequestCount += 1
        sendJson(exchange, 500, "{}")
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val table = loadWithUCDeltaRestCatalogApi()
    val properties = table.catalogTable.storage.properties
    val idMetadata = table.catalogTable.schema("id").metadata

    assert(credentialRequestCount === 0)
    assert(properties === Map(
      "delta.feature.catalogManaged" -> "supported",
      UC_TABLE_ID_KEY -> "11111111-1111-1111-1111-111111111111"))
    assert(idMetadata.getLong("delta.columnMapping.id") === 1L)
    assert(idMetadata.getString("delta.columnMapping.physicalName") === "col-123")
  }

  test("loadTable fails loudly when cloud credentials are empty") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson("s3://bucket/table"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        credentialRequestCount += 1
        sendJson(exchange, 200, """{"storage-credentials": []}""")
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val error = intercept[IllegalArgumentException] {
      loadWithUCDeltaRestCatalogApi()
    }

    assert(credentialRequestCount === 1)
    assert(error.getMessage.contains("no storage credentials"))
  }

  test("loadTable accepts trailing-slash cloud credential prefixes") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson("s3://bucket/path/to/table"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        credentialRequestCount += 1
        assert(exchange.getRequestURI.getQuery === "operation=READ_WRITE")
        sendJson(exchange, 200, s3CredentialsResponseJson(
          "s3://bucket/path/to/table/",
          "READ_WRITE"))
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val table = loadWithUCDeltaRestCatalogApi()
    val storageProperties = table.catalogTable.storage.properties
    val tableProperties = table.properties.asScala

    assert(credentialRequestCount === 1)
    assert(storageProperties(UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER) ===
      classOf[AwsVendedTokenProvider].getName)
    assert(storageProperties(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY) === "ak")
    assert(tableProperties(s"option.${UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER}") ===
      classOf[AwsVendedTokenProvider].getName)
    assert(tableProperties(s"option.${UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY}") === "ak")
    assert(storageProperties("delta.feature.catalogManaged") === "supported")
    assert(!tableProperties.contains("delta.feature.catalogManaged"))
    assert(!tableProperties.contains(
      s"option.option.${UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY}"))
  }

  test("loadTable falls back to READ credentials when READ_WRITE is denied") {
    var credentialQueries = Seq.empty[String]
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson("s3://bucket/path/to/table"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        credentialQueries :+= exchange.getRequestURI.getQuery
        exchange.getRequestURI.getQuery match {
          case "operation=READ_WRITE" =>
            sendJson(exchange, 403, """{"error_code": "PERMISSION_DENIED"}""")
          case "operation=READ" =>
            sendJson(exchange, 200, s3CredentialsResponseJson("s3://bucket/path/to/table", "READ"))
          case other =>
            fail(s"Unexpected credential query: $other")
        }
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val table = loadWithUCDeltaRestCatalogApi()

    assert(credentialQueries === Seq("operation=READ_WRITE", "operation=READ"))
    assert(table.catalogTable.storage.properties(UCDeltaRestCatalogApiCredentialConf.UC_TABLE_OPERATION_KEY) ===
      "READ")
  }

  test("loadTable uses static credential properties when renewal is disabled") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson("s3://bucket/path/to/table"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        credentialRequestCount += 1
        sendJson(
          exchange,
          200,
          s3CredentialsResponseJson("s3://bucket/path/to/table", "READ_WRITE"))
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val table = withUCDeltaRestCatalogApiRenewalDisabled { catalog =>
      catalog.loadTable(Identifier.of(Array("default"), "tbl")).get.asInstanceOf[V1Table]
    }

    assert(credentialRequestCount === 1)
    assert(table.catalogTable.storage.properties("fs.s3a.access.key") === "ak")
    assert(!table.catalogTable.storage.properties.contains(
      UCDeltaRestCatalogApiCredentialConf.S3A_CREDENTIALS_PROVIDER))
  }

  test("loadTable returns non-Delta tables from UC Delta Rest Catalog API") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 200, loadTableResponseJson(
          location = "file:/tmp/uc/iceberg-table",
          dataSourceFormat = "ICEBERG"))
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        fail("Unexpected credentials request for non-Delta table")
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val loaded = withUCDeltaRestCatalogApi { catalog =>
      catalog.loadTable(Identifier.of(Array("default"), "tbl"))
    }

    val table = loaded.get.asInstanceOf[V1Table].catalogTable
    assert(table.provider.contains("iceberg"))
    assert(table.location.toString === "file:/tmp/uc/iceberg-table")
    assert(table.schema.fieldNames === Array("id"))
  }

  test("loadTable propagates UC Delta Rest Catalog API server errors") {
    handler = exchange => exchange.getRequestURI.getPath match {
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl" =>
        sendJson(exchange, 500, """{"error_code":"INTERNAL_ERROR"}""")
      case "/api/2.1/unity-catalog/delta/v1/catalogs/uc/schemas/default/tables/tbl/credentials" =>
        fail("Unexpected credentials request after loadTable failure")
      case path =>
        fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
    }

    val error = intercept[IOException] {
      loadWithUCDeltaRestCatalogApi()
    }

    assert(error.getMessage.contains("Failed to load table uc.default.tbl"))
    assert(error.getMessage.contains("HTTP 500"))
  }

  test("apply fails when UC Delta Rest Catalog API is enabled but unsupported") {
    configHandler = exchange => sendJson(exchange, 200,
      """{
        |  "endpoints": [],
        |  "protocol-version": "1.0"
        |}""".stripMargin)
    handler = exchange =>
      fail(s"Unexpected UC Delta Rest Catalog API request path: ${exchange.getRequestURI.getPath}")

    val error = intercept[IllegalArgumentException] {
      withUCDeltaRestCatalogApi { catalog =>
        catalog.loadTable(Identifier.of(Array("default"), "tbl"))
      }
    }

    assert(error.getMessage.contains("UC Delta Rest Catalog API is enabled for catalog uc"))
    assert(error.getMessage.contains("does not support the required UC Delta Rest Catalog API endpoints"))
  }

  test("loadTable does not probe UC Delta Rest Catalog API when disabled") {
    configHandler = exchange =>
      fail(s"Unexpected UC Delta Rest Catalog API config request: ${exchange.getRequestURI}")
    handler = exchange =>
      fail(s"Unexpected UC Delta Rest Catalog API request path: ${exchange.getRequestURI.getPath}")

    withSQLConf(
      "spark.sql.catalog.uc" -> "io.unitycatalog.spark.UCSingleCatalog",
      "spark.sql.catalog.uc.uri" -> serverUri,
      "spark.sql.catalog.uc.token" -> "mock-token") {
      val catalog = DeltaCatalogClient(new TestDelegateCatalog, spark)
      assert(catalog.loadTable(Identifier.of(Array("default"), "tbl")).isEmpty)
    }
  }

  test("apply fails when UC Delta Rest Catalog API is enabled without UC config") {
    withSQLConf(
      "spark.sql.catalog.uc" -> "io.unitycatalog.spark.UCSingleCatalog",
      DeltaCatalogClient.deltaRestApiEnabledConf("uc") -> "true") {
      val error = intercept[IllegalArgumentException] {
        DeltaCatalogClient(new TestDelegateCatalog, spark)
      }
      assert(error.getMessage.contains("configuration is missing or incomplete"))
    }
  }

  private def loadWithUCDeltaRestCatalogApi(): V1Table = {
    withUCDeltaRestCatalogApi { catalog =>
      catalog.loadTable(Identifier.of(Array("default"), "tbl")).get.asInstanceOf[V1Table]
    }
  }

  private def withUCDeltaRestCatalogApi[T](f: DeltaCatalogClient => T): T = {
    withUCDeltaRestCatalogApi(new TestDelegateCatalog, renewCredentialEnabled = true)(f)
  }

  private def withUCDeltaRestCatalogApiRenewalDisabled[T](f: DeltaCatalogClient => T): T = {
    withUCDeltaRestCatalogApi(new TestDelegateCatalog, renewCredentialEnabled = false)(f)
  }

  private def withUCDeltaRestCatalogApi[T](
      delegate: TableCatalog)(
      f: DeltaCatalogClient => T): T = {
    withUCDeltaRestCatalogApi(delegate, renewCredentialEnabled = true)(f)
  }

  private def withUCDeltaRestCatalogApi[T](
      delegate: TableCatalog,
      renewCredentialEnabled: Boolean)(
      f: DeltaCatalogClient => T): T = {
    withSQLConf(
      "spark.sql.catalog.uc" -> "io.unitycatalog.spark.UCSingleCatalog",
      "spark.sql.catalog.uc.uri" -> serverUri,
      "spark.sql.catalog.uc.token" -> "mock-token",
      DeltaCatalogClient.renewCredentialEnabledConf("uc") -> renewCredentialEnabled.toString,
      DeltaCatalogClient.deltaRestApiEnabledConf("uc") -> "true") {
      val catalog = DeltaCatalogClient(delegate, spark)
      f(catalog)
    }
  }

  private def loadTableResponseJson(
      location: String,
      dataSourceFormat: String = "DELTA"): String =
    s"""{
       |  "metadata": {
       |    "data-source-format": "$dataSourceFormat",
       |    "table-type": "MANAGED",
       |    "table-uuid": "11111111-1111-1111-1111-111111111111",
       |    "location": "$location",
       |    "columns": {
       |      "type": "struct",
       |      "fields": [
       |        {
       |          "name": "id",
       |          "type": "long",
       |          "nullable": false,
       |          "metadata": {
       |            "delta.columnMapping.id": 1,
       |            "delta.columnMapping.physicalName": "col-123"
       |          }
       |        }
       |      ]
       |    },
       |    "partition-columns": [],
       |    "properties": {
       |      "delta.feature.catalogManaged": "supported",
       |      "$UC_TABLE_ID_KEY": "11111111-1111-1111-1111-111111111111"
       |    }
       |  },
       |  "commits": []
       |}""".stripMargin

  private def s3CredentialsResponseJson(prefix: String, operation: String): String =
    s"""{
       |  "storage-credentials": [
       |    {
       |      "prefix": "$prefix",
       |      "operation": "$operation",
       |      "config": {
       |        "s3.access-key-id": "ak",
       |        "s3.secret-access-key": "sk",
       |        "s3.session-token": "st"
       |      }
       |    }
       |  ]
       |}""".stripMargin

  private def sendJson(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.close()
  }

  private class TestDelegateCatalog extends TableCatalog {
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
    override def name(): String = "uc"
    override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty
    override def loadTable(ident: Identifier): Table =
      throw new IllegalStateException("unexpected loadTable call")
    override def createTable(
        ident: Identifier,
        schema: StructType,
        partitions: Array[org.apache.spark.sql.connector.expressions.Transform],
        properties: java.util.Map[String, String]): Table =
      throw new UnsupportedOperationException("not needed in this test")
    override def alterTable(ident: Identifier, changes: TableChange*): Table =
      throw new UnsupportedOperationException("not needed in this test")
    override def dropTable(ident: Identifier): Boolean =
      throw new UnsupportedOperationException("not needed in this test")
    override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
      throw new UnsupportedOperationException("not needed in this test")
  }

}
