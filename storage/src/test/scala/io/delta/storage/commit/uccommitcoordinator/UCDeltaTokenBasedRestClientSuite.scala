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

package io.delta.storage.commit.uccommitcoordinator

import java.io.IOException
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.{Arrays => JArrays, Collections, Optional, UUID}

import io.delta.storage.commit.{Commit, GetCommitsResponse, TableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.{
  CreateTableRequest,
  DeltaCommit,
  DeltaProtocol,
  TableRequirement,
  TableUpdate,
  UpdateTableRequest
}
import io.delta.storage.commit.uniform.UniformMetadata

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.unitycatalog.client.auth.TokenProvider
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class UCDeltaTokenBasedRestClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testMetastoreId = "test-metastore-123"
  private val objectMapper = new ObjectMapper()

  private var server: HttpServer = _
  private var serverUri: String = _
  private var deltaConfigHandler: HttpExchange => Unit = _
  private var deltaTablesHandler: HttpExchange => Unit = _
  private var legacyTablesHandler: HttpExchange => Unit = _

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/api/2.1/unity-catalog/delta/v1/config",
      exchange => {
        if (deltaConfigHandler != null) deltaConfigHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
        exchange.close()
      })
    server.createContext(
      "/api/2.1/unity-catalog/delta/v1/catalogs",
      exchange => {
        if (deltaTablesHandler != null) deltaTablesHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
        exchange.close()
      })
    server.createContext(
      "/api/2.1/unity-catalog/tables",
      exchange => {
        if (legacyTablesHandler != null) legacyTablesHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
        exchange.close()
      })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    deltaConfigHandler = null
    deltaTablesHandler = null
    legacyTablesHandler = null
  }

  private def readRequestBody(exchange: HttpExchange): String = {
    val is = exchange.getRequestBody
    try new String(is.readAllBytes(), StandardCharsets.UTF_8) finally is.close()
  }

  private def sendJson(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.close()
  }

  private def createTokenProvider(): TokenProvider = new TokenProvider {
    override def accessToken(): String = "mock-token"
    override def initialize(configs: java.util.Map[String, String]): Unit = {}
    override def configs(): java.util.Map[String, String] = Collections.emptyMap()
  }

  private def createDeltaClient(): UCDeltaTokenBasedRestClient =
    new UCDeltaTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")

  private def assertJsonEquals(actual: String, expected: String): Unit = {
    assert(objectMapper.readTree(actual) === objectMapper.readTree(expected))
  }

  private def withDeltaClient(fn: UCDeltaTokenBasedRestClient => Unit): Unit = {
    val client = new UCDeltaTokenBasedRestClient(
      serverUri,
      createTokenProvider(),
      Collections.emptyMap())
    try fn(client)
    finally client.close()
  }

  private def enableDeltaApiConfig(): Unit = {
    deltaConfigHandler = exchange => {
      assert(exchange.getRequestURI.getQuery.contains("catalog=main"))
      assert(exchange.getRequestURI.getQuery.contains("protocol-versions=1.0"))
      sendJson(
        exchange,
        HttpStatus.SC_OK,
        """{
          |  "endpoints": [
          |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}"
          |  ],
          |  "protocol-version": "1.0"
          |}""".stripMargin)
    }
  }

  private def loadTableResponseJson(
      schemaJson: String = simpleSchemaJson,
      includeColumns: Boolean = true): String = {
    val columnsJson = if (includeColumns) {
      s""",
         |    "columns": $schemaJson""".stripMargin
    } else {
      ""
    }
    s"""{
       |  "metadata": {
       |    "name": "tbl",
       |    "catalog-name": "main",
       |    "schema-name": "default",
       |    "table-type": "MANAGED",
       |    "data-source-format": "DELTA",
       |    "table-uuid": "11111111-1111-1111-1111-111111111111",
       |    "location": "file:/tmp/uc/table",
       |    "created-time": 10,
       |    "updated-time": 11$columnsJson,
       |    "partition-columns": [],
       |    "properties": {}
       |  },
       |  "commits": []
       |}""".stripMargin
  }

  private def simpleSchemaJson: String =
    """{
      |  "type": "struct",
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "long",
      |      "nullable": false,
      |      "metadata": {}
      |    }
      |  ]
      |}""".stripMargin

  private def complexDeltaSchemaJson: String =
    """{
      |  "type": "struct",
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "long",
      |      "nullable": false,
      |      "metadata": {
      |        "delta.columnMapping.id": 1,
      |        "delta.columnMapping.physicalName": "col-1",
      |        "aliases": ["identifier", "primary_key"],
      |        "flags": [true, false],
      |        "weights": [1.5, 2.5],
      |        "nested": {"owner": "uc"},
      |        "nestedArray": [{"name": "left"}, {"name": "right"}]
      |      }
      |    },
      |    {
      |      "name": "price",
      |      "type": "decimal(10,2)",
      |      "nullable": true,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "tags",
      |      "type": {
      |        "type": "array",
      |        "elementType": "string",
      |        "containsNull": true
      |      },
      |      "nullable": true,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "scores",
      |      "type": {
      |        "type": "map",
      |        "keyType": "string",
      |        "valueType": {
      |          "type": "struct",
      |          "fields": [
      |            {
      |              "name": "value",
      |              "type": "double",
      |              "nullable": false,
      |              "metadata": {"comment": "score value"}
      |            },
      |            {
      |              "name": "timestamp",
      |              "type": "long",
      |              "nullable": true,
      |              "metadata": {}
      |            }
      |          ]
      |        },
      |        "valueContainsNull": false
      |      },
      |      "nullable": false,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "details",
      |      "type": {
      |        "type": "struct",
      |        "fields": [
      |          {
      |            "name": "active",
      |            "type": "boolean",
      |            "nullable": false,
      |            "metadata": {}
      |          }
      |        ]
      |      },
      |      "nullable": true,
      |      "metadata": {}
      |    }
      |  ]
      |}""".stripMargin

  private def complexDeltaApiSchemaJson: String = complexDeltaSchemaJson
    .replace("\"elementType\"", "\"element-type\"")
    .replace("\"containsNull\"", "\"contains-null\"")
    .replace("\"keyType\"", "\"key-type\"")
    .replace("\"valueType\"", "\"value-type\"")
    .replace("\"valueContainsNull\"", "\"value-contains-null\"")

  private def sparkSchemaJson: String =
    """{
      |  "type": "struct",
      |  "fields": [
      |    {
      |      "name": "tags",
      |      "type": {
      |        "type": "array",
      |        "elementType": "string",
      |        "containsNull": true
      |      },
      |      "nullable": true,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "scores",
      |      "type": {
      |        "type": "map",
      |        "keyType": "string",
      |        "valueType": "long",
      |        "valueContainsNull": false
      |      },
      |      "nullable": false,
      |      "metadata": {}
      |    }
      |  ]
      |}""".stripMargin

  test("UCDeltaClient defaults fail loudly for UC Delta Rest Catalog API") {
    val client = new UCDeltaClient {
      override def getMetastoreId(): String = testMetastoreId
      override def commit(
          tableId: String,
          tableUri: URI,
          tableIdentifier: TableIdentifier,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          oldMetadata: Optional[AbstractMetadata],
          newMetadata: Optional[AbstractMetadata],
          oldProtocol: Optional[AbstractProtocol],
          newProtocol: Optional[AbstractProtocol],
          uniform: Optional[UniformMetadata]): Unit = {}
      override def getCommits(
          tableId: String,
          tableUri: URI,
          startVersion: Optional[java.lang.Long],
          endVersion: Optional[java.lang.Long]): GetCommitsResponse = null
      override def finalizeCreate(
          tableName: String,
          catalogName: String,
          schemaName: String,
          storageLocation: String,
          columns: java.util.List[UCClient.ColumnDef],
          properties: java.util.Map[String, String]): Unit = {}
      override def close(): Unit = {}
    }

    assert(!client.supportsUCDeltaRestCatalogApi())
    assert(intercept[UnsupportedOperationException] {
      client.loadTable("main", "default", "tbl")
    }.getMessage === "loadTable requires UC Delta Rest Catalog API support.")
    assert(intercept[UnsupportedOperationException] {
      client.createStagingTable("main", "default", "tbl")
    }.getMessage === "createStagingTable requires UC Delta Rest Catalog API support.")
    assert(intercept[UnsupportedOperationException] {
      client.createTable("main", "default", new CreateTableRequest().name("tbl"))
    }.getMessage === "createTable requires UC Delta Rest Catalog API support.")
    assert(intercept[UnsupportedOperationException] {
      client.updateTable("main", "default", "tbl", new UpdateTableRequest())
    }.getMessage === "updateTable requires UC Delta Rest Catalog API support.")
  }

  test("default constructor does not enable UC Delta Rest Catalog API") {
    withDeltaClient { client =>
      assert(!client.supportsUCDeltaRestCatalogApi())
      assert(intercept[UnsupportedOperationException] {
        client.loadTable("main", "default", "tbl")
      }.getMessage === "loadTable requires UC Delta Rest Catalog API support.")
      assert(intercept[UnsupportedOperationException] {
        client.createStagingTable("main", "default", "tbl")
      }.getMessage === "createStagingTable requires UC Delta Rest Catalog API support.")
      assert(intercept[UnsupportedOperationException] {
        client.createTable("main", "default", new CreateTableRequest().name("tbl"))
      }.getMessage === "createTable requires UC Delta Rest Catalog API support.")
      assert(intercept[UnsupportedOperationException] {
        client.updateTable("main", "default", "tbl", new UpdateTableRequest())
      }.getMessage === "updateTable requires UC Delta Rest Catalog API support.")
    }
  }

  test(
    "catalog-aware constructor uses UC Delta Rest Catalog API when config lists required endpoints") {
    enableDeltaApiConfig()
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestURI.getPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }
    legacyTablesHandler = exchange => fail(s"Unexpected legacy request: ${exchange.getRequestURI}")

    val client = createDeltaClient()
    try {
      assert(client.supportsUCDeltaRestCatalogApi())
      val metadata = client.loadTable("main", "default", "tbl")
      assert(metadata.getId === "11111111-1111-1111-1111-111111111111")
      assert(metadata.getName === "tbl")
      assert(metadata.getProvider === "delta")
      val adapter = metadata.asInstanceOf[UCDeltaTokenBasedRestClient.TableMetadataAdapter]
      assert(adapter.getLocation === "file:/tmp/uc/table")
      assert(adapter.getCreatedTime === Long.box(10L))
      assertJsonEquals(adapter.getSchemaString, simpleSchemaJson)
    } finally {
      client.close()
    }
  }

  test("loadTable converts UC SDK schema to Delta schema JSON") {
    enableDeltaApiConfig()
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestURI.getPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson(complexDeltaApiSchemaJson))
    }

    val client = createDeltaClient()
    try {
      val schemaString = client.loadTable("main", "default", "tbl")
        .asInstanceOf[UCDeltaTokenBasedRestClient.TableMetadataAdapter]
        .getSchemaString
      assertJsonEquals(schemaString, complexDeltaSchemaJson)
    } finally {
      client.close()
    }
  }

  test("loadTable reports missing schema columns as IOException") {
    enableDeltaApiConfig()
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestURI.getPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      sendJson(
        exchange,
        HttpStatus.SC_OK,
        loadTableResponseJson(includeColumns = false))
    }

    val client = createDeltaClient()
    try {
      val error = intercept[IOException] {
        client.loadTable("main", "default", "tbl")
      }
      assert(error.getMessage.contains("missing table schema columns"))
    } finally {
      client.close()
    }
  }

  test(
    "catalog-aware constructor fails UC Delta Rest Catalog API loadTable when config is unavailable") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
    deltaTablesHandler = exchange =>
      fail(s"Unexpected UC Delta Rest Catalog API request: ${exchange.getRequestURI}")
    legacyTablesHandler = exchange =>
      fail(s"Unexpected legacy request: ${exchange.getRequestURI}")

    val client = createDeltaClient()
    try {
      assert(!client.supportsUCDeltaRestCatalogApi())
      val e = intercept[UnsupportedOperationException] {
        client.loadTable("main", "default", "tbl")
      }
      assert(e.getMessage === "loadTable requires UC Delta Rest Catalog API support.")
      val stagingError = intercept[UnsupportedOperationException] {
        client.createStagingTable("main", "default", "tbl")
      }
      assert(stagingError.getMessage ===
        "createStagingTable requires UC Delta Rest Catalog API support.")
      val createError = intercept[UnsupportedOperationException] {
        client.createTable("main", "default", new CreateTableRequest().name("tbl"))
      }
      assert(createError.getMessage === "createTable requires UC Delta Rest Catalog API support.")
      val updateError = intercept[UnsupportedOperationException] {
        client.updateTable("main", "default", "tbl", new UpdateTableRequest())
      }
      assert(updateError.getMessage === "updateTable requires UC Delta Rest Catalog API support.")
    } finally {
      client.close()
    }
  }

  test("createStagingTable and createTable call UC Delta Rest Catalog API endpoints") {
    enableDeltaApiConfig()
    var sawStagingCreate = false
    var sawTableCreate = false
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestMethod === "POST")
      exchange.getRequestURI.getPath match {
        case "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/staging-tables" =>
          sawStagingCreate = true
          val body = objectMapper.readTree(readRequestBody(exchange))
          assert(body.get("name").asText === "tbl")
          sendJson(
            exchange,
            HttpStatus.SC_OK,
            """{
              |  "table-id": "22222222-2222-2222-2222-222222222222",
              |  "table-type": "MANAGED",
              |  "location": "s3://bucket/path/to/table",
              |  "storage-credentials": [],
              |  "required-protocol": {
              |    "min-reader-version": 1,
              |    "min-writer-version": 2
              |  },
              |  "required-properties": {
              |    "delta.feature.catalogManaged": "supported"
              |  }
              |}""".stripMargin)
        case "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables" =>
          sawTableCreate = true
          val body = objectMapper.readTree(readRequestBody(exchange))
          assert(body.get("name").asText === "tbl")
          val fields = body.get("columns").get("fields")
          val arrayType = fields.get(0).get("type")
          assert(arrayType.has("element-type"))
          assert(arrayType.has("contains-null"))
          assert(!arrayType.has("elementType"))
          assert(!arrayType.has("containsNull"))
          val mapType = fields.get(1).get("type")
          assert(mapType.has("key-type"))
          assert(mapType.has("value-type"))
          assert(mapType.has("value-contains-null"))
          assert(!mapType.has("keyType"))
          assert(!mapType.has("valueType"))
          assert(!mapType.has("valueContainsNull"))
          sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
        case path =>
          fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
      }
    }

    val client = createDeltaClient()
    try {
      val staging = client.createStagingTable("main", "default", "tbl")
      assert(staging.getTableId.toString === "22222222-2222-2222-2222-222222222222")
      assert(staging.getLocation === "s3://bucket/path/to/table")

      val created = client.createTable(
        "main",
        "default",
        new CreateTableRequest().name("tbl").schemaString(sparkSchemaJson))
      assert(
        created.asInstanceOf[UCDeltaTokenBasedRestClient.TableMetadataAdapter].getLocation ===
          "file:/tmp/uc/table")
    } finally {
      client.close()
    }
    assert(sawStagingCreate)
    assert(sawTableCreate)
  }

  test("updateTable calls UC Delta Rest Catalog API endpoint") {
    enableDeltaApiConfig()
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestMethod === "POST")
      assert(exchange.getRequestURI.getPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      val body = objectMapper.readTree(readRequestBody(exchange))
      val requirements = body.get("requirements")
      assert(requirements.size() === 1)
      assert(requirements.get(0).get("type").asText === "assert-table-uuid")
      assert(requirements.get(0).get("uuid").asText ===
        "11111111-1111-1111-1111-111111111111")

      val updates = body.get("updates")
      assert(updates.size() === 6)
      val addCommit = updates.get(0)
      assert(addCommit.get("action").asText === "add-commit")
      assert(addCommit.get("commit").get("version").asLong === 7L)
      assert(addCommit.get("commit").get("timestamp").asLong === 100L)
      assert(addCommit.get("commit").get("file-name").asText === "0007.uuid.json")
      assert(addCommit.get("commit").get("file-size").asLong === 32L)
      assert(addCommit.get("commit").get("file-modification-timestamp").asLong === 200L)

      val setColumns = updates.get(1)
      assert(setColumns.get("action").asText === "set-columns")
      val fields = setColumns.get("columns").get("fields")
      val arrayType = fields.get(0).get("type")
      assert(arrayType.has("element-type"))
      assert(arrayType.has("contains-null"))
      assert(!arrayType.has("elementType"))
      assert(!arrayType.has("containsNull"))
      val mapType = fields.get(1).get("type")
      assert(mapType.has("key-type"))
      assert(mapType.has("value-type"))
      assert(mapType.has("value-contains-null"))
      assert(!mapType.has("keyType"))
      assert(!mapType.has("valueType"))
      assert(!mapType.has("valueContainsNull"))

      val setProtocol = updates.get(2)
      assert(setProtocol.get("action").asText === "set-protocol")
      assert(setProtocol.get("protocol").get("min-reader-version").asInt === 1)
      assert(setProtocol.get("protocol").get("min-writer-version").asInt === 7)
      assert(setProtocol.get("protocol").get("writer-features").get(0).asText === "domainMetadata")

      val setProperties = updates.get(3)
      assert(setProperties.get("action").asText === "set-properties")
      assert(setProperties.get("updates").get("delta.appendOnly").asText === "true")
      val removeProperties = updates.get(4)
      assert(removeProperties.get("action").asText === "remove-properties")
      assert(removeProperties.get("removals").get(0).asText === "old.prop")
      val latestBackfilled = updates.get(5)
      assert(latestBackfilled.get("action").asText === "set-latest-backfilled-version")
      assert(latestBackfilled.get("latest-published-version").asLong === 6L)

      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }

    val client = createDeltaClient()
    try {
      val updated = client.updateTable(
        "main",
        "default",
        "tbl",
        new UpdateTableRequest()
          .addRequirementsItem(TableRequirement.assertTableUuid(
            UUID.fromString("11111111-1111-1111-1111-111111111111")))
          .addUpdatesItem(TableUpdate.addCommit(
            new DeltaCommit()
              .version(7L)
              .timestamp(100L)
              .fileName("0007.uuid.json")
              .fileSize(32L)
              .fileModificationTimestamp(200L),
            null))
          .addUpdatesItem(TableUpdate.setColumns(sparkSchemaJson))
          .addUpdatesItem(TableUpdate.setProtocolUpdate(
            new DeltaProtocol()
              .minReaderVersion(1)
              .minWriterVersion(7)
              .writerFeatures(JArrays.asList("domainMetadata"))))
          .addUpdatesItem(TableUpdate.setProperties(
            Collections.singletonMap("delta.appendOnly", "true")))
          .addUpdatesItem(TableUpdate.removeProperties(JArrays.asList("old.prop")))
          .addUpdatesItem(TableUpdate.setLatestBackfilledVersion(6L)))
      assert(
        updated.asInstanceOf[UCDeltaTokenBasedRestClient.TableMetadataAdapter].getLocation ===
          "file:/tmp/uc/table")
    } finally {
      client.close()
    }
  }

  test("createStagingTable, createTable, and updateTable wrap HTTP errors") {
    enableDeltaApiConfig()
    deltaTablesHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"boom"}""")

    val client = createDeltaClient()
    try {
      val stagingError = intercept[java.io.IOException] {
        client.createStagingTable("main", "default", "tbl")
      }
      assert(stagingError.getMessage.contains(
        "Failed to create staging table main.default.tbl via UC Delta Rest Catalog API (HTTP 500)"))

      val createError = intercept[java.io.IOException] {
        client.createTable("main", "default", new CreateTableRequest().name("tbl"))
      }
      assert(createError.getMessage.contains(
        "Failed to create table main.default.tbl via UC Delta Rest Catalog API (HTTP 500)"))

      val unnamedCreateError = intercept[java.io.IOException] {
        client.createTable("main", "default", new CreateTableRequest())
      }
      assert(unnamedCreateError.getMessage.contains(
        "Failed to create table main.default.<unknown> via UC Delta Rest Catalog API (HTTP 500)"))

      val updateError = intercept[java.io.IOException] {
        client.updateTable("main", "default", "tbl", new UpdateTableRequest())
      }
      assert(updateError.getMessage.contains(
        "Failed to update table main.default.tbl via UC Delta Rest Catalog API (HTTP 500)"))
    } finally {
      client.close()
    }
  }

  test("createStagingTable, createTable, and updateTable validate required parameters") {
    enableDeltaApiConfig()

    val client = createDeltaClient()
    try {
      intercept[NullPointerException] {
        client.createStagingTable(null, "default", "tbl")
      }
      intercept[NullPointerException] {
        client.createStagingTable("main", null, "tbl")
      }
      intercept[NullPointerException] {
        client.createStagingTable("main", "default", null)
      }
      intercept[NullPointerException] {
        client.createTable(null, "default", new CreateTableRequest().name("tbl"))
      }
      intercept[NullPointerException] {
        client.createTable("main", null, new CreateTableRequest().name("tbl"))
      }
      intercept[NullPointerException] {
        client.createTable("main", "default", null)
      }
      intercept[NullPointerException] {
        client.updateTable(null, "default", "tbl", new UpdateTableRequest())
      }
      intercept[NullPointerException] {
        client.updateTable("main", null, "tbl", new UpdateTableRequest())
      }
      intercept[NullPointerException] {
        client.updateTable("main", "default", null, new UpdateTableRequest())
      }
      intercept[NullPointerException] {
        client.updateTable("main", "default", "tbl", null)
      }
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor disables UC Delta Rest Catalog API when config does not list loadTable") {
    deltaConfigHandler = exchange =>
      sendJson(
        exchange,
        HttpStatus.SC_OK,
        """{
        |  "endpoints": [
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
        |  ],
        |  "protocol-version": "1.0"
        |}""".stripMargin)

    val client = createDeltaClient()
    try {
      assert(!client.supportsUCDeltaRestCatalogApi())
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor fails when config probe fails") {
    deltaConfigHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"boom"}""")

    val e = intercept[IllegalArgumentException] {
      createDeltaClient()
    }
    assert(e.getMessage.contains("Failed to determine UC Delta Rest Catalog API support"))
    assert(e.getMessage.contains("HTTP 500"))
  }
}
