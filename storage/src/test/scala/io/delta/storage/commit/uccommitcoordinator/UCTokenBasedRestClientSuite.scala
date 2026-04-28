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

import java.net.{InetSocketAddress, URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Optional}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException, GetCommitsResponse}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.delta.model.{CreateTableRequest, CredentialOperation}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class UCTokenBasedRestClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testTableId = "test-table-id"
  private val testTableUri = new URI("s3://bucket/path/to/table")
  private val testMetastoreId = "test-metastore-123"

  private var server: HttpServer = _
  private var serverUri: String = _
  private var metastoreHandler: HttpExchange => Unit = _
  private var commitsHandler: HttpExchange => Unit = _
  private var deltaConfigHandler: HttpExchange => Unit = _
  private var deltaTablesHandler: HttpExchange => Unit = _
  private var deltaPathCredentialsHandler: HttpExchange => Unit = _
  private var legacyTablesHandler: HttpExchange => Unit = _
  private val objectMapper = new ObjectMapper()

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/api/2.1/unity-catalog/metastore_summary", exchange => {
      if (metastoreHandler != null) metastoreHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_OK, s"""{"metastore_id":"$testMetastoreId"}""")
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/delta/preview/commits", exchange => {
      if (commitsHandler != null) commitsHandler(exchange)
      else {
        val body = if (exchange.getRequestMethod == "POST") "{}"
          else """{"commits":[],"latest_table_version":-1}"""
        sendJson(exchange, HttpStatus.SC_OK, body)
      }
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/delta/v1/config", exchange => {
      if (deltaConfigHandler != null) deltaConfigHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/delta/v1/catalogs", exchange => {
      if (deltaTablesHandler != null) deltaTablesHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/delta/v1/temporary-path-credentials", exchange => {
      if (deltaPathCredentialsHandler != null) deltaPathCredentialsHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/tables", exchange => {
      if (legacyTablesHandler != null) legacyTablesHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      exchange.close()
    })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    metastoreHandler = null
    commitsHandler = null
    deltaConfigHandler = null
    deltaTablesHandler = null
    deltaPathCredentialsHandler = null
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

  private def queryParams(exchange: HttpExchange): Map[String, String] = {
    Option(exchange.getRequestURI.getRawQuery).toSeq
      .flatMap(_.split("&"))
      .filter(_.nonEmpty)
      .map { kv =>
        val pair = kv.split("=", 2)
        val key = URLDecoder.decode(pair(0), StandardCharsets.UTF_8)
        val value = if (pair.length == 2) {
          URLDecoder.decode(pair(1), StandardCharsets.UTF_8)
        } else {
          ""
        }
        key -> value
      }.toMap
  }

  private def createTokenProvider(): TokenProvider = new TokenProvider {
    override def accessToken(): String = "mock-token"
    override def initialize(configs: java.util.Map[String, String]): Unit = {}
    override def configs(): java.util.Map[String, String] = Collections.emptyMap()
  }

  private def createClient(): UCTokenBasedRestClient =
    new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap())

  private def withClient(fn: UCTokenBasedRestClient => Unit): Unit = {
    val client = createClient()
    try fn(client) finally client.close()
  }

  private def createCommit(version: Long): Commit = {
    val fs = new FileStatus(1024L, false, 1, 4096L, System.currentTimeMillis(),
      new Path(s"/path/_delta_log/_staged_commits/$version.uuid.json"))
    new Commit(version, fs, System.currentTimeMillis())
  }

  private def createMetadata(): AbstractMetadata = new AbstractMetadata {
    override def getId: String = "id"
    override def getName: String = "name"
    override def getDescription: String = "desc"
    override def getProvider: String = "delta"
    override def getFormatOptions: java.util.Map[String, String] = Collections.emptyMap()
    override def getSchemaString: String = """{"type":"struct","fields":[]}"""
    override def getPartitionColumns: java.util.List[String] = Collections.emptyList()
    override def getConfiguration: java.util.Map[String, String] = Collections.emptyMap()
    override def getCreatedTime: java.lang.Long = 0L
  }

  private def createUniformMetadata(): UniformMetadata =
    new UniformMetadata(
      new IcebergMetadata("s3://bucket/metadata/v1.json", 42L, "2025-01-04T03:13:11.423Z"))

  private def loadTableResponseJson: String =
    """{
      |  "metadata": {
      |    "name": "tbl",
      |    "catalog-name": "main",
      |    "schema-name": "default",
      |    "table-type": "MANAGED",
      |    "data-source-format": "DELTA",
      |    "table-uuid": "11111111-1111-1111-1111-111111111111",
      |    "location": "file:/tmp/uc/table",
      |    "created-at": 10,
      |    "updated-at": 11,
      |    "columns": {
      |      "type": "struct",
      |      "fields": [
      |        {
      |          "name": "id",
      |          "type": "long",
      |          "nullable": false,
      |          "metadata": {}
      |        }
      |      ]
      |    },
      |    "partition-columns": [],
      |    "properties": {}
      |  },
      |  "commits": []
      |}""".stripMargin

  test("UCClient defaults fail loudly for UC Delta Rest Catalog API") {
    val client = new UCClient {
      override def getMetastoreId(): String = testMetastoreId
      override def commit(
          tableId: String,
          tableUri: URI,
          commit: Optional[Commit],
          lastKnownBackfilledVersion: Optional[java.lang.Long],
          disown: Boolean,
          newMetadata: Optional[AbstractMetadata],
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
      client.getTableCredentials(CredentialOperation.READ, "main", "default", "tbl")
    }.getMessage === "getTableCredentials requires UC Delta Rest Catalog API support.")
  }

  // Constructor tests
  test("constructor validates required parameters") {
    intercept[NullPointerException] {
      new UCTokenBasedRestClient(null, createTokenProvider(), Collections.emptyMap())
    }
    intercept[NullPointerException] {
      new UCTokenBasedRestClient(serverUri, null, Collections.emptyMap())
    }
    intercept[NullPointerException] {
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), null)
    }
  }

  // getMetastoreId tests
  test("getMetastoreId returns ID on success") {
    withClient { client =>
      assert(client.getMetastoreId() === testMetastoreId)
    }
  }

  test("getMetastoreId throws IOException on error") {
    metastoreHandler = exchange => sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, "{}")
    withClient { client =>
      intercept[java.io.IOException] { client.getMetastoreId() }
    }
  }

  test("catalog-aware constructor uses UC Delta Rest Catalog API when config lists required endpoints") {
    deltaConfigHandler = exchange => {
      assert(exchange.getRequestURI.getQuery.contains("catalog=main"))
      assert(exchange.getRequestURI.getQuery.contains("protocol-versions=1.0"))
      sendJson(exchange, HttpStatus.SC_OK,
        """{
          |  "endpoints": [
          |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
          |  ],
          |  "protocol-version": "1.0"
          |}""".stripMargin)
    }
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestURI.getPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson)
    }
    legacyTablesHandler = exchange => fail(s"Unexpected legacy request: ${exchange.getRequestURI}")

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    try {
      assert(client.supportsUCDeltaRestCatalogApi())
      assert(client.loadTable("main", "default", "tbl").getMetadata.getLocation ===
        "file:/tmp/uc/table")
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor gets temporary path credentials through UC Delta Rest Catalog API") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_OK,
      """{
        |  "endpoints": [
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
        |    "GET /v1/temporary-path-credentials"
        |  ],
        |  "protocol-version": "1.0"
        |}""".stripMargin)
    deltaPathCredentialsHandler = exchange => {
      assert(exchange.getRequestMethod === "GET")
      assert(queryParams(exchange) === Map(
        "location" -> "s3://bucket/path/to/table",
        "operation" -> "READ"))
      sendJson(exchange, HttpStatus.SC_OK,
        """{
          |  "storage-credentials": [
          |    {
          |      "prefix": "s3://bucket/path/to/table",
          |      "operation": "READ",
          |      "config": {
          |        "s3.access-key-id": "ak",
          |        "s3.secret-access-key": "sk",
          |        "s3.session-token": "st"
          |      },
          |      "expiration-time-ms": 123
          |    }
          |  ]
          |}""".stripMargin)
    }

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    try {
      val response = client.getTemporaryPathCredentials(
        CredentialOperation.READ,
        "s3://bucket/path/to/table")
      assert(response.getStorageCredentials.size() === 1)
      assert(response.getStorageCredentials.get(0).getPrefix === "s3://bucket/path/to/table")
      assert(response.getStorageCredentials.get(0).getOperation === CredentialOperation.READ)
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor fails temporary path credentials when endpoint is unavailable") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_OK,
      """{
        |  "endpoints": [
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
        |  ],
        |  "protocol-version": "1.0"
        |}""".stripMargin)
    deltaPathCredentialsHandler = exchange =>
      fail(s"Unexpected temporary path credentials request: ${exchange.getRequestURI}")

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    try {
      val e = intercept[UnsupportedOperationException] {
        client.getTemporaryPathCredentials(
          CredentialOperation.READ,
          "s3://bucket/path/to/table")
      }
      assert(e.getMessage ===
        "getTemporaryPathCredentials requires UC Delta Rest Catalog API temporary path credentials " +
          "support.")
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor fails UC Delta Rest Catalog API loadTable when config is unavailable") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
    deltaTablesHandler = exchange =>
      fail(s"Unexpected UC Delta Rest Catalog API request: ${exchange.getRequestURI}")
    legacyTablesHandler = exchange =>
      fail(s"Unexpected legacy request: ${exchange.getRequestURI}")

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    try {
      assert(!client.supportsUCDeltaRestCatalogApi())
      val e = intercept[UnsupportedOperationException] {
        client.loadTable("main", "default", "tbl")
      }
      assert(e.getMessage === "loadTable requires UC Delta Rest Catalog API support.")
      val credentialsError = intercept[UnsupportedOperationException] {
        client.getTableCredentials(CredentialOperation.READ, "main", "default", "tbl")
      }
      assert(credentialsError.getMessage ===
        "getTableCredentials requires UC Delta Rest Catalog API support.")
      val stagingError = intercept[UnsupportedOperationException] {
        client.createStagingTable("main", "default", "tbl")
      }
      assert(stagingError.getMessage ===
        "createStagingTable requires UC Delta Rest Catalog API support.")
      val createError = intercept[UnsupportedOperationException] {
        client.createTable("main", "default", new CreateTableRequest().name("tbl"))
      }
      assert(createError.getMessage === "createTable requires UC Delta Rest Catalog API support.")
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor disables UC Delta Rest Catalog API when config does not list loadTable") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_OK,
      """{
        |  "endpoints": [
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
        |  ],
        |  "protocol-version": "1.0"
        |}""".stripMargin)

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    try {
      assert(!client.supportsUCDeltaRestCatalogApi())
    } finally {
      client.close()
    }
  }

  test("catalog-aware constructor disables UC Delta Rest Catalog API when config does not list credentials") {
    deltaConfigHandler = exchange => sendJson(exchange, HttpStatus.SC_OK,
      """{
        |  "endpoints": [
        |    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}"
        |  ],
        |  "protocol-version": "1.0"
        |}""".stripMargin)

    val client =
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
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
      new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap(), "main")
    }
    assert(e.getMessage.contains("Failed to determine UC Delta Rest Catalog API support"))
    assert(e.getMessage.contains("HTTP 500"))
  }

  test("createStagingTable and createTable call UC Delta Rest Catalog API endpoints") {
    var sawStagingCreate = false
    var sawTableCreate = false
    deltaTablesHandler = exchange => {
      assert(exchange.getRequestMethod === "POST")
      exchange.getRequestURI.getPath match {
        case "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/staging-tables" =>
          sawStagingCreate = true
          val body = objectMapper.readTree(readRequestBody(exchange))
          assert(body.get("name").asText === "tbl")
          sendJson(exchange, HttpStatus.SC_OK,
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
          sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson)
        case path =>
          fail(s"Unexpected UC Delta Rest Catalog API request path: $path")
      }
    }

    withClient { client =>
      val staging = client.createStagingTable("main", "default", "tbl")
      assert(staging.getTableId.toString === "22222222-2222-2222-2222-222222222222")
      assert(staging.getLocation === "s3://bucket/path/to/table")

      val created = client.createTable("main", "default", new CreateTableRequest().name("tbl"))
      assert(created.getMetadata.getLocation === "file:/tmp/uc/table")
    }
    assert(sawStagingCreate)
    assert(sawTableCreate)
  }

  test("createStagingTable and createTable wrap HTTP errors") {
    deltaTablesHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"boom"}""")

    withClient { client =>
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
    }
  }

  test("createStagingTable and createTable validate required parameters") {
    withClient { client =>
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
    }
  }

  // commit tests
  test("commit succeeds with valid parameters") {
    withClient { client =>
      client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
        Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
    }
  }

  test("commit succeeds with metadata") {
    withClient { client =>
      client.commit(
        testTableId,
        testTableUri,
        Optional.of(createCommit(1L)),
        Optional.of(java.lang.Long.valueOf(0L)),
        true,
        Optional.of(createMetadata()),
        Optional.empty(),
        Optional.empty())
    }
  }

  test("commit validates required parameters") {
    withClient { client =>
      intercept[NullPointerException] {
        client.commit(null, testTableUri, Optional.empty(), Optional.empty(),
          false, Optional.empty(), Optional.empty(), Optional.empty())
      }
      intercept[NullPointerException] {
        client.commit(testTableId, null, Optional.empty(), Optional.empty(),
          false, Optional.empty(), Optional.empty(), Optional.empty())
      }
    }
  }

  test("commit throws appropriate exceptions for HTTP errors") {
    def commitWith(status: Int): Unit = {
      commitsHandler = exchange => sendJson(exchange, status, s"""{"error":"$status"}""")
      withClient { client =>
        client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
          Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
      }
    }

    // 400 -> CommitFailedException (non-retryable, non-conflict)
    val e400 = intercept[CommitFailedException] { commitWith(400) }
    assert(!e400.getRetryable && !e400.getConflict)

    // 404 -> InvalidTargetTableException
    intercept[InvalidTargetTableException] { commitWith(404) }

    // 409 -> CommitFailedException (retryable, conflict)
    val e409 = intercept[CommitFailedException] { commitWith(409) }
    assert(e409.getRetryable && e409.getConflict)

    // Note: 429 (CommitLimitReachedException) cannot be tested here because the SDK's
    // RetryingHttpClient intercepts 429 before it reaches handleCommitException.
    // The 429 path is exercised via UCCommitCoordinatorClientSuite integration tests.

    // 500 (default branch) -> CommitFailedException (retryable, non-conflict)
    val e500 = intercept[CommitFailedException] { commitWith(500) }
    assert(e500.getRetryable && !e500.getConflict)
  }

  // getCommits tests
  test("getCommits returns commits correctly") {
    val responseJson =
      """{"commits":[{"version":1,"file_name":"1.json","file_size":100,""" +
      """"timestamp":1000,"file_modification_timestamp":1001}],"latest_table_version":1}"""
    commitsHandler = exchange => sendJson(exchange, HttpStatus.SC_OK, responseJson)
    withClient { client =>
      val response = client.getCommits(
        testTableId, testTableUri, Optional.empty(), Optional.empty())
      assert(response.getCommits.size() === 1)
      assert(response.getCommits.get(0).getVersion === 1L)
      assert(response.getLatestTableVersion === 1L)
    }
  }

  test("getCommits validates required parameters") {
    withClient { client =>
      intercept[NullPointerException] {
        client.getCommits(null, testTableUri, Optional.empty(), Optional.empty())
      }
      intercept[NullPointerException] {
        client.getCommits(testTableId, null, Optional.empty(), Optional.empty())
      }
    }
  }

  test("getCommits throws InvalidTargetTableException on 404") {
    commitsHandler = exchange => sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
    withClient { client =>
      intercept[InvalidTargetTableException] {
        client.getCommits(testTableId, testTableUri, Optional.empty(), Optional.empty())
      }
    }
  }

  // uniform tests
  test("commit with uniform.iceberg sends correct snake_case JSON per all.yaml") {
    var capturedBody: String = null
    commitsHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, "{}")
    }

    withClient { client =>
      client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
        Optional.empty(), false, Optional.empty(), Optional.empty(),
        Optional.of(createUniformMetadata()))
    }

    val json: JsonNode = objectMapper.readTree(capturedBody)
    assert(json.get("table_id").asText() === testTableId)

    val iceberg = json.get("uniform").get("iceberg")
    assert(iceberg.get("metadata_location").asText() === "s3://bucket/metadata/v1.json")
    assert(iceberg.get("converted_delta_version").asLong() === 42L)
    assert(iceberg.get("converted_delta_timestamp").asText() === "2025-01-04T03:13:11.423Z")

    assert(!json.has("protocol"), "protocol is not in the OpenAPI spec and must not be sent")
  }

  test("commit without uniform does not include uniform field in JSON") {
    var capturedBody: String = null
    commitsHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, "{}")
    }

    withClient { client =>
      client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
        Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
    }

    val json = objectMapper.readTree(capturedBody)
    assert(!json.has("uniform") || json.get("uniform").isNull)
  }

  test("commit with uniform but no iceberg metadata does not include uniform field") {
    var capturedBody: String = null
    commitsHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, "{}")
    }

    withClient { client =>
      client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
        Optional.empty(), false, Optional.empty(), Optional.empty(),
        Optional.of(new UniformMetadata(null)))
    }

    val json = objectMapper.readTree(capturedBody)
    assert(!json.has("uniform") || json.get("uniform").isNull)
  }
}
