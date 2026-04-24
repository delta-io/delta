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

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Optional}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException}
import io.delta.storage.commit.actions.AbstractMetadata
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import io.unitycatalog.client.ApiClientBuilder
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.delta.model

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
  private var legacyTablesHandler: HttpExchange => Unit = _
  private var drcTablesHandler: HttpExchange => Unit = _
  private var drcCredentialsHandler: HttpExchange => Unit = _
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
    server.createContext("/api/2.1/unity-catalog/tables", exchange => {
      if (legacyTablesHandler != null) legacyTablesHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      exchange.close()
    })
    server.createContext("/api/2.1/unity-catalog/delta/v1/catalogs", exchange => {
      if (exchange.getRequestURI.getPath.endsWith("/credentials")) {
        if (drcCredentialsHandler != null) drcCredentialsHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      } else {
        if (drcTablesHandler != null) drcTablesHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
      }
      exchange.close()
    })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    metastoreHandler = null
    commitsHandler = null
    legacyTablesHandler = null
    drcTablesHandler = null
    drcCredentialsHandler = null
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

  private def createClient(): UCTokenBasedRestClient =
    new UCTokenBasedRestClient(serverUri, createTokenProvider(), Collections.emptyMap())

  private def createLegacyClient(): UCTokenBasedRestClient = {
    val apiClient = ApiClientBuilder.create()
      .uri(serverUri)
      .tokenProvider(createTokenProvider())
      .build()
    new UCTokenBasedRestClient(apiClient, false)
  }

  private def withClient(fn: UCTokenBasedRestClient => Unit): Unit = {
    val client = createClient()
    try fn(client) finally client.close()
  }

  private def withLegacyClient(fn: UCTokenBasedRestClient => Unit): Unit = {
    val client = createLegacyClient()
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

  test("loadTable falls back to legacy UC API and converts metadata") {
    legacyTablesHandler = exchange => {
      assert(exchange.getRequestMethod === "GET")
      sendJson(
        exchange,
        HttpStatus.SC_OK,
        """{
          |  "name": "tbl",
          |  "catalog_name": "main",
          |  "schema_name": "default",
          |  "table_id": "11111111-1111-1111-1111-111111111111",
          |  "table_type": "MANAGED",
          |  "data_source_format": "DELTA",
          |  "storage_location": "s3://bucket/path/to/table",
          |  "created_at": 10,
          |  "updated_at": 11,
          |  "properties": {"delta.appendOnly":"true"},
          |  "columns": [
          |    {
          |      "name":"payload",
          |      "nullable":false,
          |      "position":0,
          |      "type_json":"{\"name\":\"payload\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{\"comment\":\"nested tags\"}}]},\"nullable\":false,\"metadata\":{}}"
          |    },
          |    {"name":"value","type_text":"string","type_json":"{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}","nullable":true,"position":1},
          |    {"name":"region","type_text":"string","type_json":"{\"name\":\"region\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}","nullable":false,"position":2,"partition_index":1},
          |    {"name":"date","type_text":"date","type_json":"{\"name\":\"date\",\"type\":\"date\",\"nullable\":false,\"metadata\":{}}","nullable":false,"position":3,"partition_index":0}
          |  ]
          |}""".stripMargin)
    }

    withLegacyClient { client =>
      val response = client.loadTable("main", "default", "tbl")
      val metadata = response.getMetadata
      val payloadField = metadata.getColumns.getFields.get(0)
      val payloadType = payloadField.getType.asInstanceOf[model.StructType]
      val nestedField = payloadType.getFields.get(0)
      val nestedType = nestedField.getType.asInstanceOf[model.ArrayType]

      assert(metadata.getLocation === "s3://bucket/path/to/table")
      assert(metadata.getPartitionColumns === java.util.Arrays.asList("date", "region"))
      assert(metadata.getProperties.get("delta.appendOnly") === "true")
      assert(metadata.getColumns.getFields.size() === 4)
      assert(payloadField.getName === "payload")
      assert(!payloadField.getNullable)
      assert(metadata.getColumns.getFields.get(2).getName === "region")
      assert(!metadata.getColumns.getFields.get(2).getNullable)
      assert(nestedField.getName === "tags")
      assert(nestedField.getMetadata.get("comment") === "nested tags")
      assert(nestedType.getElementType.isInstanceOf[model.PrimitiveType])
      assert(nestedType.getContainsNull)
      assert(
        nestedType.getElementType.asInstanceOf[model.PrimitiveType].getType === "string")
    }
  }

  test("loadTable uses the DRC endpoint when available") {
    legacyTablesHandler = _ => fail("loadTable should not call the legacy UC tables API")
    drcTablesHandler = exchange => {
      assert(exchange.getRequestMethod === "GET")
      assert(
        exchange.getRequestURI.getPath ===
          "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl")
      sendJson(
        exchange,
        HttpStatus.SC_OK,
        """{
          |  "metadata": {
          |    "etag": "etag-1",
          |    "data-source-format": "DELTA",
          |    "table-type": "MANAGED",
          |    "table-uuid": "11111111-1111-1111-1111-111111111111",
          |    "location": "s3://bucket/path/to/table",
          |    "created-time": 10,
          |    "updated-time": 11,
          |    "securable-type": "TABLE",
          |    "columns": {
          |      "type": "struct",
          |      "fields": [
          |        {"name": "id", "type": "long", "nullable": false, "metadata": {}}
          |      ]
          |    },
          |    "partition-columns": [],
          |    "properties": {"delta.appendOnly": "true"}
          |  },
          |  "commits": [
          |    {"version": 7, "file-name": "7.json", "file-size": 100, "timestamp": 1000,
          |      "file-modification-timestamp": 1001}
          |  ]
          |}""".stripMargin)
    }

    withClient { client =>
      val response = client.loadTable("main", "default", "tbl")
      assert(response.getMetadata.getEtag === "etag-1")
      assert(response.getMetadata.getLocation === "s3://bucket/path/to/table")
      assert(response.getMetadata.getDataSourceFormat === model.DataSourceFormat.DELTA)
      assert(response.getMetadata.getColumns.getFields.size() === 1)
      assert(response.getCommits.size() === 1)
      assert(response.getCommits.get(0).getVersion === 7L)
    }
  }

  test("loadTable includes table identity in DRC error messages") {
    drcTablesHandler = exchange => {
      sendJson(exchange, HttpStatus.SC_FORBIDDEN, """{"error":"denied"}""")
    }

    withClient { client =>
      val e = intercept[java.io.IOException] {
        client.loadTable("main", "default", "tbl")
      }
      assert(e.getMessage.contains("main.default.tbl"))
      assert(e.getMessage.contains("HTTP 403"))
    }
  }

  test("loadTable validates required parameters") {
    withClient { client =>
      intercept[NullPointerException] {
        client.loadTable(null, "default", "tbl")
      }
      intercept[NullPointerException] {
        client.loadTable("main", null, "tbl")
      }
      intercept[NullPointerException] {
        client.loadTable("main", "default", null)
      }
    }
  }

  test("getTableCredentials returns DRC credentials") {
    drcCredentialsHandler = exchange => {
      assert(exchange.getRequestMethod === "GET")
      assert(
        exchange.getRequestURI.getPath ===
          "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/tbl/credentials")
      assert(exchange.getRequestURI.getQuery === "operation=READ")
      sendJson(
        exchange,
        HttpStatus.SC_OK,
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

    withClient { client =>
      val response =
        client.getTableCredentials(model.CredentialOperation.READ, "main", "default", "tbl")
      assert(response.getStorageCredentials.size() === 1)
      assert(response.getStorageCredentials.get(0).getPrefix === "s3://bucket/path/to/table")
      assert(response.getStorageCredentials.get(0).getConfig.get("s3.access-key-id") === "ak")
    }
  }

  test("getTableCredentials validates required parameters") {
    withClient { client =>
      intercept[NullPointerException] {
        client.getTableCredentials(null, "main", "default", "tbl")
      }
      intercept[NullPointerException] {
        client.getTableCredentials(model.CredentialOperation.READ, null, "default", "tbl")
      }
      intercept[NullPointerException] {
        client.getTableCredentials(model.CredentialOperation.READ, "main", null, "tbl")
      }
      intercept[NullPointerException] {
        client.getTableCredentials(model.CredentialOperation.READ, "main", "default", null)
      }
    }
  }

  test("getTableCredentials is unsupported without the DRC API") {
    withLegacyClient { client =>
      intercept[UnsupportedOperationException] {
        client.getTableCredentials(model.CredentialOperation.READ, "main", "default", "tbl")
      }
    }
  }

  test("getTableCredentials includes table identity in DRC error messages") {
    drcCredentialsHandler = exchange => {
      sendJson(exchange, HttpStatus.SC_FORBIDDEN, """{"error":"denied"}""")
    }

    withClient { client =>
      val e = intercept[java.io.IOException] {
        client.getTableCredentials(model.CredentialOperation.READ, "main", "default", "tbl")
      }
      assert(e.getMessage.contains("main.default.tbl"))
      assert(e.getMessage.contains("HTTP 403"))
      assert(e.getMessage.contains("denied"))
    }
  }

  test("loadTable and getTableCredentials fail after close") {
    val client = createClient()
    client.close()

    intercept[IllegalStateException] {
      client.loadTable("main", "default", "tbl")
    }
    intercept[IllegalStateException] {
      client.getTableCredentials(model.CredentialOperation.READ, "main", "default", "tbl")
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
