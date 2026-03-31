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
import io.unitycatalog.client.auth.TokenProvider

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
  private var tablesHandler: HttpExchange => Unit = _
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
      if (tablesHandler != null) tablesHandler(exchange)
      else sendJson(exchange, HttpStatus.SC_OK, """{"table_id":"new-table-id"}""")
      exchange.close()
    })
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    metastoreHandler = null
    commitsHandler = null
    tablesHandler = null
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

  // finalizeCreate tests
  test("finalizeCreate sends correct CreateTable request with complex types") {
    // Capture the HTTP request body sent to UC's createTable endpoint
    var capturedBody: String = null
    tablesHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, """{"table_id":"new-table-id"}""")
    }

    // Prepare columns with primitive, complex, and parameterized types
    val columns = new java.util.ArrayList[UCClient.ColumnDef]()
    columns.add(new UCClient.ColumnDef("id", "INT", "int", "\"integer\"", false, 0))
    columns.add(new UCClient.ColumnDef("tags", "ARRAY", "array<string>",
      """{"type":"array","elementType":"string","containsNull":true}""", true, 1))
    columns.add(new UCClient.ColumnDef("price", "DECIMAL", "decimal(10,2)",
      """{"type":"decimal","precision":10,"scale":2}""", true, 2))

    val props = new java.util.HashMap[String, String]()
    props.put("delta.minReaderVersion", "1")

    // Send the finalizeCreate request
    withClient { client =>
      client.finalizeCreate("my_table", "my_catalog", "my_schema",
        "s3://bucket/path", columns, props)
    }

    // Verify the serialized CreateTable JSON matches UC's expected schema
    val json = objectMapper.readTree(capturedBody)
    assert(json.get("name").asText() === "my_table")
    assert(json.get("catalog_name").asText() === "my_catalog")
    assert(json.get("schema_name").asText() === "my_schema")
    assert(json.get("table_type").asText() === "MANAGED")
    assert(json.get("data_source_format").asText() === "DELTA")
    assert(json.get("storage_location").asText() === "s3://bucket/path")

    // Verify properties are serialized
    val propsNode = json.get("properties")
    assert(propsNode != null && !propsNode.isNull)
    assert(propsNode.get("delta.minReaderVersion").asText() === "1")

    val cols = json.get("columns")
    assert(cols.size() === 3)

    // Verify full column metadata (name, type_name, type_text, type_json, nullable, position)
    assert(cols.get(0).get("name").asText() === "id")
    assert(cols.get(0).get("type_name").asText() === "INT")
    assert(cols.get(0).get("type_text").asText() === "int")
    assert(cols.get(0).get("type_json").asText() === "\"integer\"")
    assert(cols.get(0).get("nullable").asBoolean() === false)
    assert(cols.get(0).get("position").asInt() === 0)

    assert(cols.get(1).get("name").asText() === "tags")
    assert(cols.get(1).get("type_name").asText() === "ARRAY")
    assert(cols.get(1).get("type_text").asText() === "array<string>")
    assert(cols.get(1).get("type_json").asText() ===
      """{"type":"array","elementType":"string","containsNull":true}""")
    assert(cols.get(1).get("nullable").asBoolean() === true)
    assert(cols.get(1).get("position").asInt() === 1)

    assert(cols.get(2).get("name").asText() === "price")
    assert(cols.get(2).get("type_name").asText() === "DECIMAL")
    assert(cols.get(2).get("type_text").asText() === "decimal(10,2)")
    assert(cols.get(2).get("type_json").asText() ===
      """{"type":"decimal","precision":10,"scale":2}""")
    assert(cols.get(2).get("nullable").asBoolean() === true)
    assert(cols.get(2).get("position").asInt() === 2)
  }

  test("finalizeCreate throws IllegalArgumentException for unsupported column type") {
    // Prepare a column with a type name that doesn't map to any ColumnTypeName enum value
    val columns = new java.util.ArrayList[UCClient.ColumnDef]()
    columns.add(new UCClient.ColumnDef("col", "FAKETYPE", "faketype", "\"faketype\"", true, 0))

    // Expect IllegalArgumentException before any HTTP call is made
    withClient { client =>
      val ex = intercept[IllegalArgumentException] {
        client.finalizeCreate("tbl", "cat", "sch", "s3://bucket/tbl",
          columns, Collections.emptyMap())
      }
      assert(ex.getMessage.contains("Unsupported column type"))
      assert(ex.getMessage.contains("FAKETYPE"))
    }
  }

  test("finalizeCreate throws IOException on server error") {
    // Simulate UC returning HTTP 500
    tablesHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"boom"}""")

    val columns = new java.util.ArrayList[UCClient.ColumnDef]()
    columns.add(new UCClient.ColumnDef("id", "INT", "int", "\"integer\"", false, 0))

    // Expect IOException wrapping the server error with the FQN in the message
    withClient { client =>
      val ex = intercept[java.io.IOException] {
        client.finalizeCreate("tbl", "cat", "sch", "s3://bucket/tbl",
          columns, Collections.emptyMap())
      }
      assert(ex.getMessage.contains("Failed to finalize table creation"))
      assert(ex.getMessage.contains("cat.sch.tbl"))
    }
  }

  test("finalizeCreate validates required parameters") {
    val columns = new java.util.ArrayList[UCClient.ColumnDef]()
    columns.add(new UCClient.ColumnDef("id", "INT", "int", "\"integer\"", false, 0))

    // Each of the 6 required parameters must reject null
    withClient { client =>
      intercept[NullPointerException] {
        client.finalizeCreate(null, "cat", "sch", "s3://b", columns, Collections.emptyMap())
      }
      intercept[NullPointerException] {
        client.finalizeCreate("tbl", null, "sch", "s3://b", columns, Collections.emptyMap())
      }
      intercept[NullPointerException] {
        client.finalizeCreate("tbl", "cat", null, "s3://b", columns, Collections.emptyMap())
      }
      intercept[NullPointerException] {
        client.finalizeCreate("tbl", "cat", "sch", null, columns, Collections.emptyMap())
      }
      intercept[NullPointerException] {
        client.finalizeCreate("tbl", "cat", "sch", "s3://b", null, Collections.emptyMap())
      }
      intercept[NullPointerException] {
        client.finalizeCreate("tbl", "cat", "sch", "s3://b", columns, null)
      }
    }
  }
}
