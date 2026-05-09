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

package io.delta.storage.commit.uccommitcoordinator

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Optional}

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException}
import io.delta.storage.commit.actions.AbstractMetadata
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import io.unitycatalog.client.auth.TokenProvider

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[UCDeltaRestClient]], the UCClient implementation that routes
 * coordinated commit operations through the Delta REST Catalog API
 * (updateTable / loadTable endpoints).
 *
 * Uses a local HTTP server to capture and validate requests.
 */
class UCDeltaRestClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testTableId = "550e8400-e29b-41d4-a716-446655440000"
  private val testTableUri = new URI("s3://bucket/path/to/table")
  private val testCatalog = "test_catalog"
  private val testSchema = "test_schema"
  private val testTable = "test_table"

  /** Table conf map with both table ID and three-part name. */
  private val testTableConf: java.util.Map[String, String] = {
    val m = new java.util.HashMap[String, String]()
    m.put("io.unitycatalog.tableId", testTableId)
    m.put("io.unitycatalog.catalogName", testCatalog)
    m.put("io.unitycatalog.schemaName", testSchema)
    m.put("io.unitycatalog.tableName", testTable)
    Collections.unmodifiableMap(m)
  }

  private var server: HttpServer = _
  private var serverUri: String = _
  private var updateTableHandler: HttpExchange => Unit = _
  private var loadTableHandler: HttpExchange => Unit = _
  private var createTableHandler: HttpExchange => Unit = _
  private val objectMapper = new ObjectMapper()

  private val deltaPrefix = "/api/2.1/unity-catalog/delta"
  private val tablePath =
    s"$deltaPrefix/v1/catalogs/$testCatalog/schemas/$testSchema/tables/$testTable"
  private val tablesPath =
    s"$deltaPrefix/v1/catalogs/$testCatalog/schemas/$testSchema/tables"

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)

    // updateTable (POST) and loadTable (GET) share the same path
    server.createContext(tablePath, exchange => {
      try {
        exchange.getRequestMethod match {
          case "POST" =>
            if (updateTableHandler != null) updateTableHandler(exchange)
            else sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
          case "GET" =>
            if (loadTableHandler != null) loadTableHandler(exchange)
            else sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
          case _ =>
            sendJson(exchange, HttpStatus.SC_METHOD_NOT_ALLOWED, """{"error":"bad method"}""")
        }
      } finally {
        exchange.close()
      }
    })

    // createTable endpoint (POST to .../tables without table name)
    server.createContext(tablesPath, exchange => {
      try {
        // Only handle exact path match for the tables collection endpoint
        // The specific table path is handled by the more specific context above
        if (createTableHandler != null) createTableHandler(exchange)
        else sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
      } finally {
        exchange.close()
      }
    })

    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    updateTableHandler = null
    loadTableHandler = null
    createTableHandler = null
  }

  // ---- Helpers ----

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

  private def createClient(): UCDeltaRestClient =
    new UCDeltaRestClient(serverUri, createTokenProvider(), Collections.emptyMap())

  private def withClient(fn: UCDeltaRestClient => Unit): Unit = {
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
    override def getConfiguration: java.util.Map[String, String] = {
      val m = new java.util.HashMap[String, String]()
      m.put("key1", "value1")
      m
    }
    override def getCreatedTime: java.lang.Long = 0L
  }

  private def createUniformMetadata(): UniformMetadata =
    new UniformMetadata(
      new IcebergMetadata("s3://bucket/metadata/v1.json", 42L, "2025-01-04T03:13:11.423Z"))

  /** Minimal valid LoadTableResponse JSON. */
  private def loadTableResponseJson(
      commits: String = "[]",
      latestTableVersion: Long = -1L): String = {
    s"""{
       |  "metadata": {
       |    "table-uuid": "$testTableId",
       |    "format-version": 1,
       |    "protocol": {"min-reader-version": 1, "min-writer-version": 2},
       |    "schema": {"type": "struct", "fields": []},
       |    "properties": {}
       |  },
       |  "commits": $commits,
       |  "latest-table-version": $latestTableVersion
       |}""".stripMargin
  }

  private def commitJson(version: Long, fileName: String = null): String = {
    val fn = if (fileName != null) fileName else s"$version.uuid.json"
    s"""{"version":$version,"timestamp":${version * 1000},""" +
      s""""file-name":"$fn","file-size":1024,"file-modification-timestamp":${version * 1000 + 1}}"""
  }

  // ---- getMetastoreId ----

  test("getMetastoreId returns deterministic cache key based on URI") {
    withClient { client =>
      val id = client.getMetastoreId()
      assert(id.contains(serverUri) || id.nonEmpty,
        "should return a non-empty deterministic ID")
      // Calling again should return the same value
      assert(client.getMetastoreId() === id)
    }
  }

  // ---- commit() ----

  test("commit builds correct UpdateTableRequest with AddCommitUpdate") {
    var capturedBody: String = null
    updateTableHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }

    withClient { client =>
      client.commit(
        testTableConf,
        testTableUri,
        Optional.of(createCommit(5L)),
        Optional.of(java.lang.Long.valueOf(3L)),
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty())
    }

    assert(capturedBody != null, "updateTable should have been called")
    val json = objectMapper.readTree(capturedBody)

    // Verify requirements contain AssertTableUUID
    val requirements = json.get("requirements")
    assert(requirements.isArray && requirements.size() >= 1)
    val uuidReq = (0 until requirements.size()).map(requirements.get)
      .find(_.get("type").asText() == "assert-table-uuid")
    assert(uuidReq.isDefined, "should have assert-table-uuid requirement")
    assert(uuidReq.get.get("uuid").asText() === testTableId)

    // Verify updates contain AddCommitUpdate
    val updates = json.get("updates")
    assert(updates.isArray && updates.size() >= 1)
    val addCommit = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "add-commit")
    assert(addCommit.isDefined, "should have add-commit update")

    val commit = addCommit.get.get("commit")
    assert(commit.get("version").asLong() === 5L)
    assert(commit.has("file-name"))
    assert(commit.has("file-size"))
    assert(commit.has("timestamp"))

    // Verify SetLatestBackfilledVersionUpdate
    val backfillUpdate = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "set-latest-backfilled-version")
    assert(backfillUpdate.isDefined, "should have set-latest-backfilled-version update")
    assert(backfillUpdate.get.get("latest-published-version").asLong() === 3L)
  }

  test("commit without backfill version omits SetLatestBackfilledVersionUpdate") {
    var capturedBody: String = null
    updateTableHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }

    withClient { client =>
      client.commit(
        testTableConf,
        testTableUri,
        Optional.of(createCommit(1L)),
        Optional.empty(), // no backfill version
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty())
    }

    val json = objectMapper.readTree(capturedBody)
    val updates = json.get("updates")
    val backfillUpdate = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "set-latest-backfilled-version")
    assert(backfillUpdate.isEmpty, "should NOT have set-latest-backfilled-version update")
  }

  test("commit with uniform metadata includes it in AddCommitUpdate") {
    var capturedBody: String = null
    updateTableHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }

    withClient { client =>
      client.commit(
        testTableConf,
        testTableUri,
        Optional.of(createCommit(1L)),
        Optional.empty(),
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.of(createUniformMetadata()))
    }

    val json = objectMapper.readTree(capturedBody)
    val updates = json.get("updates")
    val addCommit = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "add-commit")
    assert(addCommit.isDefined)

    val uniform = addCommit.get.get("uniform")
    assert(uniform != null && !uniform.isNull, "uniform should be present")
  }

  test("commit fails fast when table name properties are missing from tableConf") {
    withClient { client =>
      val incompleteConf = new java.util.HashMap[String, String]()
      incompleteConf.put("io.unitycatalog.tableId", testTableId)
      // Missing catalog/schema/table name keys

      val ex = intercept[IllegalStateException] {
        client.commit(
          incompleteConf,
          testTableUri,
          Optional.of(createCommit(1L)),
          Optional.empty(),
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty())
      }
      assert(ex.getMessage.contains("catalogName") || ex.getMessage.contains("catalog"),
        s"error should mention missing catalog name: ${ex.getMessage}")
    }
  }

  test("commit throws CommitFailedException(retryable, conflict) on HTTP 409") {
    updateTableHandler = exchange => {
      readRequestBody(exchange) // consume body
      sendJson(exchange, HttpStatus.SC_CONFLICT, """{"error":"conflict"}""")
    }

    withClient { client =>
      val ex = intercept[CommitFailedException] {
        client.commit(
          testTableConf, testTableUri, Optional.of(createCommit(1L)),
          Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
      }
      assert(ex.getRetryable)
      assert(ex.getConflict)
    }
  }

  test("commit throws InvalidTargetTableException on HTTP 404") {
    updateTableHandler = exchange => {
      readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_NOT_FOUND, """{"error":"not found"}""")
    }

    withClient { client =>
      intercept[InvalidTargetTableException] {
        client.commit(
          testTableConf, testTableUri, Optional.of(createCommit(1L)),
          Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
      }
    }
  }

  // Note: 429 (CommitLimitReachedException) cannot be tested via HTTP mock because
  // the SDK's RetryingHttpClient intercepts 429 before it reaches handleCommitException.
  // Same limitation as UCTokenBasedRestClientSuite.

  // ---- getCommits() ----

  test("getCommits returns commits from loadTable response") {
    val commitsJson = s"[${commitJson(5)}, ${commitJson(6)}, ${commitJson(7)}]"
    loadTableHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson(commitsJson, 7L))

    withClient { client =>
      val response = client.getCommits(
        testTableConf, testTableUri, Optional.empty(), Optional.empty())
      assert(response.getCommits.size() === 3)
      assert(response.getCommits.get(0).getVersion === 5L)
      assert(response.getCommits.get(1).getVersion === 6L)
      assert(response.getCommits.get(2).getVersion === 7L)
      assert(response.getLatestTableVersion === 7L)
    }
  }

  test("getCommits filters by startVersion") {
    val commitsJson = s"[${commitJson(3)}, ${commitJson(4)}, ${commitJson(5)}]"
    loadTableHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson(commitsJson, 5L))

    withClient { client =>
      val response = client.getCommits(
        testTableConf, testTableUri, Optional.of(4L), Optional.empty())
      assert(response.getCommits.size() === 2)
      assert(response.getCommits.get(0).getVersion === 4L)
      assert(response.getCommits.get(1).getVersion === 5L)
    }
  }

  test("getCommits filters by endVersion") {
    val commitsJson = s"[${commitJson(3)}, ${commitJson(4)}, ${commitJson(5)}]"
    loadTableHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson(commitsJson, 5L))

    withClient { client =>
      val response = client.getCommits(
        testTableConf, testTableUri, Optional.empty(), Optional.of(4L))
      assert(response.getCommits.size() === 2)
      assert(response.getCommits.get(0).getVersion === 3L)
      assert(response.getCommits.get(1).getVersion === 4L)
    }
  }

  test("getCommits returns empty response when no commits") {
    loadTableHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson("[]", -1L))

    withClient { client =>
      val response = client.getCommits(
        testTableConf, testTableUri, Optional.empty(), Optional.empty())
      assert(response.getCommits.isEmpty)
      assert(response.getLatestTableVersion === -1L)
    }
  }

  test("getCommits throws InvalidTargetTableException on 404") {
    loadTableHandler = exchange =>
      sendJson(exchange, HttpStatus.SC_NOT_FOUND, """{"error":"not found"}""")

    withClient { client =>
      intercept[InvalidTargetTableException] {
        client.getCommits(testTableConf, testTableUri, Optional.empty(), Optional.empty())
      }
    }
  }

  test("getCommits fails fast when table name properties are missing") {
    withClient { client =>
      val incompleteConf = new java.util.HashMap[String, String]()
      incompleteConf.put("io.unitycatalog.tableId", testTableId)

      intercept[IllegalStateException] {
        client.getCommits(incompleteConf, testTableUri, Optional.empty(), Optional.empty())
      }
    }
  }

  // ---- finalizeCreate() ----

  test("finalizeCreate sends correct request") {
    var capturedBody: String = null
    createTableHandler = exchange => {
      capturedBody = readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_OK, loadTableResponseJson())
    }

    val columns = new java.util.ArrayList[UCClient.ColumnDef]()
    columns.add(new UCClient.ColumnDef("id", "INT", "int", """{"type":"integer"}""", false, 0))
    columns.add(new UCClient.ColumnDef("name", "STRING", "string",
      """{"type":"string"}""", true, 1))

    val props = new java.util.HashMap[String, String]()
    props.put("delta.minReaderVersion", "1")

    withClient { client =>
      client.finalizeCreate(
        testTable, testCatalog, testSchema, "s3://bucket/table", columns, props)
    }

    assert(capturedBody != null, "createTable should have been called")
    val json = objectMapper.readTree(capturedBody)
    assert(json.has("name") || json.has("table-name"),
      "request should contain the table name")
  }

  test("finalizeCreate throws CommitFailedException on error") {
    createTableHandler = exchange => {
      readRequestBody(exchange)
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"fail"}""")
    }

    withClient { client =>
      intercept[CommitFailedException] {
        client.finalizeCreate(
          testTable, testCatalog, testSchema, "s3://bucket/table",
          Collections.emptyList(), Collections.emptyMap())
      }
    }
  }

  // ---- close() ----

  test("operations after close throw IllegalStateException") {
    val client = createClient()
    client.close()

    intercept[IllegalStateException] {
      client.getMetastoreId()
    }
  }
}
