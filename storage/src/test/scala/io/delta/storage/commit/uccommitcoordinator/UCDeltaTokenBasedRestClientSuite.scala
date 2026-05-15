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
import java.util.{Collections, Optional, Set => JSet}

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException, TableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import io.unitycatalog.client.auth.TokenProvider

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class UCDeltaTokenBasedRestClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testTableId = "550e8400-e29b-41d4-a716-446655440000"
  private val testMetastoreId = "test-metastore-123"
  private val testCatalog = "cat"
  private val testSchema = "sch"
  private val testTable = "tbl"
  private val testIdentifier = new TableIdentifier(testCatalog, testSchema, testTable)

  private var server: HttpServer = _
  private var serverUri: String = _
  private var deltaHandler: (HttpExchange, String) => Unit = _
  private val objectMapper = new ObjectMapper()

  private val metastoreJson = s"""{"metastore_id":"$testMetastoreId"}"""

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)

    server.createContext("/", exchange => {
      val body = readBody(exchange)
      val path = exchange.getRequestURI.getPath
      if (path.contains("/metastore_summary")) {
        sendJson(exchange, HttpStatus.SC_OK, metastoreJson)
      } else if (deltaHandler != null) {
        deltaHandler(exchange, body)
      } else {
        sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
      }
      exchange.close()
    })

    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)
  override def beforeEach(): Unit = { deltaHandler = null }

  // --------------- helpers ---------------

  private def loadTableJson(
      tableUuid: String = testTableId,
      format: String = "DELTA"): String =
    s"""{"metadata":{"table-uuid":"$tableUuid","data-source-format":"$format",""" +
    s""""properties":{"key1":"val1"},"partition-columns":["date"],"created-time":1000}}"""

  private def readBody(exchange: HttpExchange): String = {
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

  private def tokenProvider(): TokenProvider = new TokenProvider {
    override def accessToken(): String = "mock-token"
    override def initialize(configs: java.util.Map[String, String]): Unit = {}
    override def configs(): java.util.Map[String, String] = Collections.emptyMap()
  }

  private def withClient(fn: UCDeltaTokenBasedRestClient => Unit): Unit = {
    val client = new UCDeltaTokenBasedRestClient(
      serverUri, tokenProvider(), Collections.emptyMap())
    try fn(client) finally client.close()
  }

  private def createCommit(version: Long): Commit = {
    val fs = new FileStatus(1024L, false, 1, 4096L, 9999L,
      new Path(s"/path/_delta_log/_staged_commits/$version.uuid.json"))
    new Commit(version, fs, 5000L)
  }

  private def metadata(
      schema: String = """{"type":"struct"}""",
      desc: String = "desc",
      partitions: java.util.List[String] = Collections.emptyList(),
      config: java.util.Map[String, String] = Collections.emptyMap()): AbstractMetadata =
    new AbstractMetadata {
      override def getId: String = "id"
      override def getName: String = "name"
      override def getDescription: String = desc
      override def getProvider: String = "delta"
      override def getFormatOptions: java.util.Map[String, String] = Collections.emptyMap()
      override def getSchemaString: String = schema
      override def getPartitionColumns: java.util.List[String] = partitions
      override def getConfiguration: java.util.Map[String, String] = config
      override def getCreatedTime: java.lang.Long = 0L
      override def equals(obj: Any): Boolean = obj match {
        case o: AbstractMetadata =>
          getId == o.getId && getName == o.getName && getDescription == o.getDescription &&
            getProvider == o.getProvider && getSchemaString == o.getSchemaString &&
            getPartitionColumns == o.getPartitionColumns && getConfiguration == o.getConfiguration
        case _ => false
      }
      override def hashCode(): Int =
        java.util.Objects.hash(getId, getName, getDescription,
          getProvider, getSchemaString, getPartitionColumns,
          getConfiguration)
    }

  private def protocol(minReader: Int, minWriter: Int,
      readerFeatures: JSet[String] = Collections.emptySet(),
      writerFeatures: JSet[String] = Collections.emptySet()): AbstractProtocol =
    new AbstractProtocol {
      override def getMinReaderVersion: Int = minReader
      override def getMinWriterVersion: Int = minWriter
      override def getReaderFeatures: JSet[String] = readerFeatures
      override def getWriterFeatures: JSet[String] = writerFeatures
      override def equals(obj: Any): Boolean = obj match {
        case o: AbstractProtocol =>
          getMinReaderVersion == o.getMinReaderVersion &&
            getMinWriterVersion == o.getMinWriterVersion &&
            getReaderFeatures == o.getReaderFeatures && getWriterFeatures == o.getWriterFeatures
        case _ => false
      }
      override def hashCode(): Int =
        java.util.Objects.hash(getMinReaderVersion: Integer, getMinWriterVersion: Integer,
          getReaderFeatures, getWriterFeatures)
    }

  // --------------- constructor tests ---------------

  test("constructor validates required parameters") {
    intercept[NullPointerException] {
      new UCDeltaTokenBasedRestClient(null, tokenProvider(), Collections.emptyMap())
    }
    intercept[NullPointerException] {
      new UCDeltaTokenBasedRestClient(serverUri, null, Collections.emptyMap())
    }
    intercept[NullPointerException] {
      new UCDeltaTokenBasedRestClient(serverUri, tokenProvider(), null)
    }
  }

  // --------------- getMetastoreId ---------------

  test("getMetastoreId returns ID on success") {
    withClient(c => assert(c.getMetastoreId() === testMetastoreId))
  }

  // --------------- loadTable ---------------

  test("loadTable returns AbstractMetadata with correct fields") {
    withClient { c =>
      val m = c.loadTable(testCatalog, testSchema, testTable)
      assert(m.getName === testTable)
      assert(m.getId === testTableId)
      assert(m.getProvider === "DELTA")
      assert(m.getConfiguration.get("key1") === "val1")
      assert(m.getPartitionColumns.get(0) === "date")
      assert(m.getCreatedTime === 1000L)
    }
  }

  test("loadTable throws IOException on server error") {
    deltaHandler = (exchange, _) => sendJson(exchange, 500, """{"error":"fail"}""")
    withClient { c =>
      val e = intercept[java.io.IOException] { c.loadTable(testCatalog, testSchema, testTable) }
      assert(e.getMessage.contains("HTTP 500"))
    }
  }

  // --------------- commit via updateTable ---------------

  test("commit sends ASSERT_TABLE_UUID, ADD_COMMIT, and SET_LATEST_BACKFILLED_VERSION") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    withClient { c =>
      c.commit(testTableId, new URI("s3://bucket/table"), testIdentifier,
        Optional.of(createCommit(5L)), Optional.of(java.lang.Long.valueOf(3L)),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
    }

    val json = objectMapper.readTree(captured)
    val reqs = json.get("requirements")
    assert(reqs.size() === 1)
    assert(reqs.get(0).get("type").asText() === "assert-table-uuid")
    assert(reqs.get(0).get("uuid").asText() === testTableId)

    val updates = json.get("updates")
    val actions = (0 until updates.size()).map(i => updates.get(i).get("action").asText()).toSet
    assert(actions === Set("add-commit", "set-latest-backfilled-version"))

    val addCommit = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "add-commit").get
    assert(addCommit.get("commit").get("version").asLong() === 5L)
    assert(addCommit.get("commit").get("timestamp").asLong() === 5000L)
    assert(addCommit.get("commit").get("file-size").asLong() === 1024L)
  }

  test("commit sends metadata diff updates") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val oldMeta = metadata(desc = "old",
      config = new java.util.HashMap[String, String]() {
        put("a", "1"); put("b", "2")
      })
    val newMeta = metadata(desc = "new",
      config = new java.util.HashMap[String, String]() {
        put("a", "1"); put("c", "3")
      })

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.of(oldMeta), Optional.of(newMeta),
        Optional.empty(), Optional.empty(), Optional.empty())
    }

    val actions = {
      val updates = objectMapper.readTree(captured).get("updates")
      (0 until updates.size()).map(i => updates.get(i).get("action").asText()).toSet
    }
    assert(actions.contains("set-table-comment"))
    assert(actions.contains("set-properties"))
    assert(actions.contains("remove-properties"))
  }

  test("commit sends SET_PROTOCOL when protocol changes") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val oldProto = protocol(1, 2)
    val newProto = protocol(3, 7,
      java.util.Set.of("columnMapping"), java.util.Set.of("columnMapping", "v2Checkpoint"))

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.empty(), Optional.empty(),
        Optional.of(oldProto), Optional.of(newProto), Optional.empty())
    }

    val updates = objectMapper.readTree(captured).get("updates")
    val proto = (0 until updates.size()).map(updates.get)
      .find(_.get("action").asText() == "set-protocol").get.get("protocol")
    assert(proto.get("min-reader-version").asInt() === 3)
    assert(proto.get("min-writer-version").asInt() === 7)
  }

  test("commit skips metadata updates when old and new are equal") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val m1 = metadata(desc = "same")
    val m2 = metadata(desc = "same")
    assert(m1 ne m2, "must be different objects")

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.of(m1), Optional.of(m2),
        Optional.empty(), Optional.empty(), Optional.empty())
    }

    val updates = objectMapper.readTree(captured).get("updates")
    val actions = (0 until updates.size()).map(i => updates.get(i).get("action").asText()).toSet
    assert(!actions.contains("set-table-comment"),
      "should skip metadata updates for equal metadata")
  }

  test("commit skips protocol update when old and new are equal") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val p1 = protocol(3, 7, java.util.Set.of("f1"), java.util.Set.of("f2"))
    val p2 = protocol(3, 7, java.util.Set.of("f1"), java.util.Set.of("f2"))
    assert(p1 ne p2, "must be different objects")

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.empty(), Optional.empty(),
        Optional.of(p1), Optional.of(p2), Optional.empty())
    }

    val updates = objectMapper.readTree(captured).get("updates")
    val actions = (0 until updates.size()).map(i => updates.get(i).get("action").asText()).toSet
    assert(!actions.contains("set-protocol"), "should skip protocol update for equal protocols")
  }

  test("commit with uniform iceberg metadata (numeric timestamp)") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val uniform = new UniformMetadata(
      new IcebergMetadata("s3://bucket/v1.json", 42L, "1704337991423"))

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.of(uniform))
    }

    val addCommit = {
      val updates = objectMapper.readTree(captured).get("updates")
      (0 until updates.size()).map(updates.get)
        .find(_.get("action").asText() == "add-commit").get
    }
    val iceberg = addCommit.get("uniform").get("iceberg")
    assert(iceberg.get("metadata-location").asText() === "s3://bucket/v1.json")
    assert(iceberg.get("converted-delta-version").asLong() === 42L)
    assert(iceberg.get("converted-delta-timestamp").asLong() === 1704337991423L)
  }

  test("commit with uniform iceberg metadata (ISO-8601 timestamp)") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      if (exchange.getRequestMethod == "POST") {
        captured = body
      }
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val uniform = new UniformMetadata(
      new IcebergMetadata("s3://bucket/v1.json", 42L, "2025-01-04T03:13:11.423Z"))

    withClient { c =>
      c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
        Optional.of(createCommit(1L)), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.of(uniform))
    }

    val addCommit = {
      val updates = objectMapper.readTree(captured).get("updates")
      (0 until updates.size()).map(updates.get)
        .find(_.get("action").asText() == "add-commit").get
    }
    val iceberg = addCommit.get("uniform").get("iceberg")
    assert(iceberg.get("converted-delta-timestamp").asLong() === 1735960391423L)
  }

  // --------------- error handling ---------------

  test("commit throws CommitFailedException on 409") {
    deltaHandler = (exchange, _) => {
      if (exchange.getRequestMethod == "POST") {
        sendJson(exchange, HttpStatus.SC_CONFLICT,
          """{"error":"conflict"}""")
      } else {
        sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
      }
    }
    withClient { c =>
      val e = intercept[CommitFailedException] {
        c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
          Optional.of(createCommit(1L)), Optional.empty(), Optional.empty(),
          Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
      }
      assert(e.getRetryable && e.getConflict)
    }
  }

  test("commit throws InvalidTargetTableException on 404") {
    deltaHandler = (exchange, _) => {
      if (exchange.getRequestMethod == "POST") {
        sendJson(exchange, HttpStatus.SC_NOT_FOUND,
          """{"error":"not found"}""")
      } else {
        sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
      }
    }
    withClient { c =>
      intercept[InvalidTargetTableException] {
        c.commit(testTableId, new URI("s3://b/t"), testIdentifier,
          Optional.of(createCommit(1L)), Optional.empty(), Optional.empty(),
          Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
      }
    }
  }

  // --------------- createStagingTable ---------------

  test("createStagingTable sends request and converts response fields") {
    var captured: String = null
    val stagingJson =
      s"""{
         |  "table-id":"$testTableId",
         |  "table-type":"MANAGED",
         |  "location":"s3://bucket/staging",
         |  "required-protocol":{
         |    "min-reader-version":3,"min-writer-version":7,
         |    "reader-features":["columnMapping"],
         |    "writer-features":["columnMapping","v2Checkpoint"]
         |  },
         |  "suggested-protocol":{"reader-features":["deletionVectors"]},
         |  "required-properties":{"delta.enableChangeDataFeed":"true"},
         |  "suggested-properties":{"delta.autoOptimize.optimizeWrite":"true"}
         |}""".stripMargin

    deltaHandler = (exchange, body) => {
      captured = body
      sendJson(exchange, HttpStatus.SC_OK, stagingJson)
    }

    withClient { c =>
      val info = c.createStagingTable(testCatalog, testSchema, testTable)

      // verify request body
      val req = objectMapper.readTree(captured)
      assert(req.get("name").asText() === testTable)

      // verify all response fields converted
      assert(info.getTableId === testTableId)
      assert(info.getTableType === UCDeltaModels.TableType.MANAGED)
      assert(info.getLocation === "s3://bucket/staging")

      val rp = info.getRequiredProtocol
      assert(rp.getMinReaderVersion === 3)
      assert(rp.getMinWriterVersion === 7)
      assert(rp.getReaderFeatures.contains("columnMapping"))
      assert(rp.getWriterFeatures.size() === 2)

      val sp = info.getSuggestedProtocol
      assert(sp.getReaderFeatures.contains("deletionVectors"))
      assert(sp.getWriterFeatures.isEmpty)

      assert(info.getRequiredProperties.get("delta.enableChangeDataFeed") === "true")
      assert(info.getSuggestedProperties.get("delta.autoOptimize.optimizeWrite") === "true")
    }
  }

  test("createStagingTable throws IOException on server error") {
    deltaHandler = (exchange, _) =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"fail"}""")
    withClient { c =>
      val e = intercept[java.io.IOException] {
        c.createStagingTable(testCatalog, testSchema, testTable)
      }
      assert(e.getMessage.contains("HTTP 500"))
    }
  }

  // --------------- getCommits ---------------

  test("getCommits throws UnsupportedOperationException") {
    withClient { c =>
      intercept[UnsupportedOperationException] {
        c.getCommits(testTableId, new URI("s3://b/t"), Optional.empty(), Optional.empty())
      }
    }
  }

  // --------------- finalizeCreate ---------------

  test("finalizeCreate sends createTable request with columns and properties") {
    var captured: String = null
    deltaHandler = (exchange, body) => {
      captured = body
      sendJson(exchange, HttpStatus.SC_OK, loadTableJson())
    }

    val columns = java.util.List.of(
      new UCClient.ColumnDef("id", "LONG", "long", """{"type":"long"}""", false, 0),
      new UCClient.ColumnDef("name", "STRING", "string", """{"type":"string"}""", true, 1))
    val props = new java.util.HashMap[String, String]()
    props.put("delta.minReaderVersion", "1")

    withClient { c =>
      c.finalizeCreate("my_table", testCatalog, testSchema, "s3://bucket/tbl", columns, props)
    }

    val json = objectMapper.readTree(captured)
    assert(json.get("name").asText() === "my_table")
    assert(json.get("location").asText() === "s3://bucket/tbl")
    assert(json.get("properties").get("delta.minReaderVersion").asText() === "1")

    val fields = json.get("columns").get("fields")
    assert(fields.size() === 2)
    assert(fields.get(0).get("name").asText() === "id")
    assert(fields.get(0).get("nullable").asBoolean() === false)
    assert(fields.get(1).get("name").asText() === "name")
    assert(fields.get(1).get("nullable").asBoolean() === true)
  }

  test("finalizeCreate throws CommitFailedException on server error") {
    deltaHandler = (exchange, _) =>
      sendJson(exchange, HttpStatus.SC_INTERNAL_SERVER_ERROR, """{"error":"fail"}""")

    withClient { c =>
      val e = intercept[CommitFailedException] {
        c.finalizeCreate("t", testCatalog, testSchema, "s3://b/t",
          Collections.emptyList(), Collections.emptyMap())
      }
      assert(e.getRetryable)
    }
  }

  test("finalizeCreate validates required parameters") {
    withClient { c =>
      intercept[NullPointerException] {
        c.finalizeCreate(null, "c", "s", "loc", Collections.emptyList(), Collections.emptyMap())
      }
      intercept[NullPointerException] {
        c.finalizeCreate("t", null, "s", "loc", Collections.emptyList(), Collections.emptyMap())
      }
    }
  }

  // --------------- close / ensureOpen ---------------

  test("operations after close throw IllegalStateException") {
    val client = new UCDeltaTokenBasedRestClient(
      serverUri, tokenProvider(), Collections.emptyMap())
    client.close()
    intercept[IllegalStateException] { client.getMetastoreId() }
    intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
  }
}
