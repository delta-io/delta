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
import java.util.{Collections, Optional, Set => JSet, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException, TableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.exceptions.NoSuchTableException
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}
import io.unitycatalog.client.auth.TokenProvider

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.http.HttpStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class UCDeltaTokenBasedRestClientSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val testTableId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
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
      tableUuid: UUID = testTableId,
      format: String = "DELTA",
      location: String = "s3://bucket/table",
      tableType: String = "MANAGED",
      commitsJson: String = "[]",
      latestTableVersion: Long = -1L): String =
    s"""{"metadata":{"table-uuid":"$tableUuid","data-source-format":"$format",""" +
    s""""table-type":"$tableType",""" +
    s""""location":"$location",""" +
    s""""columns":{"type":"struct","fields":[""" +
    s"""{"name":"date","type":"string","nullable":true,"metadata":{}},""" +
    s"""{"name":"value","type":"integer","nullable":true,"metadata":{}}""" +
    s"""]},""" +
    s""""properties":{"key1":"val1"},"partition-columns":["date"],"created-time":1000},""" +
    s""""commits":$commitsJson,"latest-table-version":$latestTableVersion}"""

  private def deltaCommitJson(
      version: Long,
      fileName: String,
      fileSize: Long,
      timestamp: Long,
      fileModificationTimestamp: Long): String =
    s"""{"version":$version,"file-name":"$fileName","file-size":$fileSize,""" +
    s""""timestamp":$timestamp,"file-modification-timestamp":$fileModificationTimestamp}"""

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

  test("loadTable returns TableInfo with catalog identity and Delta metadata") {
    withClient { c =>
      val info = c.loadTable(testIdentifier)
      assert(info.getLocation === "s3://bucket/table")
      assert(info.getTableId === testTableId)
      assert(info.getTableType === UCDeltaModels.TableType.MANAGED)
      val m = info.getMetadata
      assert(m.getName === testTable)
      // UC's loadTable response does not carry the Delta Metadata.id; UC's table_uuid is exposed
      // separately as TableInfo.getTableId.
      assert(m.getId === null)
      assert(m.getProvider === "DELTA")
      assert(m.getConfiguration.get("key1") === "val1")
      assert(m.getPartitionColumns.get(0) === "date")
      assert(m.getCreatedTime === 1000L)
      // Schema is a JSON string in Delta's wire format; parseable by Delta's schema readers.
      val parsed = objectMapper.readTree(m.getSchemaString)
      assert(parsed.get("type").asText() === "struct")
      assert(parsed.get("fields").size() === 2)
      assert(parsed.get("fields").get(0).get("name").asText() === "date")
      assert(parsed.get("fields").get(1).get("type").asText() === "integer")
    }
  }

  test("loadTable schema emits Delta camelCase wire format for array and map") {
    val nested =
      s"""{"metadata":{"table-uuid":"$testTableId","data-source-format":"DELTA",""" +
      s""""table-type":"MANAGED","location":"s3://b/t","partition-columns":[],""" +
      s""""properties":{},"created-time":1,""" +
      s""""columns":{"type":"struct","fields":[""" +
      s"""{"name":"a","type":{"type":"array","element-type":"string","contains-null":true},""" +
      s""""nullable":true,"metadata":{}},""" +
      s"""{"name":"m","type":{"type":"map","key-type":"string","value-type":"integer",""" +
      s""""value-contains-null":false},"nullable":true,"metadata":{}}""" +
      s"""]}}}"""
    deltaHandler = (exchange, _) => sendJson(exchange, HttpStatus.SC_OK, nested)
    withClient { c =>
      val schema = c.loadTable(testIdentifier).getMetadata.getSchemaString
      val parsed = objectMapper.readTree(schema)
      val aType = parsed.get("fields").get(0).get("type")
      assert(aType.get("type").asText() === "array")
      assert(aType.get("elementType").asText() === "string")
      assert(aType.get("containsNull").asBoolean() === true)
      assert(aType.has("element-type") === false)
      val mType = parsed.get("fields").get(1).get("type")
      assert(mType.get("type").asText() === "map")
      assert(mType.get("keyType").asText() === "string")
      assert(mType.get("valueType").asText() === "integer")
      assert(mType.get("valueContainsNull").asBoolean() === false)
      assert(mType.has("key-type") === false)
    }
  }

  test("loadTable throws IOException on server error") {
    deltaHandler = (exchange, _) => sendJson(exchange, 500, """{"error":"fail"}""")
    withClient { c =>
      val e = intercept[java.io.IOException] { c.loadTable(testIdentifier) }
      assert(e.getMessage.contains("HTTP 500"))
    }
  }

  test("loadTable throws NoSuchTableException on 404") {
    deltaHandler = (exchange, _) => sendJson(exchange, 404, """{"error":"not found"}""")
    withClient { c =>
      val e = intercept[NoSuchTableException] {
        c.loadTable(testIdentifier)
      }
      assert(e.getMessage.contains(s"$testCatalog.$testSchema.$testTable"))
      assert(e.getMessage.contains("not found"))
    }
  }

  test("loadTable throws UnsupportedTableFormatException on 400 with that error type") {
    deltaHandler = (exchange, _) => sendJson(
      exchange, 400,
      """{"error":{"code":400,"type":"UnsupportedTableFormatException",""" +
        s""""message":"Table is not a Delta table: ${testCatalog}.${testSchema}.${testTable}"}}""")
    withClient { c =>
      val e = intercept[exceptions.UnsupportedTableFormatException] {
        c.loadTable(testIdentifier)
      }
      assert(e.getMessage.contains(s"$testCatalog.$testSchema.$testTable"))
      assert(e.getMessage.contains("not in Delta format"))
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
      c.commit(testTableId.toString, new URI("s3://bucket/table"), testIdentifier,
        Optional.of(createCommit(5L)), Optional.of(java.lang.Long.valueOf(3L)),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
    }

    val json = objectMapper.readTree(captured)
    val reqs = json.get("requirements")
    assert(reqs.size() === 1)
    assert(reqs.get(0).get("type").asText() === "assert-table-uuid")
    assert(reqs.get(0).get("uuid").asText() === testTableId.toString)

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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
      c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
        c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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
        c.commit(testTableId.toString, new URI("s3://b/t"), testIdentifier,
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

  test("getCommits loads table by identifier and returns commits") {
    var capturedMethod: String = null
    var capturedPath: String = null
    val commitsJson = "[" +
      deltaCommitJson(2L, "00000000000000000002.uuid.json", 200L, 2000L, 2001L) +
      "]"
    deltaHandler = (exchange, _) => {
      capturedMethod = exchange.getRequestMethod
      capturedPath = exchange.getRequestURI.getPath
      sendJson(exchange, HttpStatus.SC_OK,
        loadTableJson(commitsJson = commitsJson, latestTableVersion = 2L))
    }

    withClient { c =>
      val response = c.getCommits(testTableId.toString, new URI("s3://b/t"), testIdentifier,
        Optional.empty(), Optional.empty())

      assert(capturedMethod === "GET")
      assert(capturedPath ===
        "/api/2.1/unity-catalog/delta/v1/catalogs/cat/schemas/sch/tables/tbl")
      assert(response.getLatestTableVersion === 2L)
      assert(response.getCommits.size() === 1)

      val commit = response.getCommits.get(0)
      assert(commit.getVersion === 2L)
      assert(commit.getCommitTimestamp === 2000L)
      assert(commit.getFileStatus.getLen === 200L)
      assert(commit.getFileStatus.getModificationTime === 2001L)
      assert(commit.getFileStatus.getPath.toString ===
        "s3://b/t/_delta_log/_staged_commits/00000000000000000002.uuid.json")
    }
  }

  test("getCommits filters loaded commits by requested version range") {
    val commitsJson = Seq(
      deltaCommitJson(1L, "1.uuid.json", 100L, 1000L, 1001L),
      deltaCommitJson(2L, "2.uuid.json", 200L, 2000L, 2001L),
      deltaCommitJson(3L, "3.uuid.json", 300L, 3000L, 3001L),
      deltaCommitJson(4L, "4.uuid.json", 400L, 4000L, 4001L)
    ).mkString("[", ",", "]")
    deltaHandler = (exchange, _) =>
      sendJson(exchange, HttpStatus.SC_OK,
        loadTableJson(commitsJson = commitsJson, latestTableVersion = 4L))

    withClient { c =>
      val response = c.getCommits(testTableId.toString, new URI("s3://b/t"), testIdentifier,
        Optional.of(java.lang.Long.valueOf(2L)),
        Optional.of(java.lang.Long.valueOf(3L)))

      assert(response.getLatestTableVersion === 4L)
      assert(response.getCommits.asScala.map(_.getVersion).toSeq === Seq(2L, 3L))
    }
  }

  test("getCommits validates required parameters") {
    withClient { c =>
      intercept[NullPointerException] {
        c.getCommits(null, new URI("s3://b/t"), testIdentifier,
          Optional.empty(), Optional.empty())
      }
      intercept[NullPointerException] {
        c.getCommits(testTableId.toString, null, testIdentifier,
          Optional.empty(), Optional.empty())
      }
      intercept[NullPointerException] {
        c.getCommits(testTableId.toString, new URI("s3://b/t"), null,
          Optional.empty(), Optional.empty())
      }
    }
  }

  test("getCommits throws NoSuchTableException on 404") {
    deltaHandler = (exchange, _) =>
      sendJson(exchange, HttpStatus.SC_NOT_FOUND, """{"error":"not found"}""")
    withClient { c =>
      val e = intercept[NoSuchTableException] {
        c.getCommits(testTableId.toString, new URI("s3://b/t"), testIdentifier,
          Optional.empty(), Optional.empty())
      }
      assert(e.getMessage.contains(s"$testCatalog.$testSchema.$testTable"))
      assert(e.getMessage.contains("not found"))
    }
  }

  test("getCommits throws InvalidTargetTableException when table UUID does not match") {
    deltaHandler = (exchange, _) =>
      sendJson(exchange, HttpStatus.SC_OK,
        loadTableJson(tableUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440001")))
    withClient { c =>
      intercept[InvalidTargetTableException] {
        c.getCommits(testTableId.toString, new URI("s3://b/t"), testIdentifier,
          Optional.empty(), Optional.empty())
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
    intercept[IllegalStateException] {
      client.loadTable(new TableIdentifier("c", "s", "t"))
    }
  }
}
