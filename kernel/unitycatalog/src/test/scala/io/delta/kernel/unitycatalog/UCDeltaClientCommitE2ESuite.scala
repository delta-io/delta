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

package io.delta.kernel.unitycatalog

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.unitycatalog.client.auth.TokenProvider
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end test that drives the kernel [[UCCatalogManagedCommitter]] against the real
 * [[UCDeltaTokenBasedRestClient]] (the `UCDeltaClient` impl) over real HTTP, backed by an embedded
 * stub server (JDK `HttpServer`, mirroring delta-storage's `UCDeltaTokenBasedRestClientSuite`).
 *
 * The point is to prove two things without any live Unity Catalog server:
 *   1. A `UCDeltaClient` (a `UCClient` subtype) can be injected into the existing
 *      `UCCatalogManagedCommitter` with no kernel change; the committer only ever touches the
 *      `UCClient` interface.
 *   2. A `CATALOG_WRITE` commit flows kernel to the Delta-Tables API `updateTable` correctly: the
 *      committer's forwarded args (staged commit, old/new P&M) become an `updateTable` request
 *      hitting the wire.
 */
class UCDeltaClientCommitE2ESuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with UCCatalogManagedTestUtils {

  private val testUcTableId = "11111111-1111-1111-1111-111111111111"

  private var server: HttpServer = _
  private var serverUri: String = _
  private var deltaClient: UCDeltaTokenBasedRestClient = _
  // Captures (method, path, body) of every request the UCDeltaClient sends to the stub server.
  private val requests = ArrayBuffer.empty[(String, String, String)]

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/",
      (exchange: HttpExchange) => {
        val body =
          new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
        requests.synchronized {
          requests += ((exchange.getRequestMethod, exchange.getRequestURI.getPath, body))
        }
        // Respond with a minimal loadTable-shaped JSON; updateTable returns the same shape.
        val resp = loadTableJson()
        val bytes = resp.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, bytes.length)
        exchange.getResponseBody.write(bytes)
        exchange.getResponseBody.close()
        exchange.close()
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
    requests.synchronized { requests.clear() }
    deltaClient =
      new UCDeltaTokenBasedRestClient(serverUri, tokenProvider(), Collections.emptyMap())
  }

  override def afterEach(): Unit = {
    if (deltaClient != null) deltaClient.close()
    super.afterEach()
  }

  private def loadTableJson(): String =
    s"""{"metadata":{"table-uuid":"$testUcTableId","data-source-format":"DELTA",""" +
      s""""table-type":"MANAGED","location":"s3://bucket/table",""" +
      s""""columns":{"type":"struct","fields":[]},"properties":{},""" +
      s""""partition-columns":[],"created-time":1000},""" +
      s""""commits":[],"latest-table-version":0}"""

  private def tokenProvider(): TokenProvider = new TokenProvider {
    override def accessToken(): String = "mock-token"
    override def initialize(configs: java.util.Map[String, String]): Unit = {}
    override def configs(): java.util.Map[String, String] = Collections.emptyMap()
  }

  private val jsonMapper = new ObjectMapper()

  /** Finds the single captured request matching `method` and a path ending in `pathSuffix`. */
  private def findRequest(method: String, pathSuffix: String): (String, String, String) = {
    val sent = requests.synchronized { requests.toList }
    sent
      .find { case (m, p, _) => m == method && p.endsWith(pathSuffix) }
      .getOrElse(
        fail(s"expected a $method to ...$pathSuffix, got: ${sent.map(r => r._1 + " " + r._2)}"))
  }

  private def parseJson(body: String): JsonNode = jsonMapper.readTree(body)

  test("CATALOG_WRITE flows through the real UCDeltaClient to the Delta-Tables API updateTable") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN: a committer wired to the REAL UCDeltaClient (injected as a UCClient) =====
      val committer = new UCCatalogManagedCommitter(
        deltaClient,
        testUcTableId,
        tablePath,
        new UCTableIdentifier("main", "default", "t"))

      val readProtocol = protocolWithCatalogManagedSupport
      val readMetadata = basicPartitionedMetadata
      val newMetadata = readMetadata.withMergedConfiguration(
        Map(
          "foo" -> "bar",
          "delta.minReaderVersion" -> "3",
          "delta.feature.deletionVectors" -> "supported").asJava)

      val commitMetadata = createCommitMetadata(
        version = 1,
        logPath = logPath,
        readPandMOpt =
          Optional.of(new KernelTuple2[Protocol, Metadata](readProtocol, readMetadata)),
        newProtocolOpt = Optional.empty(),
        newMetadataOpt = Optional.of(newMetadata))

      // ===== WHEN =====
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN: the UCDeltaClient issued an updateTable request carrying the commit =====
      // The Delta-Tables API updateTable endpoint is POST .../tables/{table}.
      val updateReq = findRequest("POST", "/tables/t")
      val body = parseJson(updateReq._3)

      // Requirements: assert-table-uuid carrying this table's uuid.
      val requirements = body.get("requirements")
      assert(requirements != null && requirements.isArray, s"expected requirements array: $body")
      val requirementActions = requirements.elements().asScala.map(_.get("type").asText()).toSet
      assert(requirementActions.contains("assert-table-uuid"), s"expected assert-table-uuid: $body")
      assert(
        requirements.elements().asScala.exists(_.path("uuid").asText() == testUcTableId),
        s"expected requirement carrying table uuid $testUcTableId: $body")

      // Updates: add-commit (the staged commit) and set-properties (from the metadata diff).
      val updates = body.get("updates")
      assert(updates != null && updates.isArray, s"expected updates array: $body")
      val updateActions = updates.elements().asScala.map(_.get("action").asText()).toSet
      assert(updateActions.contains("add-commit"), s"expected add-commit update: $body")
      assert(updateActions.contains("set-properties"), s"expected set-properties update: $body")

      // set-properties carries the real table property but no protocol-derived properties.
      val setProps = updates
        .elements()
        .asScala
        .find(_.get("action").asText() == "set-properties")
        .map(_.get("updates"))
        .getOrElse(fail(s"missing set-properties updates map: $body"))
      val propKeys = setProps.fieldNames().asScala.toSet
      assert(propKeys.contains("foo"), s"expected real property 'foo': $body")
      assert(
        !propKeys.contains("delta.minReaderVersion"),
        s"minReaderVersion must be stripped: $body")
      assert(
        !propKeys.contains("delta.minWriterVersion"),
        s"minWriterVersion must be stripped: $body")
      assert(
        !propKeys.exists(_.startsWith("delta.feature.")),
        s"delta.feature.* must be stripped: $body")
    }
  }

  test("CATALOG_CREATE flows through the real UCDeltaClient to the Delta-Tables API createTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucCatalogManagedClient = new UCCatalogManagedClient(deltaClient)

      // Real create flow: writes 000.json, then finalizeCreate -> Delta-Tables API createTable.
      ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          new UCTableIdentifier("main", "default", "t"))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      // ===== THEN: a createTable POST .../tables carried the protocol as a structured field. =====
      val createReq = findRequest("POST", "/tables")
      val body = parseJson(createReq._3)

      // Protocol is sent as a structured field (not flattened into properties).
      val protocol = body.get("protocol")
      assert(protocol != null && protocol.isObject, s"expected structured protocol field: $body")
      assert(protocol.path("min-reader-version").asInt() >= 3, s"expected reader version: $body")
      assert(protocol.path("min-writer-version").asInt() >= 7, s"expected writer version: $body")
      val writerFeatures =
        protocol.path("writer-features").elements().asScala.map(_.asText()).toSet
      assert(writerFeatures.contains("catalogManaged"), s"expected catalogManaged feature: $body")

      // Properties carry no protocol-derived keys (those are conveyed by the protocol field).
      val props = Option(body.get("properties"))
      val propKeys = props.map(_.fieldNames().asScala.toSet).getOrElse(Set.empty)
      assert(
        !propKeys.contains("delta.minReaderVersion"),
        s"minReaderVersion must be stripped: $body")
      assert(
        !propKeys.contains("delta.minWriterVersion"),
        s"minWriterVersion must be stripped: $body")
      assert(
        !propKeys.exists(_.startsWith("delta.feature.")),
        s"delta.feature.* must be stripped: $body")
    }
  }

  test("loadSnapshot flows through the real UCDeltaClient to the Delta-Tables API loadTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucCatalogManagedClient = new UCCatalogManagedClient(deltaClient)

      // Create first so there is a real 000.json for the snapshot build to read; this also
      // exercises createTable. Then load at version 0, which drives getCommits -> loadTable.
      ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          new UCTableIdentifier("main", "default", "t"))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      requests.synchronized { requests.clear() }
      val snapshot = ucCatalogManagedClient.loadSnapshot(
        engine,
        testUcTableId,
        tablePath,
        new UCTableIdentifier("main", "default", "t"),
        Optional.of(0L),
        Optional.empty())

      // ===== THEN: read resolved via a Delta-Tables API loadTable GET .../tables/{table}. =====
      assert(snapshot.getVersion === 0)
      findRequest("GET", "/tables/t")
    }
  }

  test("loadCommitRange flows through the real UCDeltaClient to the Delta-Tables API loadTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucCatalogManagedClient = new UCCatalogManagedClient(deltaClient)

      // Create first so there is a real 000.json to build a commit range over.
      ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          new UCTableIdentifier("main", "default", "t"))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      requests.synchronized { requests.clear() }
      val commitRange = ucCatalogManagedClient.loadCommitRange(
        engine,
        testUcTableId,
        tablePath,
        new UCTableIdentifier("main", "default", "t"),
        Optional.of(0L),
        Optional.empty(),
        Optional.of(0L),
        Optional.empty())

      // ===== THEN: the range resolved via a Delta-Tables API loadTable GET .../tables/t. =====
      findRequest("GET", "/tables/t")
      assert(commitRange.getStartVersion === 0)
      assert(commitRange.getEndVersion === 0)
      val commitActions =
        commitRange.getCommitActions(
          engine,
          java.util.Set.of(DeltaAction.PROTOCOL, DeltaAction.METADATA))
      val versions =
        try commitActions.asScala.map(_.getVersion).toList
        finally commitActions.close()
      assert(versions === List(0L))
    }
  }
}
