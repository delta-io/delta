/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.utils.CloseableIterable
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.unitycatalog.client.auth.TokenProvider
import org.scalatest.BeforeAndAfterAll
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
    with UCCatalogManagedTestUtils {

  private val testUcTableId = "11111111-1111-1111-1111-111111111111"

  private var server: HttpServer = _
  private var serverUri: String = _
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

  private def newUCDeltaClient(): UCDeltaTokenBasedRestClient =
    new UCDeltaTokenBasedRestClient(serverUri, tokenProvider(), Collections.emptyMap())

  test("CATALOG_WRITE flows through the real UCDeltaClient to the Delta-Tables API updateTable") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN: a committer wired to the REAL UCDeltaClient (injected as a UCClient) =====
      requests.synchronized { requests.clear() }
      val deltaClient = newUCDeltaClient()
      try {
        val committer = new UCCatalogManagedCommitter(
          deltaClient,
          testUcTableId,
          tablePath,
          new UCTableIdentifier("main", "default", "t"))

        val readProtocol = protocolWithCatalogManagedSupport
        val readMetadata = basicPartitionedMetadata
        val newMetadata = readMetadata.withMergedConfiguration(Map("foo" -> "bar").asJava)

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
        val sent = requests.synchronized { requests.toList }
        val updateReq = sent.find { case (method, path, _) =>
          method == "POST" && path.endsWith("/tables/t")
        }
        assert(
          updateReq.isDefined,
          s"expected an updateTable POST to .../tables/t, got: " +
            s"${sent.map(r => r._1 + " " + r._2)}")
        val body = updateReq.get._3
        // The request body must carry the add-commit update (the staged commit) and the
        // set-properties update derived from the old-vs-new metadata diff.
        assert(body.contains("add-commit"), s"expected add-commit update in body: $body")
        assert(
          body.contains("set-properties"),
          s"expected set-properties update from the metadata diff in body: $body")
        assert(
          body.contains("assert-table-uuid"),
          s"expected table-uuid requirement in body: $body")
        assert(body.contains(testUcTableId), s"expected the table uuid in body: $body")
      } finally {
        deltaClient.close()
      }
    }
  }

  test("CATALOG_CREATE flows through the real UCDeltaClient to the Delta-Tables API createTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      requests.synchronized { requests.clear() }
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val deltaClient = newUCDeltaClient()
      try {
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

        // ===== THEN: a createTable POST .../tables was issued (finalizeCreate). =====
        val sent = requests.synchronized { requests.toList }
        val createReq = sent.find { case (method, path, _) =>
          method == "POST" && path.endsWith("/tables")
        }
        assert(
          createReq.isDefined,
          s"expected a createTable POST to .../tables, got: " +
            s"${sent.map(r => r._1 + " " + r._2)}")
      } finally {
        deltaClient.close()
      }
    }
  }

  test("loadSnapshot flows through the real UCDeltaClient to the Delta-Tables API loadTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      requests.synchronized { requests.clear() }
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val deltaClient = newUCDeltaClient()
      try {
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
        val sent = requests.synchronized { requests.toList }
        val loadReq = sent.find { case (method, path, _) =>
          method == "GET" && path.endsWith("/tables/t")
        }
        assert(
          loadReq.isDefined,
          s"expected a loadTable GET to .../tables/t, got: " +
            s"${sent.map(r => r._1 + " " + r._2)}")
      } finally {
        deltaClient.close()
      }
    }
  }

  test("loadCommitRange flows through the real UCDeltaClient to the Delta-Tables API loadTable") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      requests.synchronized { requests.clear() }
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val deltaClient = newUCDeltaClient()
      try {
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
        assert(commitRange.getStartVersion === 0)
        val sent = requests.synchronized { requests.toList }
        val loadReq = sent.find { case (method, path, _) =>
          method == "GET" && path.endsWith("/tables/t")
        }
        assert(
          loadReq.isDefined,
          s"expected a loadTable GET to .../tables/t, got: " +
            s"${sent.map(r => r._1 + " " + r._2)}")
      } finally {
        deltaClient.close()
      }
    }
  }
}
