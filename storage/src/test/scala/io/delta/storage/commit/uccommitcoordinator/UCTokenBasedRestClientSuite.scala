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

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.delta.storage.commit.{Commit, CommitFailedException}
import io.delta.storage.commit.actions.AbstractMetadata
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
    server.start()
    serverUri = s"http://localhost:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  override def beforeEach(): Unit = {
    metastoreHandler = null
    commitsHandler = null
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
    new UCTokenBasedRestClient(serverUri, createTokenProvider(), "4.0.0", "4.0.0", "2.13")

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

  // Constructor tests
  test("constructor validates required parameters") {
    intercept[NullPointerException] {
      new UCTokenBasedRestClient(null, createTokenProvider(), null, null, null)
    }
    intercept[NullPointerException] {
      new UCTokenBasedRestClient(serverUri, null, null, null, null)
    }
    // null versions are allowed
    val client = new UCTokenBasedRestClient(serverUri, createTokenProvider(), null, null, null)
    client.close()
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
    // 404 -> InvalidTargetTableException
    commitsHandler = exchange => sendJson(exchange, HttpStatus.SC_NOT_FOUND, "{}")
    withClient { client =>
      intercept[InvalidTargetTableException] {
        client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
          Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
      }
    }

    // 409 -> CommitFailedException with conflict
    commitsHandler = exchange => sendJson(exchange, HttpStatus.SC_CONFLICT, "{}")
    withClient { client =>
      val ex = intercept[CommitFailedException] {
        client.commit(testTableId, testTableUri, Optional.of(createCommit(1L)),
          Optional.empty(), false, Optional.empty(), Optional.empty(), Optional.empty())
      }
      assert(ex.getConflict)
    }
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
}
