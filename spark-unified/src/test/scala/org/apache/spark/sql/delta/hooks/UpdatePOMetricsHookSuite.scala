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

package org.apache.spark.sql.delta.hooks

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdatePOMetricsHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {
  private val TEST_CATALOG_NAME = "test_catalog"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")
  }

  test("POMetricsClient: sends minimal payload with Authorization header") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(200)
      mockServer.start()

      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME",
        "io.unitycatalog.spark.UCSingleCatalog")
      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set(s"spark.sql.catalog.$TEST_CATALOG_NAME.token", "test-token-123")

      val request = ReportDeltaMetricsRequest(
        tableId = "abc-123",
        report = CommitReportEnvelope(CommitReport())
      )
      POMetricsClient.sendMetrics(spark, request, catalogName = Some(TEST_CATALOG_NAME))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")

      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined, "Authorization header should be present")
      assert(authHeader.get == "Bearer test-token-123", "Auth token must match")

      val body = mockServer.getLastRequestBody()
      assert(body.contains(""""table_id":"abc-123""""), "body must contain table_id")
      assert(body.contains(""""commit_report""""), "body must contain commit_report wrapper")
    } finally {
      mockServer.stop()
    }
  }

  test("POMetricsClient: auth.type=static with auth.token (new auth.* format)") {
    val authStaticCatalog = "auth_static_catalog" // distinct from TEST_CATALOG_NAME to avoid
    // config leakage between tests (SharedSparkSession reuses spark.conf)
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(200)
      mockServer.start()

      spark.conf.set(
        s"spark.sql.catalog.$authStaticCatalog",
        "io.unitycatalog.spark.UCSingleCatalog")
      spark.conf.set(
        s"spark.sql.catalog.$authStaticCatalog.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set(
        s"spark.sql.catalog.$authStaticCatalog.auth.type",
        "static")
      spark.conf.set(
        s"spark.sql.catalog.$authStaticCatalog.auth.token",
        "auth-static-token-456")

      val request = ReportDeltaMetricsRequest(
        tableId = "auth-test-id",
        report = CommitReportEnvelope(CommitReport())
      )
      POMetricsClient.sendMetrics(spark, request, catalogName = Some(authStaticCatalog))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")
      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined, "Authorization header should be present")
      assert(authHeader.get == "Bearer auth-static-token-456",
        "Auth token from auth.token must match")
    } finally {
      mockServer.stop()
    }
  }

  test("run(): fires HTTP POST for UC-managed table with skeleton payload") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(200)
      mockServer.start()

      withTempDir { dir =>
        spark.range(10).write.format("delta").save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val snapshot = deltaLog.snapshot

        spark.conf.set(
          "spark.sql.catalog.spark_catalog",
          "io.unitycatalog.spark.UCSingleCatalog")
        spark.conf.set(
          "spark.sql.catalog.spark_catalog.uri",
          s"http://localhost:${mockServer.getPort()}")
        spark.conf.set("spark.sql.catalog.spark_catalog.token", "smoke-token")

        val catalogTable = CatalogTable(
          identifier = TableIdentifier("t", Some("default"), Some("spark_catalog")),
          tableType = CatalogTableType.MANAGED,
          storage = CatalogStorageFormat.empty,
          schema = new StructType()
        )

        val txn = CommittedTransaction(
          txnId = "smoke-txn",
          deltaLog = deltaLog,
          catalogTable = Some(catalogTable),
          readSnapshot = snapshot,
          committedVersion = 0L,
          committedActions = Seq.empty,
          postCommitSnapshot = snapshot,
          postCommitHooks = Seq.empty,
          txnExecutionTimeMs = 0L,
          needsCheckpoint = false,
          partitionsAddedToOpt = None,
          isBlindAppend = true
        )

        UpdatePOMetricsHook(Some(catalogTable)).run(spark, txn)

        assert(mockServer.getRequestCount() == 1, "Expected exactly 1 HTTP POST")
        val body = mockServer.getLastRequestBody()
        assert(body.contains(s""""table_id":"${deltaLog.tableId}""""),
          "payload must contain the table UUID")
        assert(body.contains(""""commit_report""""), "payload must contain commit_report wrapper")
      }
    } finally {
      mockServer.stop()
    }
  }
}

class SimpleMockServer(port: Int) {
  private var serverSocket: ServerSocket = _
  private var serverThread: Thread = _
  private var running = false
  private var responseCode = 200
  private val requestCount = new AtomicInteger(0)
  private var lastRequestBody = ""
  private var lastHeaders = Map[String, String]()
  private var actualPort = port

  def setResponseCode(code: Int): Unit = { responseCode = code }

  def getPort(): Int = actualPort

  def getRequestCount(): Int = requestCount.get()

  def getLastRequestBody(): String = lastRequestBody

  def getLastHeaders(): Map[String, String] = lastHeaders

  def start(): Unit = {
    serverSocket = new ServerSocket(port)
    actualPort = serverSocket.getLocalPort
    running = true

    serverThread = new Thread(new Runnable {
      override def run(): Unit = {
        while (running) {
          try {
            val clientSocket = serverSocket.accept()
            handleRequest(clientSocket)
          } catch {
            case _: java.net.SocketException if !running =>
            case _: Exception =>
          }
        }
      }
    })
    serverThread.setDaemon(true)
    serverThread.start()

    Thread.sleep(100)
  }

  private def handleRequest(clientSocket: Socket): Unit = {
    try {
      val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      val out = new PrintWriter(clientSocket.getOutputStream, true)

      in.readLine()

      val headers = scala.collection.mutable.Map[String, String]()
      var contentLength = 0
      var line = in.readLine()
      while (line != null && line.nonEmpty) {
        val parts = line.split(":", 2)
        if (parts.length == 2) {
          val key = parts(0).trim
          val value = parts(1).trim
          headers(key) = value
          if (key.equalsIgnoreCase("Content-Length")) contentLength = value.toInt
        }
        line = in.readLine()
      }

      val body = new Array[Char](contentLength)
      if (contentLength > 0) in.read(body, 0, contentLength)

      requestCount.incrementAndGet()
      lastRequestBody = new String(body)
      lastHeaders = headers.toMap

      out.write(s"HTTP/1.1 $responseCode ${statusMessage(responseCode)}\r\n")
      out.write("Content-Type: application/json\r\n")
      out.write("Content-Length: 2\r\n")
      out.write("\r\n")
      out.write("{}")
      out.flush()

      clientSocket.close()
    } catch {
      case _: java.io.IOException => // client disconnect, etc. - expected during test teardown
    }
  }

  private def statusMessage(code: Int): String = code match {
    case 200 => "OK"
    case 400 => "Bad Request"
    case 500 => "Internal Server Error"
    case _ => "Unknown"
  }

  def stop(): Unit = {
    running = false
    if (serverSocket != null && !serverSocket.isClosed) serverSocket.close()
    if (serverThread != null) {
      serverThread.interrupt()
      serverThread.join(1000)
    }
  }
}
