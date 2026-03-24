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

package org.apache.spark.sql.delta.hooks.metrics

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{CatalogTableUtils, JsonUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdateMetricsHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {
  private val TEST_CATALOG_NAME = "test_catalog"
  private val TEST_UC_TABLE_ID = "uc-table-id-abc"

  private def ucStorageFormat(ucTableId: String = TEST_UC_TABLE_ID): CatalogStorageFormat = {
    CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map(
        UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> ucTableId,
        "delta.feature.catalogManaged" -> "supported"
      )
    )
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")
  }

  test("CatalogTableUtils.isUnityCatalogManagedTable: detection cases") {
    val ucManaged = CatalogTable(
      identifier = TableIdentifier("t", Some("default"), Some("uc_catalog")),
      tableType = CatalogTableType.MANAGED,
      storage = ucStorageFormat(),
      schema = new StructType()
    )
    assert(CatalogTableUtils.isUnityCatalogManagedTable(ucManaged),
      "UC-managed table with correct storage properties should be detected")

    val noUCTableId = CatalogTable(
      identifier = TableIdentifier("t", Some("default"), Some("uc_catalog")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map("delta.feature.catalogManaged" -> "supported")
      ),
      schema = new StructType()
    )
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(noUCTableId),
      "table without UC table ID should not be detected")

    val noCatalogFeature = CatalogTable(
      identifier = TableIdentifier("t", Some("default"), Some("uc_catalog")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map(
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> "some-id"
        )
      ),
      schema = new StructType()
    )
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(noCatalogFeature),
      "table with UC ID but no catalog feature flag should not be detected")

    val emptyStorage = CatalogTable(
      identifier = TableIdentifier("t", Some("default"), Some("uc_catalog")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
    )
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(emptyStorage),
      "table with empty storage properties should not be detected")
  }

  test("sendMetrics: sends payload with Authorization header") {
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
      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME.token",
        "test-token-123")

      val request = ReportDeltaMetricsRequest(
        tableId = "abc-123",
        report = CommitReportEnvelope(CommitReport())
      )
      UpdateMetricsHook.sendMetrics(
        spark, request, catalogName = Some(TEST_CATALOG_NAME))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")

      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined, "Authorization header should be present")
      assert(authHeader.get == "Bearer test-token-123",
        "Auth token must match")

      val body = mockServer.getLastRequestBody()
      val actualPayload =
        JsonUtils.fromJson[Map[String, Any]](body)
      val expectedPayload =
        JsonUtils.fromJson[Map[String, Any]](JsonUtils.toJson(request))
      assert(actualPayload == expectedPayload,
        "request JSON should match expected payload")
    } finally {
      mockServer.stop()
    }
  }

  test("sendMetrics: auth.type=static with auth.token") {
    val authStaticCatalog = "auth_static_catalog"
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
      UpdateMetricsHook.sendMetrics(
        spark, request, catalogName = Some(authStaticCatalog))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")
      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined,
        "Authorization header should be present")
      assert(authHeader.get == "Bearer auth-static-token-456",
        "Auth token from auth.token must match")
    } finally {
      mockServer.stop()
    }
  }

  test("run(): fires HTTP POST for UC-managed table") {
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
        spark.conf.set(
          "spark.sql.catalog.spark_catalog.token", "smoke-token")

        val catalogTable = CatalogTable(
          identifier = TableIdentifier(
            "t", Some("default"), Some("spark_catalog")),
          tableType = CatalogTableType.MANAGED,
          storage = ucStorageFormat(),
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

        UpdateMetricsHook(Some(catalogTable)).run(spark, txn)

        assert(mockServer.getRequestCount() == 1,
          "Expected exactly 1 HTTP POST")
        val body = mockServer.getLastRequestBody()
        val expectedRequest = UpdateMetricsHook.buildRequest(
          TEST_UC_TABLE_ID, Seq.empty, 0L)
        val actualPayload =
          JsonUtils.fromJson[Map[String, Any]](body)
        val expectedPayload =
          JsonUtils.fromJson[Map[String, Any]](
            JsonUtils.toJson(expectedRequest))
        assert(actualPayload == expectedPayload,
          "smoke test payload should match expected JSON")
      }
    } finally {
      mockServer.stop()
    }
  }

  test("run(): hook does not crash on HTTP 5xx (best-effort)") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(500)
      mockServer.start()

      withTempDir { dir =>
        spark.range(10).write.format("delta")
          .save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(
          spark, dir.getCanonicalPath)
        val snapshot = deltaLog.snapshot

        spark.conf.set(
          "spark.sql.catalog.spark_catalog",
          "io.unitycatalog.spark.UCSingleCatalog")
        spark.conf.set(
          "spark.sql.catalog.spark_catalog.uri",
          s"http://localhost:${mockServer.getPort()}")
        spark.conf.set(
          "spark.sql.catalog.spark_catalog.token",
          "error-token")

        val catalogTable = CatalogTable(
          identifier = TableIdentifier(
            "t", Some("default"), Some("spark_catalog")),
          tableType = CatalogTableType.MANAGED,
          storage = ucStorageFormat(),
          schema = new StructType()
        )

        val txn = CommittedTransaction(
          txnId = "error-txn",
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

        UpdateMetricsHook(Some(catalogTable)).run(spark, txn)

        assert(mockServer.getRequestCount() == 1,
          "request should still be sent on 5xx")
      }
    } finally {
      mockServer.stop()
    }
  }

  test("run(): skips metrics when table ID not in storage properties") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = deltaLog.snapshot

      val catalogTable = CatalogTable(
        identifier = TableIdentifier(
          "t", Some("default"), Some("spark_catalog")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map(
            "delta.feature.catalogManaged" -> "supported"
          )
        ),
        schema = new StructType()
      )

      val txn = CommittedTransaction(
        txnId = "skip-txn",
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

      UpdateMetricsHook(Some(catalogTable)).run(spark, txn)
    }
  }
}

class SimpleMockServer(port: Int) {
  private var serverSocket: ServerSocket = _
  private var serverThread: Thread = _
  @volatile private var running = false
  private var responseCode = 200
  private val requestCount = new AtomicInteger(0)
  @volatile private var lastRequestBody = ""
  @volatile private var lastHeaders = Map[String, String]()
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
  }

  private def handleRequest(clientSocket: Socket): Unit = {
    try {
      val in = new BufferedReader(
        new InputStreamReader(clientSocket.getInputStream))
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
          if (key.equalsIgnoreCase("Content-Length")) {
            contentLength = value.toInt
          }
        }
        line = in.readLine()
      }

      val body = new Array[Char](contentLength)
      if (contentLength > 0) in.read(body, 0, contentLength)

      requestCount.incrementAndGet()
      lastRequestBody = new String(body)
      lastHeaders = headers.toMap

      out.write(
        s"HTTP/1.1 $responseCode ${statusMessage(responseCode)}\r\n")
      out.write("Content-Type: application/json\r\n")
      out.write("Content-Length: 2\r\n")
      out.write("\r\n")
      out.write("{}")
      out.flush()

      clientSocket.close()
    } catch {
      case _: java.io.IOException =>
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
