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
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * Test suite for UpdateMetricsHook functionality.
 *
 * Tests cover:
 * - resolveTableId: UC table ID resolution priority
 * - buildRequest: file and row metric extraction without Delta infrastructure
 * - JSON payload structure matching the server contract (snake_case, nested)
 * - Error handling (commits succeed even when HTTP fails)
 * - Smoke test: run() fires HTTP POST with correct payload for UC-managed table
 */
class UpdateMetricsHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {
  private val TEST_CATALOG_NAME = "test_catalog"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "false")
  }

  // ---------------------------------------------------------------------------
  // resolveTableId tests
  // ---------------------------------------------------------------------------

  test("resolveTableId: prefers UC table ID from catalogTable properties over deltaLog.tableId") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val hook = UpdateMetricsHook(None)

      // Sub-case 1: io.unitycatalog.tableId in ct.properties (Databricks-internal connector)
      val ctWithUCId = CatalogTable(
        identifier = TableIdentifier("t", Some("s"), Some("c")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType(),
        properties = Map("io.unitycatalog.tableId" -> "uc-uuid-123")
      )
      assert(hook.resolveTableId(Some(ctWithUCId), deltaLog) == "uc-uuid-123",
        "UC table ID from ct.properties must win over deltaLog.tableId")

      // Sub-case 2: legacy ucTableId in ct.properties (Databricks-internal legacy key)
      val ctWithLegacyId = CatalogTable(
        identifier = TableIdentifier("t", Some("s"), Some("c")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType(),
        properties = Map("ucTableId" -> "legacy-uuid-456")
      )
      assert(hook.resolveTableId(Some(ctWithLegacyId), deltaLog) == "legacy-uuid-456",
        "legacy ucTableId key must be honoured")

      // Sub-case 3: fs.unitycatalog.table.id in ct.storage.properties
      // (open-source UCSingleCatalog; Delta surfaces this as "option.fs.unitycatalog.table.id")
      val ctWithStorageId = CatalogTable(
        identifier = TableIdentifier("t", Some("s"), Some("c")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map("fs.unitycatalog.table.id" -> "storage-uuid-789")
        ),
        schema = new StructType()
      )
      assert(hook.resolveTableId(Some(ctWithStorageId), deltaLog) == "storage-uuid-789",
        "fs.unitycatalog.table.id from storage.properties must win over deltaLog.tableId")

      // Sub-case 4: no UC table ID anywhere -> fall back to deltaLog.tableId
      val ctNoId = CatalogTable(
        identifier = TableIdentifier("t", Some("s"), Some("c")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType()
      )
      assert(hook.resolveTableId(Some(ctNoId), deltaLog) == deltaLog.tableId,
        "must fall back to deltaLog.tableId when no UC table ID is found")

      // Sub-case 5: no catalogTable at all -> fall back to deltaLog.tableId
      assert(hook.resolveTableId(None, deltaLog) == deltaLog.tableId,
        "must fall back to deltaLog.tableId when catalogTable is None")
    }
  }

  // ---------------------------------------------------------------------------
  // buildRequest tests - no reflection, no Delta infrastructure
  // ---------------------------------------------------------------------------

  test("buildRequest: file metrics from synthetic AddFile/RemoveFile actions") {
    val add1 = AddFile("f1.parquet", Map.empty, 1024L,
      System.currentTimeMillis(), dataChange = true)
    val add2 = AddFile("f2.parquet", Map.empty, 2048L,
      System.currentTimeMillis(), dataChange = true)
    val add3 = AddFile("f3.parquet", Map.empty, 4096L,
      System.currentTimeMillis(), dataChange = true)
    val removed = RemoveFile("old.parquet",
      Some(System.currentTimeMillis()),
      dataChange = true, size = Some(512L))

    val actions: Seq[Action] = Seq(add1, add2, add3, removed)
    val request = ReportDeltaMetrics.buildRequest(
      "tbl-123", actions, committedVersion = 7L)
    val report = request.report.commitReport

    assert(request.tableId == "tbl-123")
    assert(report.numFilesAdded == Some(3L), "3 AddFiles")
    assert(report.numFilesRemoved == Some(1L), "1 RemoveFile")
    assert(report.numBytesAdded == Some(1024L + 2048L + 4096L),
      "sum of add sizes")
    assert(report.numBytesRemoved == Some(512L),
      "sum of remove sizes")

    val hist = report.fileSizeHistogram.getOrElse(fail("histogram must be present"))
    assert(hist.commitVersion == Some(7L), "commit_version must be set")
    assert(hist.sortedBinBoundaries.head == 0L, "bins must start at 0")
    assert(hist.fileCounts.sum == 3L, "histogram covers all AddFiles")
    assert(hist.totalBytes.sum == 1024L + 2048L + 4096L, "histogram totalBytes")
  }

  test("buildRequest: row metrics prefer operationMetrics over file stats") {
    // Sub-case 1: operationMetrics present - must win over file-level numLogicalRecords
    val addFileWithStats = AddFile("f.parquet", Map.empty, 1000L,
      System.currentTimeMillis(), dataChange = true,
      stats = """{"numRecords": 999}""")
    val commitInfoWithMetrics = CommitInfo(
      version = Some(0L),
      inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None,
      operation = "WRITE",
      operationParameters = Map.empty,
      job = None, notebook = None, clusterId = None,
      readVersion = None, isolationLevel = None,
      isBlindAppend = Some(true),
      operationMetrics = Some(Map("numOutputRows" -> "100")),
      userMetadata = None, tags = None, engineInfo = None, txnId = None)

    val req1 = ReportDeltaMetrics.buildRequest(
      "t1", Seq(commitInfoWithMetrics, addFileWithStats), 0L)
    assert(req1.report.commitReport.numRowsInserted == Some(100L),
      "operationMetrics wins over file stats")

    // Sub-case 2: no operationMetrics - falls back to numLogicalRecords from file stats
    val commitInfoNoMetrics = CommitInfo(
      version = Some(1L),
      inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None,
      operation = "WRITE",
      operationParameters = Map.empty,
      job = None, notebook = None, clusterId = None,
      readVersion = None, isolationLevel = None,
      isBlindAppend = Some(true),
      operationMetrics = None,
      userMetadata = None, tags = None, engineInfo = None, txnId = None)

    val req2 = ReportDeltaMetrics.buildRequest("t2", Seq(commitInfoNoMetrics, addFileWithStats), 1L)
    assert(req2.report.commitReport.numRowsInserted == Some(999L),
      "fallback to numLogicalRecords from file stats")

    // Sub-case 3: no CommitInfo at all - falls back to numLogicalRecords
    val req3 = ReportDeltaMetrics.buildRequest("t3", Seq(addFileWithStats), 2L)
    assert(req3.report.commitReport.numRowsInserted == Some(999L),
      "fallback works with no CommitInfo in actions")
  }

  // ---------------------------------------------------------------------------
  // JSON payload structure tests - validates server contract
  // ---------------------------------------------------------------------------

  test("JSON payload validation - matches server contract (snake_case, nested)") {
    val request = ReportDeltaMetricsRequest(
      tableId = "test-table-id-123",
      report = CommitReportEnvelope(CommitReport(
        numFilesAdded = Some(10L),
        numFilesRemoved = Some(2L),
        numBytesAdded = Some(10000L),
        numBytesRemoved = Some(2000L),
        numRowsInserted = Some(1000L),
        numRowsRemoved = Some(200L),
        numRowsUpdated = Some(50L),
        fileSizeHistogram = Some(FileSizeHistogramPayload(
          sortedBinBoundaries = Seq(0L, 1024L),
          fileCounts = Seq(5L, 5L),
          totalBytes = Seq(2000L, 8000L),
          commitVersion = Some(42L)
        ))
      ))
    )

    val json = JsonUtils.toJson(request)

    // Server expects snake_case field names
    assert(json.contains(""""table_id":"test-table-id-123""""),
      "table_id must be snake_case")
    assert(json.contains(""""commit_report""""),
      "must have nested commit_report key")
    assert(json.contains(""""num_files_added":10"""),
      "num_files_added must be snake_case")
    assert(json.contains(""""num_rows_inserted":1000"""),
      "must use num_rows_inserted (not num_rows_added)")
    assert(json.contains(""""commit_version":42"""),
      "commit_version required for server staleness check")

    // Must NOT have camelCase keys
    assert(!json.contains(""""tableId""""), "no camelCase tableId")
    assert(!json.contains(""""numFilesAdded""""), "no camelCase numFilesAdded")
    assert(!json.contains(""""numRowsAdded""""), "no old numRowsAdded field")
  }

  test("JSON payload: optional fields are omitted when None") {
    val request = ReportDeltaMetricsRequest(
      tableId = "some-id",
      report = CommitReportEnvelope(CommitReport(
        numFilesAdded = Some(1L)
        // all other fields None
      ))
    )

    val json = JsonUtils.toJson(request)
    assert(!json.contains(""""num_rows_inserted""""),
      "None fields should be absent from JSON")
    assert(!json.contains(""""num_rows_updated""""),
      "None fields should be absent from JSON")
  }

  // ---------------------------------------------------------------------------
  // Hook lifecycle tests
  // ---------------------------------------------------------------------------

  test("path-based table writes continue without UC metrics hook registration") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath

      // Create and append - if hook ran it would fail (no endpoint configured)
      spark.range(10).write.format("delta").save(tablePath)
      spark.range(10, 20).write.format("delta").mode("append").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      assert(deltaLog.snapshot.version == 1, "Expected 2 commits")
    }
  }

  test("UCMetricsClient: throws RuntimeException on HTTP 5xx response") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(500)
      mockServer.start()

      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME",
        "io.unitycatalog.spark.UCSingleCatalog")
      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set(s"spark.sql.catalog.$TEST_CATALOG_NAME.token", "test-token")

      val request = ReportDeltaMetricsRequest(
        tableId = "test-id",
        report = CommitReportEnvelope(CommitReport(numFilesAdded = Some(1L)))
      )
      // Client should throw on 5xx; the hook catches and logs this as a warning
      intercept[RuntimeException] {
        UCMetricsClient.sendMetrics(spark, request, catalogName = Some(TEST_CATALOG_NAME))
      }
      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request even on error")
    } finally {
      mockServer.stop()
    }
  }

  test("UCMetricsClient: sends correct Authorization header and JSON body") {
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
        report = CommitReportEnvelope(CommitReport(
          numFilesAdded = Some(5L),
          numBytesAdded = Some(5000L),
          numRowsInserted = Some(100L)
        ))
      )
      UCMetricsClient.sendMetrics(spark, request, catalogName = Some(TEST_CATALOG_NAME))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")

      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined, "Authorization header should be present")
      assert(authHeader.get == "Bearer test-token-123", "Auth token must match")

      val body = mockServer.getLastRequestBody()
      assert(body.contains(""""table_id":"abc-123""""), "body must contain table_id")
      assert(body.contains(""""num_files_added":5"""), "body must contain num_files_added")
      assert(body.contains(""""num_rows_inserted":100"""), "body must contain num_rows_inserted")
    } finally {
      mockServer.stop()
    }
  }

  test("UCMetricsClient: auth.type=static with auth.token (new auth.* format)") {
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
        report = CommitReportEnvelope(CommitReport(numFilesAdded = Some(1L)))
      )
      UCMetricsClient.sendMetrics(spark, request, catalogName = Some(authStaticCatalog))

      assert(mockServer.getRequestCount() == 1, "Expected 1 HTTP request")
      val authHeader = mockServer.getLastHeaders().get("Authorization")
      assert(authHeader.isDefined, "Authorization header should be present")
      assert(authHeader.get == "Bearer auth-static-token-456",
        "Auth token from auth.token must match")
    } finally {
      mockServer.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Smoke test - exercises run() end-to-end with a real DeltaLog + mock server
  // ---------------------------------------------------------------------------

  test("run(): fires HTTP POST with correct payload for UC-managed table") {
    val mockServer = new SimpleMockServer(0)
    try {
      mockServer.setResponseCode(200)
      mockServer.start()

      withTempDir { dir =>
        // Real DeltaLog so tableId is a valid UUID
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

        // catalog identifier causes isUCManagedTable to return true
        val catalogTable = CatalogTable(
          identifier = TableIdentifier("t", Some("default"), Some("spark_catalog")),
          tableType = CatalogTableType.MANAGED,
          storage = CatalogStorageFormat.empty,
          schema = new StructType()
        )

        val addFile = AddFile("f1.parquet", Map.empty, 4096L, 0L, dataChange = true,
          stats = """{"numRecords":50}""")
        val commitInfo = CommitInfo(
          version = Some(0L),
          inCommitTimestamp = None,
          timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
          userId = None, userName = None,
          operation = "WRITE",
          operationParameters = Map.empty,
          job = None, notebook = None, clusterId = None,
          readVersion = None, isolationLevel = None,
          isBlindAppend = Some(true),
          operationMetrics = Some(Map("numOutputRows" -> "50")),
          userMetadata = None, tags = None, engineInfo = None, txnId = None)

        val txn = CommittedTransaction(
          txnId = "smoke-txn",
          deltaLog = deltaLog,
          catalogTable = Some(catalogTable),
          readSnapshot = snapshot,
          committedVersion = 0L,
          committedActions = Seq(commitInfo, addFile),
          postCommitSnapshot = snapshot,
          postCommitHooks = Seq.empty,
          txnExecutionTimeMs = 0L,
          needsCheckpoint = false,
          partitionsAddedToOpt = None,
          isBlindAppend = true
        )

        UpdateMetricsHook(Some(catalogTable)).run(spark, txn)

        assert(mockServer.getRequestCount() == 1, "Expected exactly 1 HTTP POST")
        val body = mockServer.getLastRequestBody()
        assert(body.contains(s""""table_id":"${deltaLog.tableId}""""),
          "payload must contain the table's UUID")
        assert(body.contains(""""num_files_added":1"""), "1 AddFile committed")
        assert(body.contains(""""num_rows_inserted":50"""),
          "numOutputRows from operationMetrics")
        assert(body.contains(""""commit_version":0"""),
          "commit_version required for server staleness check")
      }
    } finally {
      mockServer.stop()
    }
  }
}

/**
 * Simple mock HTTP server for testing.
 *
 * Accepts one connection at a time, returns a configurable status code,
 * and captures request headers and body for assertion.
 */
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
            case _: java.net.SocketException if !running => // expected on stop()
            case _: Exception =>
          }
        }
      }
    })
    serverThread.setDaemon(true)
    serverThread.start()

    Thread.sleep(100) // allow server to bind
  }

  private def handleRequest(clientSocket: Socket): Unit = {
    try {
      val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      val out = new PrintWriter(clientSocket.getOutputStream, true)

      in.readLine() // request line

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
