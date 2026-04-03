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
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
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
  private val CONF_KEY = DeltaSQLConf.DELTA_UC_COMMIT_METRICS_ENABLED.key

  private def makeTxn(
      deltaLog: DeltaLog,
      catalogTable: CatalogTable,
      actions: Seq[Action] = Seq.empty): CommittedTransaction = {
    CommittedTransaction(
      txnId = "test-txn", deltaLog = deltaLog,
      catalogTable = Some(catalogTable),
      readSnapshot = deltaLog.snapshot,
      committedVersion = 0L, committedActions = actions,
      postCommitSnapshot = deltaLog.snapshot,
      postCommitHooks = Seq.empty, txnExecutionTimeMs = 0L,
      needsCheckpoint = false, partitionsAddedToOpt = None,
      isBlindAppend = true)
  }

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

  private def withMockMetricsServer(
      responseCode: Int = 200,
      responseDelay: Int = 0,
      enableFlag: Boolean = true)(
      testBody: (SimpleMockServer, DeltaLog, CatalogTable) => Unit): Unit = {
    val mockServer = new SimpleMockServer(0)
    mockServer.setResponseCode(responseCode)
    if (responseDelay > 0) { mockServer.setResponseDelay(responseDelay) }
    mockServer.start()
    try {
      spark.conf.set(CONF_KEY, enableFlag.toString)
      spark.conf.set("spark.sql.catalog.spark_catalog",
        "io.unitycatalog.spark.UCSingleCatalog")
      spark.conf.set("spark.sql.catalog.spark_catalog.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set("spark.sql.catalog.spark_catalog.token",
        "test-token")
      withTempDir { dir =>
        spark.range(1).write.format("delta")
          .save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(
          spark, dir.getCanonicalPath)
        val ct = CatalogTable(
          identifier = TableIdentifier(
            "t", Some("d"), Some("spark_catalog")),
          tableType = CatalogTableType.MANAGED,
          storage = ucStorageFormat(), schema = new StructType())
        testBody(mockServer, deltaLog, ct)
      }
    } finally {
      spark.conf.set(CONF_KEY, "false")
      mockServer.stop()
    }
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
    val request = UpdateMetricsHook.buildRequest(
      "tbl-123", actions, committedVersion = 7L)
    val report = request.report.commitReport

    assert(request.tableId == "tbl-123")
    assert(report.numFilesAdded == 3L, "3 AddFiles")
    assert(report.numFilesRemoved == 1L, "1 RemoveFile")
    assert(report.numBytesAdded == 1024L + 2048L + 4096L,
      "sum of add sizes")
    assert(report.numBytesRemoved == 512L,
      "sum of remove sizes")

    val hist = report.fileSizeHistogram
    assert(hist.commitVersion == 7L, "commit_version must be set")
    assert(hist.sortedBinBoundaries.head == 0L, "bins must start at 0")
    assert(hist.fileCounts.sum == 3L, "histogram covers all AddFiles")
    assert(hist.totalBytes.sum == 1024L + 2048L + 4096L, "histogram totalBytes")
  }

  test("buildRequest: row metrics prefer operationMetrics over file stats") {
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

    val req1 = UpdateMetricsHook.buildRequest(
      "t1", Seq(commitInfoWithMetrics, addFileWithStats), 0L)
    assert(req1.report.commitReport.numRowsInserted == Some(100L),
      "operationMetrics wins over file stats")

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

    val req2 = UpdateMetricsHook.buildRequest(
      "t2", Seq(commitInfoNoMetrics, addFileWithStats), 1L)
    assert(req2.report.commitReport.numRowsInserted == Some(999L),
      "fallback to numLogicalRecords from file stats")

    val req3 = UpdateMetricsHook.buildRequest(
      "t3", Seq(addFileWithStats), 2L)
    assert(req3.report.commitReport.numRowsInserted == Some(999L),
      "fallback works with no CommitInfo in actions")
  }

  test("buildRequest: row metrics for MERGE operation") {
    val ci = CommitInfo(
      version = Some(0L), inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None, operation = "MERGE",
      operationParameters = Map.empty, job = None, notebook = None,
      clusterId = None, readVersion = None, isolationLevel = None,
      isBlindAppend = Some(false),
      operationMetrics = Some(Map(
        "numTargetRowsInserted" -> "10",
        "numTargetRowsDeleted" -> "5",
        "numTargetRowsUpdated" -> "3")),
      userMetadata = None, tags = None, engineInfo = None, txnId = None)
    val req = UpdateMetricsHook.buildRequest("t", Seq(ci), 0L)
    assert(req.report.commitReport.numRowsInserted == Some(10L))
    assert(req.report.commitReport.numRowsRemoved == Some(5L))
    assert(req.report.commitReport.numRowsUpdated == Some(3L))
  }

  test("buildRequest: row metrics for DELETE operation") {
    val ci = CommitInfo(
      version = Some(0L), inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None, operation = "DELETE",
      operationParameters = Map.empty, job = None, notebook = None,
      clusterId = None, readVersion = None, isolationLevel = None,
      isBlindAppend = Some(false),
      operationMetrics = Some(Map("numDeletedRows" -> "42")),
      userMetadata = None, tags = None, engineInfo = None, txnId = None)
    val req = UpdateMetricsHook.buildRequest("t", Seq(ci), 0L)
    assert(req.report.commitReport.numRowsRemoved == Some(42L))
  }

  test("buildRequest: row metrics for UPDATE operation") {
    val ci = CommitInfo(
      version = Some(0L), inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None, operation = "UPDATE",
      operationParameters = Map.empty, job = None, notebook = None,
      clusterId = None, readVersion = None, isolationLevel = None,
      isBlindAppend = Some(false),
      operationMetrics = Some(Map("numUpdatedRows" -> "17")),
      userMetadata = None, tags = None, engineInfo = None, txnId = None)
    val req = UpdateMetricsHook.buildRequest("t", Seq(ci), 0L)
    assert(req.report.commitReport.numRowsUpdated == Some(17L))
  }

  test("JSON payload validation - matches server contract (snake_case, nested)") {
    val request = ReportDeltaMetricsRequest(
      tableId = "test-table-id-123",
      report = CommitReportEnvelope(CommitReport(
        numFilesAdded = 10L,
        numFilesRemoved = 2L,
        numBytesAdded = 10000L,
        numBytesRemoved = 2000L,
        numRowsInserted = Some(1000L),
        numRowsRemoved = Some(200L),
        numRowsUpdated = Some(50L),
        fileSizeHistogram = FileSizeHistogramPayload(
          sortedBinBoundaries = Seq(0L, 1024L),
          fileCounts = Seq(5L, 5L),
          totalBytes = Seq(2000L, 8000L),
          commitVersion = 42L
        )
      ))
    )

    val json = JsonUtils.toJson(request)

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

    assert(!json.contains(""""tableId""""), "no camelCase tableId")
    assert(!json.contains(""""numFilesAdded""""), "no camelCase numFilesAdded")
    assert(!json.contains(""""numRowsAdded""""), "no old numRowsAdded field")
  }

  test("JSON payload: optional fields are omitted when None") {
    val request = ReportDeltaMetricsRequest(
      tableId = "some-id",
      report = CommitReportEnvelope(CommitReport(
        numFilesAdded = 1L,
        numFilesRemoved = 0L,
        numBytesAdded = 100L,
        numBytesRemoved = 0L,
        fileSizeHistogram = FileSizeHistogramPayload(
          sortedBinBoundaries = Seq(0L), fileCounts = Seq(1L),
          totalBytes = Seq(100L), commitVersion = 0L)
      ))
    )

    val json = JsonUtils.toJson(request)
    assert(!json.contains(""""num_rows_inserted""""),
      "None fields should be absent from JSON")
    assert(!json.contains(""""num_rows_updated""""),
      "None fields should be absent from JSON")
  }

  test("path-based table writes continue without UC metrics hook") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      spark.range(10).write.format("delta").save(tablePath)
      spark.range(10, 20).write.format("delta").mode("append").save(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      assert(deltaLog.snapshot.version == 1, "Expected 2 commits")
    }
  }

  test("sendMetrics: throws RuntimeException on HTTP 5xx") {
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
      spark.conf.set(
        s"spark.sql.catalog.$TEST_CATALOG_NAME.token", "test-token")

      val request = ReportDeltaMetricsRequest(
        tableId = "test-id",
        report = CommitReportEnvelope(CommitReport(
          numFilesAdded = 1L, numFilesRemoved = 0L,
          numBytesAdded = 100L, numBytesRemoved = 0L,
          fileSizeHistogram = FileSizeHistogramPayload(
            sortedBinBoundaries = Seq(0L), fileCounts = Seq(1L),
            totalBytes = Seq(100L), commitVersion = 0L))))
      intercept[RuntimeException] {
        UpdateMetricsHook.sendMetrics(
          spark, request, catalogName = Some(TEST_CATALOG_NAME))
      }
      assert(mockServer.getRequestCount() == 1,
        "Expected 1 HTTP request even on error")
    } finally {
      mockServer.stop()
    }
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
        report = CommitReportEnvelope(CommitReport(
          numFilesAdded = 5L, numFilesRemoved = 0L,
          numBytesAdded = 5000L, numBytesRemoved = 0L,
          numRowsInserted = Some(100L),
          fileSizeHistogram = FileSizeHistogramPayload(
            sortedBinBoundaries = Seq(0L), fileCounts = Seq(5L),
            totalBytes = Seq(5000L), commitVersion = 0L)))
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
        report = CommitReportEnvelope(CommitReport(
          numFilesAdded = 1L, numFilesRemoved = 0L,
          numBytesAdded = 100L, numBytesRemoved = 0L,
          fileSizeHistogram = FileSizeHistogramPayload(
            sortedBinBoundaries = Seq(0L), fileCounts = Seq(1L),
            totalBytes = Seq(100L), commitVersion = 0L)))
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

  // Async dispatch tests: verify end-to-end send with payload
  // validation, feature-flag gating, non-blocking
  // behavior, error resilience, and skip logic for non-UC /
  // missing-table-ID tables.

  test("run(): skips when commitMetrics.enabled is false") {
    withMockMetricsServer(enableFlag = false) {
        (mockServer, deltaLog, ct) =>
      UpdateMetricsHook(Some(ct)).run(
        spark, makeTxn(deltaLog, ct))
      UpdateMetricsHook.awaitCompletion(5000)
      assert(mockServer.getRequestCount() == 0,
        "no HTTP request when feature flag is disabled")
    }
  }

  test("run(): sends metrics asynchronously for UC-managed table") {
    withMockMetricsServer() { (mockServer, deltaLog, ct) =>
      val addFile = AddFile("f1.parquet", Map.empty, 4096L, 0L,
        dataChange = true, stats = """{"numRecords":50}""")
      val commitInfo = CommitInfo(version = Some(0L),
        inCommitTimestamp = None,
        timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
        userId = None, userName = None, operation = "WRITE",
        operationParameters = Map.empty, job = None, notebook = None,
        clusterId = None, readVersion = None, isolationLevel = None,
        isBlindAppend = Some(true),
        operationMetrics = Some(Map("numOutputRows" -> "50")),
        userMetadata = None, tags = None, engineInfo = None,
        txnId = None)
      val txn = makeTxn(deltaLog, ct, Seq(commitInfo, addFile))
      UpdateMetricsHook(Some(ct)).run(spark, txn)
      assert(UpdateMetricsHook.awaitCompletion(10000),
        "async send should complete within timeout")
      assert(mockServer.getRequestCount() == 1,
        "Expected exactly 1 HTTP POST")
      val body = mockServer.getLastRequestBody()
      val expected = UpdateMetricsHook.buildRequest(
        TEST_UC_TABLE_ID, Seq(commitInfo, addFile), 0L)
      assert(
        JsonUtils.fromJson[Map[String, Any]](body) ==
          JsonUtils.fromJson[Map[String, Any]](
            JsonUtils.toJson(expected)),
        "payload should match expected JSON")
    }
  }

  test("run(): async error path logs warning without crashing") {
    withMockMetricsServer(responseCode = 500) {
        (mockServer, deltaLog, ct) =>
      UpdateMetricsHook(Some(ct)).run(
        spark, makeTxn(deltaLog, ct))
      assert(UpdateMetricsHook.awaitCompletion(10000),
        "async send should complete even on error")
      assert(mockServer.getRequestCount() == 1,
        "request should have been sent despite 5xx")
      assert(UpdateMetricsHook.activeRequests.get() == 0,
        "activeRequests must return to 0 after error")
    }
  }

  test("run(): returns immediately (async) even with slow server") {
    withMockMetricsServer(responseDelay = 3000) {
        (mockServer, deltaLog, ct) =>
      val startMs = System.currentTimeMillis()
      UpdateMetricsHook(Some(ct)).run(
        spark, makeTxn(deltaLog, ct))
      val elapsedMs = System.currentTimeMillis() - startMs
      assert(elapsedMs < 1000,
        s"run() should return immediately but took ${elapsedMs}ms")
      assert(UpdateMetricsHook.awaitCompletion(10000),
        "async send should eventually complete")
      assert(mockServer.getRequestCount() == 1,
        "request should arrive after async completion")
    }
  }

  test("run(): skips metrics for non-UC-managed table") {
    withMockMetricsServer() { (mockServer, deltaLog, _) =>
      val nonUcTable = CatalogTable(
        identifier = TableIdentifier(
          "t", Some("d"), Some("spark_catalog")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType())
      UpdateMetricsHook(Some(nonUcTable)).run(
        spark, makeTxn(deltaLog, nonUcTable))
      UpdateMetricsHook.awaitCompletion(5000)
      assert(mockServer.getRequestCount() == 0,
        "no HTTP request for non-UC-managed table")
    }
  }

  test("run(): skips metrics when table ID not in storage properties") {
    withMockMetricsServer() { (mockServer, deltaLog, _) =>
      val noIdTable = CatalogTable(
        identifier = TableIdentifier(
          "t", Some("d"), Some("spark_catalog")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat(
          locationUri = None, inputFormat = None,
          outputFormat = None, serde = None,
          compressed = false,
          properties = Map(
            "delta.feature.catalogManaged" -> "supported")),
        schema = new StructType())
      UpdateMetricsHook(Some(noIdTable)).run(
        spark, makeTxn(deltaLog, noIdTable))
      UpdateMetricsHook.awaitCompletion(5000)
      assert(mockServer.getRequestCount() == 0,
        "no HTTP request when table ID is missing")
    }
  }
}

class SimpleMockServer(port: Int) {
  private var serverSocket: ServerSocket = _
  private var serverThread: Thread = _
  @volatile private var running = false
  private var responseCode = 200
  private var responseDelay = 0
  private val requestCount = new AtomicInteger(0)
  @volatile private var lastRequestBody = ""
  @volatile private var lastHeaders = Map[String, String]()
  private var actualPort = port

  def setResponseCode(code: Int): Unit = { responseCode = code }
  def setResponseDelay(delayMs: Int): Unit = { responseDelay = delayMs }

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

      if (responseDelay > 0) Thread.sleep(responseDelay)

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
