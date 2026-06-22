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
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{CatalogTableUtils, JsonUtils}

import org.apache.spark.sql.SaveMode

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdateMetricsHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {
  private val TEST_UC_TABLE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val CONF_KEY = DeltaSQLConf.DELTA_UC_COMMIT_METRICS_ENABLED.key

  // The Spark-registered catalog class used in run() tests; extends AbstractDeltaCatalog and
  // wires up an AbstractDeltaCatalogClient when deltaRestApi.enabled=true on the options.
  private val DELTA_CATALOG_CLASS = "org.apache.spark.sql.delta.catalog.DeltaCatalogV1"

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

  private def ucStorageFormat(
      ucTableId: UUID = TEST_UC_TABLE_ID): CatalogStorageFormat = {
    CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map(
        UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> ucTableId.toString,
        "delta.feature.catalogManaged" -> "supported"
      )
    )
  }

  // Unique-per-test catalog name so each test's Spark-cached catalog instance is rebuilt and
  // points at this test's fresh mock-server port (Spark CatalogManager caches the catalog
  // instance by name across tests within a session).
  private val catalogCounter = new AtomicInteger(0)

  /**
   * Sets up `spark.sql.catalog.<name>.*` confs pointing at the mock HTTP server. The hook
   * reads these confs at commit time, builds a fresh `UCDeltaCatalogClientImpl` inside the
   * async dispatch, and forwards to `UCDeltaTokenBasedRestClient.reportMetrics`, which
   * posts to the mock at `/delta/v1/catalogs/<cat>/schemas/<sch>/tables/<tbl>/metrics`.
   */
  private def withMockMetricsServer(
      responseCode: Int = 200,
      responseDelay: Int = 0,
      enableFlag: Boolean = true,
      catalogName: String = null)(
      testBody: (SimpleMockServer, DeltaLog, CatalogTable) => Unit): Unit = {
    val effectiveCatalog = Option(catalogName)
      .getOrElse(s"mock_cat_${catalogCounter.incrementAndGet()}")
    val mockServer = new SimpleMockServer(0)
    mockServer.setResponseCode(responseCode)
    if (responseDelay > 0) { mockServer.setResponseDelay(responseDelay) }
    mockServer.start()
    try {
      spark.conf.set(CONF_KEY, enableFlag.toString)
      spark.conf.set(s"spark.sql.catalog.$effectiveCatalog", DELTA_CATALOG_CLASS)
      spark.conf.set(s"spark.sql.catalog.$effectiveCatalog.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set(s"spark.sql.catalog.$effectiveCatalog.deltaRestApi.enabled", "true")
      spark.conf.set(s"spark.sql.catalog.$effectiveCatalog.auth.type", "static")
      spark.conf.set(s"spark.sql.catalog.$effectiveCatalog.auth.token", "test-token")
      // Eagerly resolve the catalog so initialize() runs and wires the client; otherwise the
      // hook's first lookup races against catalog construction.
      spark.sessionState.catalogManager.catalog(effectiveCatalog)
      withTempDir { dir =>
        spark.range(1).write.format("delta")
          .save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(
          spark, dir.getCanonicalPath)
        val ct = CatalogTable(
          identifier = TableIdentifier(
            "t", Some("d"), Some(effectiveCatalog)),
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
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> TEST_UC_TABLE_ID.toString
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

  // NOTE: tests for `UCDeltaCatalogClientImpl.buildCommitReport` live in
  // `UCDeltaCatalogClientImplMetricsSuite` (same-package as the function).

  // -- HTTP / wire-shape tests via mock server (end-to-end through DeltaCatalogV1) --

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

      // Pin the new endpoint path (kebab-case URL template from delta.yaml). Match on
      // suffix so we stay robust to any ApiClient base-path that future SDK changes might
      // inject ahead of the endpoint template.
      val path = mockServer.getLastRequestPath()
      val cat = ct.identifier.catalog.get
      val expectedSuffix = s"/delta/v1/catalogs/$cat/schemas/d/tables/t/metrics"
      assert(path.endsWith(expectedSuffix),
        s"path should end with the new reportMetrics endpoint $expectedSuffix, got: $path")

      // Pin the kebab-case JSON wire shape produced by the SDK serializer.
      val body = mockServer.getLastRequestBody()
      val payload = JsonUtils.fromJson[Map[String, Any]](body)
      assert(payload.contains("table-id"), "table-id (kebab) required, got keys " + payload.keys)
      assert(payload("table-id") == TEST_UC_TABLE_ID.toString,
        "table-id must match the UC table id")
      val report = payload("report").asInstanceOf[Map[String, Any]]
      assert(report.contains("commit-report"),
        "report.commit-report (kebab) required")
      val cr = report("commit-report").asInstanceOf[Map[String, Any]]
      assert(cr("num-files-added").asInstanceOf[Number].longValue == 1L)
      assert(cr("num-bytes-added").asInstanceOf[Number].longValue == 4096L)
      assert(cr("num-rows-inserted").asInstanceOf[Number].longValue == 50L)
    }
  }

  test("run(): async error path logs warning without crashing") {
    withMockMetricsServer(responseCode = 500) {
        (mockServer, deltaLog, ct) =>
      UpdateMetricsHook(Some(ct)).run(
        spark, makeTxn(deltaLog, ct))
      assert(UpdateMetricsHook.awaitCompletion(10000),
        "async send should complete even on error")
      // SDK ApiClient may retry on 5xx; accept any non-zero count rather than pinning a
      // specific retry policy.
      assert(mockServer.getRequestCount() >= 1,
        "at least one request should have been sent despite 5xx")
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

  test("run(): silent no-op when catalog opted out via deltaRestApi.enabled=false") {
    val mockServer = new SimpleMockServer(0)
    mockServer.start()
    val noClientCatalog = s"no_client_catalog_${catalogCounter.incrementAndGet()}"
    try {
      spark.conf.set(CONF_KEY, "true")
      spark.conf.set(s"spark.sql.catalog.$noClientCatalog.deltaRestApi.enabled", "false")
      spark.conf.set(s"spark.sql.catalog.$noClientCatalog.uri",
        s"http://localhost:${mockServer.getPort()}")
      withTempDir { dir =>
        spark.range(1).write.format("delta").save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val ct = CatalogTable(
          identifier = TableIdentifier("t", Some("d"), Some(noClientCatalog)),
          tableType = CatalogTableType.MANAGED,
          storage = ucStorageFormat(),
          schema = new StructType())
        UpdateMetricsHook(Some(ct)).run(spark, makeTxn(deltaLog, ct))
        UpdateMetricsHook.awaitCompletion(5000)
        assert(mockServer.getRequestCount() == 0,
          "no HTTP request when deltaRestApi.enabled is false")
      }
    } finally {
      spark.conf.set(CONF_KEY, "false")
      mockServer.stop()
    }
  }

  test("run(): silent no-op when catalog name has no spark.conf entries") {
    val unknownCatalog = s"unknown_catalog_${catalogCounter.incrementAndGet()}"
    spark.conf.set(CONF_KEY, "true")
    try {
      withTempDir { dir =>
        spark.range(1).write.format("delta").save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val ct = CatalogTable(
          identifier = TableIdentifier("t", Some("d"), Some(unknownCatalog)),
          tableType = CatalogTableType.MANAGED,
          storage = ucStorageFormat(),
          schema = new StructType())
        // No spark.sql.catalog.<unknownCatalog>.* entries -> catalogOptionsFor returns an
        // empty map -> fromCatalogOptionsIfEnabled returns None -> hook no-ops without
        // crashing or scheduling network work.
        UpdateMetricsHook(Some(ct)).run(spark, makeTxn(deltaLog, ct))
        assert(UpdateMetricsHook.awaitCompletion(5000),
          "any scheduled async work should complete promptly")
        assert(UpdateMetricsHook.activeRequests.get() == 0,
          "activeRequests must settle to 0")
      }
    } finally {
      spark.conf.set(CONF_KEY, "false")
    }
  }

  test("catalogOptionsFor: pulls only the named catalog's keys with the prefix stripped") {
    val cat = s"opts_test_${catalogCounter.incrementAndGet()}"
    val otherCat = s"opts_test_other_${catalogCounter.incrementAndGet()}"
    try {
      spark.conf.set(s"spark.sql.catalog.$cat.uri", "http://example/uc")
      spark.conf.set(s"spark.sql.catalog.$cat.deltaRestApi.enabled", "true")
      spark.conf.set(s"spark.sql.catalog.$cat.auth.type", "static")
      // Confs on a different catalog name must not leak into the result.
      spark.conf.set(s"spark.sql.catalog.$otherCat.uri", "http://example/other")

      val opts = UpdateMetricsHook.catalogOptionsFor(spark, cat)
      assert(opts.get("uri") == "http://example/uc")
      assert(opts.get("deltaRestApi.enabled") == "true")
      assert(opts.get("auth.type") == "static")
      assert(opts.size() == 3,
        s"only the named catalog's keys should appear, got: $opts")
    } finally {
      spark.conf.unset(s"spark.sql.catalog.$cat.uri")
      spark.conf.unset(s"spark.sql.catalog.$cat.deltaRestApi.enabled")
      spark.conf.unset(s"spark.sql.catalog.$cat.auth.type")
      spark.conf.unset(s"spark.sql.catalog.$otherCat.uri")
    }
  }

  test("catalogOptionsFor: returns empty map when no entries exist for the name") {
    val opts = UpdateMetricsHook.catalogOptionsFor(
      spark, s"definitely_unconfigured_${catalogCounter.incrementAndGet()}")
    assert(opts.isEmpty, s"expected empty map, got: $opts")
  }

  test("run(): each commit produces its own request (no cross-commit client caching)") {
    withMockMetricsServer() { (mockServer, deltaLog, ct) =>
      val hook = UpdateMetricsHook(Some(ct))
      val commits = 3
      for (_ <- 1 to commits) {
        hook.run(spark, makeTxn(deltaLog, ct))
      }
      assert(UpdateMetricsHook.awaitCompletion(10000),
        "all async sends should complete within timeout")
      assert(mockServer.getRequestCount() == commits,
        s"Expected exactly $commits HTTP POSTs (one per commit), " +
        s"got ${mockServer.getRequestCount()}")
    }
  }

  test("UnusedLoader: throws when invoked so a future caller failure is loud, not silent") {
    val ex = intercept[UnsupportedOperationException] {
      UpdateMetricsHook.UnusedLoader(Identifier.of(Array("x"), "y"))
    }
    assert(ex.getMessage.contains("loadTable"),
      s"error message should mention loadTable, got: ${ex.getMessage}")
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

  test("run(): does not trigger state reconstruction") {
    // Disable test-only CRC verification paths that would force state reconstruction.
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
        "false"
    ) {
      withMockMetricsServer() { (mockServer, deltaLog, ct) =>
        deltaLog
          .startTransaction()
          .commit(Seq.empty, DeltaOperations.Write(SaveMode.Append))
        val postCommitSnapshot = deltaLog.snapshot
        assert(!postCommitSnapshot.stateReconstructionTriggered,
          "pre-condition: snapshot should not have state reconstruction before hook runs")

        val txn = makeTxn(deltaLog, ct)
        UpdateMetricsHook(Some(ct)).run(spark, txn)
        assert(UpdateMetricsHook.awaitCompletion(10000),
          "async hook should complete within timeout")
        assert(mockServer.getRequestCount() == 1,
          "hook should have sent metrics")
        assert(!postCommitSnapshot.stateReconstructionTriggered,
          "hook must not trigger state reconstruction")
      }
    }
  }
}

/**
 * Minimal HTTP/1.1 server that records request line / headers / body for the last request.
 * Sufficient to pin the request path and JSON shape produced by the SDK's
 * `TablesApi.reportMetrics`.
 */
class SimpleMockServer(port: Int) {
  private var serverSocket: ServerSocket = _
  private var serverThread: Thread = _
  @volatile private var running = false
  private var responseCode = 200
  private var responseDelay = 0
  private val requestCount = new AtomicInteger(0)
  @volatile private var lastRequestBody = ""
  @volatile private var lastHeaders = Map[String, String]()
  @volatile private var lastRequestPath = ""
  private var actualPort = port

  def setResponseCode(code: Int): Unit = { responseCode = code }
  def setResponseDelay(delayMs: Int): Unit = { responseDelay = delayMs }

  def getPort(): Int = actualPort

  def getRequestCount(): Int = requestCount.get()

  def getLastRequestBody(): String = lastRequestBody

  def getLastHeaders(): Map[String, String] = lastHeaders

  def getLastRequestPath(): String = lastRequestPath

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

      // Request line: "POST /path HTTP/1.1"
      val requestLine = in.readLine()
      val requestPath = if (requestLine != null) {
        val parts = requestLine.split(" ", 3)
        if (parts.length >= 2) parts(1) else ""
      } else ""

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
      lastRequestPath = requestPath

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
