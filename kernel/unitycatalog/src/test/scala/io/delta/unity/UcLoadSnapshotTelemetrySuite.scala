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

package io.delta.unity

import java.util.Optional

import io.delta.kernel.engine.Engine
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.CloseableIterable
import io.delta.unity.metrics.UcLoadSnapshotTelemetry

import org.scalatest.funsuite.AnyFunSuite

class UcLoadSnapshotTelemetrySuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with MockFileSystemClientUtils {

  /**
   * Helper to set up a table with v0 published and v1, v2 as staged commits.
   *
   * @return timestamp between v1 and v2 creation
   */
  private def setupTableWithCommits(
      engine: Engine,
      tablePath: String,
      ucClient: InMemoryUCClient,
      ucCatalogManagedClient: UCCatalogManagedClient): Long = {
    val result0 = ucCatalogManagedClient
      .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
      .build(engine)
      .commit(engine, CloseableIterable.emptyIterable())

    val tableData = new InMemoryUCClient.TableData(-1, scala.collection.mutable.ArrayBuffer())
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)

    val result1 = result0.getPostCommitSnapshot.get()
      .buildUpdateTableTransaction("engineInfo", io.delta.kernel.Operation.MANUAL_UPDATE)
      .build(engine)
      .commit(engine, CloseableIterable.emptyIterable())

    val timestampBetweenV1AndV2 = System.currentTimeMillis()

    Thread.sleep(100) // Ensure v2 timestamp is sufficiently after our captured timestamp

    val result2 = result1.getPostCommitSnapshot.get()
      .buildUpdateTableTransaction("engineInfo", io.delta.kernel.Operation.MANUAL_UPDATE)
      .build(engine)
      .commit(engine, CloseableIterable.emptyIterable())

    timestampBetweenV1AndV2
  }

  test("snapshot loading metrics for latest version") {
    withTempDirAndEngine { case (tablePathUnresolved, _) =>
      // ===== GIVEN =====
      val reporter = new CapturingMetricsReporter[UcLoadSnapshotTelemetry#Report]
      val engine = createEngineWithMetricsCapture(reporter)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (ucClient, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()
      setupTableWithCommits(engine, tablePath, ucClient, ucCatalogManagedClient)
      reporter.reports.clear()

      // ===== WHEN =====
      ucCatalogManagedClient.loadSnapshot(
        engine,
        "ucTableId",
        tablePath,
        Optional.empty(),
        Optional.empty())

      // ===== THEN =====
      assert(reporter.reports.size === 1)
      val report = reporter.reports.head
      assert(report.operationType === "UcLoadSnapshot")
      assert(report.ucTableId === "ucTableId")
      assert(report.ucTablePath === tablePath)
      assert(report.versionOpt.isEmpty)
      assert(report.timestampOpt.isEmpty)
      assert(!report.exception.isPresent)

      val metrics = report.metrics
      assert(metrics.totalLoadSnapshotDurationNs > 0)
      assert(metrics.getCommitsDurationNs > 0)
      assert(metrics.numCatalogCommits === 2) // v1.uuid.json and v2.uuid.json
      assert(metrics.kernelSnapshotBuildDurationNs > 0)
      assert(metrics.loadLatestSnapshotForTimestampTimeTravelDurationNs === 0)
      assert(metrics.resolvedSnapshotVersion === 2) // v2 is the latest
      assert(
        metrics.totalLoadSnapshotDurationNs >=
          metrics.getCommitsDurationNs + metrics.kernelSnapshotBuildDurationNs)
    }
  }

  test("snapshot loading metrics with timestamp") {
    withTempDirAndEngine { case (tablePathUnresolved, _) =>
      // ===== GIVEN =====
      val reporter = new CapturingMetricsReporter[UcLoadSnapshotTelemetry#Report]
      val engine = createEngineWithMetricsCapture(reporter)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (ucClient, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()

      val timestampBetweenV1AndV2 =
        setupTableWithCommits(engine, tablePath, ucClient, ucCatalogManagedClient)
      reporter.reports.clear()

      // Time travel to timestamp between v1 and v2 - should resolve to v1
      ucCatalogManagedClient.loadSnapshot(
        engine,
        "ucTableId",
        tablePath,
        Optional.empty(),
        Optional.of(timestampBetweenV1AndV2))

      // Verify snapshot loading metrics
      assert(reporter.reports.size === 1)
      val report = reporter.reports.head
      assert(report.operationType === "UcLoadSnapshot")
      assert(report.ucTableId === "ucTableId")
      assert(report.ucTablePath === tablePath)
      assert(report.versionOpt.isEmpty)
      assert(report.timestampOpt.isPresent)
      assert(report.timestampOpt.get() === timestampBetweenV1AndV2)
      assert(!report.exception.isPresent)

      val metrics = report.metrics
      assert(metrics.totalLoadSnapshotDurationNs > 0)
      assert(metrics.getCommitsDurationNs > 0)
      assert(metrics.numCatalogCommits === 2) // v1.uuid.json and v2.uuid.json from loading latest
      assert(metrics.kernelSnapshotBuildDurationNs > 0)
      assert(metrics.loadLatestSnapshotForTimestampTimeTravelDurationNs > 0)
      assert(metrics.resolvedSnapshotVersion === 1) // v1, since timestamp is between v1 and v2
    }
  }

  test("telemetry captures exceptions during snapshot loading") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val reporter = new CapturingMetricsReporter[UcLoadSnapshotTelemetry#Report]
      val engineWithReporter = new io.delta.kernel.defaults.engine.DefaultEngine(
        new io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO(
          new org.apache.hadoop.conf.Configuration())) {
        override def getMetricsReporters = java.util.Arrays.asList(reporter)
      }

      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (_, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()

      intercept[RuntimeException] {
        // Try to load snapshot from non-existent table
        ucCatalogManagedClient.loadSnapshot(
          engineWithReporter,
          "nonExistentTableId",
          tablePath,
          Optional.empty(),
          Optional.empty())
      }

      assert(reporter.reports.size === 1)
      val report = reporter.reports.head
      assert(report.operationType === "UcLoadSnapshot")
      assert(report.ucTableId === "nonExistentTableId")
      assert(report.exception.isPresent)
      assert(report.metrics.numCatalogCommits === -1) // Not set due to failure
      assert(report.metrics.resolvedSnapshotVersion === -1) // Not set due to failure
    }
  }

  test("JSON serialization: success report for latest version") {
    val telemetry = new UcLoadSnapshotTelemetry(
      "ucTableId",
      "ucTablePath",
      Optional.empty(), // versionOpt
      Optional.empty() // timestampOpt
    )

    telemetry.getMetricsCollector.totalSnapshotLoadTimer.record(500)
    telemetry.getMetricsCollector.getCommitsTimer.record(200)
    telemetry.getMetricsCollector.kernelSnapshotBuildTimer.record(250)
    telemetry.getMetricsCollector.setNumCatalogCommits(5)
    telemetry.getMetricsCollector.setResolvedSnapshotVersion(3)

    val report = telemetry.createSuccessReport()

    // scalastyle:off line.size.limit
    val expectedJson =
      s"""
         |{"operationType":"UcLoadSnapshot",
         |"reportUUID":"${report.reportUUID}",
         |"ucTableId":"ucTableId",
         |"ucTablePath":"ucTablePath",
         |"versionOpt":null,
         |"timestampOpt":null,
         |"metrics":{"totalLoadSnapshotDurationNs":500,"getCommitsDurationNs":200,"numCatalogCommits":5,"kernelSnapshotBuildDurationNs":250,"loadLatestSnapshotForTimestampTimeTravelDurationNs":0,"resolvedSnapshotVersion":3},
         |"exception":null}
         |""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit

    assert(report.toJson() === expectedJson)
  }

  test("JSON serialization: failure report") {
    val telemetry = new UcLoadSnapshotTelemetry(
      "ucTableId",
      "ucTablePath",
      Optional.empty(), // versionOpt
      Optional.of(123456789L) // timestampOpt
    )

    telemetry.getMetricsCollector.totalSnapshotLoadTimer.record(100)
    telemetry.getMetricsCollector.getCommitsTimer.record(100)

    val exception = new RuntimeException("Failed to load snapshot")
    val report = telemetry.createFailureReport(exception)

    // scalastyle:off line.size.limit
    val expectedJson =
      s"""
         |{"operationType":"UcLoadSnapshot",
         |"reportUUID":"${report.reportUUID}",
         |"ucTableId":"ucTableId",
         |"ucTablePath":"ucTablePath",
         |"versionOpt":null,
         |"timestampOpt":123456789,
         |"metrics":{"totalLoadSnapshotDurationNs":100,"getCommitsDurationNs":100,"numCatalogCommits":-1,"kernelSnapshotBuildDurationNs":0,"loadLatestSnapshotForTimestampTimeTravelDurationNs":0,"resolvedSnapshotVersion":-1},
         |"exception":"java.lang.RuntimeException: Failed to load snapshot"}
         |""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit

    assert(report.toJson() === expectedJson)
  }
}
