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

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Operation
import io.delta.kernel.commit.{CommitFailedException, CommitMetadata}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.{Engine, MetricsReporter}
import io.delta.kernel.exceptions.MaxCommitRetryLimitReachedException
import io.delta.kernel.metrics.MetricsReport
import io.delta.kernel.test.{BaseMockJsonHandler, MockFileSystemClientUtils}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}
import io.delta.unity.metrics.UcCommitTelemetry

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class UcCommitterTelemetrySuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with MockFileSystemClientUtils {

  /** Custom MetricsReporter that captures UcCommitTelemetry.Report instances */
  class CapturingMetricsReporter extends MetricsReporter {
    val reports = ArrayBuffer[UcCommitTelemetry#Report]()

    override def report(report: MetricsReport): Unit = {
      report match {
        case ucReport: UcCommitTelemetry#Report => reports.append(ucReport)
        case _ => // Ignore other report types
      }
    }
  }

  /** Creates an Engine with a custom MetricsReporter for testing telemetry */
  private def createEngineWithMetricsCapture(reporter: MetricsReporter): Engine = {
    val hadoopConf = new Configuration()
    new DefaultEngine(
      new io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO(hadoopConf)) {
      override def getMetricsReporters: java.util.List[MetricsReporter] = {
        val reporters = new java.util.ArrayList[MetricsReporter]()
        reporters.add(reporter)
        reporters
      }
    }
  }

  test("commit metrics for CREATE and WRITE operations") {
    withTempDirAndEngine { case (tablePathUnresolved, _) =>
      val reporter = new CapturingMetricsReporter()
      val engine = createEngineWithMetricsCapture(reporter)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (ucClient, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()

      // CREATE -- v0.json
      val result0 = ucCatalogManagedClient
        .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable() /* dataActions */ )
      initializeUCTable(ucClient, "ucTableId")

      // Verify CREATE metrics
      assert(reporter.reports.size === 1)
      val createReport = reporter.reports.head
      assert(createReport.operationType === "UcCommit")
      assert(createReport.ucTableId === "ucTableId")
      assert(createReport.ucTablePath === tablePath)
      assert(createReport.commitVersion === 0)
      assert(createReport.commitType === CommitMetadata.CommitType.CATALOG_CREATE)
      assert(createReport.exception.isEmpty)

      val createMetrics = createReport.metrics
      assert(createMetrics.totalCommitDurationNs > 0)
      assert(createMetrics.writeCommitFileDurationNs > 0)
      assert(createMetrics.commitToUcServerDurationNs === 0)

      reporter.reports.clear()

      // WRITE -- v1.uuid.json
      result0
        .getPostCommitSnapshot
        .get()
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      // Verify WRITE metrics
      assert(reporter.reports.size === 1)
      val writeReport = reporter.reports.head
      assert(writeReport.operationType === "UcCommit")
      assert(writeReport.ucTableId === "ucTableId")
      assert(writeReport.ucTablePath === tablePath)
      assert(writeReport.commitVersion === 1)
      assert(writeReport.commitType === CommitMetadata.CommitType.CATALOG_WRITE)
      assert(writeReport.exception.isEmpty)

      val writeMetrics = writeReport.metrics
      assert(writeMetrics.totalCommitDurationNs > 0)
      assert(writeMetrics.writeCommitFileDurationNs > 0)
      assert(writeMetrics.commitToUcServerDurationNs > 0)
      assert(
        writeMetrics.totalCommitDurationNs >=
          writeMetrics.writeCommitFileDurationNs + writeMetrics.commitToUcServerDurationNs)
      assert(writeReport.reportUUID != createReport.reportUUID)
    }
  }

  test("telemetry captures exceptions during commit") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      // ===== GIVEN =====
      val reporter = new CapturingMetricsReporter()
      val throwingJsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            path: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          throw new java.io.IOException("Simulated network failure")
      }
      val throwingEngineWithReporter = new DefaultEngine(
        new io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO(new Configuration())) {
        override def getJsonHandler = throwingJsonHandler
        override def getMetricsReporters = java.util.Arrays.asList(reporter)
      }

      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (_, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()

      // ===== WHEN =====
      intercept[MaxCommitRetryLimitReachedException] {
        ucCatalogManagedClient
          .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
          .withMaxRetries(0)
          .build(throwingEngineWithReporter)
          .commit(throwingEngineWithReporter, CloseableIterable.emptyIterable())
      }

      // ===== THEN =====
      assert(reporter.reports.size === 1)
      val report = reporter.reports.head
      assert(report.operationType === "UcCommit")
      assert(report.ucTableId === "ucTableId")
      assert(report.commitVersion === 0)
      assert(report.commitType === CommitMetadata.CommitType.CATALOG_CREATE)
      assert(report.exception.isPresent)
      assert(report.exception.get().contains("CommitFailedException"))
      assert(report.exception.get().contains("Simulated network failure"))
    }
  }
}
