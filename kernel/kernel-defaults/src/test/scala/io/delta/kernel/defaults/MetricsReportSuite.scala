/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.util
import java.util.{Objects, Optional}

import scala.collection.mutable.ArrayBuffer

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.{CommitCoordinatorClientHandler, Engine, ExpressionHandler, FileSystemClient, JsonHandler, MetricsReporter, ParquetHandler}
import io.delta.kernel.metrics.{MetricsReport, SnapshotReport}
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.internal.metrics.Timer
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite to test the Kernel-API created [[MetricsReport]]s. This suite is in the defaults
 * package to be able to use real tables and avoid having to mock both file listings AND file
 * contents.
 */
class MetricsReportSuite extends AnyFunSuite with TestUtils {

  ///////////////////////////
  // SnapshotReport tests //
  //////////////////////////

  /**
   * Given a function [[f]] that generates a snapshot from a [[Table]], runs [[f]] and looks for
   * a generated [[SnapshotReport]]. Exactly 1 [[SnapshotReport]] is expected. Times and returns
   * the duration it takes to run [[f]]. Uses a custom engine to collect emitted metrics reports.
   *
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param path path of the table to query
   * @param expectException whether we expect [[f]] to throw an exception, which if so, is caught
   *                        and returned with the other results
   * @returns (SnapshotReport, durationToRunF, ExceptionIfThrown)
   */
  def getSnapshotReport(
    f: (Table, Engine) => Snapshot,
    path: String,
    expectException: Boolean
  ): (SnapshotReport, Long, Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports: Seq[MetricsReport], exception: Option[Exception]) = if (expectException) {
      collectMetricsReportsAndException { engine =>
        val table = Table.forPath(engine, path)
        timer.time(() => f(table, engine)) // Time the actual operation
      }
    } else {
      (collectMetricsReports { engine =>
        val table = Table.forPath(engine, path)
        timer.time(() => f(table, engine)) // Time the actual operation
      }, Option.empty)
    }

    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(snapshotReports.length == 1, "Expected exactly 1 SnapshotReport")
    (snapshotReports.head.asInstanceOf[SnapshotReport], timer.totalDuration(), exception)
  }

  /**
   * Given a table path and a function [[f]] to generate a snapshot, runs [[f]] and collects the
   * generated [[SnapshotReport]]. Checks that the report is as expected.
   *
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param tablePath table path to query from
   * @param expectException whether we expect f to throw an exception, if so we will check that the
   *                        report contains the thrown exception
   * @param expectedVersion the expected version for the SnapshotReport
   * @param expectedProvidedTimestamp the expected providedTimestamp for the SnapshotReport
   * @param expectNonEmptyTimestampToVersionResolutionDuration whether we expect
   *                                                           timestampToVersionResolutionDuration
   *                                                           to be non-empty (should be true
   *                                                           for any time-travel by timestamp
   *                                                           queries)
   * @param expectNonZeroLoadProtocolAndMetadataDuration whether we expect
   *                                                     loadProtocolAndMetadataDuration to be
   *                                                     non-zero (should be true except when an
   *                                                     exception is thrown before log replay)
   */
  def checkSnapshotReport(
    f: (Table, Engine) => Snapshot,
    path: String,
    expectException: Boolean,
    expectedVersion: Optional[Long],
    expectedProvidedTimestamp: Optional[Long],
    expectNonEmptyTimestampToVersionResolutionDuration: Boolean,
    expectNonZeroLoadProtocolAndMetadataDuration: Boolean
  ): Unit = {

    val (snapshotReport, duration, exception) = getSnapshotReport(f, path, expectException)

    // Verify contents
    assert(snapshotReport.tablePath == resolvePath(path))
    assert(snapshotReport.operationType == "Snapshot")
    exception match {
      case Some(e) =>
        assert(snapshotReport.exception().isPresent &&
          Objects.equals(snapshotReport.exception().get(), e))
      case None => assert(!snapshotReport.exception().isPresent)
    }
    assert(snapshotReport.reportUUID != null)
    assert(Objects.equals(snapshotReport.version, expectedVersion))
    assert(Objects.equals(snapshotReport.providedTimestamp, expectedProvidedTimestamp))

    // Since we cannot know the actual durations of these we sanity check that they are > 0 and
    // less than the total operation duration whenever they are expected to be non-zero/non-empty
    if (expectNonEmptyTimestampToVersionResolutionDuration) {
      assert(snapshotReport.snapshotMetrics.timestampToVersionResolutionDuration.isPresent)
      assert(snapshotReport.snapshotMetrics.timestampToVersionResolutionDuration.get > 0)
      assert(snapshotReport.snapshotMetrics.timestampToVersionResolutionDuration.get <
        duration)
    } else {
      assert(!snapshotReport.snapshotMetrics.timestampToVersionResolutionDuration.isPresent)
    }
    if (expectNonZeroLoadProtocolAndMetadataDuration) {
      assert(snapshotReport.snapshotMetrics.loadInitialDeltaActionsDuration > 0)
      assert(snapshotReport.snapshotMetrics.loadInitialDeltaActionsDuration < duration)
    } else {
      assert(snapshotReport.snapshotMetrics.loadInitialDeltaActionsDuration == 0)
    }
  }

  test("SnapshotReport valid queries") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with version 0, 1
      spark.range(10).write.format("delta").mode("append").save(path)
      val version0timestamp = System.currentTimeMillis
      // Since filesystem modification time might be truncated to the second, we sleep to make sure
      // the next commit is after this timestamp
      Thread.sleep(1000)
      spark.range(10).write.format("delta").mode("append").save(path)

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        path,
        expectException = false,
        expectedVersion = Optional.of(1),
        expectedProvidedTimestamp = Optional.empty(), // No time travel
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.empty(), // No time travel
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version0timestamp),
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.of(version0timestamp),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )
    }
  }

  test("Snapshot report - table does not exist") {
    withTempDir { tempDir =>
      // This fails during either log segment building or timestamp -> version resolution
    val path = tempDir.getCanonicalPath

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        path,
        expectException = true,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.empty(), // No time travel
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, 1000),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(1000),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )
    }
  }

  test("Snapshot report - log is corrupted") {
    // This fails during log segment building for all the following queries
    val path = goldenTablePath("versions-not-contiguous") // version 0, version 2

    // Test getLatestSnapshot
    checkSnapshotReport(
      (table, engine) => table.getLatestSnapshot(engine),
      path,
      expectException = true,
      expectedVersion = Optional.empty(),
      expectedProvidedTimestamp = Optional.empty(), // No time travel
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
      expectNonZeroLoadProtocolAndMetadataDuration = false
    )

    // Test getSnapshotAsOfVersion
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfVersion(engine, 2),
      path,
      expectException = true,
      expectedVersion = Optional.of(2),
      expectedProvidedTimestamp = Optional.empty(), // No time travel
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
      expectNonZeroLoadProtocolAndMetadataDuration = false
    )

    // Test getSnapshotAsOfTimestamp
    // We use timestamp of version 2
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfTimestamp(engine, 1709764755001L),
      path,
      expectException = true,
      expectedVersion = Optional.of(2),
      expectedProvidedTimestamp = Optional.of(1709764755001L),
      expectNonEmptyTimestampToVersionResolutionDuration = true,
      expectNonZeroLoadProtocolAndMetadataDuration = false
    )
  }

  test("Snapshot report - missing metadata") {
    // This fails during P&M loading for all of the following queries
    val path = goldenTablePath("deltalog-state-reconstruction-without-metadata")

    // Test getLatestSnapshot
    checkSnapshotReport(
      (table, engine) => table.getLatestSnapshot(engine),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.empty(), // No time travel
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )

    // Test getSnapshotAsOfVersion
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.empty(), // No time travel
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )

    // Test getSnapshotAsOfTimestamp
    // We use timestamp of version 2
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfTimestamp(engine, 1709764754000L),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.of(1709764754000L),
      expectNonEmptyTimestampToVersionResolutionDuration = true,
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )
  }

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  // For now this just uses the default engine since we have no need to override it, if we would
  // like to use a specific engine in the future for other tests we can simply add another arg here
  /** Executes [[f]] using a special engine implementation to collect and return metrics reports */
  def collectMetricsReports(f: Engine => Unit): Seq[MetricsReport] = {
    // Initialize a buffer for any metric reports and wrap the engine so that they are recorded
    val reports = ArrayBuffer.empty[MetricsReport]
    f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine))
    reports
  }

  /**
   * Executes [[f]] using a special engine implementation to collect and return metrics reports when
   * it is expected [[f]] will throw an exception. Collects said exception and returns with
   * the reports.
   */
  def collectMetricsReportsAndException(
      f: Engine => Unit): (Seq[MetricsReport], Option[Exception]) = {
    // Initialize a buffer for any metric reports and wrap the engine so that they are recorded
    val reports = ArrayBuffer.empty[MetricsReport]
    val e = intercept[Exception](
      f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine))
    )
    (reports, Some(e))
  }

  def resolvePath(path: String): String = {
    defaultEngine.getFileSystemClient.resolvePath(path)
  }

  /**
   * Wraps an {@link Engine} to implement the metrics reporter such that it appends any reports
   * to the provided in memory buffer.
   */
  class EngineWithInMemoryMetricsReporter(buf: ArrayBuffer[MetricsReport], baseEngine: Engine)
    extends Engine {

    private val metricsReporter = new MetricsReporter {
      override def report(report: MetricsReport): Unit = buf.append(report)
    }

    override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler

    override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler

    override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient

    override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler

    override def getCommitCoordinatorClientHandler(
      name: String, conf: util.Map[String, String]): CommitCoordinatorClientHandler =
      baseEngine.getCommitCoordinatorClientHandler(name, conf)

    override def getMetricsReporters(): java.util.List[MetricsReporter] = {
      java.util.Collections.singletonList(metricsReporter)
    }
  }
}
