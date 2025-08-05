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
package io.delta.kernel.defaults.metrics

import java.io.File
import java.util.{Objects, Optional}

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel._
import io.delta.kernel.defaults.test.AbstractTableManagerAdapter
import io.delta.kernel.engine._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.Timer
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.metrics.SnapshotReport

import org.scalatest.funsuite.AnyFunSuite

// Concrete implementation for legacy Table API
class LegacySnapshotReportSuite extends AbstractSnapshotReportSuite {
  import io.delta.kernel.defaults.test.LegacyTableManagerAdapter
  override def tableManager: AbstractTableManagerAdapter = new LegacyTableManagerAdapter()
}

// Concrete implementation for new TableManager API
class TableManagerSnapshotReportSuite extends AbstractSnapshotReportSuite {
  import io.delta.kernel.defaults.test.TableManagerAdapter
  override def tableManager: AbstractTableManagerAdapter = new TableManagerAdapter()
}

abstract class AbstractSnapshotReportSuite extends AnyFunSuite with MetricsReportTestUtils {

  def tableManager: AbstractTableManagerAdapter

  case class SnapshotReportExpectations(
      expectedReportCount: Int,
      expectException: Boolean,
      expectedVersion: Optional[Long],
      expectedCheckpointVersion: Optional[Long],
      expectedProvidedTimestamp: Optional[Long],
      expectNonEmptyTimestampToVersionResolutionDuration: Boolean = false,
      expectNonZeroLoadProtocolAndMetadataDuration: Boolean = true,
      expectNonZeroBuildLogSegmentDuration: Boolean = true,
      expectNonZeroDurationToGetCrcInfo: Boolean = true)

  /**
   * Given a function [[f]] that generates a snapshot from an engine and path, runs [[f]] and looks
   * for a generated [[SnapshotReport]]. Times and returns the duration it takes to run [[f]].
   * Uses a custom engine to collect emitted metrics reports. If more than one report is
   * generated (e.g. during timestamp-based time travel), only the last one is returned.
   *
   * @param f function to generate a snapshot from an engine and path
   * @param path path of the table to query
   * @param expectedReportCount the expected number of [[SnapshotReport]]s to be generated. This
   *                            can be greater than 1 for timestamp-based time travel queries.
   * @param expectException whether we expect [[f]] to throw an exception, which if so, is caught
   *                        and returned with the other results
   * @returns (SnapshotReport, durationToRunF, ExceptionIfThrown)
   */
  def getSnapshotReport(
      f: (Engine, String) => Snapshot,
      path: String,
      expectedReportCount: Int,
      expectException: Boolean): (SnapshotReport, Long, Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        timer.time(() => f(engine, path)) // Time the actual operation
      },
      expectException)

    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(
      snapshotReports.length == expectedReportCount,
      s"Expected exactly $expectedReportCount SnapshotReport")
    (snapshotReports.last.asInstanceOf[SnapshotReport], timer.totalDurationNs(), exception)
  }

  /**
   * Given a table path and a function [[f]] to generate a snapshot, runs [[f]] and collects the
   * generated [[SnapshotReport]]. Checks that the report is as expected.
   *
   * @param f function to generate a snapshot from an engine and path
   * @param path table path to query from
   * @param expectations encapsulates all the expected values and behaviors for the snapshot report.
   *                     See [[SnapshotReportExpectations]] for detailed parameter descriptions.
   */
  def checkSnapshotReport(
      f: (Engine, String) => Snapshot,
      path: String,
      expectations: SnapshotReportExpectations): Unit = {

    val (snapshotReport, duration, exception) =
      getSnapshotReport(f, path, expectations.expectedReportCount, expectations.expectException)

    // Verify contents
    assert(snapshotReport.getTablePath == defaultEngine.getFileSystemClient.resolvePath(path))
    assert(snapshotReport.getOperationType == "Snapshot")
    exception match {
      case Some(e) =>
        assert(snapshotReport.getException().isPresent &&
          Objects.equals(snapshotReport.getException().get(), e))
      case None => assert(!snapshotReport.getException().isPresent)
    }
    assert(snapshotReport.getReportUUID != null)
    assert(
      Objects.equals(snapshotReport.getVersion, expectations.expectedVersion),
      s"Expected version ${expectations.expectedVersion} found ${snapshotReport.getVersion}")
    assert(
      Objects.equals(
        snapshotReport.getCheckpointVersion,
        expectations.expectedCheckpointVersion),
      s"Expected checkpoint version ${expectations.expectedCheckpointVersion}, found " +
        s"${snapshotReport.getCheckpointVersion}")
    assert(Objects.equals(
      snapshotReport.getProvidedTimestamp,
      expectations.expectedProvidedTimestamp))

    // Since we cannot know the actual durations of these we sanity check that they are > 0 and
    // less than the total operation duration whenever they are expected to be non-zero/non-empty

    val metrics = snapshotReport.getSnapshotMetrics

    // ===== Metric: getLoadSnapshotTotalDurationNs =====
    if (!expectations.expectException) {
      assert(metrics.getLoadSnapshotTotalDurationNs > 0)
      assert(metrics.getLoadSnapshotTotalDurationNs <= duration)
    } else {
      assert(metrics.getLoadSnapshotTotalDurationNs >= 0)
    }

    // ===== Metric: getComputeTimestampToVersionTotalDurationNs =====
    if (expectations.expectNonEmptyTimestampToVersionResolutionDuration) {
      assert(metrics.getComputeTimestampToVersionTotalDurationNs.isPresent)
      assert(metrics.getComputeTimestampToVersionTotalDurationNs.get > 0)
      assert(metrics.getComputeTimestampToVersionTotalDurationNs.get < duration)
      assert(metrics.getComputeTimestampToVersionTotalDurationNs.get <=
        metrics.getLoadSnapshotTotalDurationNs)
    } else {
      assert(!metrics.getComputeTimestampToVersionTotalDurationNs.isPresent)
    }

    // ===== Metric: getLoadProtocolMetadataTotalDurationNs  =====
    if (expectations.expectNonZeroLoadProtocolAndMetadataDuration) {
      assert(metrics.getLoadProtocolMetadataTotalDurationNs > 0)
      assert(metrics.getLoadProtocolMetadataTotalDurationNs < duration)
      assert(
        metrics.getLoadProtocolMetadataTotalDurationNs <= metrics.getLoadSnapshotTotalDurationNs)
    } else {
      assert(metrics.getLoadProtocolMetadataTotalDurationNs == 0)
    }

    // ===== Metric: getLoadLogSegmentTotalDurationNs =====
    if (expectations.expectNonZeroBuildLogSegmentDuration) {
      assert(metrics.getLoadLogSegmentTotalDurationNs > 0)
      assert(metrics.getLoadLogSegmentTotalDurationNs < duration)
      assert(metrics.getLoadLogSegmentTotalDurationNs <= metrics.getLoadSnapshotTotalDurationNs)
    } else {
      assert(metrics.getLoadLogSegmentTotalDurationNs == 0)
    }

    // ===== Metric: getLoadCrcTotalDurationNs =====
    if (expectations.expectNonZeroDurationToGetCrcInfo) {
      assert(metrics.getLoadCrcTotalDurationNs > 0)
      assert(metrics.getLoadCrcTotalDurationNs < duration)
      assert(metrics.getLoadCrcTotalDurationNs <= metrics.getLoadSnapshotTotalDurationNs)
    } else {
      assert(metrics.getLoadCrcTotalDurationNs == 0)
    }
  }

  test("SnapshotReport valid queries - no checkpoint") {
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
        (engine, path) => tableManager.getSnapshotAtLatest(engine, path),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = false,
          expectedVersion = Optional.of(1),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty() // No time travel by timestamp
        ))

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 0),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = false,
          expectedVersion = Optional.of(0),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty() // No time travel by timestamp
        ))

      // Test getSnapshotAsOfTimestamp - only if adapter supports it
      if (tableManager.supportsTimestampResolution) {
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, version0timestamp),
          path,
          SnapshotReportExpectations(
            expectedReportCount = 2,
            expectException = false,
            expectedVersion = Optional.of(0),
            expectedCheckpointVersion = Optional.empty(),
            expectedProvidedTimestamp = Optional.of(version0timestamp),
            expectNonEmptyTimestampToVersionResolutionDuration = true))
      }
    }
  }

  test("SnapshotReport valid queries - with checkpoint") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with version 0 to 11 with checkpoint at version 10
      (0 until 11).foreach(_ =>
        spark.range(10).write.format("delta").mode("append").save(path))

      val version11timestamp = System.currentTimeMillis
      // Since filesystem modification time might be truncated to the second, we sleep to make sure
      // the next commit is after this timestamp
      Thread.sleep(1000)
      // create version 11
      spark.range(10).write.format("delta").mode("append").save(path)

      // Test getLatestSnapshot
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtLatest(engine, path),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = false,
          expectedVersion = Optional.of(11),
          expectedCheckpointVersion = Optional.of(10),
          expectedProvidedTimestamp = Optional.empty() // No time travel by timestamp
        ))

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 11),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = false,
          expectedVersion = Optional.of(11),
          expectedCheckpointVersion = Optional.of(10),
          expectedProvidedTimestamp = Optional.empty() // No time travel by timestamp
        ))

      // Test getSnapshotAsOfTimestamp - only if adapter supports it
      if (tableManager.supportsTimestampResolution) {
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, version11timestamp),
          path,
          SnapshotReportExpectations(
            expectedReportCount = 2,
            expectException = false,
            expectedVersion = Optional.of(10),
            expectedCheckpointVersion = Optional.of(10),
            expectedProvidedTimestamp = Optional.of(version11timestamp),
            expectNonEmptyTimestampToVersionResolutionDuration = true))
      }
    }
  }

  test("Snapshot report - invalid time travel parameters") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // Test getSnapshotAsOfVersion with version 1 (does not exist)
      // This fails during log segment building
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 1),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          expectedVersion = Optional.of(1),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
          expectNonZeroLoadProtocolAndMetadataDuration = false,
          expectNonZeroDurationToGetCrcInfo = false))

      // Test getSnapshotAsOfTimestamp with timestamp=0 (does not exist)
      if (tableManager.supportsTimestampResolution) {
        // This fails during timestamp -> version resolution
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, 0),
          path,
          SnapshotReportExpectations(
            expectedReportCount = 2,
            expectException = true,
            expectedVersion = Optional.empty(),
            expectedCheckpointVersion = Optional.empty(),
            expectedProvidedTimestamp = Optional.of(0),
            expectNonEmptyTimestampToVersionResolutionDuration = true,
            expectNonZeroLoadProtocolAndMetadataDuration = false,
            expectNonZeroBuildLogSegmentDuration = false,
            expectNonZeroDurationToGetCrcInfo = false))

        // Test getSnapshotAsOfTimestamp with timestamp=currentTime (does not exist)
        // This fails during timestamp -> version resolution
        val currentTimeMillis = System.currentTimeMillis
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, currentTimeMillis),
          path,
          SnapshotReportExpectations(
            expectedReportCount = 2,
            expectException = true,
            expectedVersion = Optional.empty(),
            expectedCheckpointVersion = Optional.empty(),
            expectedProvidedTimestamp = Optional.of(currentTimeMillis),
            expectNonEmptyTimestampToVersionResolutionDuration = true,
            expectNonZeroLoadProtocolAndMetadataDuration = false,
            expectNonZeroBuildLogSegmentDuration = false,
            expectNonZeroDurationToGetCrcInfo = false))
      }
    }
  }

  test("Snapshot report - table does not exist") {
    withTempDir { tempDir =>
      // This fails during either log segment building or timestamp -> version resolution
      val path = tempDir.getCanonicalPath

      // Test getLatestSnapshot
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtLatest(engine, path),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          expectedVersion = Optional.empty(),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
          expectNonZeroLoadProtocolAndMetadataDuration = false,
          expectNonZeroDurationToGetCrcInfo = false))

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 0),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          expectedVersion = Optional.of(0),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
          expectNonZeroLoadProtocolAndMetadataDuration = false,
          expectNonZeroDurationToGetCrcInfo = false))

      // Test getSnapshotAsOfTimestamp - only if adapter supports it
      if (tableManager.supportsTimestampResolution) {
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, 1000),
          path,
          SnapshotReportExpectations(
            expectedReportCount = 1,
            expectException = true,
            expectedVersion = Optional.empty(),
            expectedCheckpointVersion = Optional.empty(),
            // Query will fail before timestamp -> version resolution. The failure
            // will happen when `getLatestSnapshot` is called.
            expectedProvidedTimestamp = Optional.empty(),
            expectNonZeroLoadProtocolAndMetadataDuration = false,
            // It will first build a lastest snapshot, and a logSegment is built there.
            expectNonZeroBuildLogSegmentDuration = true,
            expectNonZeroDurationToGetCrcInfo = false))
      }
    }
  }

  test("Snapshot report - log is corrupted") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up table with non-contiguous version (0, 2) which will fail during log segment building
      // for all the following queries
      (0 until 3).foreach(_ =>
        spark.range(3).write.format("delta").mode("append").save(path))
      assert(
        new File(FileNames.deltaFile(new Path(tempDir.getCanonicalPath, "_delta_log"), 1)).delete())

      // Test getLatestSnapshot
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtLatest(engine, path),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          expectedVersion = Optional.empty(),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
          expectNonZeroLoadProtocolAndMetadataDuration = false,
          expectNonZeroDurationToGetCrcInfo = false))

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 2),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          expectedVersion = Optional.of(2),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
          expectNonZeroLoadProtocolAndMetadataDuration = false,
          expectNonZeroDurationToGetCrcInfo = false))

      // Test getSnapshotAsOfTimestamp - only if adapter supports it
      if (tableManager.supportsTimestampResolution) {
        val version2Timestamp = new File(
          FileNames.deltaFile(new Path(tempDir.getCanonicalPath, "_delta_log"), 2)).lastModified()
        checkSnapshotReport(
          (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, version2Timestamp),
          tempDir.getCanonicalPath,
          SnapshotReportExpectations(
            expectedReportCount = 1,
            expectException = true,
            // Query will fail before timestamp -> version resolution. The failure
            // will happen when `getLatestSnapshot` is called.
            expectedVersion = Optional.empty(),
            expectedCheckpointVersion = Optional.empty(),
            expectedProvidedTimestamp = Optional.empty(),
            expectNonZeroLoadProtocolAndMetadataDuration = false,
            expectNonZeroDurationToGetCrcInfo = false))
      }
    }
  }

  test("Snapshot report - missing metadata") {
    // This fails during P&M loading for all of the following queries
    val path = goldenTablePath("deltalog-state-reconstruction-without-metadata")

    // Test getLatestSnapshot
    checkSnapshotReport(
      (engine, path) => tableManager.getSnapshotAtLatest(engine, path),
      path,
      SnapshotReportExpectations(
        expectedReportCount = 1,
        expectException = true,
        expectedVersion = Optional.of(0),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        // No CRC for golden table
        expectNonZeroDurationToGetCrcInfo = false))

    // Test getSnapshotAsOfVersion
    checkSnapshotReport(
      (engine, path) => tableManager.getSnapshotAtVersion(engine, path, 0),
      path,
      SnapshotReportExpectations(
        expectedReportCount = 1,
        expectException = true,
        expectedVersion = Optional.of(0),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        // No CRC for golden table
        expectNonZeroDurationToGetCrcInfo = false))

    // Test getSnapshotAsOfTimestamp - only if adapter supports it
    if (tableManager.supportsTimestampResolution) {
      // We use the timestamp of version 0
      val version0Timestamp = new File(FileNames.deltaFile(new Path(path, "_delta_log"), 0))
        .lastModified()
      checkSnapshotReport(
        (engine, path) => tableManager.getSnapshotAtTimestamp(engine, path, version0Timestamp),
        path,
        SnapshotReportExpectations(
          expectedReportCount = 1,
          expectException = true,
          // Query will fail before timestamp -> version resolution. The failure
          // will happen when `getLatestSnapshot` is called.
          expectedVersion = Optional.of(0),
          expectedCheckpointVersion = Optional.empty(),
          expectedProvidedTimestamp = Optional.empty(),
          // This is due to the `getLatestSnapshot` call
          expectNonZeroLoadProtocolAndMetadataDuration = true,
          // No CRC for golden table
          expectNonZeroDurationToGetCrcInfo = false))
    }
  }
}
