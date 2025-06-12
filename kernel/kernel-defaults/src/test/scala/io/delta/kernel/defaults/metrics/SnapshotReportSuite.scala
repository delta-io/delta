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
import io.delta.kernel.engine._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.Timer
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.metrics.SnapshotReport

import org.scalatest.funsuite.AnyFunSuite

class SnapshotReportSuite extends AnyFunSuite with MetricsReportTestUtils {

  /**
   * Given a function [[f]] that generates a snapshot from a [[Table]], runs [[f]] and looks for
   * a generated [[SnapshotReport]]. Times and returns the duration it takes to run [[f]].
   * Uses a custom engine to collect emitted metrics reports. If more than one report is
   * generated (e.g. during timestamp-based time travel), only the last one is returned.
   *
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param path path of the table to query
   * @param expectedReportCount the expected number of [[SnapshotReport]]s to be generated. This
   *                            can be greater than 1 for timestamp-based time travel queries.
   * @param expectException whether we expect [[f]] to throw an exception, which if so, is caught
   *                        and returned with the other results
   * @returns (SnapshotReport, durationToRunF, ExceptionIfThrown)
   */
  def getSnapshotReport(
      f: (Table, Engine) => Snapshot,
      path: String,
      expectedReportCount: Int,
      expectException: Boolean): (SnapshotReport, Long, Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        val table = Table.forPath(engine, path)
        timer.time(() => f(table, engine)) // Time the actual operation
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
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param tablePath table path to query from
   * @param expectException whether we expect f to throw an exception, if so we will check that the
   *                        report contains the thrown exception
   * @param expectedVersion the expected version for the SnapshotReport
   * @param expectedProvidedTimestamp the expected providedTimestamp for the SnapshotReport
   * @param expectedCheckpointVersion the expected checkpoint version for the SnapshotReport
   * @param expectNonEmptyTimestampToVersionResolutionDuration whether we expect
   *                                                           timestampToVersionResolution-
   *                                                           DurationNs to be non-empty (should
   *                                                           be true for any time-travel by
   *                                                           timestamp queries)
   * @param expectNonZeroLoadProtocolAndMetadataDuration whether we expect
   *                                                     loadInitialDeltaActionsDurationNs to be
   *                                                     non-zero (should be true except when an
   *                                                     exception is thrown before log replay)
   * @param expectNonZeroBuildLogSegmentDuration whether we expect
   *                                             buildLogSegmentForVersionDurationNs to be
   *                                             non-zero (should be true except when an
   *                                             exception is thrown before log segment building)
   */
  def checkSnapshotReport(
      f: (Table, Engine) => Snapshot,
      expectedReportCount: Int,
      path: String,
      expectException: Boolean,
      expectedVersion: Optional[Long],
      expectedCheckpointVersion: Optional[Long],
      expectedProvidedTimestamp: Optional[Long],
      expectNonEmptyTimestampToVersionResolutionDuration: Boolean,
      expectNonZeroLoadProtocolAndMetadataDuration: Boolean,
      expectNonZeroBuildLogSegmentDuration: Boolean = true): Unit = {

    val (snapshotReport, duration, exception) =
      getSnapshotReport(f, path, expectedReportCount, expectException)

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
      Objects.equals(snapshotReport.getVersion, expectedVersion),
      s"Expected version $expectedVersion found ${snapshotReport.getVersion}")
    assert(
      Objects.equals(
        snapshotReport.getCheckpointVersion,
        expectedCheckpointVersion),
      s"Expected checkpoint version $expectedCheckpointVersion, found " +
        s"${snapshotReport.getCheckpointVersion}")
    assert(Objects.equals(snapshotReport.getProvidedTimestamp, expectedProvidedTimestamp))

    // Since we cannot know the actual durations of these we sanity check that they are > 0 and
    // less than the total operation duration whenever they are expected to be non-zero/non-empty
    if (expectNonEmptyTimestampToVersionResolutionDuration) {
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.isPresent)
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.get > 0)
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.get <
        duration)
    } else {
      assert(!snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.isPresent)
    }
    if (expectNonZeroLoadProtocolAndMetadataDuration) {
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs > 0)
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs < duration)
    } else {
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs == 0)
    }
    if (expectNonZeroBuildLogSegmentDuration) {
      assert(snapshotReport.getSnapshotMetrics.getTimeToBuildLogSegmentForVersionNs > 0)
      assert(snapshotReport.getSnapshotMetrics.getTimeToBuildLogSegmentForVersionNs < duration)
    } else {
      assert(snapshotReport.getSnapshotMetrics.getTimeToBuildLogSegmentForVersionNs == 0)
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
        (table, engine) => table.getLatestSnapshot(engine),
        expectedReportCount = 1,
        path,
        expectException = false,
        expectedVersion = Optional.of(1),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true)

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        expectedReportCount = 1,
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true)

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version0timestamp),
        expectedReportCount = 2,
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(version0timestamp),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = true)
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
        (table, engine) => table.getLatestSnapshot(engine),
        expectedReportCount = 1,
        path,
        expectException = false,
        expectedVersion = Optional.of(11),
        expectedCheckpointVersion = Optional.of(10),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true)

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 11),
        expectedReportCount = 1,
        path,
        expectException = false,
        expectedVersion = Optional.of(11),
        expectedCheckpointVersion = Optional.of(10),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true)

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version11timestamp),
        expectedReportCount = 2,
        path,
        expectException = false,
        expectedVersion = Optional.of(10),
        expectedCheckpointVersion = Optional.of(10),
        expectedProvidedTimestamp = Optional.of(version11timestamp),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = true)
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
        (table, engine) => table.getSnapshotAsOfVersion(engine, 1),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.of(1),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false)

      // Test getSnapshotAsOfTimestamp with timestamp=0 (does not exist)
      // This fails during timestamp -> version resolution
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, 0),
        expectedReportCount = 2,
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(0),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false,
        expectNonZeroBuildLogSegmentDuration = false)

      // Test getSnapshotAsOfTimestamp with timestamp=currentTime (does not exist)
      // This fails during timestamp -> version resolution
      val currentTimeMillis = System.currentTimeMillis
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, currentTimeMillis),
        expectedReportCount = 2,
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(currentTimeMillis),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false,
        expectNonZeroBuildLogSegmentDuration = false)
    }
  }

  test("Snapshot report - table does not exist") {
    withTempDir { tempDir =>
      // This fails during either log segment building or timestamp -> version resolution
      val path = tempDir.getCanonicalPath

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false)

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.of(0),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false)

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, 1000),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        // Query will fail before timestamp -> version resolution. The failure
        // will happen when `getLatestSnapshot` is called.
        expectedProvidedTimestamp = Optional.empty(),
        expectNonEmptyTimestampToVersionResolutionDuration = false,
        expectNonZeroLoadProtocolAndMetadataDuration = false)
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
        (table, engine) => table.getLatestSnapshot(engine),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false)

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 2),
        expectedReportCount = 1,
        path,
        expectException = true,
        expectedVersion = Optional.of(2),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false)

      // Test getSnapshotAsOfTimestamp
      val version2Timestamp = new File(
        FileNames.deltaFile(new Path(tempDir.getCanonicalPath, "_delta_log"), 2)).lastModified()
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version2Timestamp),
        expectedReportCount = 1,
        tempDir.getCanonicalPath,
        expectException = true,
        // Query will fail before timestamp -> version resolution. The failure
        // will happen when `getLatestSnapshot` is called.
        expectedVersion = Optional.empty(),
        expectedCheckpointVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(),
        expectNonEmptyTimestampToVersionResolutionDuration = false,
        expectNonZeroLoadProtocolAndMetadataDuration = false)
    }
  }

  test("Snapshot report - missing metadata") {
    // This fails during P&M loading for all of the following queries
    val path = goldenTablePath("deltalog-state-reconstruction-without-metadata")

    // Test getLatestSnapshot
    checkSnapshotReport(
      (table, engine) => table.getLatestSnapshot(engine),
      expectedReportCount = 1,
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedCheckpointVersion = Optional.empty(),
      expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
      expectNonZeroLoadProtocolAndMetadataDuration = true)

    // Test getSnapshotAsOfVersion
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
      expectedReportCount = 1,
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedCheckpointVersion = Optional.empty(),
      expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
      expectNonZeroLoadProtocolAndMetadataDuration = true)

    // Test getSnapshotAsOfTimestamp
    // We use the timestamp of version 0
    val version0Timestamp = new File(FileNames.deltaFile(new Path(path, "_delta_log"), 0))
      .lastModified()
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfTimestamp(engine, version0Timestamp),
      expectedReportCount = 1,
      path,
      expectException = true,
      // Query will fail before timestamp -> version resolution. The failure
      // will happen when `getLatestSnapshot` is called.
      expectedVersion = Optional.of(0),
      expectedCheckpointVersion = Optional.empty(),
      expectedProvidedTimestamp = Optional.empty(),
      expectNonEmptyTimestampToVersionResolutionDuration = false,
      // This is due to the `getLatestSnapshot` call.
      expectNonZeroLoadProtocolAndMetadataDuration = true)
  }
}
