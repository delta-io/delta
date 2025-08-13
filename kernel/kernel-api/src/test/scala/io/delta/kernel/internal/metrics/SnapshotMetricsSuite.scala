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
package io.delta.kernel.internal.metrics

import io.delta.kernel.metrics.{ScanMetricsResult, SnapshotMetricsResult}

import org.scalatest.funsuite.AnyFunSuite

class SnapshotMetricsSuite extends AnyFunSuite {

  test("SnapshotMetrics result capture") {
    val snapshotMetrics = new SnapshotMetrics()

    // Update metrics
    snapshotMetrics.loadSnapshotTotalTimer.record(5000)
    snapshotMetrics.computeTimestampToVersionTotalDurationTimer.record(1000)
    snapshotMetrics.loadProtocolMetadataTotalDurationTimer.record(2000)
    snapshotMetrics.loadLogSegmentTotalDurationTimer.record(3000)
    snapshotMetrics.loadCrcTotalDurationTimer.record(4000)
    snapshotMetrics.scanMetrics.addFilesCounter.increment(10)
    snapshotMetrics.scanMetrics.parquetActionSourceFilesCounter.increment(1024)

    // Capture metrics result
    val result: SnapshotMetricsResult = snapshotMetrics.captureSnapshotMetricsResult()

    // Verify captured values
    assert(result.getLoadSnapshotTotalDurationNs() === 5000)
    assert(result.getComputeTimestampToVersionTotalDurationNs().isPresent())
    assert(result.getComputeTimestampToVersionTotalDurationNs().get() === 1000)
    assert(result.getLoadProtocolMetadataTotalDurationNs() === 2000)
    assert(result.getLoadLogSegmentTotalDurationNs() === 3000)
    assert(result.getLoadCrcTotalDurationNs() === 4000)

    // Verify scan metrics are captured correctly
    val scanResult: ScanMetricsResult = result.getScanMetricsResult()
    assert(scanResult.getNumAddFilesSeen() === 10)
    assert(scanResult.getNumParquetActionSourceFiles() === 1)
    assert(scanResult.getParquetActionSourceFilesTotalSizeBytes() === 1024)

    // Update metrics after capturing result
    snapshotMetrics.loadSnapshotTotalTimer.record(1000)
    snapshotMetrics.scanMetrics.addFilesCounter.increment(5)

    // Verify that the captured result doesn't change
    assert(result.getLoadSnapshotTotalDurationNs() === 5000)
    assert(scanResult.getNumAddFilesSeen() === 10)

    // But the original metrics object does change
    assert(snapshotMetrics.loadSnapshotTotalTimer.totalDurationNs() === 6000)
    assert(snapshotMetrics.scanMetrics.addFilesCounter.value() === 15)
  }

  test("SnapshotMetrics with empty timestamp to version duration") {
    val snapshotMetrics = new SnapshotMetrics()

    // Don't record any timestamp to version duration
    snapshotMetrics.loadSnapshotTotalTimer.record(5000)

    // Capture metrics result
    val result: SnapshotMetricsResult = snapshotMetrics.captureSnapshotMetricsResult()

    // Verify that the timestamp to version duration is empty
    assert(!result.getComputeTimestampToVersionTotalDurationNs().isPresent())
  }

  test("SnapshotMetrics toString representation") {
    val snapshotMetrics = new SnapshotMetrics()

    // Update some metrics
    snapshotMetrics.loadSnapshotTotalTimer.record(5000)
    snapshotMetrics.loadProtocolMetadataTotalDurationTimer.record(2000)

    val stringRepresentation = snapshotMetrics.toString()

    // Just verify that the string contains the metrics we updated
    assert(stringRepresentation.contains("loadSnapshotTotalTimer=Timer(duration=5000 ns, count=1)"))
    assert(stringRepresentation.contains(
      "loadProtocolMetadataTotalDurationTimer=Timer(duration=2000 ns, count=1)"))
  }
}
