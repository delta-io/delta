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

import io.delta.kernel.metrics.ScanMetricsResult

import org.scalatest.funsuite.AnyFunSuite

class ScanMetricsSuite extends AnyFunSuite {

  test("ScanMetrics result capture") {
    val scanMetrics = new ScanMetrics()

    // Update metrics
    scanMetrics.totalPlanningTimer.record(1000)
    scanMetrics.addFilesCounter.increment(10)
    scanMetrics.addFilesFromDeltaFilesCounter.increment(8)
    scanMetrics.activeAddFilesCounter.increment(7)
    scanMetrics.duplicateAddFilesCounter.increment(3)
    scanMetrics.removeFilesFromDeltaFilesCounter.increment(2)
    scanMetrics.parquetActionSourceFilesCounter.increment(1024)
    scanMetrics.jsonActionSourceFilesCounter.increment(512)
    scanMetrics.crcFilesCounter.increment(256)

    // Capture metrics result
    val result: ScanMetricsResult = scanMetrics.captureScanMetricsResult()

    // Verify captured values
    assert(result.getTotalPlanningDurationNs() === 1000)
    assert(result.getNumAddFilesSeen() === 10)
    assert(result.getNumAddFilesSeenFromDeltaFiles() === 8)
    assert(result.getNumActiveAddFiles() === 7)
    assert(result.getNumDuplicateAddFiles() === 3)
    assert(result.getNumRemoveFilesSeenFromDeltaFiles() === 2)
    assert(result.getNumParquetActionSourceFiles() === 1)
    assert(result.getParquetActionSourceFilesTotalSizeBytes() === 1024)
    assert(result.getNumJsonActionSourceFiles() === 1)
    assert(result.getJsonActionSourceFilesTotalSizeBytes() === 512)
    assert(result.getNumCrcFiles() === 1)
    assert(result.getCrcFilesTotalSizeBytes() === 256)

    // Update metrics after capturing result
    scanMetrics.addFilesCounter.increment(5)

    // Verify that the captured result doesn't change
    assert(result.getNumAddFilesSeen() === 10)
    // But the original metrics object does change
    assert(scanMetrics.addFilesCounter.value() === 15)
  }

  test("ScanMetrics toString representation") {
    val scanMetrics = new ScanMetrics()

    // Update some metrics
    scanMetrics.addFilesCounter.increment(10)
    scanMetrics.activeAddFilesCounter.increment(7)

    val stringRepresentation = scanMetrics.toString()

    // Just verify that the string contains the metrics we updated
    assert(stringRepresentation.contains("addFilesCounter=Counter(10)"))
    assert(stringRepresentation.contains("activeAddFilesCounter=Counter(7)"))
  }
}
