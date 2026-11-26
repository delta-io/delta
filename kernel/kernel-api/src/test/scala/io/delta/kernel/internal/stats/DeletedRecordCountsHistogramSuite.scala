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
package io.delta.kernel.internal.stats

import java.util.Optional

import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.internal.metrics.TransactionMetrics

import org.scalatest.funsuite.AnyFunSuite

class DeletedRecordCountsHistogramSuite extends AnyFunSuite {
  test("bucket boundaries and signed file-count deltas") {
    val histogram = DeletedRecordCountsHistogram.empty()
    val boundaries =
      Seq(0L, 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, Int.MaxValue.toLong)
    val oneFile = 1L
    // Add one file at each end of every bucket. The final bucket ends at Long.MaxValue.
    boundaries.foreach(histogram.applyDvChange(_, oneFile))
    boundaries.tail.foreach(n => histogram.applyDvChange(n - 1, oneFile))
    histogram.applyDvChange(Long.MaxValue, oneFile)
    assert(histogram === new DeletedRecordCountsHistogram(Array.fill(10)(2L)))
    val delta = DeletedRecordCountsHistogram.empty()
    delta.applyDvChange(0L, -oneFile)
    delta.applyDvChange(100L, oneFile)
    assert(delta.applyToBase(histogram) ===
      new DeletedRecordCountsHistogram(Array(1L, 2L, 2L, 3L, 2L, 2L, 2L, 2L, 2L, 2L)))
    assert(histogram === new DeletedRecordCountsHistogram(Array.fill(10)(2L)))
    assert(DeletedRecordCountsHistogram.fromRow(histogram.toRow) === histogram)
    assert(histogram.hashCode === new DeletedRecordCountsHistogram(Array.fill(10)(2L)).hashCode)
    intercept[IllegalArgumentException](histogram.applyDvChange(-1, 1))
    intercept[IllegalArgumentException](new DeletedRecordCountsHistogram(Array(1L)))
  }

  test("transaction histogram deltas reset on retry") {
    val metrics = TransactionMetrics.forNewTable()
    metrics.updateForAddFile(100L, Optional.empty())
    metrics.updateForAddFile(100L, Optional.empty())
    metrics.updateForRemoveFile(100L, Optional.empty())
    val dv = new DeletionVectorDescriptor("p", "file:/dv.bin", Optional.empty(), 32, 100L)
    metrics.updateForAddFile(100L, Optional.of(dv))
    metrics.updateForRemoveFile(100L, Optional.of(dv))
    metrics.updateForAddFile(100L, Optional.of(dv))
    val captured = metrics.captureTransactionMetricsResult()
    val expected = Seq(1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L)
    val delta = captured.getDeletedRecordCountsHistogramDelta.get()
    assert(delta.toSeq === expected)
    delta(0) = 42L
    metrics.updateForAddFile(100L, Optional.empty())
    assert(captured.getDeletedRecordCountsHistogramDelta.get().toSeq === expected)
    metrics.resetActionMetricsForRetry()
    def currentDelta: Optional[Array[Long]] =
      metrics.captureTransactionMetricsResult().getDeletedRecordCountsHistogramDelta
    assert(currentDelta.get().toSeq === Seq.fill(10)(0L))
    metrics.updateForAddFile(100L, Optional.of(dv))
    assert(currentDelta.get().toSeq === Seq(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L))
  }
}
