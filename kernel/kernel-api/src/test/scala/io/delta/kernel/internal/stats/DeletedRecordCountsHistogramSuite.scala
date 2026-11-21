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
  test("DBR bucket boundaries, including Long.MaxValue, and signed deltas") {
    val histogram = DeletedRecordCountsHistogram.empty()
    val boundaries =
      Seq(0L, 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, Int.MaxValue.toLong)
    boundaries.foreach(histogram.add(_, 1))
    boundaries.tail.foreach(n => histogram.add(n - 1, 1))
    histogram.add(Long.MaxValue, 1)
    assert(histogram === new DeletedRecordCountsHistogram(Array.fill(10)(2L)))
    val delta = DeletedRecordCountsHistogram.empty()
    delta.add(0L, -1)
    delta.add(100L, 1)
    assert(histogram.plus(delta) ===
      new DeletedRecordCountsHistogram(Array(1L, 2L, 2L, 3L, 2L, 2L, 2L, 2L, 2L, 2L)))
    assert(histogram === new DeletedRecordCountsHistogram(Array.fill(10)(2L)))
    assert(DeletedRecordCountsHistogram.fromRow(histogram.toRow) === histogram)
    assert(histogram.hashCode === new DeletedRecordCountsHistogram(Array.fill(10)(2L)).hashCode)
    intercept[IllegalArgumentException](histogram.add(-1, 1))
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
    val expected = DeletedRecordCountsHistogram.empty()
    expected.add(0L, 1)
    expected.add(100L, 1)
    assert(metrics.getDeletedRecordCountsHistogramDelta === expected)
    metrics.resetActionMetricsForRetry()
    assert(metrics.getDeletedRecordCountsHistogramDelta === DeletedRecordCountsHistogram.empty())
    metrics.updateForAddFile(100L, Optional.of(dv))
    val retried = DeletedRecordCountsHistogram.empty()
    retried.add(100L, 1)
    assert(metrics.getDeletedRecordCountsHistogramDelta === retried)
  }
}
