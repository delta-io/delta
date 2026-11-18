/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.ContiguousVersionIterator.GapEvent

import org.apache.spark.SparkFunSuite

class ContiguousVersionIteratorSuite extends SparkFunSuite {

  /**
   * Wraps `items` in a [[ContiguousVersionIterator]] that treats the items' Long themselves as
   * versions and collects any [[GapEvent]]s into the returned buffer.
   */
  private def wrap(
      items: Seq[Long],
      treatGapAsFatal: Boolean)
    : (ContiguousVersionIterator[Long], ArrayBuffer[GapEvent[Long]]) = {
    val gaps = ArrayBuffer.empty[GapEvent[Long]]
    val iter = new ContiguousVersionIterator[Long](
      underlying = items.iterator,
      getVersionFromItem = identity,
      treatGapAsFatal = treatGapAsFatal,
      logGap = gap => gaps += gap)
    (iter, gaps)
  }

  test("empty iterator: no gaps, no events") {
    val (iter, gaps) = wrap(Seq.empty, treatGapAsFatal = true)
    assert(iter.toSeq.isEmpty)
    assert(gaps.isEmpty)
  }

  test("single-element iterator: no contiguity check on the first item") {
    // First emitted version is unconstrained -- even though 42 != 0, no gap should fire.
    val (iter, gaps) = wrap(Seq(42L), treatGapAsFatal = true)
    assert(iter.toSeq === Seq(42L))
    assert(gaps.isEmpty)
  }

  test("contiguous sequence starting at 0: no events fire") {
    val (iter, gaps) = wrap(Seq(0L, 1L, 2L, 3L), treatGapAsFatal = true)
    assert(iter.toSeq === Seq(0L, 1L, 2L, 3L))
    assert(gaps.isEmpty)
  }

  test("contiguous sequence starting later than 0: no events fire") {
    // First version is unconstrained, only successive contiguity matters.
    val (iter, gaps) = wrap(Seq(5L, 6L, 7L), treatGapAsFatal = true)
    assert(iter.toSeq === Seq(5L, 6L, 7L))
    assert(gaps.isEmpty)
  }

  test("gap with treatGapAsFatal = true: records GapEvent then throws") {
    val (iter, gaps) = wrap(Seq(0L, 1L, 3L), treatGapAsFatal = true)
    val e = intercept[IllegalStateException] {
      iter.toList
    }
    assert(e.getMessage.contains("expected 2"))
    assert(e.getMessage.contains("got 3"))
    // FATAL must still call logGap so the event lands in telemetry before the throw.
    assert(gaps === Seq(GapEvent(prevItem = 1L, prevVersion = 1L, nextItem = 3L, nextVersion = 3L)))
  }

  test("gap with treatGapAsFatal = false: records GapEvent and continues") {
    val (iter, gaps) = wrap(Seq(0L, 1L, 3L, 4L), treatGapAsFatal = false)
    assert(iter.toSeq === Seq(0L, 1L, 3L, 4L))
    assert(gaps === Seq(GapEvent(prevItem = 1L, prevVersion = 1L, nextItem = 3L, nextVersion = 3L)))
  }

  test("multiple gaps with treatGapAsFatal = false: all gaps recorded") {
    val (iter, gaps) = wrap(Seq(0L, 2L, 5L, 6L, 8L), treatGapAsFatal = false)
    assert(iter.toSeq === Seq(0L, 2L, 5L, 6L, 8L))
    assert(gaps === Seq(
      GapEvent(prevItem = 0L, prevVersion = 0L, nextItem = 2L, nextVersion = 2L),
      GapEvent(prevItem = 2L, prevVersion = 2L, nextItem = 5L, nextVersion = 5L),
      GapEvent(prevItem = 6L, prevVersion = 6L, nextItem = 8L, nextVersion = 8L)))
  }

  test("getVersionFromItem extractor: works with arbitrary payload types") {
    case class Row(version: Long, payload: String)
    val rows = Seq(Row(10, "a"), Row(11, "b"), Row(13, "c"))
    val gaps = ArrayBuffer.empty[GapEvent[Row]]
    val iter = new ContiguousVersionIterator[Row](
      underlying = rows.iterator,
      getVersionFromItem = _.version,
      treatGapAsFatal = false,
      logGap = gap => gaps += gap)
    assert(iter.toSeq === rows)
    assert(gaps === Seq(GapEvent(
      prevItem = Row(11, "b"), prevVersion = 11L,
      nextItem = Row(13, "c"), nextVersion = 13L)))
  }

  test("lazy consumption: items before a fatal gap are still emitted") {
    val (iter, _) = wrap(Seq(0L, 1L, 5L), treatGapAsFatal = true)
    assert(iter.next() === 0L)
    assert(iter.next() === 1L)
    // The gap is only detected when we try to consume the third item.
    intercept[IllegalStateException] {
      iter.next()
    }
  }
}
