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

package io.delta.kernel

import scala.collection.JavaConverters._

import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.utils.CloseableIterator.BreakableFilterResult

import org.scalatest.funsuite.AnyFunSuite

class CloseableIteratorSuite extends AnyFunSuite {

  private def toCloseableIter[T](elems: Seq[T]): CloseableIterator[T] = {
    Utils.toCloseableIterator(elems.iterator.asJava)
  }

  private def toList[T](iter: CloseableIterator[T]): List[T] = {
    iter.toInMemoryList.asScala.toList
  }

  private def normalDataIter = toCloseableIter(Seq(1, 2, 3, 4, 5))

  private def throwingDataIter = toCloseableIter(Seq(1, 2, 3, 4, 5)).map { x =>
    if (x > 4) {
      throw new RuntimeException("Underlying data evaluated at element > 4")
    }
    x
  }

  /**
   * A CloseableIterator wrapper that tracks whether close() was called.
   * Used for testing resource cleanup.
   */
  private class TrackingCloseableIterator(
      elems: Seq[Int],
      onClose: () => Unit) extends CloseableIterator[Int] {
    private val iter = elems.iterator
    private var closed = false

    override def hasNext(): Boolean = {
      assert(!closed)
      iter.hasNext
    }
    override def next(): Int = iter.next()
    override def close(): Unit = {
      if (!closed) {
        onClose()
        closed = true
      }
    }
  }

  test("CloseableIterator::filter -- returns filtered result") {
    val result = normalDataIter.filter(x => x <= 3 || x == 5)
    assert(toList(result) === List(1, 2, 3, 5))
  }

  test("CloseableIterator::filter -- iterates over all elements") {
    intercept[RuntimeException] {
      toList(throwingDataIter.filter(x => x <= 3))
    }
  }

  test("CloseableIterator::takeWhile -- stops iteration at first false condition") {
    // we expect it to evaluate 1, 2, 3, 4; break when it sees x == 4; and only return 1, 2, 3
    val result = throwingDataIter.takeWhile(x => x <= 3)
    assert(toList(result) === List(1, 2, 3))
  }

  test("CloseableIterator::breakableFilter -- correctly filters and breaks iteration") {
    val result = throwingDataIter.breakableFilter { x =>
      if (x <= 1 || x == 3) {
        BreakableFilterResult.INCLUDE
      } else if (x == 2) {
        BreakableFilterResult.EXCLUDE
      } else if (x == 4) {
        BreakableFilterResult.BREAK
      } else {
        throw new RuntimeException("This should never be reached")
      }
    }
    // we except it to include 1; exclude 2; include 3; and break at 4, thus never seeing 5
    assert(toList(result) === List(1, 3))
  }

  test("flatMap -- flattens nested iterators") {
    // Create an iterator of iterators
    val nestedIter = toCloseableIter(
      Seq(
        toCloseableIter(Seq(1, 2)),
        toCloseableIter(Seq(3, 4, 5)),
        toCloseableIter(Seq(6))))

    val result = Utils.flatMap(nestedIter)
    assert(toList(result) === List(1, 2, 3, 4, 5, 6))
  }

  test("flatMap -- handles empty inner iterators") {
    val nestedIter = toCloseableIter(
      Seq(
        toCloseableIter(Seq(1, 2)),
        toCloseableIter(Seq[Int]()),
        toCloseableIter(Seq(3, 4)),
        toCloseableIter(Seq[Int]()),
        toCloseableIter(Seq(5))))

    val result = Utils.flatMap(nestedIter)

    assert(toList(result) === List(1, 2, 3, 4, 5))
  }

  test("flatMap -- handles empty outer iterator") {
    val nestedIter = toCloseableIter(Seq[CloseableIterator[Int]]())

    val result = Utils.flatMap(nestedIter)

    assert(toList(result) === List())
  }

  test("flatMap -- properly closes inner iterators") {
    var innerClosedCount = 0
    var outerClosed = false
    val nestedIter = new CloseableIterator[CloseableIterator[Int]] {
      private val iter =
        Seq(
          new TrackingCloseableIterator(Seq(1, 2), () => innerClosedCount += 1),
          new TrackingCloseableIterator(Seq(3, 4), () => innerClosedCount += 1)).iterator
      override def hasNext(): Boolean = iter.hasNext
      override def next(): CloseableIterator[Int] = iter.next()
      override def close(): Unit = {
        outerClosed = true
      }
    }

    val result = Utils.flatMap(nestedIter)

    // Consume the iterator fully
    toList(result)

    // All inner iterators should have been closed (2 inner iterators)
    assert(innerClosedCount === 2)
    // Outer iterator should also be closed
    assert(outerClosed === true)
  }

  test("flatMap -- closes iterators even when not fully consumed") {
    var innerClosedCount = 0
    var outerClosed = false

    val nestedIter = new CloseableIterator[CloseableIterator[Int]] {
      private val iter = Seq(
        new TrackingCloseableIterator(Seq(1, 2), () => innerClosedCount += 1),
        new TrackingCloseableIterator(Seq(3, 4), () => innerClosedCount += 1),
        new TrackingCloseableIterator(Seq(5, 6), () => innerClosedCount += 1)).iterator
      override def hasNext(): Boolean = iter.hasNext
      override def next(): CloseableIterator[Int] = iter.next()
      override def close(): Unit = {
        outerClosed = true
      }
    }

    val result = Utils.flatMap(nestedIter)

    // Only consume first 3 elements (from first 2 inner iterators)
    assert(result.hasNext === true)
    assert(result.next() === 1)
    assert(result.next() === 2)
    assert(result.next() === 3)

    // Explicitly close without consuming all
    result.close()
    // First two are closed.
    assert(innerClosedCount === 2)
    assert(outerClosed === true)
  }

  test("flatMap -- handles exception during iteration and cleans up") {
    var innerClosedCount = 0
    var outerClosed = false

    val nestedIter = new CloseableIterator[CloseableIterator[Int]] {
      private var count = 0
      override def hasNext(): Boolean = count < 3
      override def next(): CloseableIterator[Int] = {
        count += 1
        if (count == 2) {
          throw new RuntimeException("Test exception during next()")
        }
        new TrackingCloseableIterator(Seq(1, 2), () => innerClosedCount += 1)
      }
      override def close(): Unit = {
        outerClosed = true
      }
    }

    val result = Utils.flatMap(nestedIter)

    // Consume first inner iterator completely
    assert(result.hasNext === true)
    assert(result.next() === 1)
    assert(result.next() === 2)

    // This should trigger the exception when trying to get the next inner iterator
    val exception = intercept[RuntimeException] {
      result.hasNext
    }
    assert(exception.getMessage === "Test exception during next()")

    // Verify that the outer iterator was closed due to exception
    assert(outerClosed === true)
  }
}
