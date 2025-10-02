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

  test("CloseableIterator::flatMap -- simple case 1") {
    val innerIter1 = toCloseableIter(Seq("a", "b"))
    val innerIter2 = toCloseableIter(Seq("c"))
    val innerIter3 = toCloseableIter(Seq("d", "e", "f"))
    val iterOfIters = toCloseableIter(Seq(innerIter1, innerIter2, innerIter3))

    val flattened = iterOfIters.flatMap(x => x)

    assert(toList(flattened) === List("a", "b", "c", "d", "e", "f"))
  }

  test("CloseableIterator::flatMap -- simple case 2") {
    val result = normalDataIter.flatMap { x => toCloseableIter(Seq(x, x * 10)) }
    assert(toList(result) === List(1, 10, 2, 20, 3, 30, 4, 40, 5, 50))
  }

  test("CloseableIterator::flatMap -- handles empty inner iterators") {
    val result = normalDataIter.flatMap { x =>
      if (x % 2 == 0) {
        toCloseableIter(Seq(x))
      } else {
        toCloseableIter(Seq.empty[Int])
      }
    }
    assert(toList(result) === List(2, 4))
  }

  test("CloseableIterator::flatMap -- properly closes inner iterators") {
    var closedCount = 0

    val trackingIter = toCloseableIter(Seq(1, 2, 3)).flatMap { x =>
      new CloseableIterator[Int] {
        private val inner = toCloseableIter(Seq(x, x * 10))

        override def hasNext: Boolean = inner.hasNext
        override def next(): Int = inner.next()
        override def close(): Unit = {
          closedCount += 1
          inner.close()
        }
      }
    }

    assert(toList(trackingIter) === List(1, 10, 2, 20, 3, 30)) // Consume all elements
    assert(closedCount === 3) // Verify that all 3 inner iterators were closed
  }

  test("CloseableIterator::flatMap -- chains with other operations") {
    val result = normalDataIter
      .filter(x => x <= 3) // [1, 2, 3
      .flatMap(x => toCloseableIter(Seq(x, x * 10))) // [1, 10, 2, 20, 3, 30]
      .map(_ + 1) // [2, 11, 3, 21, 4, 31]

    assert(toList(result) === List(2, 11, 3, 21, 4, 31))
  }

}
