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

}
