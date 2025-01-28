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
package io.delta.kernel.utils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util

class CloseableIteratorSuite extends AnyFunSuite {

  test("test take while with all elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input > 0)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2, 3, 2, 2)
  }

  test("test take while with no elements matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input < 0)
      .forEachRemaining((input: Int) => res.add(input))
    assert(res.isEmpty)
  }

  test("test take while with some element matching filter") {
    val res = new util.ArrayList[Int]();
    new IntArrayIterator(List(1, 2, 3, 2, 2))
      .takeWhile((input: Int) => input < 3)
      .forEachRemaining((input: Int) => res.add(input))
    res should contain theSameElementsAs List(1, 2)
  }

}

class IntArrayIterator(val intArray: List[Int]) extends CloseableIterator[Int] {
  val data: List[Int] = intArray
  var curIdx = 0
  override def hasNext: Boolean = {
    data.size > curIdx
  }

  override def next(): Int = {
    val res = data(curIdx)
    curIdx = curIdx + 1
    res
  }
  override def close(): Unit = {}
}
