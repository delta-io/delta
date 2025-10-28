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
import io.delta.kernel.utils.IteratorWithHead

import org.scalatest.funsuite.AnyFunSuite

class IteratorWithHeadSuite extends AnyFunSuite {

  test("getHead returns empty for empty iterator") {
    val iterator =
      new IteratorWithHead(Utils.toCloseableIterator(Seq.empty[Int].iterator.asJava))
    assert(!iterator.getHead.isPresent)
  }

  test("getHead returns the first element") {
    val iterator = new IteratorWithHead(Utils.toCloseableIterator(Seq(1, 2, 3).iterator.asJava))
    assert(iterator.getHead.isPresent)
    assert(iterator.getHead.get() === 1)
  }

  test("getHead can be called multiple times") {
    val iterator =
      new IteratorWithHead(Utils.toCloseableIterator(Seq(1, 2, 3, 4, 5).iterator.asJava))
    assert(iterator.getHead.get() === 1)
    assert(iterator.getHead.get() === 1)
    assert(iterator.getHead.get() === 1)
  }

  test("getFullIterator returns all elements including head") {
    val iterator =
      new IteratorWithHead(Utils.toCloseableIterator(Seq(1, 2, 3, 4, 5).iterator.asJava))
    val fullIter = iterator.getFullIterator
    assert(fullIter.next() === 1)
    assert(fullIter.next() === 2)
    assert(fullIter.next() === 3)
    assert(fullIter.next() === 4)
    assert(fullIter.next() === 5)
    assert(!fullIter.hasNext())
  }

  test("getFullIterator for single element iterator") {
    val iterator = new IteratorWithHead(Utils.toCloseableIterator(Seq(42).iterator.asJava))
    assert(iterator.getHead.get() === 42)
    val fullIter = iterator.getFullIterator
    assert(fullIter.next() === 42)
    assert(!fullIter.hasNext())
  }

  test("getFullIterator for empty iterator") {
    val iterator = new IteratorWithHead(Utils.toCloseableIterator(Seq.empty[Int].iterator.asJava))
    assert(!iterator.getHead.isPresent)
    val fullIter = iterator.getFullIterator
    assert(!fullIter.hasNext())
    intercept[NoSuchElementException] {
      fullIter.next()
    }
  }
}
