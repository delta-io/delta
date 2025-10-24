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
import io.delta.kernel.utils.PeekableIterator

import org.scalatest.funsuite.AnyFunSuite

class PeekableIteratorSuite extends AnyFunSuite {

  test("hasNext is false for empty iterator") {
    val emptyIterator =
      new PeekableIterator(Utils.toCloseableIterator(Seq.empty[Int].iterator.asJava))
    assert(!emptyIterator.hasNext())
  }

  test("peek is empty for empty iterator") {
    val emptyIterator =
      new PeekableIterator(Utils.toCloseableIterator(Seq.empty[Int].iterator.asJava))
    assert(!emptyIterator.peek().isPresent)
  }

  test("next retrieves the next item") {
    val iterator = new PeekableIterator(Utils.toCloseableIterator(Seq(1, 2, 3).iterator.asJava))
    assert(iterator.next() === 1)
    assert(iterator.next() === 2)
    assert(iterator.next() === 3)
    intercept[NoSuchElementException] {
      iterator.next()
    }
  }

  test("peek retrieves the next item without advancing the iterator") {
    val iterator =
      new PeekableIterator(Utils.toCloseableIterator(Seq(1, 2, 3, 4, 5).iterator.asJava))
    assert(iterator.peek().get() === 1)
    assert(iterator.next() === 1)
    assert(iterator.peek().get() === 2)
    assert(iterator.peek().get() === 2)
    assert(iterator.next() === 2)
    assert(iterator.next() === 3)
    assert(iterator.next() === 4)
    assert(iterator.peek().get() === 5)
    assert(iterator.next() === 5)
    assert(!iterator.peek().isPresent)
    intercept[NoSuchElementException] {
      iterator.next()
    }
    assert(!iterator.peek().isPresent)
  }
}
