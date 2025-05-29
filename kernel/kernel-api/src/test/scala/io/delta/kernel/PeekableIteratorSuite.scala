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
import java.util.{Iterator => JIterator, NoSuchElementException}

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.delta.kernel.utils.PeekableIterator

import org.scalatest.funsuite.AnyFunSuite

class PeekableIteratorSuite extends AnyFunSuite {

  test("peek should return the next element without consuming it") {
    val list = List(1, 2, 3).asJava
    val peekable = new PeekableIterator(list.iterator())

    assert(peekable.peek() === 1)
    assert(peekable.peek() === 1) // Should return same element
    assert(peekable.hasNext === true)
    assert(peekable.next() === 1) // Should return the peeked element
  }

  test("peek should throw NoSuchElementException when no elements available") {
    val emptyList = List.empty[Int].asJava
    val peekable = new PeekableIterator(emptyList.iterator())

    assertThrows[NoSuchElementException] {
      peekable.peek()
    }
  }

  test("peek and next should work correctly ") {
    val list = List(1, 2).asJava
    val peekable = new PeekableIterator(list.iterator())

    assert(peekable.hasNext === true)
    assert(peekable.peek() === 1)
    assert(peekable.next() === 1)

    assert(peekable.hasNext === true)
    assert(peekable.peek() === 2)
    assert(peekable.next() === 2)

    assert(peekable.hasNext === false)
  }
}
