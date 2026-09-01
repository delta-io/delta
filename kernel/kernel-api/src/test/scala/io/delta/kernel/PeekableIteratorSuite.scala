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

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.delta.kernel.utils.PeekableIterator

import org.scalatest.funsuite.AnyFunSuite

class PeekableIteratorSuite extends AnyFunSuite {

  test("basic peek and next functionality") {
    val list = List(1, 2, 3).asJava
    val peekable = new PeekableIterator(list.iterator())

    assert(peekable.peek() === 1)
    assert(peekable.peek() === 1) // Should return same element
    assert(peekable.next() === 1)
    assert(peekable.next() === 2)
    assert(peekable.peek() === 3)
    assert(peekable.next() === 3)
    assert(peekable.hasNext === false)
  }

  test("empty iterator throws exception") {
    val emptyList = List.empty[Int].asJava
    val peekable = new PeekableIterator(emptyList.iterator())

    assert(peekable.hasNext === false)
    assertThrows[NoSuchElementException] {
      peekable.peek()
    }
  }

  test("handles null values correctly") {
    val list = List(null, "test").asJava
    val peekable = new PeekableIterator[String](list.iterator())

    assert(peekable.hasNext === true)
    assert(peekable.peek() === null)
    assert(peekable.next() === null)
    assert(peekable.peek() === "test")
    assert(peekable.next() === "test")
  }
}
