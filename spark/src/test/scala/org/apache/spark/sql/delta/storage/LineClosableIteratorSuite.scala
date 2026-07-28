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

package org.apache.spark.sql.delta.storage

import java.io.{Reader, StringReader}

import org.apache.spark.SparkFunSuite

abstract class LineClosableIteratorSuiteBase extends SparkFunSuite {

  protected def createIter(_reader: Reader): ClosableIterator[String]

  test("empty") {
    var iter = createIter(new StringReader(""))
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader(""))
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader(""))
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }
  }

  test("one elem") {
    var iter = createIter(new StringReader("foo"))
    assert(iter.hasNext)
    assert(iter.next() == "foo")
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader("foo"))
    assert(iter.next() == "foo")
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader("foo"))
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }
  }

  test("two elems") {
    var iter = createIter(new StringReader("foo\nbar"))
    assert(iter.hasNext)
    assert(iter.next() == "foo")
    assert(iter.hasNext)
    assert(iter.next() == "bar")
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader("foo\nbar"))
    assert(iter.next() == "foo")
    assert(iter.next() == "bar")
    intercept[NoSuchElementException] { iter.next() }

    iter = createIter(new StringReader("foo\nbar"))
    assert(iter.next() == "foo")
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }

    iter = createIter(new StringReader("foo\nbar"))
    assert(iter.hasNext) // Cache `nextValue`
    iter.close()
    // We should throw `IllegalStateException` even if there is a cached `nextValue`.
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }
  }

  test("close should be called when the iterator reaches the end") {
    var closed = false
    val reader = new StringReader("foo") {
      override def close(): Unit = {
        super.close()
        closed = true
      }
    }
    val iter = createIter(reader)
    assert(iter.toList == "foo" :: Nil)
    assert(closed)
  }

  test("close should be called when the iterator is closed") {
    var closed = false
    val reader = new StringReader("foo") {
      override def close(): Unit = {
        super.close()
        closed = true
      }
    }
    val iter = createIter(reader)
    iter.close()
    assert(closed)
  }

  test("close should be called only once") {
    var closed = 0
    val reader = new StringReader("foo") {
      override def close(): Unit = {
        super.close()
        closed += 1
      }
    }
    val iter = createIter(reader)
    assert(iter.toList == "foo" :: Nil)
    iter.close()
    assert(closed == 1)
  }

  test("flatMapWithClose does not open any iterators on creation") {
    var opened = 0
    var closed = 0
    val outerReader = new StringReader("b\na\nr")
    createIter(outerReader).flatMapWithClose(_ => {
      val innerReader = new StringReader("f\no\no") {
        opened += 1
        override def close(): Unit = {
          super.close()
          closed += 1
        }
      }
      createIter(innerReader)
    })
    assert(opened == 0)
    assert(closed == 0)
  }

  test("flatMapWithClose calls close only for opened iterators") {
    var opened = 0
    var closed = 0
    val outerReader = new StringReader("b\na\nr")
    val iter = createIter(outerReader).flatMapWithClose(_ => {
      val innerReader = new StringReader("f\no\no") {
        opened += 1
        override def close(): Unit = {
          super.close()
          closed += 1
        }
      }
      createIter(innerReader)
    })
    assert(iter.take(5).toList == List("f", "o", "o", "f", "o"))
    iter.close()
    assert(opened == 2)
    assert(closed == 2)
  }

  test("flatMapWithClose calls close only for opened iterators - iter boundary") {
    var opened = 0
    var closed = 0
    val outerReader = new StringReader("b\na\nr")
    val iter = createIter(outerReader).flatMapWithClose(_ => {
      val innerReader = new StringReader("f\no\no") {
        opened += 1
        override def close(): Unit = {
          super.close()
          closed += 1
        }
      }
      createIter(innerReader)
    })
    assert(iter.take(3).toList == List("f", "o", "o"))
    iter.close()
    assert(opened == 1)
    assert(closed == 1)
  }
}

class InternalLineClosableIteratorSuite extends LineClosableIteratorSuiteBase {
  override protected def createIter(_reader: Reader): ClosableIterator[String] = {
    new LineClosableIterator(_reader)
  }
}

class PublicLineClosableIteratorSuite extends LineClosableIteratorSuiteBase {
  override protected def createIter(_reader: Reader): ClosableIterator[String] = {
    val impl = new io.delta.storage.LineCloseableIterator(_reader)
    new LineClosableIteratorAdaptor(impl)
  }
}

private class LineClosableIteratorAdaptor(
    impl: io.delta.storage.LineCloseableIterator) extends ClosableIterator[String] {

  override def hasNext(): Boolean = impl.hasNext

  override def next(): String = impl.next()

  override def close(): Unit = impl.close()
}
