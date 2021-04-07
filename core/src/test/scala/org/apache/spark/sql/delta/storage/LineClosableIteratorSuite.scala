/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.io.StringReader

import org.apache.spark.SparkFunSuite

class LineClosableIteratorSuite extends SparkFunSuite {

  test("empty") {
    var iter = new LineClosableIterator(new StringReader(""))
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader(""))
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader(""))
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }
  }

  test("one elem") {
    var iter = new LineClosableIterator(new StringReader("foo"))
    assert(iter.hasNext)
    assert(iter.next() == "foo")
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader("foo"))
    assert(iter.next() == "foo")
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader("foo"))
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }
  }

  test("two elems") {
    var iter = new LineClosableIterator(new StringReader("foo\nbar"))
    assert(iter.hasNext)
    assert(iter.next() == "foo")
    assert(iter.hasNext)
    assert(iter.next() == "bar")
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader("foo\nbar"))
    assert(iter.next() == "foo")
    assert(iter.next() == "bar")
    intercept[NoSuchElementException] { iter.next() }

    iter = new LineClosableIterator(new StringReader("foo\nbar"))
    assert(iter.next() == "foo")
    iter.close()
    intercept[IllegalStateException] { iter.hasNext }
    intercept[IllegalStateException] { iter.next() }

    iter = new LineClosableIterator(new StringReader("foo\nbar"))
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
    val iter = new LineClosableIterator(reader)
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
    val iter = new LineClosableIterator(reader)
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
    val iter = new LineClosableIterator(reader)
    assert(iter.toList == "foo" :: Nil)
    iter.close()
    assert(closed == 1)
  }
}
