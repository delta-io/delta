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

import java.io.Closeable

trait SupportsRewinding[T] extends Iterator[T] {
  // Overrides if class supports rewinding the iterator to the beginning efficiently.
  def rewind(): Unit
}

trait ClosableIterator[T] extends Iterator[T] with Closeable {
  /** Calls f(this) and always closes the iterator afterwards. */
  def processAndClose[R](f: Iterator[T] => R): R = {
    try {
      f(this)
    } finally {
      close()
    }
  }
}

object ClosableIterator {
  /**
   * An implicit class for applying a function to a [[ClosableIterator]] and returning the
   * resulting iterator as a [[ClosableIterator]] with the original `close()` method.
   */
  implicit class IteratorCloseOps[A](val closableIter: ClosableIterator[A]) extends AnyVal {
    def withClose[B](f: Iterator[A] => Iterator[B]): ClosableIterator[B] = new ClosableIterator[B] {
      private val iter =
        try {
          f(closableIter)
        } catch {
          case e: Throwable =>
            closableIter.close()
            throw e
        }
      override def next(): B = iter.next()
      override def hasNext: Boolean = iter.hasNext
      override def close(): Unit = closableIter.close()
    }
  }

  /**
   * An implicit class for a `flatMap` implementation that returns a [[ClosableIterator]]
   * which (a) closes inner iterators upon reaching their end, and (b) has a `close()` method
   * that closes any opened and unclosed inner iterators.
   */
  implicit class IteratorFlatMapCloseOp[A](val closableIter: Iterator[A]) extends AnyVal {
    def flatMapWithClose[B](f: A => ClosableIterator[B]): ClosableIterator[B] =
      new ClosableIterator[B] {
        private var iter_curr =
          if (closableIter.hasNext) {
            f(closableIter.next())
          } else {
            null
          }
        override def next(): B = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          iter_curr.next()
        }
        @scala.annotation.tailrec
        override def hasNext: Boolean = {
          if (iter_curr == null) {
            false
          }
          else if (iter_curr.hasNext) {
            true
          }
          else {
            iter_curr.close()
            if (closableIter.hasNext) {
              iter_curr = f(closableIter.next())
              hasNext
            } else {
              iter_curr = null
              false
            }
          }
        }
        override def close(): Unit = {
          if (iter_curr != null) {
            iter_curr.close()
          }
        }
      }
  }

  /**
   * An implicit class for wrapping an iterator to be a [[ClosableIterator]] with a `close` method
   * that does nothing.
   */
  implicit class ClosableWrapper[A](val iter: Iterator[A]) extends AnyVal {
    def toClosable: ClosableIterator[A] = new ClosableIterator[A] {
      override def next(): A = iter.next()
      override def hasNext: Boolean = iter.hasNext
      override def close(): Unit = ()
    }
  }
}
