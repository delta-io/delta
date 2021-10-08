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

package io.delta.standalone.internal.storage

import java.io.Reader

import io.delta.standalone.data.CloseableIterator
import org.apache.commons.io.IOUtils

/**
 * Turn a `Reader` to `ClosableIterator` which can be read on demand. Each element is
 * a trimmed line.
 */
class LineCloseableIterator(_reader: Reader) extends CloseableIterator[String] {
  private val reader = IOUtils.toBufferedReader(_reader)
  // Whether `nextValue` is valid. If it's invalid, we should try to read the next line.
  private var gotNext = false
  // The next value to return when `next` is called. This is valid only if `getNext` is true.
  private var nextValue: String = _
  // Whether the reader is closed.
  private var closed = false
  // Whether we have consumed all data in the reader.
  private var finished = false

  override def hasNext(): Boolean = {
    if (!finished) {
      // Check whether we have closed the reader before reading. Even if `nextValue` is valid, we
      // still don't return `nextValue` after a reader is closed. Otherwise, it would be confusing.
      if (closed) {
        throw new IllegalStateException("Iterator is closed")
      }
      if (!gotNext) {
        val nextLine = reader.readLine()
        if (nextLine == null) {
          finished = true
          close()
        } else {
          nextValue = nextLine.trim
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): String = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      reader.close()
    }
  }
}
