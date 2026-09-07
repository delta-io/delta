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

import java.io.{ByteArrayOutputStream, IOException, OutputStream}

import com.google.common.io.CountingOutputStream
import org.scalatest.funsuite.AnyFunSuite

class S3SingleDriverLogStoreWriteActionsSuite extends AnyFunSuite {
  test("the write resource closes when action iteration fails") {
    val output = new TrackingOutputStream
    val stream = new CountingOutputStream(output)
    val actions = new Iterator[String] {
      override def hasNext: Boolean = true
      override def next(): String = throw new IOException("injected iterator failure")
    }

    assertThrows[IOException] {
      S3SingleDriverLogStore.writeActions(stream, actions)
    }
    assert(output.closed)
  }

  private class TrackingOutputStream extends OutputStream {
    private val delegate = new ByteArrayOutputStream
    var closed = false

    override def write(value: Int): Unit = delegate.write(value)

    override def close(): Unit = {
      closed = true
      delegate.close()
    }
  }
}
