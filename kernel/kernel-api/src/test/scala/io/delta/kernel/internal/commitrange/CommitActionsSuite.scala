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
package io.delta.kernel.internal.commitrange

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class CommitActionsSuite extends AnyFunSuite with VectorTestUtils {

  test("Test getters and lifecycle") {
    val version = 1L
    val timestamp = 100L
    var closeCalled = 0

    val actionsIter = new CloseableIterator[ColumnarBatch] {
      override def hasNext: Boolean = false
      override def next(): ColumnarBatch = throw new NoSuchElementException()
      override def close(): Unit = closeCalled += 1
    }

    val commitActions = new CommitActionsImpl(version, timestamp, actionsIter)

    // Verify basic properties
    assert(commitActions.getVersion == version)
    assert(commitActions.getTimestamp == timestamp)

    // Verify can access actions before close
    val retrievedIter = commitActions.getActions
    assert(!retrievedIter.hasNext)

    // Close multiple times should be idempotent
    commitActions.close()
    commitActions.close()
    assert(closeCalled == 1)

    // Should throw after close
    val e = intercept[IllegalStateException] {
      commitActions.getActions
    }
    assert(e.getMessage.contains("CommitActions for version 1 is already closed"))
  }
}
