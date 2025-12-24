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

package io.delta.kernel.internal.replay

import java.util.{Collections, Optional}
import java.util.NoSuchElementException

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, Row}
import io.delta.kernel.engine._
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class ActionsIteratorSuite extends AnyFunSuite with MockEngineUtils {

  private class EmptyParquetHandler extends BaseMockParquetHandler {
    override def readParquetFiles(
        fileIter: CloseableIterator[FileStatus],
        physicalSchema: StructType,
        predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
      new CloseableIterator[FileReadResult] {
        override def close(): Unit = {}
        override def hasNext: Boolean = false
        override def next(): FileReadResult =
          throw new NoSuchElementException("empty iterator")
      }
    }
  }

  test("sidecar files without version prefix do not throw") {
    val engine = mockEngine(parquetHandler = new EmptyParquetHandler)
    val iterator =
      new ActionsIterator(engine, Collections.emptyList(), new StructType(), Optional.empty())

    val filesListField = classOf[ActionsIterator].getDeclaredField("filesList")
    filesListField.setAccessible(true)
    val filesList =
      filesListField.get(iterator).asInstanceOf[java.util.LinkedList[DeltaLogFile]]

    val sidecarStatus =
      FileStatus.of("/tmp/_delta_log/_sidecars/part-0000-random.c000.snappy.parquet", 0L, 0L)
    filesList.addFirst(DeltaLogFile.ofSideCar(sidecarStatus, 5L))

    assert(!iterator.hasNext())
    iterator.close()
  }

  /**
   * Test for ActionsIterator resource leak fix validation
   *
   * This test validates that the fix applied in ActionsIterator.java prevents resource
   * leaks by ensuring that CloseableIterators are properly closed when exceptions occur.
   *
   * The specific fix being tested: Utils.closeCloseablesSilently(dataIter) in the catch block of
   * readCommitOrCompactionFile method.
   */
  test("ActionsIterator readCommitOrCompactionFile resource cleanup") {
    var iteratorClosed = false

    val engine = mockEngine(jsonHandler = new BaseMockJsonHandler {
      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {

        // Return an empty iterator that tracks closure
        new CloseableIterator[ColumnarBatch] {
          override def hasNext(): Boolean =
            throw new NoSuchElementException("This is a test exception")
          override def next(): ColumnarBatch =
            throw new UnsupportedOperationException("Not needed for this test")
          override def close(): Unit = iteratorClosed = true
        }
      }
    })

    val testFile = FileStatus.of(
      "/path/to/00000000000000000000.json",
      100L,
      System.currentTimeMillis())
    val files = Collections.singletonList(testFile)
    val schema = new StructType()

    val actionsIterator =
      new ActionsIterator(engine, files, schema, Optional.empty[Predicate]())

    assertThrows[NoSuchElementException] {
      actionsIterator.hasNext()
    }

    // Verify that resources were cleaned up
    assert(iteratorClosed, "Internal iterator should be closed after exception in ActionsIterator")
  }
}
