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

import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.test.{BaseMockParquetHandler, MockEngineUtils}
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
}
