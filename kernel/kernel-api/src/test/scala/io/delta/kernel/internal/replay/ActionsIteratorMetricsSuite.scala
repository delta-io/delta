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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.engine.FileReadResult
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.actions.{AddFile, Metadata, Protocol, RemoveFile}
import io.delta.kernel.internal.metrics.ScanMetrics
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockEngineUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class ActionsIteratorMetricsSuite extends AnyFunSuite with MockEngineUtils {

  private val TEST_SCHEMA = new StructType()
    .add("add", AddFile.FULL_SCHEMA)
    .add("remove", RemoveFile.FULL_SCHEMA)
    .add("metadata", Metadata.FULL_SCHEMA)
    .add("protocol", Protocol.FULL_SCHEMA)

  test("ActionsIterator should increment json file counter for JSON commit files") {
    val metrics = new ScanMetrics()

    // Create test JSON commit files with different sizes
    val jsonFile1 = FileStatus.of("/path/to/00000000000000000001.json", 1024L, 0L)
    val jsonFile2 = FileStatus.of("/path/to/00000000000000000002.json", 2048L, 0L)
    val files = List(jsonFile1, jsonFile2).asJava

    // Mock JSON handler to return empty iterator (not testing actual data reading)
    val mockJsonHandler = new BaseMockJsonHandler {
      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
        new CloseableIterator[ColumnarBatch] {
          override def hasNext: Boolean = false
          override def next(): ColumnarBatch = throw new NoSuchElementException()
          override def close(): Unit = {}
        }
      }
    }

    val testEngine = mockEngine(jsonHandler = mockJsonHandler)

    // Create ActionsIterator
    val iterator = new ActionsIterator(
      testEngine,
      files,
      TEST_SCHEMA,
      Optional.empty[Predicate](),
      metrics)

    // Iterate through actions to trigger file reading
    while (iterator.hasNext) {
      iterator.next()
    }
    iterator.close()

    // Verify metrics
    assert(metrics.jsonActionSourceFilesCounter.fileCount() === 2)
    assert(metrics.jsonActionSourceFilesCounter.sizeInBytes() === 3072L) // 1024 + 2048
    assert(metrics.parquetActionSourceFilesCounter.fileCount() === 0)
  }

  test("ActionsIterator should increment parquet file counter for checkpoint files") {
    val metrics = new ScanMetrics()

    // Create test checkpoint files with different sizes
    val checkpointFile1 =
      FileStatus.of("/path/to/00000000000000000010.checkpoint.parquet", 4096L, 0L)
    val checkpointFile2 =
      FileStatus.of("/path/to/00000000000000000020.checkpoint.parquet", 8192L, 0L)
    val files = List(checkpointFile1, checkpointFile2).asJava

    // Mock Parquet handler
    val mockParquetHandler = new BaseMockParquetHandler {
      override def readParquetFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[FileReadResult] = {
        new CloseableIterator[FileReadResult] {
          override def hasNext: Boolean = false
          override def next(): FileReadResult = throw new NoSuchElementException()
          override def close(): Unit = {}
        }
      }
    }

    val testEngine = mockEngine(parquetHandler = mockParquetHandler)

    // Create ActionsIterator
    val iterator = new ActionsIterator(
      testEngine,
      files,
      TEST_SCHEMA,
      Optional.empty[Predicate](),
      metrics)

    // Iterate through actions to trigger file reading
    while (iterator.hasNext) {
      iterator.next()
    }
    iterator.close()

    // Verify metrics
    assert(metrics.parquetActionSourceFilesCounter.fileCount() === 2)
    assert(metrics.parquetActionSourceFilesCounter.sizeInBytes() === 12288L) // 4096 + 8192
    assert(metrics.jsonActionSourceFilesCounter.fileCount() === 0)
  }

  test("ActionsIterator should increment counters for V2 checkpoint JSON manifest files") {
    val metrics = new ScanMetrics()

    // Create V2 checkpoint manifest file (UUID-named JSON file)
    val v2CheckpointFile = FileStatus.of(
      "/path/to/00000000000000000010.checkpoint.e5b1c3d8-8b1a-4f98-a1b0-1234567890ab.json",
      3072L,
      0L)
    val files = List(v2CheckpointFile).asJava

    // Mock JSON handler for V2 checkpoint
    val mockJsonHandler = new BaseMockJsonHandler {
      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
        new CloseableIterator[ColumnarBatch] {
          override def hasNext: Boolean = false
          override def next(): ColumnarBatch = throw new NoSuchElementException()
          override def close(): Unit = {}
        }
      }
    }

    val testEngine = mockEngine(jsonHandler = mockJsonHandler)

    // Create ActionsIterator
    val iterator = new ActionsIterator(
      testEngine,
      files,
      TEST_SCHEMA,
      Optional.empty[Predicate](),
      metrics)

    // Iterate through actions
    while (iterator.hasNext) {
      iterator.next()
    }
    iterator.close()

    // V2 checkpoint JSON files should be counted as JSON files
    assert(metrics.jsonActionSourceFilesCounter.fileCount() === 1)
    assert(metrics.jsonActionSourceFilesCounter.sizeInBytes() === 3072L)
    assert(metrics.parquetActionSourceFilesCounter.fileCount() === 0)
  }
}
