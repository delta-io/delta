/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.checkpoints

import io.delta.kernel.client._
import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.{DataType, LongType, StructType}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{FileNotFoundException, IOException}
import java.util.Optional

class CheckpointerSuite extends AnyFunSuite {
  import CheckpointerSuite._

  test("load a valid last checkpoint metadata file") {
    val jsonHandler = new TestJsonHandler(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load a zero-sized last checkpoint metadata file") {
    val jsonHandler = new TestJsonHandler(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load an invalid last checkpoint metadata file") {
    val jsonHandler = new TestJsonHandler(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(INVALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("retry metadata loading - succeeds at third attempt") {
    val jsonHandler = new TestJsonHandler(maxFailures = 2)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 2)
  }

  test("retry metadata loading - exceeds max failures") {
    val jsonHandler = new TestJsonHandler(maxFailures = 4)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 3) // 3 is the max retries
  }

  test("try to load when the file is missing") {
    val jsonHandler = new TestJsonHandler(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  /** Assert that the checkpoint metadata is same as [[SAMPLE_LAST_CHECKPOINT_FILE_CONTENT]] */
  def assertValidCheckpointMetadata(actual: Optional[CheckpointMetaData]): Unit = {
    assert(actual.isPresent)
    val metadata = actual.get()
    assert(metadata.version == 40L)
    assert(metadata.size == 44L)
    assert(metadata.parts == Optional.of(20L))
  }
}

object CheckpointerSuite {
  val SAMPLE_LAST_CHECKPOINT_FILE_CONTENT: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

    override def getColumnVector(ordinal: Int): ColumnVector = {
      ordinal match {
        case 0 => longVector(40) // version
        case 1 => longVector(44) // size
        case 2 => longVector(20); // parts
      }
    }

    override def getSize: Int = 1
  }

  val ZERO_ENTRIES_COLUMNAR_BATCH: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

    // empty vector for all columns
    override def getColumnVector(ordinal: Int): ColumnVector = longVector()

    override def getSize: Int = 0
  }

  val VALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/valid")
  val ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE = new Path("/zero_sized")
  val INVALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/invalid")
  val LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE = new Path("/filenotfoundtable")

  def longVector(values: Long*): ColumnVector = new ColumnVector {
    override def getDataType: DataType = LongType.LONG

    override def getSize: Int = 1

    override def close(): Unit = {}

    override def isNullAt(rowId: Int): Boolean = false

    override def getLong(rowId: Int): Long = values(rowId)
  }

  def mockTableClient(jsonHandler: JsonHandler): TableClient = {
    new TableClient() {
      override def getExpressionHandler: ExpressionHandler =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getJsonHandler: JsonHandler = jsonHandler

      override def getFileSystemClient: FileSystemClient =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getParquetHandler: ParquetHandler =
        throw new UnsupportedOperationException("not supported for in this test suite")
    }
  }
}

/** `maxFailures` allows how many times to fail before returning the valid data */
class TestJsonHandler(maxFailures: Int) extends JsonHandler {
  import CheckpointerSuite._
  var currentFailCount = 0;

  override def parseJson(
      jsonStringVector: ColumnVector,
      outputSchema: StructType,
      selectionVector: Optional[ColumnVector]): ColumnarBatch =
    throw new UnsupportedOperationException("not supported for in this test suite")

  override def deserializeStructType(structTypeJson: String): StructType =
    throw new UnsupportedOperationException("not supported for in this test suite")

  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    val file = fileIter.next()
    val path = new Path(file.getPath)

    if (currentFailCount < maxFailures) {
      currentFailCount += 1
      throw new IOException("Retryable exception")
    }

    Utils.singletonCloseableIterator(
      path.getParent match {
        case VALID_LAST_CHECKPOINT_FILE_TABLE => SAMPLE_LAST_CHECKPOINT_FILE_CONTENT
        case ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE => ZERO_ENTRIES_COLUMNAR_BATCH
        case INVALID_LAST_CHECKPOINT_FILE_TABLE =>
          throw new IOException("Invalid last checkpoint file")
        case LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE =>
          throw new FileNotFoundException("File not found")
        case _ => throw new IOException("Unknown table")
      })
  }
}
