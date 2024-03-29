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

import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockFileSystemClientUtils, MockTableClientUtils}
import org.scalatest.funsuite.AnyFunSuite
import java.io.{FileNotFoundException, IOException}
import java.util.Optional

class CheckpointerSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with MockTableClientUtils {
  import CheckpointerSuite._

  //////////////////////////////////////////////////////////////////////////////////
  // readLastCheckpointFile tests
  //////////////////////////////////////////////////////////////////////////////////
  test("load a valid last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load a zero-sized last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load an invalid last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(INVALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("retry last checkpoint metadata loading - succeeds at third attempt") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 2)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 2)
  }

  test("retry last checkpoint metadata loading - exceeds max failures") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 4)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 3) // 3 is the max retries
  }

  test("try to load last checkpoint metadata when the file is missing") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE)
      .readLastCheckpointFile(mockTableClient(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("test loading sidecar files from checkpoint manifest") {
    val jsonHandler = new MockSidecarJsonHandler()
    val parquetHandler = new MockSidecarParquetHandler()
    Seq("json", "parquet").foreach { format =>
      val sidecars = new Checkpointer(CHECKPOINT_MANIFEST_FILE_TABLE)
        .loadSidecarFiles(
          mockTableClient(jsonHandler = jsonHandler, parquetHandler = parquetHandler),
          new Path(CHECKPOINT_MANIFEST_FILE_TABLE, s"001.checkpoint.abc-def.$format"))
      assert(sidecars.isPresent && sidecars.get().size() == 2)
      assert(sidecars.get().get(0).equals(new SidecarFile("abc", 44L, 20L)))
      assert(sidecars.get().get(1).equals(new SidecarFile("def", 45L, 30L)))
    }
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

object CheckpointerSuite extends MockTableClientUtils {
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

  val SAMPLE_SIDECAR_FILE_CONTENT: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = SidecarFile.READ_SCHEMA

    override def getColumnVector(ordinal: Int): ColumnVector = {
      ordinal match {
        case 0 => stringVector("abc", "def") // path
        case 1 => longVector(44, 45) // size
        case 2 => longVector(20, 30); // modification time
      }
    }

    override def getSize: Int = 2
  }

  val ZERO_ENTRIES_COLUMNAR_BATCH: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

    // empty vector for all columns
    override def getColumnVector(ordinal: Int): ColumnVector = longVector()

    override def getSize: Int = 0
  }

  val CHECKPOINT_MANIFEST_FILE_TABLE = new Path("/ckpt-manifest-test")
  val VALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/valid")
  val ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE = new Path("/zero_sized")
  val INVALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/invalid")
  val LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE = new Path("/filenotfoundtable")
}

/** `maxFailures` allows how many times to fail before returning the valid data */
class MockLastCheckpointMetadataFileReader(maxFailures: Int) extends BaseMockJsonHandler {
  import CheckpointerSuite._
  var currentFailCount = 0

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
        case CHECKPOINT_MANIFEST_FILE_TABLE => SAMPLE_SIDECAR_FILE_CONTENT
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

class MockSidecarJsonHandler extends BaseMockJsonHandler {
  import CheckpointerSuite._

  override def readJsonFiles(
    fileIter: CloseableIterator[FileStatus],
    physicalSchema: StructType,
    predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    val file = fileIter.next()
    val path = new Path(file.getPath)

    Utils.singletonCloseableIterator(
      path.getParent match {
        case CHECKPOINT_MANIFEST_FILE_TABLE => SAMPLE_SIDECAR_FILE_CONTENT
        case _ => throw new IOException("Unknown table")
      })
  }
}

class MockSidecarParquetHandler extends BaseMockParquetHandler {
  import CheckpointerSuite._

  override def readParquetFiles(
    fileIter: CloseableIterator[FileStatus],
    physicalSchema: StructType,
    predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    val file = fileIter.next()
    val path = new Path(file.getPath)

    Utils.singletonCloseableIterator(
      path.getParent match {
        case CHECKPOINT_MANIFEST_FILE_TABLE => SAMPLE_SIDECAR_FILE_CONTENT
        case _ => throw new IOException("Unknown table")
      })
  }
}