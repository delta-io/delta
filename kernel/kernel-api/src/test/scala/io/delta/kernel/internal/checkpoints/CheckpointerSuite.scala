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
import io.delta.kernel.internal.checkpoints.Checkpointer.findLastCompleteCheckpointBeforeHelper
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames.checkpointFileSingular
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.{BaseMockJsonHandler, MockFileSystemClientUtils, MockTableClientUtils, VectorTestUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.scalatest.funsuite.AnyFunSuite
import java.io.{FileNotFoundException, IOException}
import java.util.Optional

class CheckpointerSuite extends AnyFunSuite with MockFileSystemClientUtils {
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

  //////////////////////////////////////////////////////////////////////////////////
  // findLastCompleteCheckpointBefore tests
  //////////////////////////////////////////////////////////////////////////////////
  test("findLastCompleteCheckpointBefore - no checkpoints") {
    val files = deltaFileStatuses(Seq.range(0, 25))

    Seq((0, 0), (10, 10), (20, 20), (27, 25 /* no delta log files after version 24 */)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - single checkpoint") {
    // 25 delta files and 1 checkpoint file = total 26 files.
    val files = deltaFileStatuses(Seq.range(0, 25)) ++ singularCheckpointFileStatuses(Seq(10))

    Seq((0, 0), (4, 4), (9, 9), (10, 10)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq((14, 10, 15), (25, 10, 26), (27, 10, 26)).foreach {
      case (beforeVersion, expCheckpointVersion, expNumFilesListed) =>
        assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - multi-part checkpoint") {
    // 25 delta files and 20 checkpoint files = total 45 files.
    val files = deltaFileStatuses(Seq.range(0, 25)) ++ multiCheckpointFileStatuses(Seq(10), 20)

    Seq((0, 0), (4, 4), (9, 9), (10, 10)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq((14, 10, 14 + 20), (25, 10, 25 + 20), (27, 10, 25 + 20)).foreach {
      case (beforeVersion, expCheckpointVersion, expNumFilesListed) =>
        assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - multi-part checkpoint per 10commits - 10K commits") {
    // 10K delta files and 1K checkpoints * 20 files for each checkpoint = total 30K files.
    val files = deltaFileStatuses(Seq.range(0, 10000)) ++
      multiCheckpointFileStatuses(Seq.range(10, 10000, 10), 20)

    Seq(0, 4, 9, 10).foreach { beforeVersion =>
      val expNumFilesListed = beforeVersion
      assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq(789, 1005, 5787, 9999).foreach { beforeVersion =>
      val expCheckpointVersion = (beforeVersion / 10) * 10
      // Listing size is 1000 delta versions (i.e list _delta_log/0001000* to _delta_log/0001999*)
      val versionsListed = Math.min(beforeVersion, 1000)
      val expNumFilesListed =
          versionsListed /* delta files */ +
          (versionsListed / 10) * 20 /* checkpoints */
      assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - multi-part checkpoint per 2.5K commits - 10K commits") {
    // 10K delta files and 4 checkpoints * 50 files for each checkpoint = total 10,080 files.
    val files = deltaFileStatuses(Seq.range(0, 10000)) ++
      multiCheckpointFileStatuses(Seq.range(2500, 10000, 2500), 50)

    Seq((0, 0), (889, 889), (1001, 1002), (2400, 2402)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq(2600, 5002, 7980, 9999).foreach { beforeVersion =>
      val expCheckpointVersion = (beforeVersion / 2500) * 2500
      // Listing size is 1000 delta versions (i.e list _delta_log/0001000* to _delta_log/0001999*)
      // We list until the checkpoint is encounters in increments of 1000 versions at a time
      val numListCalls = ((beforeVersion - expCheckpointVersion) / 1000) + 1
      val versionsListed = 1000 * numListCalls
      val expNumFilesListed =
        numListCalls - 1 /* last file scanned that fails the search and stops */ +
          versionsListed /* delta files */ +
           50 /* one multi-part checkpoint */
      assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - two checkpoints (one is zero-sized - not valid)") {
    // 25 delta files and 2 checkpoint file = total 27 files.
    val files = deltaFileStatuses(Seq.range(0, 25)) ++
      singularCheckpointFileStatuses(Seq(10)) ++
      Seq(FileStatus.of(
        checkpointFileSingular(logPath, 20).toString, 0, 0)) // zero-sized CP

    Seq((0, 0), (4, 4), (9, 9), (10, 10)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq((14, 10, 15), (25, 10, 27), (27, 10, 27)).foreach {
      case (beforeVersion, expCheckpointVersion, expNumFilesListed) =>
        assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
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

  def assertLastCheckpoint(
      deltaLogFiles: Seq[FileStatus],
      beforeVersion: Long,
      expCheckpointVersion: Long,
      expNumFilesListed: Long): Unit = {
    val tableClient = createMockFSListFromTableClient(deltaLogFiles)
    val result = findLastCompleteCheckpointBeforeHelper(tableClient, logPath, beforeVersion)
    assert(result._1.isPresent, s"Checkpoint should be found for version=$beforeVersion")
    assert(
      result._1.get().version === expCheckpointVersion,
      s"Incorrect checkpoint version before version=$beforeVersion")
    assert(result._2 === expNumFilesListed, s"Invalid number of files listed: $beforeVersion")
  }

  def assertNoLastCheckpoint(
      deltaLogFiles: Seq[FileStatus],
      beforeVersion: Long,
      expNumFilesListed: Long): Unit = {
    val tableClient = createMockFSListFromTableClient(deltaLogFiles)
    val result = findLastCompleteCheckpointBeforeHelper(tableClient, logPath, beforeVersion)
    assert(!result._1.isPresent, s"No checkpoint should be found for version=$beforeVersion")
    assert(result._2 == expNumFilesListed, s"Invalid number of files listed: $beforeVersion")
  }
}

object CheckpointerSuite extends MockTableClientUtils with VectorTestUtils {
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
        case 0 => stringVector(Seq("abc", "def")) // path
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

  val CHECKPOINT_V2_FILE_TABLE = new Path("/ckpt-v2-test")
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
        case CHECKPOINT_V2_FILE_TABLE => SAMPLE_SIDECAR_FILE_CONTENT
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
