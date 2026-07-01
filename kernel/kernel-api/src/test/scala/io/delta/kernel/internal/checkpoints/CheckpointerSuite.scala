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

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.util.Optional

import scala.util.control.NonFatal

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.engine.FileReadRequest
import io.delta.kernel.exceptions.KernelEngineException
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.Checkpointer.findLastCompleteCheckpointBeforeHelper
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames.checkpointFileSingular
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class CheckpointerSuite extends AnyFunSuite with MockFileSystemClientUtils {
  import CheckpointerSuite._

  //////////////////////////////////////////////////////////////////////////////////
  // readLastCheckpointFile tests
  //////////////////////////////////////////////////////////////////////////////////
  test("load a valid last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load a zero-sized last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("load an invalid last checkpoint metadata file") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(INVALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  test("retry last checkpoint metadata loading - succeeds at third attempt") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 2)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(jsonHandler.currentFailCount == 2)
  }

  test("retry last checkpoint metadata loading - exceeds max failures") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 4)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 3) // 3 is the max retries
  }

  test("try to load last checkpoint metadata when the file is missing") {
    val jsonHandler = new MockLastCheckpointMetadataFileReader(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE)
      .readLastCheckpointFile(mockEngine(jsonHandler = jsonHandler))
    assert(!lastCheckpoint.isPresent)
    assert(jsonHandler.currentFailCount == 0)
  }

  //////////////////////////////////////////////////////////////////////////////////
  // readLastCheckpointHintBytes tests
  //////////////////////////////////////////////////////////////////////////////////
  test("read valid last checkpoint hint bytes") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(hintBytes.isPresent)
    assert(hintBytes.get() sameElements SAMPLE_LAST_CHECKPOINT_HINT_BYTES)
    // The raw read preserves the V2 checkpoint block (sidecarFiles + checkpointMetadata) that the
    // parsed CheckpointMetaData.READ_SCHEMA (version/size/parts/tags) path would drop
    val json = new String(hintBytes.get(), StandardCharsets.UTF_8)
    assert(json.contains("v2Checkpoint"))
    assert(json.contains("sidecarFiles"))
  }

  test("read zero-sized last checkpoint hint bytes") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent)
  }

  test("read last checkpoint hint bytes when read fails") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(INVALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent)
  }

  test("read last checkpoint hint bytes when the file is missing") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent)
  }

  test("retry last checkpoint hint bytes loading - succeeds at third attempt") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient(maxFailures = 2)
    val hintBytes = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(hintBytes.isPresent)
    assert(hintBytes.get() sameElements SAMPLE_LAST_CHECKPOINT_HINT_BYTES)
    assert(fsClient.currentFailCount == 2)
  }

  test("retry last checkpoint hint bytes loading - exceeds max failures") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient(maxFailures = 4)
    val hintBytes = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent)
    assert(fsClient.currentFailCount == 3) // 3 is the max retries
  }

  test("read last checkpoint hint bytes returns empty when file exceeds Integer.MAX_VALUE") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(OVERSIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent) // size guard fires before readFiles
  }

  test(
    "read last checkpoint hint bytes returns empty when content is empty after a non-zero stat") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(EMPTY_CONTENT_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent) // exercises the post-read bytes.length == 0 branch
  }

  test("read last checkpoint hint bytes when engine wraps FileNotFound in KernelEngineException") {
    val fsClient = new MockLastCheckpointHintBytesFileSystemClient()
    val hintBytes = new Checkpointer(WRAPPED_NOT_FOUND_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointHintBytes(mockEngine(fileSystemClient = fsClient))
    assert(!hintBytes.isPresent) // exercises catch(KernelEngineException w/ FileNotFound cause)
  }

  //////////////////////////////////////////////////////////////////////////////////
  // findLastCompleteCheckpointBefore tests
  //////////////////////////////////////////////////////////////////////////////////
  test("findLastCompleteCheckpointBefore - no checkpoints") {
    val files = deltaFileStatuses(Seq.range(0, 25))

    Seq((0, 0), (10, 10), (20, 20), (27, 25 /* no delta log files after version 24 */ )).foreach {
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
        checkpointFileSingular(logPath, 20).toString,
        0,
        0
      )) // zero-sized CP

    Seq((0, 0), (4, 4), (9, 9), (10, 10)).foreach {
      case (beforeVersion, expNumFilesListed) =>
        assertNoLastCheckpoint(files, beforeVersion, expNumFilesListed)
    }

    Seq((14, 10, 15), (25, 10, 27), (27, 10, 27)).foreach {
      case (beforeVersion, expCheckpointVersion, expNumFilesListed) =>
        assertLastCheckpoint(files, beforeVersion, expCheckpointVersion, expNumFilesListed)
    }
  }

  test("findLastCompleteCheckpointBefore - checksum files don't break first search iteration") {
    // Unrecognized files (e.g. .crc) used to break the first search window immediately,
    // causing the search to miss nearby checkpoints and fall back to older ones.
    val versions = Seq.range(0L, 2100L)
    val files = deltaFileStatuses(versions) ++
      versions.map(checksumFileStatus) ++
      singularCheckpointFileStatuses(Seq(1000, 2000))

    // First search window covers v1100-2099. Should find checkpoint 2000, not fall back to 1000.
    assertLastCheckpoint(
      files,
      beforeVersion = 2100,
      expCheckpointVersion = 2000,
      expNumFilesListed = 1000 /* deltas */ + 1000 /* checksums */ + 1 /* checkpoint at 2000 */ )

    // Checkpoint well within the first search window (v1050-2049) is also found.
    // Files scanned: 1000 deltas + 1000 checksums (v1050-2049) + 1 checkpoint (v2000)
    // + 1 crc at v2050 (skipped but counted, before break on v2050 delta)
    assertLastCheckpoint(
      files,
      beforeVersion = 2050,
      expCheckpointVersion = 2000,
      expNumFilesListed = 2002)
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
    val engine = createMockFSListFromEngine(deltaLogFiles)
    val result = findLastCompleteCheckpointBeforeHelper(engine, logPath, beforeVersion)
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
    val engine = createMockFSListFromEngine(deltaLogFiles)
    val result = findLastCompleteCheckpointBeforeHelper(engine, logPath, beforeVersion)
    assert(!result._1.isPresent, s"No checkpoint should be found for version=$beforeVersion")
    assert(result._2 == expNumFilesListed, s"Invalid number of files listed: $beforeVersion")
  }
}

object CheckpointerSuite extends VectorTestUtils {
  val SAMPLE_LAST_CHECKPOINT_FILE_CONTENT: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

    override def getColumnVector(ordinal: Int): ColumnVector = {
      ordinal match {
        case 0 => longVector(Seq(40)) // version
        case 1 => longVector(Seq(44)) // size
        case 2 => longVector(Seq(20)); // parts
        case 3 => mapTypeVector(Seq(Map.empty[String, String])) // tags
      }
    }

    override def getSize: Int = 1
  }

  val ZERO_ENTRIES_COLUMNAR_BATCH: ColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

    // empty vector for all columns
    override def getColumnVector(ordinal: Int): ColumnVector = longVector(Seq.empty)

    override def getSize: Int = 0
  }

  // scalastyle:off line.size.limit
  // A real V2 _last_checkpoint (from golden/v2-checkpoint-json)
  val SAMPLE_LAST_CHECKPOINT_HINT_BYTES: Array[Byte] =
    ("""{"version":2,"size":9,"sizeInBytes":19554,"numOfAddFiles":4,"v2Checkpoint":{""" +
      """"path":"00000000000000000002.checkpoint.6374b053-df23-479b-b2cf-c9c550132b49.json",""" +
      """"sizeInBytes":891,"modificationTime":1714496115810,""" +
      """"nonFileActions":[{"protocol":{"minReaderVersion":3,"minWriterVersion":7,""" +
      """"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint","appendOnly",""" +
      """"invariants"]}},{"metaData":{"id":"8a390218-e4ee-4341-b6de-4920e27d3f78",""" +
      """"format":{"provider":"parquet","options":{}},""" +
      """"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",""" +
      """\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{""" +
      """"delta.checkpointInterval":"2","delta.checkpointPolicy":"v2"},"createdTime":1714496114564}},""" +
      """{"checkpointMetadata":{"version":2}}],"sidecarFiles":[{""" +
      """"path":"00000000000000000002.checkpoint.0000000001.0000000002.bd1885fd-6ec0-4370-b0f5-43b5162fd4de.parquet",""" +
      """"sizeInBytes":9367,"modificationTime":1714496115780},{""" +
      """"path":"00000000000000000002.checkpoint.0000000002.0000000002.0a8d73ee-aa83-49d0-9583-c99db75b89b2.parquet",""" +
      """"sizeInBytes":9296,"modificationTime":1714496115788}]},""" +
      """"checksum":"d09f95a326aab562c60d415a32ddd216"}""").getBytes(StandardCharsets.UTF_8)
  // scalastyle:on line.size.limit
  val VALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/valid")
  val ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE = new Path("/zero_sized")
  val INVALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/invalid")
  val LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE = new Path("/filenotfoundtable")
  val OVERSIZED_LAST_CHECKPOINT_FILE_TABLE = new Path("/oversized")
  val EMPTY_CONTENT_LAST_CHECKPOINT_FILE_TABLE = new Path("/empty_content")
  val WRAPPED_NOT_FOUND_LAST_CHECKPOINT_FILE_TABLE = new Path("/wrapped_not_found")
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

    Utils.singletonCloseableIterator(
      try {
        if (currentFailCount < maxFailures) {
          currentFailCount += 1
          throw new IOException("Retryable exception")
        }

        path.getParent match {
          case VALID_LAST_CHECKPOINT_FILE_TABLE => SAMPLE_LAST_CHECKPOINT_FILE_CONTENT
          case ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE => ZERO_ENTRIES_COLUMNAR_BATCH
          case INVALID_LAST_CHECKPOINT_FILE_TABLE =>
            throw new IOException("Invalid last checkpoint file")
          case LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE =>
            throw new FileNotFoundException("File not found")
          case _ => throw new IOException("Unknown table")
        }
      } catch {
        case NonFatal(e) => throw new KernelEngineException("Failed to read last checkpoint", e);
      })
  }
}

/** Mocks [[io.delta.kernel.engine.FileSystemClient]] for raw `_last_checkpoint` byte reads. */
class MockLastCheckpointHintBytesFileSystemClient(maxFailures: Int = 0)
    extends BaseMockFileSystemClient {
  import CheckpointerSuite._
  var currentFailCount = 0

  override def getFileStatus(path: String): FileStatus = {
    new Path(path).getParent match {
      case VALID_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, SAMPLE_LAST_CHECKPOINT_HINT_BYTES.length, 0)
      case ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, 0, 0)
      case INVALID_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, SAMPLE_LAST_CHECKPOINT_HINT_BYTES.length, 0)
      case OVERSIZED_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, Integer.MAX_VALUE.toLong + 1, 0)
      case EMPTY_CONTENT_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, 1, 0)
      case LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE =>
        throw new FileNotFoundException("File not found")
      case WRAPPED_NOT_FOUND_LAST_CHECKPOINT_FILE_TABLE =>
        throw new KernelEngineException(
          "wrapped",
          new FileNotFoundException("File not found"))
      case _ => throw new IOException("Unknown table")
    }
  }

  override def readFiles(
      readRequests: CloseableIterator[FileReadRequest]): CloseableIterator[ByteArrayInputStream] = {
    if (currentFailCount < maxFailures) {
      currentFailCount += 1
      throw new IOException("Retryable exception")
    }

    val request = readRequests.next()
    Utils.singletonCloseableIterator(
      new Path(request.getPath).getParent match {
        case VALID_LAST_CHECKPOINT_FILE_TABLE =>
          new ByteArrayInputStream(SAMPLE_LAST_CHECKPOINT_HINT_BYTES)
        case INVALID_LAST_CHECKPOINT_FILE_TABLE =>
          throw new IOException("Invalid last checkpoint file")
        case EMPTY_CONTENT_LAST_CHECKPOINT_FILE_TABLE =>
          new ByteArrayInputStream(Array.emptyByteArray)
        case _ => throw new IOException("Unknown table")
      })
  }
}
