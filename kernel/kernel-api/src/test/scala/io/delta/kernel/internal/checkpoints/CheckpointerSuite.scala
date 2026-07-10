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
import io.delta.kernel.internal.checkpoints.Checkpointer.findLastCompleteCheckpointBeforeHelper
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames.checkpointFileSingular
import io.delta.kernel.internal.util.JsonUtils
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.test.{BaseMockFileSystemClient, BaseMockJsonHandler, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class CheckpointerSuite extends AnyFunSuite with MockFileSystemClientUtils {
  import CheckpointerSuite._

  /** Parses two JSON strings and asserts semantic equality (object key order is irrelevant). */
  private def assertJsonEquals(expected: String, actual: String): Unit = {
    assert(
      JsonUtils.mapper.readTree(expected) == JsonUtils.mapper.readTree(actual),
      s"JSON mismatch.\n expected: $expected\n actual:   $actual")
  }

  /**
   * Builds a [[CheckpointMetaData]] via the full constructor with typed, defaulted arguments. The
   * explicit `Optional` element types are needed because Scala cannot infer them for the Java
   * `Optional` parameters (e.g. a bare `Optional.empty()` would be `Optional[Nothing]`).
   */
  private def checkpointMetaData(
      version: Long,
      size: Long,
      parts: Optional[java.lang.Long] = Optional.empty(),
      tags: java.util.Map[String, String] = java.util.Map.of()): CheckpointMetaData =
    new CheckpointMetaData(version, size, parts, tags)

  //////////////////////////////////////////////////////////////////////////////////
  // readLastCheckpointFile tests
  //////////////////////////////////////////////////////////////////////////////////
  private def mockLastCheckpointEngine(
      maxFailures: Int): (io.delta.kernel.engine.Engine, MockLastCheckpointFileSystemClient) = {
    val fsClient = new MockLastCheckpointFileSystemClient(maxFailures)
    val jsonHandler = new MockLastCheckpointMetadataFileReader()
    (mockEngine(jsonHandler = jsonHandler, fileSystemClient = fsClient), fsClient)
  }

  test("load a valid last checkpoint metadata file") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(engine)
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(fsClient.currentFailCount == 0)
  }

  test("load a zero-sized last checkpoint metadata file") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(engine)
    assert(!lastCheckpoint.isPresent)
    assert(fsClient.currentFailCount == 0)
  }

  test("load an invalid last checkpoint metadata file") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(INVALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(engine)
    assert(!lastCheckpoint.isPresent)
    assert(fsClient.currentFailCount == 0)
  }

  test("retry last checkpoint metadata loading - succeeds at third attempt") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 2)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(engine)
    assertValidCheckpointMetadata(lastCheckpoint)
    assert(fsClient.currentFailCount == 2)
  }

  test("retry last checkpoint metadata loading - exceeds max failures") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 4)
    val lastCheckpoint = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointFile(engine)
    assert(!lastCheckpoint.isPresent)
    assert(fsClient.currentFailCount == 3) // 3 is the max retries
  }

  test("try to load last checkpoint metadata when the file is missing") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val lastCheckpoint = new Checkpointer(LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE)
      .readLastCheckpointFile(engine)
    assert(!lastCheckpoint.isPresent)
    assert(fsClient.currentFailCount == 0)
  }

  test("readLastCheckpointSerialized returns the opaque JSON blob") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val serialized = new Checkpointer(VALID_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointSerialized(engine)
    assert(serialized.isPresent)
    assert(serialized.get().json() == SAMPLE_LAST_CHECKPOINT_JSON)
    assert(fsClient.currentFailCount == 0)
  }

  test("readLastCheckpointSerialized rejects malformed JSON after retries") {
    val (engine, fsClient) = mockLastCheckpointEngine(maxFailures = 0)
    val serialized = new Checkpointer(MALFORMED_JSON_LAST_CHECKPOINT_FILE_TABLE)
      .readLastCheckpointSerialized(engine)
    assert(!serialized.isPresent)
    assert(fsClient.currentFailCount == 0)
  }

  //////////////////////////////////////////////////////////////////////////////////
  // CheckpointMetaData.toJson / toRow tests
  //////////////////////////////////////////////////////////////////////////////////
  test("classic (V1) CheckpointMetaData serializes only the present fields") {
    val cpm = checkpointMetaData(40L, 44L, parts = Optional.of(20L))
    assertJsonEquals("""{"version":40,"size":44,"parts":20}""", cpm.toJson())
  }

  test("classic (V1) CheckpointMetaData without parts omits parts") {
    assertJsonEquals("""{"version":7,"size":3}""", checkpointMetaData(7L, 3L).toJson())
  }

  test("CheckpointMetaData round-trips classic fields through toRow -> fromRow") {
    val cpm = checkpointMetaData(
      12L,
      34L,
      parts = Optional.of(2L),
      tags = java.util.Map.of("k", "v"))
    val restored = CheckpointMetaData.fromRow(cpm.toRow())
    assert(restored.version == 12L)
    assert(restored.size == 34L)
    assert(restored.parts == Optional.of(2L))
    assert(restored.tags == java.util.Map.of("k", "v"))
  }

  test("CheckpointMetaData.toJson returns the captured blob when present") {
    val blob = """{"version":2,"size":9,"checkpointSchema":{"type":"struct"}}"""
    val cpm = CheckpointMetaData.fromRow(checkpointMetaData(2L, 9L).toRow(), Optional.of(blob))
    assert(cpm.toJson() == blob)
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
        case 2 => longVector(Seq(20)) // parts
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

  val VALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/valid")
  val ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE = new Path("/zero_sized")
  val INVALID_LAST_CHECKPOINT_FILE_TABLE = new Path("/invalid")
  val MALFORMED_JSON_LAST_CHECKPOINT_FILE_TABLE = new Path("/malformed_json")
  val LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE = new Path("/filenotfoundtable")

  val SAMPLE_LAST_CHECKPOINT_JSON: String = """{"version":40,"size":44,"parts":20}"""
  val MALFORMED_LAST_CHECKPOINT_JSON: String = """{not valid json"""
}

/** `maxFailures` allows how many times to fail before returning the valid data */
class MockLastCheckpointFileSystemClient(maxFailures: Int) extends BaseMockFileSystemClient {
  import CheckpointerSuite._
  var currentFailCount = 0

  private def tableFor(path: String): Path = new Path(path).getParent

  override def getFileStatus(path: String): FileStatus = {
    tableFor(path) match {
      case LAST_CHECKPOINT_FILE_NOT_FOUND_TABLE =>
        throw new FileNotFoundException("File not found")
      case ZERO_SIZED_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(path, 0, 0)
      case VALID_LAST_CHECKPOINT_FILE_TABLE | INVALID_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(
          path,
          SAMPLE_LAST_CHECKPOINT_JSON.getBytes(StandardCharsets.UTF_8).length,
          0)
      case MALFORMED_JSON_LAST_CHECKPOINT_FILE_TABLE =>
        FileStatus.of(
          path,
          MALFORMED_LAST_CHECKPOINT_JSON.getBytes(StandardCharsets.UTF_8).length,
          0)
      case _ => throw new IOException("Unknown table")
    }
  }

  override def readFiles(
      readRequests: CloseableIterator[FileReadRequest]): CloseableIterator[ByteArrayInputStream] = {
    val request = readRequests.next()
    val table = tableFor(request.getPath)

    Utils.singletonCloseableIterator(
      try {
        if (currentFailCount < maxFailures) {
          currentFailCount += 1
          throw new IOException("Retryable exception")
        }

        table match {
          case VALID_LAST_CHECKPOINT_FILE_TABLE =>
            new ByteArrayInputStream(
              SAMPLE_LAST_CHECKPOINT_JSON.getBytes(StandardCharsets.UTF_8))
          case MALFORMED_JSON_LAST_CHECKPOINT_FILE_TABLE =>
            new ByteArrayInputStream(
              MALFORMED_LAST_CHECKPOINT_JSON.getBytes(StandardCharsets.UTF_8))
          case INVALID_LAST_CHECKPOINT_FILE_TABLE =>
            throw new IOException("Invalid last checkpoint file")
          case _ => throw new IOException("Unknown table")
        }
      } catch {
        case NonFatal(e) => throw new KernelEngineException("Failed to read last checkpoint", e)
      })
  }
}

class MockLastCheckpointMetadataFileReader extends BaseMockJsonHandler {
  import CheckpointerSuite._

  override def parseJson(
      jsonStringVector: ColumnVector,
      outputSchema: StructType,
      selectionVector: Optional[ColumnVector]): ColumnarBatch =
    SAMPLE_LAST_CHECKPOINT_FILE_CONTENT
}
