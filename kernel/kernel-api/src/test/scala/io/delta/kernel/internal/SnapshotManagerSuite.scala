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
package io.delta.kernel.internal

import java.util.{Arrays, Collections, Optional}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.exceptions.{InvalidTableException, TableNotFoundException}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.{CheckpointInstance, CheckpointMetaData, SidecarFile}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.snapshot.{LogSegment, SnapshotManager}
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockFileSystemClientUtils, MockListFromFileSystemClient, VectorTestUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class SnapshotManagerSuite extends AnyFunSuite with MockFileSystemClientUtils {

  test("verifyDeltaVersionsContiguous") {
    val path = new Path("/path/to/table")
    // empty array
    SnapshotManager.verifyDeltaVersionsContiguous(Collections.emptyList(), path)
    // array of size 1
    SnapshotManager.verifyDeltaVersionsContiguous(Collections.singletonList(1), path)
    // contiguous versions
    SnapshotManager.verifyDeltaVersionsContiguous(Arrays.asList(1, 2, 3), path)
    // non-contiguous versions
    intercept[InvalidTableException] {
      SnapshotManager.verifyDeltaVersionsContiguous(Arrays.asList(1, 3), path)
    }
    // duplicates in versions
    intercept[InvalidTableException] {
      SnapshotManager.verifyDeltaVersionsContiguous(Arrays.asList(1, 2, 2, 3), path)
    }
    // unsorted versions
    intercept[InvalidTableException] {
      SnapshotManager.verifyDeltaVersionsContiguous(Arrays.asList(3, 2, 1), path)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // getLogSegmentForVersion tests
  //////////////////////////////////////////////////////////////////////////////////

  private val snapshotManager = new SnapshotManager(dataPath)

  /* ------------------HELPER METHODS------------------ */

  private def checkLogSegment(
      logSegment: LogSegment,
      expectedVersion: Long,
      expectedDeltas: Seq[FileStatus],
      expectedCompactions: Seq[FileStatus],
      expectedCheckpoints: Seq[FileStatus],
      expectedCheckpointVersion: Option[Long],
      expectedLastCommitTimestamp: Long): Unit = {

    assert(logSegment.getLogPath == logPath)
    assert(logSegment.getVersion == expectedVersion)
    assert(expectedDeltas.map(f => (f.getPath, f.getSize, f.getModificationTime)) sameElements
      logSegment.getDeltas.asScala.map(f => (f.getPath, f.getSize, f.getModificationTime)))
    assert(expectedCompactions.map(f => (f.getPath, f.getSize, f.getModificationTime)) sameElements
      logSegment.getCompactions.asScala.map(f => (f.getPath, f.getSize, f.getModificationTime)))

    val expectedCheckpointStatuses = expectedCheckpoints
      .map(f => (f.getPath, f.getSize, f.getModificationTime)).sortBy(_._1)
    val actualCheckpointStatuses = logSegment.getCheckpoints.asScala
      .map(f => (f.getPath, f.getSize, f.getModificationTime)).sortBy(_._1)
    assert(
      expectedCheckpointStatuses sameElements actualCheckpointStatuses,
      s"expected:\n$expectedCheckpointStatuses\nactual:\n$actualCheckpointStatuses")

    expectedCheckpointVersion match {
      case Some(v) =>
        assert(logSegment.getCheckpointVersionOpt.isPresent() &&
          logSegment.getCheckpointVersionOpt.get == v)
      case None => assert(!logSegment.getCheckpointVersionOpt.isPresent())
    }
    assert(expectedLastCommitTimestamp == logSegment.getLastCommitTimestamp)
  }

  /**
   * Test `getLogSegmentForVersion` for a given set of delta versions, singular checkpoint versions,
   * and multi-part checkpoint versions with a given _last_checkpoint starting checkpoint and
   * a versionToLoad.
   *
   * @param deltaVersions versions of the delta JSON files in the delta log
   * @param checkpointVersions version of the singular checkpoint parquet files in the delta log
   * @param multiCheckpointVersions versions of the multi-part checkpoint files in the delta log
   * @param numParts number of parts for the multi-part checkpoints if applicable
   * @param startCheckpoint starting checkpoint to list from, in practice provided by the
   *                        _last_checkpoint file; if not provided list from 0
   * @param versionToLoad specific version to load; if not provided load the latest
   * @param v2CheckpointSpec Versions of V2 checkpoints to be included along with the number of
   *                         sidecars in each checkpoint and the naming scheme for each checkpoint.
   */
  def testWithCheckpoints(
      deltaVersions: Seq[Long],
      checkpointVersions: Seq[Long],
      multiCheckpointVersions: Seq[Long],
      numParts: Int = -1,
      startCheckpoint: Optional[java.lang.Long] = Optional.empty(),
      versionToLoad: Optional[java.lang.Long] = Optional.empty(),
      v2CheckpointSpec: Seq[(Long, Boolean, Int)] = Seq.empty,
      compactionVersions: Seq[(Long, Long)] = Seq.empty): Unit = {
    val deltas = deltaFileStatuses(deltaVersions)
    val singularCheckpoints = singularCheckpointFileStatuses(checkpointVersions)
    val multiCheckpoints = multiCheckpointFileStatuses(multiCheckpointVersions, numParts)

    // Only test both filetypes if we have to read the checkpoint top-level file.
    val topLevelFileTypes = if (v2CheckpointSpec.nonEmpty) {
      Seq("parquet", "json")
    } else {
      Seq("parquet")
    }
    topLevelFileTypes.foreach { topLevelFileType =>
      val v2Checkpoints =
        v2CheckpointFileStatuses(v2CheckpointSpec, topLevelFileType)
      val checkpointFiles = v2Checkpoints.flatMap {
        case (topLevelCheckpointFile, sidecars) =>
          Seq(topLevelCheckpointFile) ++ sidecars
      } ++ singularCheckpoints ++ multiCheckpoints

      val expectedCheckpointVersion = (checkpointVersions ++ multiCheckpointVersions ++
        v2CheckpointSpec.map(_._1))
        .filter(_ <= versionToLoad.orElse(Long.MaxValue))
        .sorted
        .lastOption

      val (expectedV2Checkpoint, expectedSidecars) = expectedCheckpointVersion.map { v =>
        val matchingCheckpoints = v2Checkpoints.filter { case (topLevelFile, _) =>
          FileNames.checkpointVersion(topLevelFile.getPath) == v
        }
        if (matchingCheckpoints.nonEmpty) {
          matchingCheckpoints.maxBy(f => new CheckpointInstance(f._1.getPath)) match {
            case (c, sidecars) => (Seq(c), sidecars)
          }
        } else {
          (Seq.empty, Seq.empty)
        }
      }.getOrElse((Seq.empty, Seq.empty))

      val compactions = compactedFileStatuses(compactionVersions)

      val logSegment = snapshotManager.getLogSegmentForVersion(
        createMockFSListFromEngine(
          listFromProvider(deltas ++ compactions ++ checkpointFiles)("/"),
          new MockSidecarParquetHandler(expectedSidecars),
          new MockSidecarJsonHandler(expectedSidecars)),
        versionToLoad)

      val expectedDeltas = deltaFileStatuses(
        deltaVersions.filter { v =>
          v > expectedCheckpointVersion.getOrElse(-1L) && v <= versionToLoad.orElse(Long.MaxValue)
        })
      val expectedCheckpoints = expectedCheckpointVersion.map { v =>
        if (expectedV2Checkpoint.nonEmpty) {
          expectedV2Checkpoint
        } else if (checkpointVersions.toSet.contains(v)) {
          singularCheckpointFileStatuses(Seq(v))
        } else {
          multiCheckpointFileStatuses(Seq(v), numParts)
        }
      }.getOrElse(Seq.empty)
      val expectedCompactions = compactedFileStatuses(
        compactionVersions.filter { case (s, e) =>
          // we can only use a compaction if it starts after the checkpoint and ends at or before
          // the version we're trying to load
          s > expectedCheckpointVersion.getOrElse(-1L) && e <= versionToLoad.orElse(Long.MaxValue)
        })

      checkLogSegment(
        logSegment,
        expectedVersion = versionToLoad.orElse(deltaVersions.max),
        expectedDeltas = expectedDeltas,
        expectedCompactions = expectedCompactions,
        expectedCheckpoints = expectedCheckpoints,
        expectedCheckpointVersion = expectedCheckpointVersion,
        expectedLastCommitTimestamp = versionToLoad.orElse(deltaVersions.max) * 10)
    }
  }

  /** Simple test for a log with only JSON files and no checkpoints */
  def testNoCheckpoint(
      deltaVersions: Seq[Long],
      versionToLoad: Optional[java.lang.Long] = Optional.empty()): Unit = {
    testWithCheckpoints(
      deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = versionToLoad)
  }

  /** Simple test with only json and compactions */
  def testWithCompactionsNoCheckpoint(
      deltaVersions: Seq[Long],
      compactionVersions: Seq[(Long, Long)],
      versionToLoad: Optional[java.lang.Long] = Optional.empty()): Unit = {
    testWithCheckpoints(
      deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = versionToLoad,
      compactionVersions = compactionVersions)
  }

  /**
   * Test `getLogSegmentForVersion` for a set of delta versions and checkpoint versions. Tests
   * with (1) singular checkpoint (2) multi-part checkpoints with 5 parts
   * (3) multi-part checkpoints with 1 part
   */
  def testWithSingularAndMultipartCheckpoint(
      deltaVersions: Seq[Long],
      checkpointVersions: Seq[Long],
      startCheckpoint: Optional[java.lang.Long] = Optional.empty(),
      versionToLoad: Optional[java.lang.Long] = Optional.empty()): Unit = {

    // test with singular checkpoint
    testWithCheckpoints(
      deltaVersions = deltaVersions,
      checkpointVersions = checkpointVersions,
      multiCheckpointVersions = Seq.empty,
      startCheckpoint = startCheckpoint,
      versionToLoad = versionToLoad)

    // test with multi-part checkpoint  numParts=5
    testWithCheckpoints(
      deltaVersions = deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = checkpointVersions,
      numParts = 5,
      startCheckpoint = startCheckpoint,
      versionToLoad = versionToLoad)

    // test with multi-part checkpoint numParts=1
    testWithCheckpoints(
      deltaVersions = deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = checkpointVersions,
      numParts = 1,
      startCheckpoint = startCheckpoint,
      versionToLoad = versionToLoad)
  }

  /**
   * For a given set of _delta_log files check for error.
   */
  def testExpectedError[T <: Throwable](
      files: Seq[FileStatus],
      lastCheckpointVersion: Optional[java.lang.Long] = Optional.empty(),
      versionToLoad: Optional[java.lang.Long] = Optional.empty(),
      expectedErrorMessageContains: String = "")(implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      snapshotManager.getLogSegmentForVersion(
        createMockFSAndJsonEngineForLastCheckpoint(files, lastCheckpointVersion),
        versionToLoad)
    }
    assert(e.getMessage.contains(expectedErrorMessageContains))
  }

  /* ------------------- VALID DELTA LOG FILE LISTINGS ----------------------- */

  test("getLogSegmentForVersion: 000.json only") {
    testNoCheckpoint(Seq(0))
    testNoCheckpoint(Seq(0), Optional.of(0))
  }

  test("getLogSegmentForVersion: 000.json .. 009.json") {
    testNoCheckpoint(0L until 10L)
    testNoCheckpoint(0L until 10L, Optional.of(9))
    testNoCheckpoint(0L until 10L, Optional.of(5))
  }

  test("getLogSegmentForVersion: 000.json..010.json + checkpoint(10)") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      versionToLoad = Optional.of(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      versionToLoad = Optional.of(6))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(6))
  }

  test("getLogSegmentForVersion: 000.json...20.json + checkpoint(10) + checkpoint(20)") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(20))
    // _last_checkpoint hasn't been updated yet
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      versionToLoad = Optional.of(15))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(15))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(15))
  }

  test("getLogSegmentForVersion: outdated _last_checkpoint that does not exist") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L until 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(10))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L until 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(20))
  }

  test("getLogSegmentForVersion: 20.json...25.json + checkpoint(20)") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(20))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20),
      versionToLoad = Optional.of(23))
  }

  test("getLogSegmentForVersion: empty delta log") {
    val exMsg = intercept[TableNotFoundException] {
      snapshotManager.getLogSegmentForVersion(
        createMockFSListFromEngine(Seq.empty),
        Optional.empty() /* versionToLoad */
      )
    }.getMessage

    assert(exMsg.contains("No delta files found in the directory"))
  }

  test("getLogSegmentForVersion: no delta files in the delta log") {
    // listDeltaAndCheckpointFiles = Optional.of(EmptyList)
    val files = Seq("foo", "notdelta.parquet", "foo.json", "001.checkpoint.00f.oo0.parquet")
      .map(FileStatus.of(_, 10, 10))
    testExpectedError[TableNotFoundException](
      files,
      expectedErrorMessageContains =
        "No delta files found in the directory: /fake/path/to/table/_delta_log")
    testExpectedError[TableNotFoundException](
      files,
      versionToLoad = Optional.of(5),
      expectedErrorMessageContains =
        "No delta files found in the directory: /fake/path/to/table/_delta_log")
  }

  test("getLogSegmentForVersion: versionToLoad higher than possible") {
    testExpectedError[RuntimeException](
      files = deltaFileStatuses(Seq(0L)),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains =
        "Cannot load table version 15 as it does not exist. The latest available version is 0")
    testExpectedError[RuntimeException](
      files = deltaFileStatuses((10L until 13L)) ++ singularCheckpointFileStatuses(Seq(10L)),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains =
        "Cannot load table version 15 as it does not exist. The latest available version is 12")
  }

  test("getLogSegmentForVersion: start listing from _last_checkpoint when it is provided") {
    val deltas = deltaFileStatuses(0L to 24)
    val checkpoints = singularCheckpointFileStatuses(Seq(10, 20))

    for (lastCheckpointVersion <- Seq(10, 20)) {
      val lastCheckpointFileStatus = FileStatus.of(s"$logPath/_last_checkpoint", 2, 2)
      val files = deltas ++ checkpoints ++ Seq(lastCheckpointFileStatus)

      def listFrom(filePath: String): Seq[FileStatus] = {
        if (filePath < FileNames.listingPrefix(logPath, lastCheckpointVersion)) {
          throw new RuntimeException(
            s"Listing from before the checkpoint version referenced by _last_checkpoint. " +
              s"Last checkpoint version: $lastCheckpointVersion. Listing from: $filePath")
        }
        listFromProvider(files)(filePath)
      }

      val logSegment = snapshotManager.getLogSegmentForVersion(
        mockEngine(
          jsonHandler = new MockReadLastCheckpointFileJsonHandler(
            lastCheckpointFileStatus.getPath,
            lastCheckpointVersion),
          fileSystemClient = new MockListFromFileSystemClient(listFrom)),
        Optional.empty() /* versionToLoad */
      )

      checkLogSegment(
        logSegment,
        expectedVersion = 24,
        expectedDeltas = deltaFileStatuses(21L until 25L),
        expectedCompactions = Seq.empty,
        expectedCheckpoints = singularCheckpointFileStatuses(Seq(20L)),
        expectedCheckpointVersion = Some(20),
        expectedLastCommitTimestamp = 240L)
    }
  }

  test("getLogSegmentForVersion: multi-part and single-part checkpoints in same log") {
    testWithCheckpoints(
      (0L to 50L),
      Seq(10, 30, 50),
      Seq(20, 40),
      numParts = 5)
    testWithCheckpoints(
      (0L to 50L),
      Seq(10, 30, 50),
      Seq(20, 40),
      numParts = 5,
      startCheckpoint = Optional.of(40))
  }

  test("getLogSegmentForVersion: versionToLoad not constructable from history") {
    testExpectedError[RuntimeException](
      deltaFileStatuses(20L until 25L) ++ singularCheckpointFileStatuses(Seq(20L)),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains = "Cannot load table version 15")
  }

  /* ------------------- V2 CHECKPOINT TESTS ------------------ */
  test("v2 checkpoint exists at version") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((0L, true, 2), (5L, true, 2)))
  }

  test("multiple v2 checkpoint exist at version") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((5L, true, 2), (5L, true, 2)))
  }

  test("v2 checkpoint exists before version") {
    testWithCheckpoints(
      deltaVersions = (0L to 7L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(6L),
      v2CheckpointSpec = Seq((0L, true, 2), (5L, true, 2)))
  }

  test("v1 and v2 checkpoints in table") {
    testWithCheckpoints(
      deltaVersions = (0L to 12L),
      checkpointVersions = Seq(0L, 10L),
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(8L),
      v2CheckpointSpec = Seq((5L, true, 2)))
    testWithCheckpoints(
      (0L to 12L),
      checkpointVersions = Seq(0L, 10L),
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(12L),
      v2CheckpointSpec = Seq((5L, true, 2)))
  }

  test("multipart and v2 checkpoints in table") {
    testWithCheckpoints(
      deltaVersions = (0L to 12L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq(0L, 10L),
      numParts = 5,
      versionToLoad = Optional.of(8L),
      v2CheckpointSpec = Seq((5L, true, 2)))
    testWithCheckpoints(
      deltaVersions = (0L to 12L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq(0L, 10L),
      numParts = 5,
      versionToLoad = Optional.of(12L),
      v2CheckpointSpec = Seq((5L, true, 2)))
  }

  test("no checkpoint prior to version") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(3L),
      v2CheckpointSpec = Seq((5L, true, 2)))
  }

  test("read from compatibility checkpoint") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((5L, false, 5)))
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((0L, true, 5), (5L, false, 5)))
  }

  test("read from V2 checkpoint with compatibility checkpoint at same version") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((5L, true, 5), (5L, false, 5)))
  }

  test("read from V2 checkpoint with compatibility checkpoint at previous version") {
    testWithCheckpoints(
      deltaVersions = (0L to 5L),
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointSpec = Seq((3L, false, 5), (5L, true, 5)))
  }

  /* ------------------- CORRUPT DELTA LOG FILE LISTINGS ------------------ */

  test("getLogSegmentForVersion: corrupt listing with only checkpoint file") {
    Seq(Optional.empty(), Optional.of(10L)).foreach { lastCheckpointVersion =>
      Seq(Optional.empty(), Optional.of(10L)).foreach { versionToLoad =>
        testExpectedError[InvalidTableException](
          files = singularCheckpointFileStatuses(Seq(10L)),
          lastCheckpointVersion.map(Long.box),
          versionToLoad.map(Long.box),
          expectedErrorMessageContains = "Missing delta file for version 10")
      }
    }
  }

  test("getLogSegmentForVersion: corrupt listing with missing log files") {
    // checkpoint(10), 010.json, 011.json, 013.json
    val fileList = deltaFileStatuses(Seq(10L, 11L)) ++ deltaFileStatuses(Seq(13L)) ++
      singularCheckpointFileStatuses(Seq(10L))
    Seq(Optional.empty(), Optional.of(10L)).foreach { lastCheckpointVersion =>
      Seq(Optional.empty(), Optional.of(13L)).foreach { versionToLoad =>
        testExpectedError[InvalidTableException](
          fileList,
          lastCheckpointVersion.map(Long.box),
          versionToLoad.map(Long.box),
          expectedErrorMessageContains = "versions are not contiguous: ([11, 13])")
      }
    }
  }

  test("getLogSegmentForVersion: corrupt listing 000.json...009.json + checkpoint(10)") {
    val fileList = deltaFileStatuses((0L until 10L)) ++ singularCheckpointFileStatuses(Seq(10L))
    Seq(Optional.empty(), Optional.of(10L)).foreach { lastCheckpointVersion =>
      Seq(Optional.empty(), Optional.of(15L)).foreach { versionToLoad =>
        testExpectedError[InvalidTableException](
          fileList,
          lastCheckpointVersion.map(Long.box),
          versionToLoad.map(Long.box),
          expectedErrorMessageContains = "Missing delta file for version 10")
      }
    }
  }

  test("getLogSegmentForVersion: corrupt listing: checkpoint(10); 11 to 14.json; no 10.json") {
    val fileList = singularCheckpointFileStatuses(Seq(10L)) ++ deltaFileStatuses((11L until 15L))
    Seq(Optional.empty(), Optional.of(10L)).foreach { lastCheckpointVersion =>
      Seq(Optional.empty(), Optional.of(10L)).foreach { versionToLoad =>
        testExpectedError[InvalidTableException](
          fileList,
          lastCheckpointVersion.map(Long.box),
          versionToLoad.map(Long.box),
          expectedErrorMessageContains = "Missing delta file for version 10")
      }
    }
  }

  test("getLogSegmentForVersion: corrupted log missing json files / no way to construct history") {
    testExpectedError[InvalidTableException](
      deltaFileStatuses(1L until 10L),
      expectedErrorMessageContains = "Cannot compute snapshot. Missing delta file version 0.")
    testExpectedError[InvalidTableException](
      deltaFileStatuses(15L until 25L) ++ singularCheckpointFileStatuses(Seq(20L)),
      versionToLoad = Optional.of(17),
      expectedErrorMessageContains = "Cannot compute snapshot. Missing delta file version 0.")
    testExpectedError[InvalidTableException](
      deltaFileStatuses((0L until 5L) ++ (6L until 9L)),
      expectedErrorMessageContains = "are not contiguous")
    // corrupt incomplete multi-part checkpoint
    val corruptedCheckpointStatuses = FileNames.checkpointFileWithParts(logPath, 10, 5).asScala
      .map(p => FileStatus.of(p.toString, 10, 10))
      .take(4)
    val deltas = deltaFileStatuses(10L to 13L)
    testExpectedError[InvalidTableException](
      corruptedCheckpointStatuses.toSeq ++ deltas,
      expectedErrorMessageContains = "Cannot compute snapshot. Missing delta file version 0.")
  }

  test("getLogSegmentForVersion: corrupt log but reading outside corrupted range") {
    testNoCheckpoint(
      deltaVersions = (0L until 5L) ++ (6L until 9L),
      versionToLoad = Optional.of(4))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = 15L until 25L,
      checkpointVersions = Seq(20),
      versionToLoad = Optional.of(22))
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = 15L until 25L,
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(22))
  }

  test("getLogSegmentForVersion: corrupt _last_checkpoint (is after existing versions)") {
    // in the case of a corrupted _last_checkpoint we revert to listing from version 0
    // (on first run newFiles.isEmpty() but since startingCheckpointOpt.isPresent() re-list from 0)
    testWithSingularAndMultipartCheckpoint(
      (0L until 25L),
      Seq(10L, 20L),
      startCheckpoint = Optional.of(30))
  }

  test("getLogSegmentForVersion: corrupt _last_checkpoint refers to in range version " +
    "but no valid checkpoint") {
    // _last_checkpoint refers to a v1 checkpoint at version 20 that is missing
    testExpectedError[RuntimeException](
      deltaFileStatuses(0L until 25L) ++ singularCheckpointFileStatuses(Seq(10L)),
      lastCheckpointVersion = Optional.of(20),
      expectedErrorMessageContains = "Missing checkpoint at version 20")
    // _last_checkpoint refers to incomplete multi-part checkpoint at version 20 that is missing
    val corruptedCheckpointStatuses = FileNames.checkpointFileWithParts(logPath, 20, 5).asScala
      .map(p => FileStatus.of(p.toString, 10, 10))
      .take(4)
    testExpectedError[RuntimeException](
      files = corruptedCheckpointStatuses.toSeq ++ deltaFileStatuses(10L to 20L) ++
        singularCheckpointFileStatuses(Seq(10L)),
      lastCheckpointVersion = Optional.of(20),
      expectedErrorMessageContains = "Missing checkpoint at version 20")
  }

  test("getLogSegmentForVersion: corrupted incomplete multi-part checkpoint with no" +
    "_last_checkpoint or a valid _last_checkpoint provided") {
    val cases: Seq[(Long, Seq[Long], Seq[Long], Optional[java.lang.Long])] = Seq(
      /* (corruptedCheckpointVersion, validCheckpointVersions, deltaVersions, lastCheckpointV) */
      (20, Seq(10), (10L to 20L), Optional.empty()),
      (20, Seq(10), (10L to 20L), Optional.of(10)),
      (10, Seq.empty, (0L to 10L), Optional.empty()))
    cases.foreach { case (corruptedVersion, validVersions, deltaVersions, lastCheckpointVersion) =>
      val corruptedCheckpoint = FileNames.checkpointFileWithParts(logPath, corruptedVersion, 5)
        .asScala
        .map(p => FileStatus.of(p.toString, 10, 10))
        .take(4)
      val checkpoints = singularCheckpointFileStatuses(validVersions)
      val deltas = deltaFileStatuses(deltaVersions)
      val allFiles = deltas ++ corruptedCheckpoint ++ checkpoints
      val logSegment = snapshotManager.getLogSegmentForVersion(
        createMockFSAndJsonEngineForLastCheckpoint(allFiles, lastCheckpointVersion),
        Optional.empty())
      val checkpointVersion = validVersions.sorted.lastOption
      checkLogSegment(
        logSegment,
        expectedVersion = deltaVersions.max,
        expectedDeltas = deltaFileStatuses(
          deltaVersions.filter(_ > checkpointVersion.getOrElse(-1L))),
        expectedCompactions = Seq.empty,
        expectedCheckpoints = checkpoints,
        expectedCheckpointVersion = checkpointVersion,
        expectedLastCommitTimestamp = deltaVersions.max * 10)
    }
  }

  test("getLogSegmentForVersion: corrupt _last_checkpoint with empty delta log") {
    val exMsg = intercept[InvalidTableException] {
      snapshotManager.getLogSegmentForVersion(
        createMockFSAndJsonEngineForLastCheckpoint(Seq.empty, Optional.of(1)),
        Optional.empty())
    }.getMessage

    assert(exMsg.contains("Missing checkpoint at version 1"))
  }

  test("One compaction") {
    testWithCompactionsNoCheckpoint(
      deltaVersions = 0L until 5L,
      compactionVersions = Seq((0, 4)))

    testWithCompactionsNoCheckpoint(
      deltaVersions = 0L until 5L,
      compactionVersions = Seq((0, 4)),
      versionToLoad = Optional.of(4))
  }

  test("Compaction extends too far") {
    testWithCompactionsNoCheckpoint(
      deltaVersions = 0L until 5L,
      compactionVersions = Seq((3, 5)),
      versionToLoad = Optional.of(4))
  }

  test("Compaction after checkpoint") {
    testWithCheckpoints(
      deltaVersions = 0L until 6L,
      checkpointVersions = Seq(2),
      multiCheckpointVersions = Seq.empty,
      compactionVersions = Seq((3, 5)))
  }

  test("Compaction starting before checkpoint") {
    testWithCheckpoints(
      deltaVersions = 0L until 6L,
      checkpointVersions = Seq(2),
      multiCheckpointVersions = Seq.empty,
      compactionVersions = Seq((1, 5)))
  }

  test("Compaction starting same as checkpoint") {
    testWithCheckpoints(
      deltaVersions = 0L until 5L,
      checkpointVersions = Seq(2),
      multiCheckpointVersions = Seq.empty,
      compactionVersions = Seq((2, 5)))
  }
}

trait SidecarIteratorProvider extends VectorTestUtils {
  def singletonSidecarIterator(sidecars: Seq[FileStatus])
      : CloseableIterator[ColumnarBatch] = Utils.singletonCloseableIterator(
    new ColumnarBatch {
      override def getSchema: StructType = SidecarFile.READ_SCHEMA

      override def getColumnVector(ordinal: Int): ColumnVector = {
        ordinal match {
          case 0 => stringVector(sidecars.map(_.getPath)) // path
          case 1 => longVector(sidecars.map(_.getSize): _*) // size
          case 2 =>
            longVector(sidecars.map(_.getModificationTime): _*); // modification time
        }
      }

      override def getSize: Int = sidecars.length
    })
}

class MockSidecarParquetHandler(sidecars: Seq[FileStatus])
    extends BaseMockParquetHandler with SidecarIteratorProvider {
  override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
    singletonSidecarIterator(sidecars)
}

class MockSidecarJsonHandler(sidecars: Seq[FileStatus])
    extends BaseMockJsonHandler
    with SidecarIteratorProvider {
  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
    singletonSidecarIterator(sidecars)
}

class MockReadLastCheckpointFileJsonHandler(
    lastCheckpointPath: String,
    lastCheckpointVersion: Long)
    extends BaseMockJsonHandler with VectorTestUtils {
  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    assert(fileIter.hasNext)
    assert(fileIter.next.getPath == lastCheckpointPath)

    Utils.singletonCloseableIterator(
      new ColumnarBatch {
        override def getSchema: StructType = CheckpointMetaData.READ_SCHEMA

        override def getColumnVector(ordinal: Int): ColumnVector = {
          ordinal match {
            case 0 => longVector(lastCheckpointVersion) /* version */
            case 1 => longVector(100) /* size */
            case 2 => longVector(1) /* parts */
          }
        }

        override def getSize: Int = 1
      })
  }
}
