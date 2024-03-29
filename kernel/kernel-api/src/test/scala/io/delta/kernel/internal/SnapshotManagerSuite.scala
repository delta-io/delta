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

import java.io.IOException
import java.util.{Arrays, Collections, Optional}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.SidecarFile
import io.delta.kernel.internal.fs.Path
import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.internal.snapshot.{LogSegment, SnapshotManager}
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.test.{BaseMockJsonHandler, BaseMockParquetHandler, MockFileSystemClientUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

class SnapshotManagerSuite extends AnyFunSuite with MockFileSystemClientUtils {

  test("verifyDeltaVersions") {
    // empty array
    SnapshotManager.verifyDeltaVersions(
      Collections.emptyList(),
      Optional.empty(),
      Optional.empty())
    // contiguous versions
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.empty(),
      Optional.empty())
    // contiguous versions with correct `expectedStartVersion` and `expectedStartVersion`
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.empty(),
      Optional.of(3))
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.of(1),
      Optional.empty())
    SnapshotManager.verifyDeltaVersions(
      Arrays.asList(1, 2, 3),
      Optional.of(1),
      Optional.of(3))
    // `expectedStartVersion` or `expectedEndVersion` doesn't match
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2),
        Optional.of(0),
        Optional.empty())
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2),
        Optional.empty(),
        Optional.of(3))
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Collections.emptyList(),
        Optional.of(0),
        Optional.empty())
    }
    intercept[IllegalArgumentException] {
      SnapshotManager.verifyDeltaVersions(
        Collections.emptyList(),
        Optional.empty(),
        Optional.of(3))
    }
    // non contiguous versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 3),
        Optional.empty(),
        Optional.empty())
    }
    // duplicates in versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(1, 2, 2, 3),
        Optional.empty(),
        Optional.empty())
    }
    // unsorted versions
    intercept[IllegalStateException] {
      SnapshotManager.verifyDeltaVersions(
        Arrays.asList(3, 2, 1),
        Optional.empty(),
        Optional.empty())
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // getLogSegmentForVersion tests
  //////////////////////////////////////////////////////////////////////////////////

  private val snapshotManager = new SnapshotManager(logPath, dataPath)

  /* ------------------HELPER METHODS------------------ */

  private def checkLogSegment(
      logSegment: LogSegment,
      expectedVersion: Long,
      expectedDeltas: Seq[FileStatus],
      expectedCheckpoints: Seq[FileStatus],
      expectedCheckpointVersion: Option[Long],
      expectedLastCommitTimestamp: Long): Unit = {

    assert(logSegment.logPath == logPath)
    assert(logSegment.version == expectedVersion)
    assert(expectedDeltas.map(f => (f.getPath, f.getSize, f.getModificationTime)) sameElements
      logSegment.deltas.asScala.map(f => (f.getPath, f.getSize, f.getModificationTime)))

    val expectedCheckpointStatuses = expectedCheckpoints
      .map(f => (f.getPath, f.getSize, f.getModificationTime)).sortBy(_._1)
    val actualCheckpointStatuses = logSegment.checkpoints.asScala
      .map(f => (f.getPath, f.getSize, f.getModificationTime)).sortBy(_._1)
    assert(expectedCheckpointStatuses sameElements actualCheckpointStatuses,
      s"expected:\n$expectedCheckpointStatuses\nactual:\n$actualCheckpointStatuses")

    expectedCheckpointVersion match {
      case Some(v) =>
        assert(logSegment.checkpointVersionOpt.isPresent() &&
          logSegment.checkpointVersionOpt.get == v)
      case None => assert(!logSegment.checkpointVersionOpt.isPresent())
    }
    assert(expectedLastCommitTimestamp == logSegment.lastCommitTimestamp)
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
   */
  def testWithCheckpoints(
      deltaVersions: Seq[Long],
      checkpointVersions: Seq[Long],
      multiCheckpointVersions: Seq[Long],
      numParts: Int = -1,
      startCheckpoint: Optional[java.lang.Long] = Optional.empty(),
      versionToLoad: Optional[java.lang.Long] = Optional.empty(),
      v2CheckpointVersionsAndNumSidecars: Seq[(Long, Int)] = Seq.empty): Unit = {
    val deltas = deltaFileStatuses(deltaVersions)
    val singularCheckpoints = singularCheckpointFileStatuses(checkpointVersions)
    val multiCheckpoints = multiCheckpointFileStatuses(multiCheckpointVersions, numParts)

    // Only test both filetypes if we have to read the checkpoint manifest.
    val manifestFileTypes = if (v2CheckpointVersionsAndNumSidecars.nonEmpty) {
      Seq("parquet", "json")
    } else {
      Seq("parquet")
    }
    manifestFileTypes.foreach { manifestFileType =>
      val v2Checkpoints =
        v2CheckpointFileStatuses(v2CheckpointVersionsAndNumSidecars, manifestFileType)
      val checkpointFiles = v2Checkpoints.flatMap {
        case (checkpointManifest, sidecars) => Seq(checkpointManifest) ++ sidecars
      } ++ singularCheckpoints ++ multiCheckpoints

      val expectedCheckpointVersion = (checkpointVersions ++ multiCheckpointVersions ++
        v2CheckpointVersionsAndNumSidecars.map(_._1))
        .filter(_ <= versionToLoad.orElse(Long.MaxValue))
        .sorted
        .lastOption

      val (expectedV2Checkpoint, expectedSidecars) = expectedCheckpointVersion.map { v =>
        val matchingCheckpoints = v2Checkpoints.filter { case (manifest, _) =>
          FileNames.checkpointVersion(manifest.getPath) == v
        }
        if (matchingCheckpoints.nonEmpty) {
          matchingCheckpoints.head match {
            case (c, sidecars) => (Seq(c), sidecars)
          }
        } else {
          (Seq.empty, Seq.empty)
        }
      }.getOrElse((Seq.empty, Seq.empty))

      val logSegmentOpt = snapshotManager.getLogSegmentForVersion(
        createMockFSListFromTableClient(listFromProvider(deltas ++ checkpointFiles)("/"),
          new MockSidecarParquetHandler(expectedSidecars),
          new MockSidecarJsonHandler(expectedSidecars)),
        Optional.empty(),
        versionToLoad
      )
      assert(logSegmentOpt.isPresent())

      val expectedDeltas = deltaFileStatuses(
        deltaVersions.filter { v =>
          v > expectedCheckpointVersion.getOrElse(-1L) && v <= versionToLoad.orElse(Long.MaxValue)
        }
      )
      val expectedCheckpoints = expectedCheckpointVersion.map { v =>
        if (expectedV2Checkpoint.nonEmpty) {
          expectedV2Checkpoint ++ expectedSidecars
        }
        else if (checkpointVersions.toSet.contains(v)) {
          singularCheckpointFileStatuses(Seq(v))
        } else {
          multiCheckpointFileStatuses(Seq(v), numParts)
        }
      }.getOrElse(Seq.empty)

      checkLogSegment(
        logSegmentOpt.get(),
        expectedVersion = versionToLoad.orElse(deltaVersions.max),
        expectedDeltas = expectedDeltas,
        expectedCheckpoints = expectedCheckpoints,
        expectedCheckpointVersion = expectedCheckpointVersion,
        expectedLastCommitTimestamp = versionToLoad.orElse(deltaVersions.max) * 10
      )
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
      versionToLoad = versionToLoad
    )
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
      versionToLoad = versionToLoad
    )

    // test with multi-part checkpoint  numParts=5
    testWithCheckpoints(
      deltaVersions = deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = checkpointVersions,
      numParts = 5,
      startCheckpoint = startCheckpoint,
      versionToLoad = versionToLoad
    )

    // test with multi-part checkpoint numParts=1
    testWithCheckpoints(
      deltaVersions = deltaVersions,
      checkpointVersions = Seq.empty,
      multiCheckpointVersions = checkpointVersions,
      numParts = 1,
      startCheckpoint = startCheckpoint,
      versionToLoad = versionToLoad
    )
  }

  /**
   * For a given set of _delta_log files check for error.
   */
  def testExpectedError[T <: Throwable](
      files: Seq[FileStatus],
      startCheckpoint: Optional[java.lang.Long] = Optional.empty(),
      versionToLoad: Optional[java.lang.Long] = Optional.empty(),
      expectedErrorMessageContains: String = "")(implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      snapshotManager.getLogSegmentForVersion(
        createMockFSListFromTableClient(files),
        startCheckpoint,
        versionToLoad
      )
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
      checkpointVersions = Seq(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      versionToLoad = Optional.of(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      versionToLoad = Optional.of(6)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 10L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(6)
    )
  }

  test("getLogSegmentForVersion: 000.json...20.json + checkpoint(10) + checkpoint(20)") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(20)
    )
    // _last_checkpoint hasn't been updated yet
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      versionToLoad = Optional.of(15)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(15)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (0L to 20L),
      checkpointVersions = Seq(10, 20),
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(15)
    )
  }

  test("getLogSegmentForVersion: outdated _last_checkpoint that does not exist") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L until 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L until 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(20)
    )
  }

  test("getLogSegmentForVersion: 20.json...25.json + checkpoint(20)") {
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(20)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (20L to 25L),
      checkpointVersions = Seq(20),
      versionToLoad = Optional.of(23)
    )
  }

  test("getLogSegmentForVersion: empty delta log") {
    // listDeltaAndCheckpointFiles = Optional.empty()
    val logSegmentOpt = snapshotManager.getLogSegmentForVersion(
      createMockFSListFromTableClient(Seq.empty),
      Optional.empty(),
      Optional.empty()
    )
    assert(!logSegmentOpt.isPresent())
  }

  test("getLogSegmentForVersion: no delta files in the delta log") {
    // listDeltaAndCheckpointFiles = Optional.of(EmptyList)
    val files = Seq("foo", "notdelta.parquet", "foo.json", "001.checkpoint.00f.oo0.parquet")
      .map(FileStatus.of(_, 10, 10))
    testExpectedError[RuntimeException](
      files,
      expectedErrorMessageContains =
        "No delta files found in the directory: /fake/path/to/table/_delta_log"
    )
    testExpectedError[RuntimeException](
      files,
      versionToLoad = Optional.of(5),
      expectedErrorMessageContains =
        "No delta files found in the directory: /fake/path/to/table/_delta_log"
    )
  }

  test("getLogSegmentForVersion: versionToLoad higher than possible") {
    testExpectedError[RuntimeException](
      files = deltaFileStatuses(Seq(0L)),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains =
        "Trying to load a non-existent version 15. The latest version available is 0"
    )
    testExpectedError[RuntimeException](
      files = deltaFileStatuses((10L until 13L)) ++ singularCheckpointFileStatuses(Seq(10L)),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains =
        "Trying to load a non-existent version 15. The latest version available is 12"
    )
  }

  test("getLogSegmentForVersion: start listing from _last_checkpoint when it is provided") {
    val deltas = deltaFileStatuses(0L until 25)
    val checkpoints = singularCheckpointFileStatuses(Seq(10L, 20L))
    val files = deltas ++ checkpoints
    def listFrom(minVersion: Long)(filePath: String): Seq[FileStatus] = {
      if (filePath < FileNames.listingPrefix(logPath, minVersion)) {
        throw new RuntimeException("Listing from before provided _last_checkpoint")
      }
      listFromProvider(files)(filePath)
    }
    for (checkpointV <- Seq(10, 20)) {
      val logSegmentOpt = snapshotManager.getLogSegmentForVersion(
        createMockFSListFromTableClient(listFrom(checkpointV)(_)),
        Optional.of(checkpointV),
        Optional.empty()
      )
      assert(logSegmentOpt.isPresent())
      checkLogSegment(
        logSegmentOpt.get(),
        expectedVersion = 24,
        expectedDeltas = deltaFileStatuses(21L until 25L),
        expectedCheckpoints = singularCheckpointFileStatuses(Seq(20L)),
        expectedCheckpointVersion = Some(20),
        expectedLastCommitTimestamp = 240L
      )
    }
  }

  test("getLogSegmentForVersion: multi-part and single-part checkpoints in same log") {
    testWithCheckpoints(
      (0L to 50L),
      Seq(10, 30, 50),
      Seq(20, 40),
      numParts = 5
    )
    testWithCheckpoints(
      (0L to 50L),
      Seq(10, 30, 50),
      Seq(20, 40),
      numParts = 5,
      startCheckpoint = Optional.of(40)
    )
  }

  test("getLogSegmentForVersion: versionToLoad not constructable from history") {
    val files = deltaFileStatuses(20L until 25L) ++ singularCheckpointFileStatuses(Seq(20L))
    testExpectedError[RuntimeException](
      files,
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains = "Unable to reconstruct state at version 15"
    )
    testExpectedError[RuntimeException](
      files,
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains = "Unable to reconstruct state at version 15"
    )
  }

  test("v2 checkpoint exists at version") {
    testWithCheckpoints(
      (0L to 5L),
      Seq.empty,
      Seq.empty,
      versionToLoad = Optional.of(5L),
      v2CheckpointVersionsAndNumSidecars = Seq((0L, 2), (5L, 2)))
  }

  test("v2 checkpoint exists before version") {
    testWithCheckpoints(
      (0L to 7L),
      Seq.empty,
      Seq.empty,
      versionToLoad = Optional.of(6L),
      v2CheckpointVersionsAndNumSidecars = Seq((0L, 2), (5L, 2)))
  }

  test("v1 and v2 checkpoints in table") {
    testWithCheckpoints(
      (0L to 12L),
      Seq(0L, 10L),
      Seq.empty,
      versionToLoad = Optional.of(8L),
      v2CheckpointVersionsAndNumSidecars = Seq((5L, 2)))
    testWithCheckpoints(
      (0L to 12L),
      Seq(0L, 10L),
      Seq.empty,
      versionToLoad = Optional.of(12L),
      v2CheckpointVersionsAndNumSidecars = Seq((5L, 2)))
  }

  test("multipart and v2 checkpoints in table") {
    testWithCheckpoints(
      (0L to 12L),
      Seq.empty,
      Seq(0L, 10L),
      numParts = 5,
      versionToLoad = Optional.of(8L),
      v2CheckpointVersionsAndNumSidecars = Seq((5L, 2)))
    testWithCheckpoints(
      (0L to 12L),
      Seq.empty,
      Seq(0L, 10L),
      numParts = 5,
      versionToLoad = Optional.of(12L),
      v2CheckpointVersionsAndNumSidecars = Seq((5L, 2)))
  }

  test("no checkpoint prior to version") {
    testWithCheckpoints(
      (0L to 5L),
      Seq.empty,
      Seq.empty,
      versionToLoad = Optional.of(3L),
      v2CheckpointVersionsAndNumSidecars = Seq((5L, 2)))
  }

  /* ------------------- CORRUPT DELTA LOG FILE LISTINGS ------------------ */

  test("getLogSegmentForVersion: corrupt listing with only checkpoint file") {
    for (versionToLoad <-
           Seq(Optional.empty(), Optional.of(10L)): Seq[Optional[java.lang.Long]]) {
      for (startCheckpoint <-
             Seq(Optional.empty(), Optional.of(10L)): Seq[Optional[java.lang.Long]]) {
        testExpectedError[IllegalStateException](
          files = singularCheckpointFileStatuses(Seq(10L)),
          startCheckpoint = startCheckpoint,
          versionToLoad = versionToLoad,
          expectedErrorMessageContains = "Could not find any delta files for version 10"
        )
      }
    }
  }

  test("getLogSegmentForVersion: corrupt listing with missing log files") {
    // checkpoint(10), 010.json, 011.json, 013.json
    val fileList = deltaFileStatuses(Seq(10L, 11L)) ++ deltaFileStatuses(Seq(13L)) ++
      singularCheckpointFileStatuses(Seq(10L))
    testExpectedError[RuntimeException](
      fileList,
      expectedErrorMessageContains = "Versions ([11, 13]) are not continuous"
    )
    testExpectedError[RuntimeException](
      fileList,
      startCheckpoint = Optional.of(10),
      expectedErrorMessageContains = "Versions ([11, 13]) are not continuous"
    )
    testExpectedError[RuntimeException](
      fileList,
      versionToLoad = Optional.of(13),
      expectedErrorMessageContains = "Versions ([11, 13]) are not continuous"
    )
  }

  // TODO address the inconsistent behaviors and throw better error messages for corrupt listings?
  //  (delta-io/delta#2283)
  test("getLogSegmentForVersion: corrupt listing 000.json...009.json + checkpoint(10)") {
    val fileList = deltaFileStatuses((0L until 10L)) ++ singularCheckpointFileStatuses(Seq(10L))

    /* ----------  version to load is 15 (greater than latest checkpoint/delta file) ---------- */
    // (?) different error messages
    testExpectedError[RuntimeException](
      fileList,
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains = "Trying to load a non-existent version 15"
    )
    testExpectedError[IllegalStateException](
      fileList,
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(15),
      expectedErrorMessageContains = "Could not find any delta files for version 10"
    )

    /* ---------- versionToLoad is latest (10) ---------- */
    // (?) fails when startCheckpoint is provided, passes when it's not
    testExpectedError[IllegalStateException](
      fileList,
      startCheckpoint = Optional.of(10),
      expectedErrorMessageContains = "Could not find any delta files for version 10"
    )
    val logSegment = snapshotManager.getLogSegmentForVersion(
      createMockFSListFromTableClient(fileList),
      Optional.empty(),
      Optional.empty()
    )
    assert(logSegment.isPresent())
    checkLogSegment(
      logSegment.get(),
      10,
      Seq.empty,
      singularCheckpointFileStatuses(Seq(10L)),
      Some(10),
      90 // is the last available delta file
    )
  }

  // it's weird that checkpoint(10) fails but 011.json...014.json + checkpoint(10) does not
  test("getLogSegmentForVersion: corrupt listing 011.json...014.json + checkpoint(10)") {
    val fileList = singularCheckpointFileStatuses(Seq(10L)) ++ deltaFileStatuses((11L until 15L))
    /* ---------- versionToLoad is latest (14) ---------- */
    // no error
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (11L until 15L),
      checkpointVersions = Seq(10)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = (11L until 15L),
      checkpointVersions = Seq(10),
      startCheckpoint = Optional.of(10)
    )
    /* ---------- versionToLoad is 10 ---------- */
    // (?) throws an error
    testExpectedError[IllegalStateException](
      fileList,
      versionToLoad = Optional.of(10),
      expectedErrorMessageContains = "Could not find any delta files for version 10"
    )
    testExpectedError[IllegalStateException](
      fileList,
      startCheckpoint = Optional.of(10),
      versionToLoad = Optional.of(10),
      expectedErrorMessageContains = "Could not find any delta files for version 10"
    )
  }

  test("getLogSegmentForVersion: corrupted log missing json files / no way to construct history") {
    def expectedErrorMessage(v: Int): String = {
      s"""|Log file not found.
          |Expected: ${FileNames.deltaFile(logPath, 0)}
          |Found: ${FileNames.deltaFile(logPath, v)}""".stripMargin
    }
    testExpectedError[RuntimeException](
      deltaFileStatuses(1L until 10L),
      expectedErrorMessageContains = expectedErrorMessage(1)
    )
    testExpectedError[RuntimeException](
      deltaFileStatuses(15L until 25L) ++ singularCheckpointFileStatuses(Seq(20L)),
      versionToLoad = Optional.of(17),
      expectedErrorMessageContains = expectedErrorMessage(15)
    )
    testExpectedError[RuntimeException](
      deltaFileStatuses(15L until 25L) ++ singularCheckpointFileStatuses(Seq(20L)),
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(17),
      expectedErrorMessageContains = expectedErrorMessage(15)
    )
    testExpectedError[RuntimeException](
      deltaFileStatuses((0L until 5L) ++ (6L until 9L)),
      expectedErrorMessageContains = "are not continuous"
    )
    // corrupt incomplete multi-part checkpoint
    val corruptedCheckpointStatuses = FileNames.checkpointFileWithParts(logPath, 10, 5).asScala
      .map(p => FileStatus.of(p.toString, 10, 10))
      .take(4)
    val deltas = deltaFileStatuses(10L to 13L)
    testExpectedError[RuntimeException](
      corruptedCheckpointStatuses ++ deltas,
      Optional.empty(),
      Optional.empty(),
      expectedErrorMessageContains = expectedErrorMessage(10)
    )
  }

  test("getLogSegmentForVersion: corrupt log but reading outside corrupted range") {
    testNoCheckpoint(
      deltaVersions = (0L until 5L) ++ (6L until 9L),
      versionToLoad = Optional.of(4)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = 15L until 25L,
      checkpointVersions = Seq(20),
      versionToLoad = Optional.of(22)
    )
    testWithSingularAndMultipartCheckpoint(
      deltaVersions = 15L until 25L,
      checkpointVersions = Seq(20),
      startCheckpoint = Optional.of(20),
      versionToLoad = Optional.of(22)
    )
  }

  test("getLogSegmentForVersion: corrupt _last_checkpoint (is after existing versions)") {
    // in the case of a corrupted _last_checkpoint we revert to listing from version 0
    // (on first run newFiles.isEmpty() but since startingCheckpointOpt.isPresent() re-list from 0)
    testWithSingularAndMultipartCheckpoint(
      (0L until 25L),
      Seq(10L, 20L),
      startCheckpoint = Optional.of(30)
    )
  }

  // TODO recover from missing checkpoint (getLogSegmentWithMaxExclusiveCheckpointVersion)
  test("getLogSegmentForVersion: corrupt _last_checkpoint refers to in range version " +
    "but no valid checkpoint") {
    testExpectedError[RuntimeException](
      deltaFileStatuses(0L until 25L) ++ singularCheckpointFileStatuses(Seq(10L)),
      startCheckpoint = Optional.of(20),
      expectedErrorMessageContains = "Checkpoint file to load version: 20 is missing."
    )
    // _last_checkpoint refers to incomplete multi-part checkpoint
    val corruptedCheckpointStatuses = FileNames.checkpointFileWithParts(logPath, 20, 5).asScala
      .map(p => FileStatus.of(p.toString, 10, 10))
      .take(4)
    testExpectedError[RuntimeException](
      files = corruptedCheckpointStatuses ++ deltaFileStatuses(10L to 20L) ++
        singularCheckpointFileStatuses(Seq(10L)),
      startCheckpoint = Optional.of(20),
      expectedErrorMessageContains = "Checkpoint file to load version: 20 is missing."
    )
  }

  test("getLogSegmentForVersion: corrupted incomplete multi-part checkpoint with no" +
    "_last_checkpoint or a valid _last_checkpoint provided") {
    val cases: Seq[(Long, Seq[Long], Seq[Long], Optional[java.lang.Long])] = Seq(
      /* (corruptedCheckpointVersion, validCheckpointVersions, deltaVersions, startCheckpoint) */
      (20, Seq(10), (10L to 20L), Optional.empty()),
      (20, Seq(10), (10L to 20L), Optional.of(10)),
      (10, Seq.empty, (0L to 10L), Optional.empty())
    )
    cases.foreach { case (corruptedVersion, validVersions, deltaVersions, startCheckpoint) =>
      val corruptedCheckpoint = FileNames.checkpointFileWithParts(logPath, corruptedVersion, 5)
        .asScala
        .map(p => FileStatus.of(p.toString, 10, 10))
        .take(4)
      val checkpoints = singularCheckpointFileStatuses(validVersions)
      val deltas = deltaFileStatuses(deltaVersions)
      val logSegmentOpt = snapshotManager.getLogSegmentForVersion(
        createMockFSListFromTableClient(deltas ++ corruptedCheckpoint ++ checkpoints),
        Optional.empty(),
        Optional.empty()
      )
      val checkpointVersion = validVersions.sorted.lastOption
      assert(logSegmentOpt.isPresent())
      checkLogSegment(
        logSegment = logSegmentOpt.get(),
        expectedVersion = deltaVersions.max,
        expectedDeltas = deltaFileStatuses(
          deltaVersions.filter(_ > checkpointVersion.getOrElse(-1L))),
        expectedCheckpoints = checkpoints,
        expectedCheckpointVersion = checkpointVersion,
        expectedLastCommitTimestamp = deltaVersions.max*10
      )
    }
  }

  test("getLogSegmentForVersion: corrupt _last_checkpoint with empty delta log") {
    // listDeltaAndCheckpointFiles = Optional.empty()
    val logSegmentOpt = snapshotManager.getLogSegmentForVersion(
      createMockFSListFromTableClient(Seq.empty),
      Optional.of(1),
      Optional.empty()
    )
    assert(!logSegmentOpt.isPresent())
  }
}

class MockSidecarParquetHandler(sidecars: Seq[FileStatus]) extends BaseMockParquetHandler {
  override def readParquetFiles(
    fileIter: CloseableIterator[FileStatus],
    physicalSchema: StructType,
    predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {

    Utils.singletonCloseableIterator(
      new ColumnarBatch {
        override def getSchema: StructType = SidecarFile.READ_SCHEMA

        override def getColumnVector(ordinal: Int): ColumnVector = {
          ordinal match {
            case 0 => stringVector(sidecars.map(_.getPath): _*) // path
            case 1 => longVector(sidecars.map(_.getSize): _*) // size
            case 2 =>
              longVector(sidecars.map(_.getModificationTime): _*); // modification time
          }
        }

        override def getSize: Int = 2
      })
  }
}

class MockSidecarJsonHandler(sidecars: Seq[FileStatus]) extends BaseMockJsonHandler {
  override def readJsonFiles(
    fileIter: CloseableIterator[FileStatus],
    physicalSchema: StructType,
    predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {

    Utils.singletonCloseableIterator(
      new ColumnarBatch {
        override def getSchema: StructType = SidecarFile.READ_SCHEMA

        override def getColumnVector(ordinal: Int): ColumnVector = {
          ordinal match {
            case 0 => stringVector(sidecars.map(_.getPath): _*) // path
            case 1 => longVector(sidecars.map(_.getSize): _*) // size
            case 2 =>
              longVector(sidecars.map(_.getModificationTime): _*); // modification time
          }
        }

        override def getSize: Int = 2
      })
  }
}
