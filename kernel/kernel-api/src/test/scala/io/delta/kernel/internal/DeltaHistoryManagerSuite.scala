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

import java.io.FileNotFoundException
import java.util
import java.util.Optional

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.TransactionSuite.testSchema
import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.lang.Lazy
import io.delta.kernel.internal.metrics.SnapshotQueryContext
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.util.InCommitTimestampUtils
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.test.MockSnapshotUtils.getMockSnapshot
import io.delta.kernel.types.StringType
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class DeltaHistoryManagerSuite extends AnyFunSuite with MockFileSystemClientUtils {

  def checkGetActiveCommitAtTimestamp(
      fileList: Seq[FileStatus],
      timestamp: Long,
      expectedVersion: Long,
      mustBeRecreatable: Boolean = true,
      canReturnLastCommit: Boolean = false,
      canReturnEarliestCommit: Boolean = false): Unit = {
    val lastDelta = fileList.map(_.getPath).filter(FileNames.isCommitFile).last
    val latestVersion = FileNames.getFileVersion(new Path(lastDelta))
    val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
      createMockFSListFromEngine(fileList),
      getMockSnapshot(dataPath, latestVersion = latestVersion),
      logPath,
      timestamp,
      mustBeRecreatable,
      canReturnLastCommit,
      canReturnEarliestCommit)
    assert(
      activeCommit.getVersion == expectedVersion,
      s"Expected version $expectedVersion but got $activeCommit for timestamp=$timestamp")

    if (mustBeRecreatable) {
      // When mustBeRecreatable=true, we should have the same answer as mustBeRecreatable=false
      // for valid queries that do not throw an error
      val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(fileList),
        getMockSnapshot(dataPath, latestVersion),
        logPath,
        timestamp,
        false, // mustBeRecreatable
        canReturnLastCommit,
        canReturnEarliestCommit)
      assert(
        activeCommit.getVersion == expectedVersion,
        s"Expected version $expectedVersion but got $activeCommit for timestamp=$timestamp")
    }
  }

  def checkGetActiveCommitAtTimestampError[T <: Throwable](
      fileList: Seq[FileStatus],
      latestVersion: Long,
      timestamp: Long,
      expectedErrorMessageContains: String,
      mustBeRecreatable: Boolean = true,
      canReturnLastCommit: Boolean = false,
      canReturnEarliestCommit: Boolean = false)(implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(fileList),
        getMockSnapshot(dataPath, latestVersion = latestVersion),
        logPath,
        timestamp,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit)
    }
    assert(e.getMessage.contains(expectedErrorMessageContains))
  }

  test("getActiveCommitAtTimestamp: basic listing from 0 with no checkpoints") {
    val deltaFiles = deltaFileStatuses(Seq(0L, 1L, 2L))
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 0, 0)
    checkGetActiveCommitAtTimestamp(deltaFiles, 1, 0)
    checkGetActiveCommitAtTimestamp(deltaFiles, 10, 1)
    checkGetActiveCommitAtTimestamp(deltaFiles, 11, 1)
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      -1,
      DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, -1, 0, 0).getMessage)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      21,
      DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 21, 20, 2).getMessage)
    // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
    checkGetActiveCommitAtTimestamp(deltaFiles, -1, 0, canReturnEarliestCommit = true)
    checkGetActiveCommitAtTimestamp(deltaFiles, 21, 2, canReturnLastCommit = true)
  }

  test("getActiveCommitAtTimestamp: basic listing from 0 with a checkpoint") {
    val deltaFiles = deltaFileStatuses(Seq(0L, 1L, 2L)) ++ singularCheckpointFileStatuses(Seq(2L))
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 0, 0)
    checkGetActiveCommitAtTimestamp(deltaFiles, 1, 0)
    checkGetActiveCommitAtTimestamp(deltaFiles, 10, 1)
    checkGetActiveCommitAtTimestamp(deltaFiles, 11, 1)
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      -1,
      DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, -1, 0, 0).getMessage)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      21,
      DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 21, 20, 2).getMessage)
    // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
    checkGetActiveCommitAtTimestamp(deltaFiles, -1, 0, canReturnEarliestCommit = true)
    checkGetActiveCommitAtTimestamp(deltaFiles, 21, 2, canReturnLastCommit = true)
  }

  test("getActiveCommitAtTimestamp: truncated delta log") {
    val deltaFiles = deltaFileStatuses(Seq(2L, 3L)) ++ singularCheckpointFileStatuses(Seq(2L))
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    checkGetActiveCommitAtTimestamp(deltaFiles, 25, 2)
    checkGetActiveCommitAtTimestamp(deltaFiles, 30, 3)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 3L,
      8,
      DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, 8, 20, 2).getMessage)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 3L,
      31,
      DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 31, 30, 3).getMessage)
    // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
    checkGetActiveCommitAtTimestamp(deltaFiles, 8, 2, canReturnEarliestCommit = true)
    checkGetActiveCommitAtTimestamp(deltaFiles, 31, 3, canReturnLastCommit = true)
  }

  test("getActiveCommitAtTimestamp: truncated delta log only checkpoint version") {
    val deltaFiles = deltaFileStatuses(Seq(2L)) ++ singularCheckpointFileStatuses(Seq(2L))
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      8,
      DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, 8, 20, 2).getMessage)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 2L,
      21,
      DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 21, 20, 2).getMessage)
    // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
    checkGetActiveCommitAtTimestamp(deltaFiles, 8, 2, canReturnEarliestCommit = true)
    checkGetActiveCommitAtTimestamp(deltaFiles, 21, 2, canReturnLastCommit = true)
  }

  test("getActiveCommitAtTimestamp: truncated delta log with multi-part checkpoint") {
    val deltaFiles = deltaFileStatuses(Seq(2L, 3L)) ++ multiCheckpointFileStatuses(Seq(2L), 2)
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    checkGetActiveCommitAtTimestamp(deltaFiles, 25, 2)
    checkGetActiveCommitAtTimestamp(deltaFiles, 30, 3)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 3L,
      8,
      DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, 8, 20, 2).getMessage)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      latestVersion = 3L,
      31,
      DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 31, 30, 3).getMessage)
    // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
    checkGetActiveCommitAtTimestamp(deltaFiles, 8, 2, canReturnEarliestCommit = true)
    checkGetActiveCommitAtTimestamp(deltaFiles, 31, 3, canReturnLastCommit = true)
  }

  test("getActiveCommitAtTimestamp: throws table not found exception") {
    // Non-existent path
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(p => throw new FileNotFoundException(p)),
        getMockSnapshot(dataPath, latestVersion = 1L),
        logPath,
        0,
        true, // mustBeRecreatable
        false, // canReturnLastCommit
        false // canReturnEarliestCommit
      ))
    // Empty _delta_log directory
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(p => Seq()),
        getMockSnapshot(dataPath, latestVersion = 1L),
        logPath,
        0,
        true, // mustBeRecreatable
        false, // canReturnLastCommit
        false // canReturnEarliestCommit
      ))
  }

  // TODO: corrects commit timestamps for increasing commits (monotonizeCommitTimestamps)?
  //  (see test "getCommits should monotonize timestamps" in DeltaTimeTravelSuite)?

  test("getActiveCommitAtTimestamp: corrupt listings") {
    // No checkpoint or 000.json present
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFileStatuses(Seq(1L, 2L, 3L)),
      latestVersion = 3L,
      25,
      "No recreatable commits found")
    // Must have corresponding delta file for a checkpoint
    checkGetActiveCommitAtTimestampError[RuntimeException](
      singularCheckpointFileStatuses(Seq(1L)) ++ deltaFileStatuses(Seq(2L, 3L)),
      latestVersion = 3L,
      25,
      "No recreatable commits found")
    // No commit files at all (only checkpoint files)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      singularCheckpointFileStatuses(Seq(1L)),
      latestVersion = 1L,
      25,
      "No commits found")
    // No delta files
    checkGetActiveCommitAtTimestampError[RuntimeException](
      Seq("foo", "notdelta.parquet", "foo.json", "001.checkpoint.00f.oo0.parquet")
        .map(FileStatus.of(_, 10, 10)),
      latestVersion = 1L,
      25,
      "No delta files found in the directory")
    // No complete checkpoint
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFileStatuses(Seq(2L, 3L)) ++ multiCheckpointFileStatuses(Seq(2L), 3).take(2),
      latestVersion = 3L,
      25,
      "No recreatable commits found")
  }

  test("getActiveCommitAtTimestamp: when mustBeRecreatable=false") {
    Seq(
      deltaFileStatuses(Seq(1L, 2L, 3L)), // w/o checkpoint
      singularCheckpointFileStatuses(Seq(2L)) ++ deltaFileStatuses(Seq(1L, 2L, 3L)) // w/checkpoint
    ).foreach { deltaFiles =>
      // Valid queries
      checkGetActiveCommitAtTimestamp(deltaFiles, 10, 1, mustBeRecreatable = false)
      checkGetActiveCommitAtTimestamp(deltaFiles, 11, 1, mustBeRecreatable = false)
      checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2, mustBeRecreatable = false)
      checkGetActiveCommitAtTimestamp(deltaFiles, 21, 2, mustBeRecreatable = false)
      checkGetActiveCommitAtTimestamp(deltaFiles, 30, 3, mustBeRecreatable = false)
      // Invalid queries
      checkGetActiveCommitAtTimestampError[RuntimeException](
        deltaFiles,
        latestVersion = 3L,
        -1,
        DeltaErrors.timestampBeforeFirstAvailableCommit(dataPath.toString, -1, 10, 1).getMessage,
        mustBeRecreatable = false)
      checkGetActiveCommitAtTimestampError[RuntimeException](
        deltaFiles,
        latestVersion = 3L,
        31,
        DeltaErrors.timestampAfterLatestCommit(dataPath.toString, 31, 30, 3).getMessage,
        mustBeRecreatable = false)
      // Valid queries with canReturnLastCommit=true and canReturnEarliestCommit=true
      checkGetActiveCommitAtTimestamp(
        deltaFiles,
        0,
        1,
        mustBeRecreatable = false,
        canReturnEarliestCommit = true)
      checkGetActiveCommitAtTimestamp(
        deltaFiles,
        31,
        3,
        mustBeRecreatable = false,
        canReturnLastCommit = true)
    }
  }

  test("getActiveCommitAtTimestamp: mustBeRecreatable=false error cases") {
    /* ---------- TABLE NOT FOUND --------- */
    // Non-existent path
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(p => throw new FileNotFoundException(p)),
        getMockSnapshot(dataPath, latestVersion = 1L),
        logPath,
        0,
        false, // mustBeRecreatable
        false, // canReturnLastCommit
        false // canReturnEarliestCommit
      ))
    // Empty _delta_log directory
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockFSListFromEngine(p => Seq()),
        getMockSnapshot(dataPath, latestVersion = 1L),
        logPath,
        0,
        true, // mustBeRecreatable
        false, // canReturnLastCommit
        false // canReturnEarliestCommit
      ))
    /* ---------- CORRUPT LISTINGS --------- */
    // No commit files at all (only checkpoint files)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      singularCheckpointFileStatuses(Seq(1L)),
      latestVersion = 1L,
      25,
      "No delta files found in the directory",
      mustBeRecreatable = false)
    // No delta files
    checkGetActiveCommitAtTimestampError[RuntimeException](
      Seq("foo", "notdelta.parquet", "foo.json", "001.checkpoint.00f.oo0.parquet")
        .map(FileStatus.of(_, 10, 10)),
      latestVersion = 1L,
      25,
      "No delta files found in the directory",
      mustBeRecreatable = false)
  }

  // ========== ICT TIME TRAVEL TESTS ==========

  /**
   * Common function to test getActiveCommitAtTimestamp with all combinations of boolean flags.
   * This reduces duplication and ensures comprehensive testing of flag combinations.
   */
  def checkGetActiveCommitAtTimestampWithAllFlags(
      fileList: Seq[FileStatus],
      timestamp: Long,
      expectedVersion: Long,
      ictEnablementInfoOpt: Option[(Long, Long)] = None,
      shouldSucceed: Boolean = true,
      expectedErrorMessageContains: String = ""): Unit = {

    val lastDelta = fileList.map(_.getPath).filter(FileNames.isCommitFile).last
    val latestVersion = FileNames.getFileVersion(new Path(lastDelta))

    // Test all combinations of boolean flags
    val flagCombinations = for {
      mustBeRecreatable <- Seq(true, false)
      canReturnLastCommit <- Seq(true, false)
      canReturnEarliestCommit <- Seq(true, false)
    } yield (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit)

    flagCombinations.foreach {
      case (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit) =>
        if (shouldSucceed) {
          val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
            createMockFSListFromEngine(fileList),
            getMockSnapshot(dataPath, latestVersion = latestVersion, ictEnablementInfoOpt),
            logPath,
            timestamp,
            mustBeRecreatable,
            canReturnLastCommit,
            canReturnEarliestCommit)
          assert(
            activeCommit.getVersion == expectedVersion,
            s"Expected version $expectedVersion but got ${activeCommit.getVersion}  " +
              s"for timestamp=$timestamp with flags: " +
              s"mustBeRecreatable=$mustBeRecreatable, " +
              s"canReturnLastCommit=$canReturnLastCommit, " +
              s"canReturnEarliestCommit=$canReturnEarliestCommit")
        } else {
          val e = intercept[Exception] {
            DeltaHistoryManager.getActiveCommitAtTimestamp(
              createMockFSListFromEngine(fileList),
              getMockSnapshot(dataPath, latestVersion = latestVersion, ictEnablementInfoOpt),
              logPath,
              timestamp,
              mustBeRecreatable,
              canReturnLastCommit,
              canReturnEarliestCommit)
          }
          assert(
            e.getMessage.contains(expectedErrorMessageContains),
            s"Expected error message to contain " +
              s"'$expectedErrorMessageContains' but got '${e.getMessage}' " +
              s"with flags: " +
              s"mustBeRecreatable=$mustBeRecreatable, " +
              s"canReturnLastCommit=$canReturnLastCommit, " +
              s"canReturnEarliestCommit=$canReturnEarliestCommit")
        }
    }
  }

  /**
   * Common function to test ICT time travel scenarios.
   */
  def testICTTimeTravelScenario(
      icts: Seq[Long],
      modTimes: Seq[Long],
      ictEnablementVersion: Long,
      testCases: Seq[(Long, Long, String)] // (searchTimestamp, expectedVersion, description)
  ): Unit = {
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(
        FileNames.deltaFile(logPath, v),
        1, /* size */
        ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = icts.size - 1,
      Some((ictEnablementVersion, deltaToICTMap(ictEnablementVersion))))

    testCases.foreach { case (timestamp, expectedVersion, description) =>
      val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        mockSnapshot,
        logPath,
        timestamp,
        true, /* mustBeRecreatable */
        false, /* canReturnLastCommit */
        false /* canReturnEarliestCommit */
      )
      assert(
        activeCommit.getVersion == expectedVersion,
        s"$description: Expected version $expectedVersion " +
          s"but got ${activeCommit.getVersion} for timestamp=$timestamp")

      // Verify timestamp is correct based on ICT enablement
      val expectedTimestamp = if (expectedVersion >= ictEnablementVersion) {
        icts(expectedVersion.toInt)
      } else {
        modTimes(expectedVersion.toInt)
      }
      assert(
        activeCommit.getTimestamp == expectedTimestamp,
        s"$description: Expected timestamp $expectedTimestamp but got ${activeCommit.getTimestamp}")
    }
  }

  test("ICT time travel: comprehensive enablement scenarios") {
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L)
    val modTimes = Seq(4L, 14L, 24L, 34L, 54L, 64L)

    // Test enablement at version 0 (entire history has ICT)
    testICTTimeTravelScenario(
      icts,
      modTimes,
      ictEnablementVersion = 0L,
      Seq(
        (1L, 0L, "Exact match at first ICT"),
        (5L, 0L, "Between first and second ICT"),
        (11L, 1L, "Exact match at second ICT"),
        (25L, 2L, "Between third and fourth ICT"),
        (60L, 5L, "Exact match at last ICT")))

    // Test enablement at version 1 (mixed ICT/non-ICT)
    testICTTimeTravelScenario(
      icts,
      modTimes,
      ictEnablementVersion = 1L,
      Seq(
        (4L, 0L, "Non-ICT commit using modification time"),
        (11L, 1L, "First ICT commit"),
        (25L, 2L, "Between ICT commits"),
        (31L, 3L, "Exact ICT match"),
        (60L, 5L, "Last ICT commit")))

    // Test enablement at version 3 (mixed ICT/non-ICT)
    testICTTimeTravelScenario(
      icts,
      modTimes,
      ictEnablementVersion = 3L,
      Seq(
        (4L, 0L, "Non-ICT commit"),
        (14L, 1L, "Non-ICT commit"),
        (24L, 2L, "Non-ICT commit before enablement"),
        (31L, 3L, "First ICT commit"),
        (50L, 4L, "ICT commit"),
        (60L, 5L, "Last ICT commit")))

    // Test enablement at last version
    testICTTimeTravelScenario(
      icts,
      modTimes,
      ictEnablementVersion = 5L,
      Seq(
        (4L, 0L, "Non-ICT commit"),
        (54L, 4L, "Non-ICT commit before enablement"),
        (60L, 5L, "Only ICT commit")))
  }

  test("ICT time travel: boundary conditions and edge cases") {
    val icts = Seq(10L, 20L, 30L, 40L, 50L)
    val modTimes = Seq(5L, 15L, 25L, 35L, 45L)

    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)

    // Test with ICT enabled from version 0
    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = icts.size - 1,
      Some((0L, deltaToICTMap(0L))))

    // Test timestamp exactly at ICT enablement
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      10L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 0L)
    assert(activeCommit1.getTimestamp == 10L)

    // Test timestamp just before first ICT
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      9L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      true /* canReturnEarliestCommit */ )
    assert(activeCommit2.getVersion == 0L) // Should return earliest commit
    assert(activeCommit2.getTimestamp == 10L)

    // Test timestamp just after last ICT
    intercept[io.delta.kernel.exceptions.KernelException] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        mockSnapshot,
        logPath,
        51L, /* timestamp */
        true, /* mustBeRecreatable */
        false, /* canReturnLastCommit */
        false /* canReturnEarliestCommit */ )
    }

    // Test with canReturnLastCommit=true
    val activeCommit3 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      51L, /* timestamp */
      true, /* mustBeRecreatable */
      true, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit3.getVersion == 4L)
    assert(activeCommit3.getTimestamp == 50L)
  }

  test("ICT time travel: latest snapshot timestamp optimization") {
    val icts = Seq(10L, 20L, 30L)
    val modTimes = Seq(5L, 15L, 25L)

    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)

    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = icts.size - 1,
      Some((0L, deltaToICTMap(0L))))

    // Test timestamp equal to latest snapshot timestamp
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      30L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 2L)
    assert(activeCommit1.getTimestamp == 30L)

    // Test timestamp greater than latest snapshot timestamp
    intercept[io.delta.kernel.exceptions.KernelException] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        mockSnapshot,
        logPath,
        35L, /* timestamp */
        true, /* mustBeRecreatable */
        false, /* canReturnLastCommit */
        false /* canReturnEarliestCommit */ )
    }

    // Test with canReturnLastCommit=true
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      35L, /* timestamp */
      true, /* mustBeRecreatable */
      true, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit2.getVersion == 2L)
    assert(activeCommit2.getTimestamp == 30L)
  }

  test("ICT time travel: mixed ICT and non-ICT commits with truncated log") {
    val icts = Seq(100L, 200L, 300L, 400L) // ICT enabled from version 2
    val modTimes = Seq(50L, 150L, 250L, 350L)

    // Simulate truncated log starting from version 1
    val deltasWithModTimes = modTimes.drop(1).zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v + 1), 1, ts)
    }
    // Add a checkpoint file at version 1 to make the table recreatable
    val checkpointFile = FileStatus.of(
      FileNames.checkpointFileSingular(logPath, 1L).toString,
      1,
      150L)
    val allFiles = checkpointFile +: deltasWithModTimes

    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(allFiles, deltaToICTMap)

    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = 3L,
      Some((2L, deltaToICTMap(2L)))
    ) // ICT enabled at version 2

    // Test timestamp before ICT enablement (should use modification time)
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      150L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 1L)
    assert(activeCommit1.getTimestamp == 150L) // modification time

    // Test timestamp after ICT enablement (should use ICT)
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      300L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit2.getVersion == 2L)
    assert(activeCommit2.getTimestamp == 300L) // ICT

    // Test timestamp between ICT commits
    val activeCommit3 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      350L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit3.getVersion == 2L)
    assert(activeCommit3.getTimestamp == 300L) // Should return previous ICT commit
  }

  test("ICT time travel: non-ICT commits missing scenario") {
    val icts = Seq(100L, 200L, 300L)
    val modTimes = Seq(50L, 150L, 250L)

    // Simulate scenario where ICT enablement version <= earliest available version
    val deltasWithModTimes = modTimes.drop(2).zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v + 2), 1, ts)
    }
    // Add a checkpoint file at version 2 to make the table recreatable
    val checkpointFile = FileStatus.of(
      FileNames.checkpointFileSingular(logPath, 2L).toString,
      1,
      250L)
    val allFiles = checkpointFile +: deltasWithModTimes

    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(allFiles, deltaToICTMap)

    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = 2L,
      Some((1L, deltaToICTMap(1L)))
    ) // ICT enabled at version 1, but earliest available is 2

    // Test timestamp before ICT enablement but non-ICT commits are missing
    // Should return earliest available commit with its ICT
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      50L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      true /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 2L)
    assert(activeCommit1.getTimestamp == 300L) // ICT of earliest available commit

    // Test error case when canReturnEarliestCommit=false
    intercept[io.delta.kernel.exceptions.KernelException] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        mockSnapshot,
        logPath,
        50L, /* timestamp */
        true, /* mustBeRecreatable */
        false, /* canReturnLastCommit */
        false /* canReturnEarliestCommit */ )
    }
  }

  test("ICT time travel: binary search edge cases") {
    // Test with odd number of commits
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L, 70L)
    val modTimes = Seq(4L, 14L, 24L, 34L, 54L, 64L, 74L)
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = icts.size - 1,
      Some((0L, deltaToICTMap(0L))))

    // Test searchTimestamp is the exact match with the middle commit
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      31L, /* timestamp */ // Exact match with version 3
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 3L)

    // Test searchTimestamp = start case
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      1L, /* timestamp */ // First ICT
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit2.getVersion == 0L)

    // Test searchTimestamp = end case
    val activeCommit3 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      70L, /* timestamp */ // Last ICT
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit3.getVersion == 6L)

    // Test with even number of commits
    val ictsEven = Seq(1L, 11L, 21L, 31L, 50L, 60L)
    val modTimesEven = Seq(4L, 14L, 24L, 34L, 54L, 64L)
    val deltasWithModTimesEven = modTimesEven.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMapEven = ictsEven.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engineEven = createMockFSAndJsonEngineForICT(deltasWithModTimesEven, deltaToICTMapEven)
    val mockSnapshotEven = getMockSnapshot(
      dataPath,
      latestVersion = ictsEven.size - 1,
      Some((0L, deltaToICTMapEven(0L))))

    val activeCommit4 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engineEven,
      mockSnapshotEven,
      logPath,
      25L, /* timestamp */ // Between version 2 and 3
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit4.getVersion == 2L)
    assert(activeCommit4.getTimestamp == 21L)
  }

  test("ICT time travel: single commit scenario") {
    val icts = Seq(100L)
    val modTimes = Seq(50L)

    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)

    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = 0L,
      Some((0L, deltaToICTMap(0L))))

    // Test exact match
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      100L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 0L)
    assert(activeCommit1.getTimestamp == 100L)

    // Test timestamp before single commit
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      50L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      true /* canReturnEarliestCommit */ )
    assert(activeCommit2.getVersion == 0L)
    assert(activeCommit2.getTimestamp == 100L)

    // Test timestamp after single commit
    val activeCommit3 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      150L, /* timestamp */
      true, /* mustBeRecreatable */
      true, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit3.getVersion == 0L)
    assert(activeCommit3.getTimestamp == 100L)
  }

  test("ICT time travel: modification times out of order") {
    val icts = Seq(10L, 20L, 30L, 40L)
    val modTimes = Seq(40L, 30L, 20L, 10L) // Reversed modification times

    testICTTimeTravelScenario(
      icts,
      modTimes,
      ictEnablementVersion = 0L,
      Seq(
        (10L, 0L, "First ICT despite reversed mod times"),
        (15L, 0L, "Between first and second ICT"),
        (20L, 1L, "Second ICT"),
        (40L, 3L, "Last ICT")))
  }

  test("ICT time travel: comprehensive boolean flag combinations") {
    val icts = Seq(10L, 20L, 30L)
    val modTimes = Seq(5L, 15L, 25L)

    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)

    val mockSnapshot = getMockSnapshot(
      dataPath,
      latestVersion = 2L,
      Some((0L, deltaToICTMap(0L))))

    // Test all flag combinations for valid timestamp
    val flagCombinations = for {
      mustBeRecreatable <- Seq(true, false)
      canReturnLastCommit <- Seq(true, false)
      canReturnEarliestCommit <- Seq(true, false)
    } yield (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit)

    flagCombinations.foreach {
      case (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit) =>
        val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
          engine,
          mockSnapshot,
          logPath,
          20L, /* timestamp */
          mustBeRecreatable,
          canReturnLastCommit,
          canReturnEarliestCommit)
        assert(activeCommit.getVersion == 1L)
        assert(activeCommit.getTimestamp == 20L)
    }

    // Test edge cases with different flag combinations
    // Timestamp before earliest commit
    flagCombinations.foreach {
      case (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit) =>
        if (canReturnEarliestCommit) {
          val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
            engine,
            mockSnapshot,
            logPath,
            5L, /* timestamp */
            mustBeRecreatable,
            canReturnLastCommit,
            canReturnEarliestCommit)
          assert(activeCommit.getVersion == 0L)
        } else {
          intercept[io.delta.kernel.exceptions.KernelException] {
            DeltaHistoryManager.getActiveCommitAtTimestamp(
              engine,
              mockSnapshot,
              logPath,
              5L, /* timestamp */
              mustBeRecreatable,
              canReturnLastCommit,
              canReturnEarliestCommit)
          }
        }
    }

    // Timestamp after latest commit
    flagCombinations.foreach {
      case (mustBeRecreatable, canReturnLastCommit, canReturnEarliestCommit) =>
        if (canReturnLastCommit) {
          val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
            engine,
            mockSnapshot,
            logPath,
            35L, /* timestamp */
            mustBeRecreatable,
            canReturnLastCommit,
            canReturnEarliestCommit)
          assert(activeCommit.getVersion == 2L)
        } else {
          intercept[io.delta.kernel.exceptions.KernelException] {
            DeltaHistoryManager.getActiveCommitAtTimestamp(
              engine,
              mockSnapshot,
              logPath,
              35L, /* timestamp */
              mustBeRecreatable,
              canReturnLastCommit,
              canReturnEarliestCommit)
          }
        }
    }
  }

  test("ICT time travel: error handling and edge cases") {
    val icts = Seq(10L, 20L, 30L)
    val modTimes = Seq(5L, 15L, 25L)

    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(FileNames.deltaFile(logPath, v), 1, ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)

    // Test with ICT not enabled
    val nonICTSnapshot = getMockSnapshot(dataPath, latestVersion = 2L, None)
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      createMockFSListFromEngine(deltasWithModTimes),
      nonICTSnapshot,
      logPath,
      15L, /* timestamp */
      true, /* mustBeRecreatable */
      false, /* canReturnLastCommit */
      false /* canReturnEarliestCommit */ )
    assert(activeCommit1.getVersion == 1L)
    assert(activeCommit1.getTimestamp == 15L) // Should use modification time

    // Test with malformed ICT enablement info (only version set)
    val malformedConfig = Map(
      TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
      TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1"
      // Missing enablement timestamp
    )
    val malformedMetadata = new Metadata(
      "id",
      Optional.empty(),
      Optional.empty(),
      new Format(),
      testSchema.toJson,
      testSchema,
      buildArrayValue(java.util.Arrays.asList("c3"), StringType.STRING),
      Optional.of(123),
      stringStringMapValue(malformedConfig.asJava))

    val malformedSnapshot = new SnapshotImpl(
      dataPath,
      2L,
      new Lazy(() =>
        new LogSegment(
          logPath,
          2L,
          Seq(deltaFileStatus(2L)).asJava,
          Seq.empty.asJava,
          Seq.empty.asJava,
          deltaFileStatus(2L),
          Optional.empty())),
      null, /* logReplay */
      new Protocol(1, 2),
      malformedMetadata,
      DefaultFileSystemManagedTableOnlyCommitter.INSTANCE,
      SnapshotQueryContext.forLatestSnapshot(dataPath.toString))

    intercept[IllegalStateException] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        malformedSnapshot,
        logPath,
        15L, /* timestamp */
        true, /* mustBeRecreatable */
        false, /* canReturnLastCommit */
        false /* canReturnEarliestCommit */ )
    }
  }

  test("greatestLowerBound: basic functionality") {
    // Test with a simple sequence of timestamps
    val timestamps = Seq(1L, 3L, 5L, 7L, 9L)
    val indexToValueMapper = new java.util.function.Function[java.lang.Long, java.lang.Long] {
      override def apply(index: java.lang.Long): java.lang.Long = timestamps(index.toInt)
    }

    // Test exact match (should return index 2, value 5)
    val result1 = InCommitTimestampUtils.greatestLowerBound(5L, 0L, 4L, indexToValueMapper)
    assert(result1.isPresent)
    assert(result1.get._1 == 2L)
    assert(result1.get._2 == 5L)

    // Test between values (should return index 1, value 3)
    val result2 = InCommitTimestampUtils.greatestLowerBound(4L, 0L, 4L, indexToValueMapper)
    assert(result2.isPresent)
    assert(result2.get._1 == 1L)
    assert(result2.get._2 == 3L)

    // Test before first value (should not be present)
    val result3 = InCommitTimestampUtils.greatestLowerBound(0L, 0L, 4L, indexToValueMapper)
    assert(!result3.isPresent)

    // Test after last value (should return last index/value)
    val result4 = InCommitTimestampUtils.greatestLowerBound(10L, 0L, 4L, indexToValueMapper)
    assert(result4.isPresent)
    assert(result4.get._1 == 4L)
    assert(result4.get._2 == 9L)
  }

  test("greatestLowerBound: single element in search range") {
    // Test with single element
    val singleElement = Seq(5L)
    val singleElementMapper = new java.util.function.Function[java.lang.Long, java.lang.Long] {
      override def apply(index: java.lang.Long): java.lang.Long = singleElement(index.toInt)
    }

    // Target equals the element
    val result1 = InCommitTimestampUtils.greatestLowerBound(5L, 0L, 0L, singleElementMapper)
    assert(result1.isPresent)
    assert(result1.get._1 == 0L)
    assert(result1.get._2 == 5L)

    // Target less than the element
    val result2 = InCommitTimestampUtils.greatestLowerBound(4L, 0L, 0L, singleElementMapper)
    assert(!result2.isPresent)

    // Target greater than the element
    val result3 = InCommitTimestampUtils.greatestLowerBound(6L, 0L, 0L, singleElementMapper)
    assert(result3.isPresent)
    assert(result3.get._1 == 0L)
    assert(result3.get._2 == 5L)

    // Test with empty range (should not be present)
    val result4 = InCommitTimestampUtils.greatestLowerBound(5L, 1L, 0L, singleElementMapper)
    assert(!result4.isPresent)
  }

  test("greatestLowerBound: binary search correctness") {
    // Test with a larger sequence to verify binary search correctness
    val timestamps = (0L until 100L by 2).toSeq // 0, 2, 4, ..., 98
    val indexToValueMapper = new java.util.function.Function[java.lang.Long, java.lang.Long] {
      override def apply(index: java.lang.Long): java.lang.Long = timestamps(index.toInt)
    }

    // Test various positions in the sequence (exact matches)
    for (i <- 0 until 50) {
      val target = i * 2L
      val result = InCommitTimestampUtils.greatestLowerBound(target, 0L, 49L, indexToValueMapper)
      assert(result.isPresent)
      assert(result.get._1 == i)
      assert(result.get._2 == target)
    }

    // Test values between elements (should return the lower index/value)
    for (i <- 1 until 50) {
      val target = i * 2L - 1
      val result = InCommitTimestampUtils.greatestLowerBound(target, 0L, 49L, indexToValueMapper)
      assert(result.isPresent)
      assert(result.get._1 == i - 1)
      assert(result.get._2 == (i - 1) * 2L)
    }

    // Test value less than all (should not be present)
    val resultLow = InCommitTimestampUtils.greatestLowerBound(-1L, 0L, 49L, indexToValueMapper)
    assert(!resultLow.isPresent)

    // Test value greater than all (should return last index/value)
    val resultHigh = InCommitTimestampUtils.greatestLowerBound(100L, 0L, 49L, indexToValueMapper)
    assert(resultHigh.isPresent)
    assert(resultHigh.get._1 == 49L)
    assert(resultHigh.get._2 == 98L)
  }
}
