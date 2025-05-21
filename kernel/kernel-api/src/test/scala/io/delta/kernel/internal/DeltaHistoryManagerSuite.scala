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
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.SnapshotQueryContext
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.util.InCommitTimestampUtils
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.types.StringType
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class DeltaHistoryManagerSuite extends AnyFunSuite with MockFileSystemClientUtils {

  def getMockSnapshot(
      latestVersion: Long,
      ictEnablementInfoOpt: Option[(Long, Long)] = None): SnapshotImpl = {
    val configuration = ictEnablementInfoOpt match {
      case Some((version, _)) if version == 0L =>
        Map(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")
      case Some((version, ts)) =>
        Map(
          TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> version.toString,
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> ts.toString)
      case None =>
        Map[String, String]()
    }
    val metadata = new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      testSchema.toJson,
      testSchema,
      buildArrayValue(java.util.Arrays.asList("c3"), StringType.STRING),
      Optional.of(123),
      stringStringMapValue(configuration.asJava));

    val logSegment = new LogSegment(
      logPath, /* logPath */
      latestVersion,
      Seq(deltaFileStatus(latestVersion)).asJava, /* deltas */
      Seq.empty.asJava, /* compactions */
      Seq.empty.asJava, /* checkpoints */
      Optional.empty(), /* lastSeenChecksum */
      0L /* lastCommitTimestamp */
    )
    val snapshotQueryContext = SnapshotQueryContext.forLatestSnapshot(dataPath.toString)
    new SnapshotImpl(
      dataPath, /* dataPath */
      logSegment, /* logSegment */
      null, /* logReplay */
      null, /* protocol */
      metadata,
      snapshotQueryContext /* snapshotContext */
    )
  }

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
      getMockSnapshot(latestVersion = latestVersion),
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
        getMockSnapshot(latestVersion),
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
        getMockSnapshot(latestVersion = latestVersion),
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
        getMockSnapshot(latestVersion = 1L),
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
        getMockSnapshot(latestVersion = 1L),
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
        getMockSnapshot(latestVersion = 1L),
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
        getMockSnapshot(latestVersion = 1L),
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

  def basicICTTimeTravelTest(
      ictEnablementVersion: Long,
      reversedModTimes: Boolean = false): Unit = {
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L)
    val modTimes = if (reversedModTimes) {
      icts.reverse
    } else {
      Seq(4L, 14L, 24L, 34L, 54L, 64L)
    }
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(
        FileNames.deltaFile(logPath, v),
        1, /* size */
        ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      latestVersion = icts.size - 1,
      Some((ictEnablementVersion, deltaToICTMap(ictEnablementVersion))))

    icts.indices.foreach { idx =>
      val timestamp = if (idx >= ictEnablementVersion) {
        icts(idx)
      } else {
        modTimes(idx)
      }
      val expectedVersion = idx // For enablement at 0, version matches index
      val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        mockSnapshot,
        logPath,
        timestamp,
        true, // mustBeRecreatable
        false, // canReturnLastCommit
        false // canReturnEarliestCommit
      )
      assert(activeCommit.getVersion == expectedVersion)
      if (idx != icts.size - 1) {
        val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
          engine,
          mockSnapshot,
          logPath,
          timestamp + 1,
          true, // mustBeRecreatable
          false, // canReturnLastCommit
          false // canReturnEarliestCommit
        )
        assert(activeCommit.getVersion == expectedVersion)
      } else {
        // Querying for a timestamp greater than the last commit should throw an exception
        // when canReturnLastCommit is false.
        intercept[io.delta.kernel.exceptions.KernelException] {
          DeltaHistoryManager.getActiveCommitAtTimestamp(
            engine,
            mockSnapshot,
            logPath,
            timestamp + 1,
            true, // mustBeRecreatable
            false, // canReturnLastCommit
            false // canReturnEarliestCommit
          )
        }
        // If canReturnLastCommit is true, it should return the last commit.
        val lastCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
          engine,
          mockSnapshot,
          logPath,
          timestamp + 1,
          true, // mustBeRecreatable
          true, // canReturnLastCommit
          false // canReturnEarliestCommit
        )
        assert(lastCommit.getVersion == expectedVersion)
      }
    }
  }

  test("basic ICT time travel: enablement at 0") {
    basicICTTimeTravelTest(ictEnablementVersion = 0)
  }

  test("ICT time travel: modification times out of order") {
    basicICTTimeTravelTest(ictEnablementVersion = 0, reversedModTimes = true)
  }

  test("basic ICT time travel: enablement at 1") {
    basicICTTimeTravelTest(ictEnablementVersion = 1)
  }

  test("basic ICT time travel: enablement at 3") {
    basicICTTimeTravelTest(ictEnablementVersion = 3)
  }
  test("basic ICT time travel: enablement at the last version") {
    basicICTTimeTravelTest(ictEnablementVersion = 5)
  }

  test("ICT time travel: snapshot timestamp before ICT enablement") {
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L)
    val modTimes = Seq(4L, 14L, 24L, 34L, 54L, 64L)
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(
        FileNames.deltaFile(logPath, v),
        1, /* size */
        ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      latestVersion = icts.size - 1,
      Some((3L, deltaToICTMap(3L))))

    // Test snapshot timestamp before ICT enablement
    val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      20L, // Before ICT enablement
      true, // mustBeRecreatable
      false, // canReturnLastCommit
      false // canReturnEarliestCommit
    )
    assert(activeCommit.getVersion == 1L) // Should use modification time
  }

  test("ICT time travel: binary search edge cases") {
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L, 70L) // Odd number of commits
    val modTimes = Seq(4L, 14L, 24L, 34L, 54L, 64L, 74L)
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(
        FileNames.deltaFile(logPath, v),
        1, /* size */
        ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      latestVersion = icts.size - 1,
      Some((0L, deltaToICTMap(0L))))

    // Test searchTimestamp is the exact match with the middle commit.
    val activeCommit1 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      31L, // Exact match with version 3
      true, // mustBeRecreatable
      false, // canReturnLastCommit
      false // canReturnEarliestCommit
    )
    assert(activeCommit1.getVersion == 3L)

    // Test searchTimestamp = start case
    val activeCommit2 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      1L, // First ICT
      true, // mustBeRecreatable
      false, // canReturnLastCommit
      false // canReturnEarliestCommit
    )
    assert(activeCommit2.getVersion == 0L)

    // Test searchTimestamp = end case
    val activeCommit3 = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      70L, // Last ICT
      true, // mustBeRecreatable
      false, // canReturnLastCommit
      false // canReturnEarliestCommit
    )
    assert(activeCommit3.getVersion == 6L)
  }

  test("ICT time travel: even number of commits") {
    val icts = Seq(1L, 11L, 21L, 31L, 50L, 60L) // Even number of commits
    val modTimes = Seq(4L, 14L, 24L, 34L, 54L, 64L)
    val deltasWithModTimes = modTimes.zipWithIndex.map { case (ts, v) =>
      FileStatus.of(
        FileNames.deltaFile(logPath, v),
        1, /* size */
        ts)
    }
    val deltaToICTMap = icts.zipWithIndex.map { case (ts, v) => v.toLong -> ts }.toMap
    val engine = createMockFSAndJsonEngineForICT(deltasWithModTimes, deltaToICTMap)
    val mockSnapshot = getMockSnapshot(
      latestVersion = icts.size - 1,
      Some((0L, deltaToICTMap(0L))))

    // Test with even number of commits
    val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
      engine,
      mockSnapshot,
      logPath,
      25L, // Between version 2 and 3
      true, // mustBeRecreatable
      false, // canReturnLastCommit
      false // canReturnEarliestCommit
    )
    assert(activeCommit.getVersion == 2L)
    assert(activeCommit.getTimestamp == 21L)
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
