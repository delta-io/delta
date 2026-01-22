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
package io.delta.kernel.internal

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.{CommitRange, TableManager}
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{InvalidTableException, KernelException}
import io.delta.kernel.internal.commitrange.{CommitRangeBuilderImpl, CommitRangeImpl}
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedDeltaData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{MockFileSystemClientUtils, MockListFromFileSystemClient, MockReadICTFileJsonHandler, MockSnapshotUtils, VectorTestUtils}
import io.delta.kernel.test.MockSnapshotUtils.getMockSnapshot
import io.delta.kernel.utils.FileStatus

import junit.runner.Version
import org.scalatest.funsuite.AnyFunSuite

class CommitRangeBuilderSuite extends AnyFunSuite with MockFileSystemClientUtils
    with VectorTestUtils {

  private def checkQueryBoundaries(
      commitRange: CommitRange,
      startBoundary: RequiredBoundaryDef,
      endBoundary: BoundaryDef): Unit = {
    def assertBoundaryVersion(boundary: CommitBoundary, version: Long) = {
      assert(boundary.isVersion && boundary.getVersion == version)
    }
    def assertBoundaryTimestamp(boundary: CommitBoundary, timestamp: Long) = {
      assert(boundary.isTimestamp && boundary.getTimestamp == timestamp)
    }
    if (startBoundary.version.nonEmpty) {
      assertBoundaryVersion(commitRange.getQueryStartBoundary, startBoundary.version.get)
    } else if (startBoundary.timestamp.nonEmpty) {
      assertBoundaryTimestamp(commitRange.getQueryStartBoundary, startBoundary.timestamp.get)
    } else {
      throw new IllegalStateException("RequiredBoundaryDef must have either timestamp or version")
    }
    if (endBoundary.version.nonEmpty) {
      assert(commitRange.getQueryEndBoundary.isPresent)
      assertBoundaryVersion(commitRange.getQueryEndBoundary.get, endBoundary.version.get)
    } else if (endBoundary.timestamp.nonEmpty) {
      assert(commitRange.getQueryEndBoundary.isPresent)
      assertBoundaryTimestamp(commitRange.getQueryEndBoundary.get, endBoundary.timestamp.get)
    } else {
      assert(!commitRange.getQueryEndBoundary.isPresent)
    }
  }

  private def buildCommitRange(
      engine: Engine,
      fileList: Seq[FileStatus],
      startBoundary: RequiredBoundaryDef,
      endBoundary: BoundaryDef,
      logData: Option[Seq[ParsedLogData]] = None,
      ictEnablementInfo: Option[(Long, Long)] = None): CommitRange = {
    def getVersionFromFS(fs: FileStatus): Long = FileNames.getFileVersion(new Path(fs.getPath))
    val latestVersion = fileList.map(getVersionFromFS).max
    // If we have a ratified commit file at the end version, we want to use this in the log segment
    // for our mockLatestSnapshot, so we get the ICT from that file
    val deltaFileAtEndVersion = fileList
      .filter(fs => FileNames.isStagedDeltaFile(fs.getPath))
      .find(getVersionFromFS(_) == latestVersion)

    lazy val mockLatestSnapshot = getMockSnapshot(
      dataPath,
      latestVersion,
      ictEnablementInfoOpt = ictEnablementInfo,
      deltaFileAtEndVersion = deltaFileAtEndVersion)

    // Determine the start boundary
    val startBound = if (startBoundary.version.isDefined) {
      CommitBoundary.atVersion(startBoundary.version.get)
    } else if (startBoundary.timestamp.isDefined) {
      CommitBoundary.atTimestamp(startBoundary.timestamp.get, mockLatestSnapshot)
    } else {
      throw new IllegalStateException("RequiredBoundaryDef must have either timestamp or version")
    }

    var commitRangeBuilder = TableManager.loadCommitRange(dataPath.toString, startBound)
    endBoundary.version.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withEndBoundary(CommitBoundary.atVersion(v))
    }
    endBoundary.timestamp.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withEndBoundary(
        CommitBoundary.atTimestamp(v, mockLatestSnapshot))
    }
    logData.foreach { l =>
      commitRangeBuilder = commitRangeBuilder.withLogData(l.asJava)
    }
    commitRangeBuilder.build(engine)
  }

  private def checkCommitRange(
      fileList: Seq[FileStatus],
      expectedStartVersion: Long,
      expectedEndVersion: Long,
      startBoundary: RequiredBoundaryDef,
      endBoundary: BoundaryDef): Unit = {
    val commitRange = buildCommitRange(
      createMockFSListFromEngine(fileList),
      fileList,
      startBoundary,
      endBoundary)
    assert(commitRange.getStartVersion == expectedStartVersion)
    assert(commitRange.getEndVersion == expectedEndVersion)
    checkQueryBoundaries(commitRange, startBoundary, endBoundary)
    val expectedFileList = fileList
      .filter(fs => {
        val version = FileNames.getFileVersion(new Path(fs.getPath))
        version >= expectedStartVersion && version <= expectedEndVersion
      }).filter(fs => FileNames.isCommitFile(fs.getPath))
    assert(expectedFileList.toSet ==
      commitRange.asInstanceOf[CommitRangeImpl].getDeltaFiles.asScala.toSet)
  }

  /**
   * Base class for boundary definitions used in testing.
   * @param expectedVersion the expected version this def will be resolved to
   * @param expectError whether we expect this def to inherently fail for the corresponding listing
   */
  private abstract class BoundaryDef(
      val expectedVersion: Long,
      val expectError: Boolean = false) {

    def version: Option[Long] = None
    def timestamp: Option[Long] = None
  }

  /**
   * Base class for boundary definitions that are NOT the default (i.e. are provided).
   *
   * At least one of `version` or `timestamp` must be defined in this case.
   */
  private abstract class RequiredBoundaryDef(
      expectedVersion: Long,
      expectError: Boolean = false) extends BoundaryDef(expectedVersion, expectError) {
    assert(version.isDefined || timestamp.isDefined)
  }

  /**
   * Version-based boundary definition.
   * @param versionValue the version to use as boundary
   * @param expectsError whether we expect this def to inherently fail
   */
  private case class VersionBoundaryDef(
      versionValue: Long,
      expectsError: Boolean = false) extends RequiredBoundaryDef(versionValue, expectsError) {

    override def version: Option[Long] = Some(versionValue)
  }

  /**
   * Timestamp-based boundary definition.
   * @param timestampValue the timestamp to use as boundary
   * @param resolvedVersion the expected version this timestamp will resolve to
   * @param expectsError whether we expect this def to inherently fail
   */
  private case class TimestampBoundaryDef(
      timestampValue: Long,
      resolvedVersion: Long,
      expectsError: Boolean = false) extends RequiredBoundaryDef(resolvedVersion, expectsError) {

    override def timestamp: Option[Long] = Some(timestampValue)
  }

  /**
   * Default boundary definition (no specific version or timestamp).
   * @param resolvedVersion the expected version this will resolve to
   * @param expectsError whether we expect this def to inherently fail
   */
  private case class DefaultBoundaryDef(
      resolvedVersion: Long,
      expectsError: Boolean = false) extends BoundaryDef(resolvedVersion, expectsError)

  def getExpectedException(
      startBoundary: RequiredBoundaryDef,
      endBoundary: BoundaryDef): Option[(Class[_ <: Throwable], String)] = {
    // These two cases fail on CommitRangeBuilderImpl.validateInputOnBuild
    if (startBoundary.version.isDefined && endBoundary.version.isDefined) {
      if (startBoundary.version.get > endBoundary.version.get) {
        return Some(classOf[IllegalArgumentException], "startVersion must be <= endVersion")
      }
    }
    if (startBoundary.timestamp.isDefined && endBoundary.timestamp.isDefined) {
      if (startBoundary.timestamp.get > endBoundary.timestamp.get) {
        return Some(classOf[IllegalArgumentException], "startTimestamp must be <= endTimestamp")
      }
    }
    // We try to resolve any timestamps, first startVersion then endVersion (CommitRangeFactory)
    if (startBoundary.expectError && startBoundary.timestamp.isDefined) {
      return Some(classOf[KernelException], "is after the latest available version")
    }
    if (endBoundary.expectError && endBoundary.timestamp.isDefined) {
      return Some(classOf[KernelException], "is before the earliest available version")
    }
    // Now we hit an exception if resolved startVersion > resolvedEndVersion (CommitRangeFactory)
    // (endVersion is only resolved before listing if either TS or Version was provided)
    if (
      startBoundary.expectedVersion > endBoundary.expectedVersion &&
      (endBoundary.timestamp.isDefined || endBoundary.version.isDefined)
    ) {
      return Some(
        classOf[KernelException],
        s"startVersion=${startBoundary.expectedVersion} > " +
          s"endVersion=${endBoundary.expectedVersion}")
    }
    // Now we query the file list, this is where we fail if the provided versions do not exist
    if (startBoundary.expectError) {
      // These either hit DeltaErrors.noCommitFilesFoundForVersionRange or
      // DeltaErrors.startVersionNotFound
      return Some(classOf[KernelException], s"no log file")
    }
    if (endBoundary.expectError) {
      // These either hit DeltaErrors.noCommitFilesFoundForVersionRange or
      // DeltaErrors.endVersionNotFound
      return Some(classOf[KernelException], s"no log file")
    }
    None
  }

  def testStartAndEndBoundaryCombinations(
      description: String,
      fileStatuses: Seq[FileStatus],
      startBoundaries: Seq[RequiredBoundaryDef],
      endBoundaries: Seq[BoundaryDef]): Unit = {
    startBoundaries.foreach { startBound =>
      endBoundaries.foreach { endBound =>
        test(s"$description: build CommitRange with startBound=$startBound endBound=$endBound") {
          val expectedException = getExpectedException(startBound, endBound)
          if (expectedException.isDefined) {
            val e = intercept[Throwable] {
              buildCommitRange(
                createMockFSListFromEngine(fileStatuses),
                fileList = fileStatuses,
                startBoundary = startBound,
                endBoundary = endBound)
            }
            assert(
              expectedException.get._1.isInstance(e),
              s"Expected exception of ${expectedException.get._1} but found $e")
            assert(e.getMessage.contains(expectedException.get._2))
          } else {
            checkCommitRange(
              fileList = fileStatuses,
              expectedStartVersion = startBound.expectedVersion,
              expectedEndVersion = endBound.expectedVersion,
              startBoundary = startBound,
              endBoundary = endBound)
          }
        }
      }
    }
  }

  /* --------------- Without catalog commits --------------- */

  // Test with negative timestamps - manually create FileStatus with negative timestamps
  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(0, 1) with negative timestamps", // v0 -> -100, v1 -> -50
    fileStatuses = Seq(
      FileStatus.of(FileNames.deltaFile(logPath, 0L), 0L, -100L),
      FileStatus.of(FileNames.deltaFile(logPath, 1L), 1L, -50L)),
    startBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
      TimestampBoundaryDef(-150, resolvedVersion = 0L), // before v0
      TimestampBoundaryDef(-100, resolvedVersion = 0L), // at v0
      TimestampBoundaryDef(-75, resolvedVersion = 1), // between v0, v1
      TimestampBoundaryDef(-50, resolvedVersion = 1), // at v1
      TimestampBoundaryDef(-40, resolvedVersion = -1, expectsError = true), // after v1
      VersionBoundaryDef(2L, expectsError = true) // version DNE
    ),
    endBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
      TimestampBoundaryDef(-150, resolvedVersion = -1, expectsError = true), // before v0
      TimestampBoundaryDef(-100, resolvedVersion = 0L), // at v0
      TimestampBoundaryDef(-75, resolvedVersion = 0), // between v0, v1
      TimestampBoundaryDef(-50, resolvedVersion = 1), // at v1
      TimestampBoundaryDef(-40, resolvedVersion = 1), // after v1
      DefaultBoundaryDef(resolvedVersion = 1), // default to latest
      VersionBoundaryDef(2L, expectsError = true) // version DNE
    ))

  // The below test cases mimic the cases in TableImplSuite for the timestamp-resolution
  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(0, 1)", // (version -> timestamp) = v0 -> 0, v1 -> 10
    fileStatuses = deltaFileStatuses(Seq(0L, 1L)),
    startBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
      TimestampBoundaryDef(-5, resolvedVersion = 0L), // before v0, negative timestamp
      TimestampBoundaryDef(0, resolvedVersion = 0L), // at v0
      TimestampBoundaryDef(5, resolvedVersion = 1), // between v0, v1
      TimestampBoundaryDef(10, resolvedVersion = 1), // at v1
      TimestampBoundaryDef(11, resolvedVersion = -1, expectsError = true), // after v1
      VersionBoundaryDef(2L, expectsError = true) // version DNE
    ),
    endBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
      TimestampBoundaryDef(-5, resolvedVersion = -1, expectsError = true), // before v0, negative
      TimestampBoundaryDef(0, resolvedVersion = 0L), // at v0
      TimestampBoundaryDef(5, resolvedVersion = 0), // between v0, v1
      TimestampBoundaryDef(10, resolvedVersion = 1), // at v1
      TimestampBoundaryDef(11, resolvedVersion = 1), // after v1
      DefaultBoundaryDef(resolvedVersion = 1), // default to latest
      VersionBoundaryDef(2L, expectsError = true) // version DNE
    ))

  testStartAndEndBoundaryCombinations(
    // (version -> timestamp) = v10 -> 100, v11 -> 110, v12 -> 120
    description = "deltaFiles=(10, 11, 12)",
    fileStatuses = deltaFileStatuses(Seq(10L, 11L, 12L)) ++
      singularCheckpointFileStatuses(Seq(10L)),
    startBoundaries = Seq(
      VersionBoundaryDef(10L),
      VersionBoundaryDef(11L),
      VersionBoundaryDef(12L),
      TimestampBoundaryDef(99, resolvedVersion = 10), // before v10
      TimestampBoundaryDef(100, resolvedVersion = 10L), // at v10
      TimestampBoundaryDef(105, resolvedVersion = 11L), // between v10, v11
      TimestampBoundaryDef(110, resolvedVersion = 11L), // at v11
      TimestampBoundaryDef(115, resolvedVersion = 12L), // between v11, v12
      TimestampBoundaryDef(120, resolvedVersion = 12L), // at v12
      TimestampBoundaryDef(125, resolvedVersion = -1, expectsError = true), // after v12
      VersionBoundaryDef(9L, expectsError = true), // version DNE
      VersionBoundaryDef(13L, expectsError = true) // version DNE
    ),
    endBoundaries = Seq(
      VersionBoundaryDef(10L),
      VersionBoundaryDef(11L),
      VersionBoundaryDef(12L),
      TimestampBoundaryDef(100, resolvedVersion = 10), // at v10
      TimestampBoundaryDef(105, resolvedVersion = 10), // between v10, v11
      TimestampBoundaryDef(110, resolvedVersion = 11), // at v11
      TimestampBoundaryDef(115, resolvedVersion = 11), // between v11, v12
      TimestampBoundaryDef(120, resolvedVersion = 12), // at v12
      TimestampBoundaryDef(125, resolvedVersion = 12), // after v12
      DefaultBoundaryDef(resolvedVersion = 12), // default to latest
      TimestampBoundaryDef(99, resolvedVersion = -1, expectsError = true), // before V10
      VersionBoundaryDef(9L, expectsError = true), // version DNE
      VersionBoundaryDef(13L, expectsError = true) // version DNE
    ))

  // Check case with only 1 delta file
  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(10)", // (version -> timestamp) = v10 -> 100
    fileStatuses = deltaFileStatuses(Seq(10L)) ++ singularCheckpointFileStatuses(Seq(10L)),
    startBoundaries = Seq(
      VersionBoundaryDef(10L),
      TimestampBoundaryDef(99L, resolvedVersion = 10L), // before v10
      TimestampBoundaryDef(100L, resolvedVersion = 10L), // at v10
      TimestampBoundaryDef(101L, resolvedVersion = -1, expectsError = true), // after v10
      VersionBoundaryDef(1L, expectsError = true) // version DNE
    ),
    endBoundaries = Seq(
      VersionBoundaryDef(10L),
      TimestampBoundaryDef(100L, resolvedVersion = 10L), // at v10
      TimestampBoundaryDef(101L, resolvedVersion = 10L), // after v10
      DefaultBoundaryDef(resolvedVersion = 10L), // default to latest
      TimestampBoundaryDef(99L, resolvedVersion = -1, expectsError = true), // before v10
      VersionBoundaryDef(1L, expectsError = true) // version DNE
    ))

  /* --------------- With catalog commits --------------- */

  private def checkCommitRangeWithCatalogCommits(
      fileList: Seq[FileStatus],
      logData: Seq[ParsedLogData],
      versionToICT: Map[Long, Long],
      startBound: RequiredBoundaryDef,
      endBound: BoundaryDef,
      expectedFileList: Seq[FileStatus]): Unit = {
    // Create mock engine with ICT reading support
    val commitRange = buildCommitRange(
      createMockFSAndJsonEngineForICT(fileList, versionToICT),
      fileList,
      startBoundary = startBound,
      endBoundary = endBound,
      Some(logData),
      ictEnablementInfo = Some((0, 0)))
    assert(commitRange.getStartVersion == startBound.expectedVersion)
    assert(commitRange.getEndVersion == endBound.expectedVersion)
    checkQueryBoundaries(
      commitRange,
      startBound,
      endBound)
    assert(expectedFileList.toSet ==
      commitRange.asInstanceOf[CommitRangeImpl].getDeltaFiles.asScala.toSet)
  }

  /**
   * @param expectedFileList takes in a tuple (startVersion, endVersion) of a range and returns the
   *                         expected file list for that version range
   */
  private def testStartAndEndBoundaryCombinationsWithCatalogCommits(
      description: String,
      fileStatuses: Seq[FileStatus],
      expectedFileList: (Long, Long) => Seq[FileStatus],
      logData: Seq[ParsedLogData],
      versionToICT: Map[Long, Long],
      startBoundaries: Seq[RequiredBoundaryDef],
      endBoundaries: Seq[BoundaryDef]): Unit = {
    startBoundaries.foreach { startBound =>
      endBoundaries.foreach { endBound =>
        test(s"$description: build CommitRange with startBound=$startBound endBound=$endBound") {
          val expectedException = getExpectedException(startBound, endBound)
          if (expectedException.isDefined) {
            val e = intercept[Throwable] {
              buildCommitRange(
                createMockFSAndJsonEngineForICT(fileStatuses, versionToICT),
                fileStatuses,
                startBoundary = startBound,
                endBoundary = endBound,
                Some(logData),
                ictEnablementInfo = Some((0, 0)))
            }
            assert(
              expectedException.get._1.isInstance(e),
              s"Expected exception of ${expectedException.get._1} but found $e")
            assert(e.getMessage.contains(expectedException.get._2))
          } else {
            checkCommitRangeWithCatalogCommits(
              fileStatuses,
              logData,
              versionToICT,
              startBound,
              endBound,
              expectedFileList(startBound.expectedVersion, endBound.expectedVersion))
          }
        }
      }
    }
  }

  // Basic case with no overlap: P0, P1, C2, C3, C4
  {
    val catalogCommitFiles = Seq(stagedCommitFile(2), stagedCommitFile(3), stagedCommitFile(4))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val fileList = Seq(deltaFileStatus(0), deltaFileStatus(1)) ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L, 3L -> 3050L, 4L -> 4050L)

    val startBoundaries = Seq(
      // V0 (first published commit)
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1 (last published commit)
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // V2 (first catalog commit)
      VersionBoundaryDef(2),
      TimestampBoundaryDef(1500, 2), // between V1 and V2
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // V3 (middle catalog commit)
      VersionBoundaryDef(3L),
      // V4 (last catalog commit)
      VersionBoundaryDef(4L),
      TimestampBoundaryDef(3500L, 4L), // between V3 and V4
      TimestampBoundaryDef(4050L, 4), // exactly at V4
      // Some error cases
      TimestampBoundaryDef(4500, 4L, expectsError = true), // after V4
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0 (first published commit)
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1 (last published commit)
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1500L, 1), // between V1 and V2
      // V2 (first catalog commit)
      VersionBoundaryDef(2),
      TimestampBoundaryDef(2500, 2), // between V2 and V3
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // V3 (middle catalog commit)
      VersionBoundaryDef(3L),
      TimestampBoundaryDef(3500L, 3L), // between V3 and V4
      // V4 (last catalog commit)
      VersionBoundaryDef(4L),
      TimestampBoundaryDef(4050L, 4), // exactly at V4
      DefaultBoundaryDef(4L),
      TimestampBoundaryDef(4500, 4L), // after V4
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits basic case no overlap",
      fileList,
      (startV, endV) => fileList.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  // Basic case with overlap: P0, P1, P2, R1, R2, R3 (+ prioritize catalog commits)
  {
    val catalogCommitFiles = Seq(stagedCommitFile(1), stagedCommitFile(2), stagedCommitFile(3))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0), deltaFileStatus(1), deltaFileStatus(2))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L, 3L -> 3050L)
    // We expect catalog commits to take precedence over published deltas
    val expectedFileList = publishedDeltaFiles.slice(0, 1) ++ catalogCommitFiles

    val startBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // at V1
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // V2
      VersionBoundaryDef(2),
      TimestampBoundaryDef(1500, 2), // between V1 and V2
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // V3
      VersionBoundaryDef(3L),
      TimestampBoundaryDef(2500, 3), // between V2 and V3
      TimestampBoundaryDef(3050, 3), // exactly at V3
      // Some error cases
      TimestampBoundaryDef(4500, 3L, expectsError = true), // after V4
      VersionBoundaryDef(4, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1500L, 1), // between V1 and V2
      // V2
      VersionBoundaryDef(2),
      TimestampBoundaryDef(2500, 2), // between V2 and V3
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // V3
      VersionBoundaryDef(3L),
      TimestampBoundaryDef(3050L, 3), // exactly at V3
      DefaultBoundaryDef(3L),
      TimestampBoundaryDef(3500, 3L), // after V3
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits basic case with overlap",
      fileList,
      (startV, endV) => expectedFileList.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  // Only catalog commits: C0, C1
  {
    val catalogCommitFiles = Seq(stagedCommitFile(0), stagedCommitFile(1))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val versionToICT = Map(0L -> 50L, 1L -> 1050L)

    val startBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // at V1
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // Some error cases
      TimestampBoundaryDef(4500, 1L, expectsError = true), // after V1
      VersionBoundaryDef(4, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // exactly at V1
      TimestampBoundaryDef(1500L, 1), // after V1
      DefaultBoundaryDef(1),
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits no published deltas",
      catalogCommitFiles,
      (startV, endV) => catalogCommitFiles.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  // Single published commit + single catalog commit: P0, C1
  {
    val catalogCommitFiles = Seq(stagedCommitFile(1))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L)

    val startBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // at V1
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // Some error cases
      TimestampBoundaryDef(4500, 1L, expectsError = true), // after V1
      VersionBoundaryDef(4, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // exactly at V1
      TimestampBoundaryDef(1500L, 1), // after V1
      DefaultBoundaryDef(1),
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits single catalog commit single published commit",
      fileList,
      (startV, endV) => fileList.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  // Overlap by just 1: P0, P1, R1, R2
  {
    val catalogCommitFiles = Seq(stagedCommitFile(1), stagedCommitFile(2))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0), deltaFileStatus(1))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L)
    // We expect catalog commits to take precedence over published deltas
    val expectedFileList = publishedDeltaFiles.slice(0, 1) ++ catalogCommitFiles

    val startBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // at V1
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // V2
      VersionBoundaryDef(2L),
      TimestampBoundaryDef(1500, 2), // between V1 and V2
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // Some error cases
      TimestampBoundaryDef(4500, 3L, expectsError = true), // after V2
      VersionBoundaryDef(4, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1500L, 1), // between V1 and V2
      // V2
      VersionBoundaryDef(2L),
      TimestampBoundaryDef(2050L, 2), // exactly at V2
      DefaultBoundaryDef(2L),
      TimestampBoundaryDef(2500, 2), // after V2
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits single commit overlap",
      fileList,
      (startV, endV) => expectedFileList.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  // Full overlap: P0, P1, P2 + C0, C1, C2
  {
    val catalogCommitFiles = Seq(stagedCommitFile(0), stagedCommitFile(1), stagedCommitFile(2))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0), deltaFileStatus(1), deltaFileStatus(2))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L)

    val startBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(5L, 0), // before V0
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1050L, 1), // at V1
      TimestampBoundaryDef(1000L, 1), // between V0 and V1
      // V2
      VersionBoundaryDef(2L),
      TimestampBoundaryDef(1500, 2), // between V1 and V2
      TimestampBoundaryDef(2050, 2), // exactly at V2
      // Some error cases
      TimestampBoundaryDef(4500, 3L, expectsError = true), // after V2
      VersionBoundaryDef(4, expectsError = true) // version DNE
    )
    val endBoundaries = Seq(
      // V0
      VersionBoundaryDef(0),
      TimestampBoundaryDef(50L, 0L), // exactly at V0
      TimestampBoundaryDef(500L, 0L), // between V0 and V1
      // V1
      VersionBoundaryDef(1),
      TimestampBoundaryDef(1500L, 1), // between V1 and V2
      // V2
      VersionBoundaryDef(2L),
      TimestampBoundaryDef(2050L, 2), // exactly at V2
      DefaultBoundaryDef(2L),
      TimestampBoundaryDef(2500, 2), // after V2
      // Some error cases
      TimestampBoundaryDef(5L, 0, expectsError = true), // before V0
      VersionBoundaryDef(5, expectsError = true) // version DNE
    )
    testStartAndEndBoundaryCombinationsWithCatalogCommits(
      "catalog commits full overlap",
      fileList,
      (startV, endV) => catalogCommitFiles.slice(startV.toInt, endV.toInt + 1),
      parsedLogData,
      versionToICT,
      startBoundaries,
      endBoundaries)
  }

  test("build CommitRange fails if catalog commits pre-curse published commits") {
    val catalogCommitFiles = Seq(stagedCommitFile(0), stagedCommitFile(1), stagedCommitFile(2))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(1), deltaFileStatus(2), deltaFileStatus(3))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L)

    val e = intercept[InvalidTableException] {
      buildCommitRange(
        createMockFSAndJsonEngineForICT(fileList, versionToICT),
        fileList,
        startBoundary = VersionBoundaryDef(0),
        endBoundary = VersionBoundaryDef(3),
        logData = Some(parsedLogData),
        ictEnablementInfo = Some((0, 0)))
    }
    assert(e.getMessage.contains(
      "Missing delta file: found staged ratified commit for version 0 but no published " +
        "delta file. Found published deltas for later versions: [1, 2, 3]"))
  }

  test("build CommitRange fails if published commits and catalog commits are not contiguous") {
    val catalogCommitFiles = Seq(stagedCommitFile(2))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L)

    val e = intercept[InvalidTableException] {
      buildCommitRange(
        createMockFSAndJsonEngineForICT(fileList, versionToICT),
        fileList,
        startBoundary = VersionBoundaryDef(0),
        endBoundary = VersionBoundaryDef(2),
        logData = Some(parsedLogData),
        ictEnablementInfo = Some((0, 0)))
    }
    assert(e.getMessage.contains(
      "Missing delta files: found published delta files for versions [0] and staged " +
        "ratified commits for versions [2]"))
  }

  test("build CommitRange fails if published deltas are not contiguous") {
    val publishedDeltaFiles = Seq(deltaFileStatus(0), deltaFileStatus(2), deltaFileStatus(3))
    val e = intercept[InvalidTableException] {
      buildCommitRange(
        createMockFSListFromEngine(publishedDeltaFiles),
        publishedDeltaFiles,
        startBoundary = VersionBoundaryDef(0),
        endBoundary = VersionBoundaryDef(3))
    }
    assert(e.getMessage.contains(
      "Missing delta files: versions are not contiguous: ([0, 2, 3])"))
  }

  Seq(
    ParsedCatalogCommitData.forInlineData(1, emptyColumnarBatch),
    ParsedLogData.forFileStatus(logCompactionStatus(0, 1))).foreach { parsedLogData =>
    val suffix = s"- type=${parsedLogData.getGroupByCategoryClass.toString}"
    test(s"withLogData: non-staged-ratified-commit throws IllegalArgumentException $suffix") {
      val builder = TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withLogData(Collections.singletonList(parsedLogData))

      val exMsg = intercept[IllegalArgumentException] {
        builder.build(mockEngine())
      }.getMessage

      assert(exMsg.contains("Only staged ratified commits are supported"))
    }
  }

  test("withLogData: non-contiguous input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 2)).toList.asJava)
        .build(mockEngine())
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  test("withLogData: non-sorted input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withLogData(parsedRatifiedStagedCommits(Seq(2, 1, 0)).toList.asJava)
        .build(mockEngine())
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }
}
