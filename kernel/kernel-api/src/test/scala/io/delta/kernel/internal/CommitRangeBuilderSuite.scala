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

import java.util.{Collections, NoSuchElementException, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.{CommitFileStatus, CommitRange, TableManager}
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{InvalidTableException, KernelException, StartVersionNotFoundException}
import io.delta.kernel.internal.commitrange.{CommitRangeBuilderImpl, CommitRangeImpl}
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedDeltaData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.test.{BaseMockFileSystemClient, MockFileSystemClientUtils, MockListFromFileSystemClient, MockReadICTFileJsonHandler, MockSnapshotUtils, VectorTestUtils}
import io.delta.kernel.test.MockSnapshotUtils.getMockSnapshot
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

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
      ictEnablementInfo: Option[(Long, Long)] = None,
      commitRangeValidation: Option[Boolean] = None,
      failOnDataLoss: Option[Boolean] = None): CommitRange = {
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
    commitRangeValidation.foreach { enabled =>
      commitRangeBuilder = commitRangeBuilder.withCommitRangeValidation(enabled)
    }
    failOnDataLoss.foreach { enabled =>
      commitRangeBuilder = commitRangeBuilder.withFailOnDataLoss(enabled)
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
      endBoundary: BoundaryDef,
      fileStatuses: Seq[FileStatus]): Option[(Class[_ <: Throwable], String)] = {
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
    if (endBoundary.version.isDefined) {
      val stagedCommits = fileStatuses
        .filter(fs => FileNames.isStagedDeltaFile(fs.getPath))
      if (stagedCommits.nonEmpty) {
        val tailStagedCommit = stagedCommits(stagedCommits.length - 1)
        if (endBoundary.version.get > FileNames.deltaVersion(tailStagedCommit.getPath)) {
          return Some(
            classOf[IllegalArgumentException],
            "When endVersion is specified with logData, the last logData version")
        }
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
          val expectedException = getExpectedException(startBound, endBound, fileStatuses)
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
          val expectedException = getExpectedException(startBound, endBound, fileStatuses)
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
        endBoundary = VersionBoundaryDef(2),
        logData = Some(parsedLogData),
        ictEnablementInfo = Some((0, 0)))
    }
    assert(e.getMessage.contains(
      "Missing delta file: found staged ratified commit for version 0 but no published " +
        "delta file. Found published deltas for later versions: [1, 2]"))
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

  test("build CommitRange allows published/catalog gaps with relaxed validation") {
    val catalogCommitFiles = Seq(stagedCommitFile(2))
    val parsedLogData = catalogCommitFiles.map(ParsedCatalogCommitData.forFileStatus(_))
    val publishedDeltaFiles = Seq(deltaFileStatus(0))
    val fileList = publishedDeltaFiles ++ catalogCommitFiles
    val versionToICT = Map(0L -> 50L, 1L -> 1050L, 2L -> 2050L)

    val commitRange = buildCommitRange(
      createMockFSAndJsonEngineForICT(fileList, versionToICT),
      fileList,
      startBoundary = VersionBoundaryDef(0),
      endBoundary = VersionBoundaryDef(2),
      logData = Some(parsedLogData),
      ictEnablementInfo = Some((0, 0)),
      commitRangeValidation = Some(false),
      failOnDataLoss = Some(false))

    val commitVersions = commitRange.getCommitFiles.toInMemoryList.asScala.map(_.getVersion)
    assert(commitVersions == Seq(0L, 2L))
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

  test("getCommitFiles returns commit metadata") {
    val fileList = deltaFileStatuses(Seq(0L, 1L))
    val commitRange = buildCommitRange(
      createMockFSListFromEngine(fileList),
      fileList,
      startBoundary = VersionBoundaryDef(0),
      endBoundary = VersionBoundaryDef(1))

    val commitFiles = commitRange.getCommitFiles.toInMemoryList.asScala
    assert(commitFiles.map(_.getVersion) == Seq(0L, 1L))
    assert(commitFiles.map(_.getFileStatus.getPath) == fileList.map(_.getPath))
    intercept[NullPointerException](new CommitFileStatus(0L, null))
  }

  private case class RelaxedCommitFilesTestCase(
      description: String,
      availableVersions: Seq[Long],
      endBoundary: BoundaryDef,
      failOnDataLoss: Boolean,
      expectedVersions: Seq[Long],
      expectedError: Option[(Class[_ <: KernelException], Seq[String])] = None)

  Seq(
    RelaxedCommitFilesTestCase(
      "skips gaps when data loss is allowed",
      Seq(0L, 2L),
      VersionBoundaryDef(2),
      failOnDataLoss = false,
      Seq(0L, 2L)),
    RelaxedCommitFilesTestCase(
      "reports an internal gap lazily and fuses",
      Seq(0L, 2L),
      VersionBoundaryDef(2),
      failOnDataLoss = true,
      Seq(0L),
      Some(classOf[InvalidTableException] -> Seq(
        "versions are not contiguous",
        "Table version 1 is missing or unavailable for this log operation."))),
    RelaxedCommitFilesTestCase(
      "reports a missing start lazily",
      Seq(2L, 3L),
      VersionBoundaryDef(3),
      failOnDataLoss = true,
      Seq.empty,
      Some(classOf[KernelException] -> Seq(
        "startVersion=0",
        "Earliest available version is 2"))),
    RelaxedCommitFilesTestCase(
      "does not report a missing suffix without a later commit",
      Seq(0L, 1L),
      VersionBoundaryDef(3),
      failOnDataLoss = true,
      Seq(0L, 1L)),
    RelaxedCommitFilesTestCase(
      "resolves an implicit end to the last listed commit",
      Seq(0L, 2L),
      DefaultBoundaryDef(2),
      failOnDataLoss = false,
      Seq(0L, 2L))).foreach { testCase =>
    test(s"relaxed getCommitFiles ${testCase.description}") {
      val fileList = deltaFileStatuses(testCase.availableVersions)
      val commitRange = buildCommitRange(
        createMockFSListFromEngine(fileList),
        fileList,
        startBoundary = VersionBoundaryDef(0),
        endBoundary = testCase.endBoundary,
        commitRangeValidation = Some(false),
        failOnDataLoss = Some(testCase.failOnDataLoss))
      val iterator = commitRange.getCommitFiles

      assert(commitRange.getEndVersion == testCase.endBoundary.expectedVersion)
      testCase.expectedVersions.foreach(version => assert(iterator.next().getVersion == version))
      testCase.expectedError.foreach { case (errorClass, messageParts) =>
        val error = intercept[KernelException](iterator.hasNext)
        assert(errorClass.isInstance(error))
        messageParts.foreach(messagePart => assert(error.getMessage.contains(messagePart)))
      }
      assert(!iterator.hasNext)
      intercept[NoSuchElementException](iterator.next())
    }
  }

  test("validated construction still rejects an empty listing") {
    val engine = createMockFSListFromEngine(Seq.empty)
    val error = intercept[KernelException] {
      TableManager.loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .build(engine)
    }
    assert(error.getMessage.contains("no log files found"))
  }

  test("relaxed ranges reject action iteration") {
    val fileList = deltaFileStatuses(Seq(0L, 1L))
    val commitRange = buildCommitRange(
      createMockFSListFromEngine(fileList),
      fileList,
      startBoundary = VersionBoundaryDef(0),
      endBoundary = VersionBoundaryDef(1),
      commitRangeValidation = Some(false))

    val actionsError = intercept[KernelException](commitRange.getActions(null, null, null))
    assert(actionsError.getMessage.contains("Commit action iteration is not supported"))
    val commitsError = intercept[KernelException](commitRange.getCommitActions(null, null, null))
    assert(commitsError.getMessage.contains("Commit action iteration is not supported"))
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

      assert(exMsg.contains("Only file-based ratified catalog commits are supported"))
    }
  }

  Seq(true, false).foreach { commitRangeValidation =>
    test(s"withLogData: non-contiguous input throws IllegalArgumentException - " +
      s"commitRangeValidation=$commitRangeValidation") {
      val exMsg = intercept[IllegalArgumentException] {
        TableManager.loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
          .withLogData(parsedRatifiedStagedCommits(Seq(0, 2)).toList.asJava)
          .withCommitRangeValidation(commitRangeValidation)
          .build(mockEngine())
      }.getMessage

      assert(exMsg.contains("Log data must be sorted and contiguous"))
    }
  }

  test("withLogData: non-sorted input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withLogData(parsedRatifiedStagedCommits(Seq(2, 1, 0)).toList.asJava)
        .build(mockEngine())
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  //////////////////////////////////////////////
  // withMaxCatalogVersion Tests
  //////////////////////////////////////////////

  test("withMaxCatalogVersion: negative version throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxCatalogVersion(-1)
    }.getMessage

    assert(exMsg.contains("maxCatalogVersion must be >= 0"))
  }

  test("withMaxCatalogVersion: zero is valid") {
    val fileList = deltaFileStatuses(Seq(0, 1, 2))
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(0)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 0)
  }

  test("withMaxCatalogVersion: positive version is valid") {
    val fileList = deltaFileStatuses(0L to 10L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(5)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 5)
  }

  test("withMaxCatalogVersion: start version must be <= maxCatalogVersion") {
    val fileList = deltaFileStatuses(0L to 10L)
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(6))
        .withMaxCatalogVersion(5)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("startVersion (6) must be <= maxCatalogVersion (5)"))
  }

  test("withMaxCatalogVersion: start version equal to maxCatalogVersion is valid") {
    val fileList = deltaFileStatuses(0L to 10L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(5))
      .withMaxCatalogVersion(5)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 5)
    assert(commitRange.getEndVersion == 5)
  }

  test("withMaxCatalogVersion: end version must be <= maxCatalogVersion") {
    val fileList = deltaFileStatuses(0L to 10L)
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxCatalogVersion(5)
        .withEndBoundary(CommitBoundary.atVersion(6))
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("endVersion (6) must be <= maxCatalogVersion (5)"))
  }

  test("withMaxCatalogVersion: end version equal to maxCatalogVersion is valid") {
    val fileList = deltaFileStatuses(0L to 10L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(5)
      .withEndBoundary(CommitBoundary.atVersion(5))

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 5)
  }

  test(
    "withMaxCatalogVersion: start timestamp boundary requires snapshot version = " +
      "maxCatalogVersion") {
    val fileList = deltaFileStatuses(0L to 10L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 10)

    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(
          dataPath.toString,
          CommitBoundary.atTimestamp(50, mockLatestSnapshot))
        .withMaxCatalogVersion(5)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("the provided snapshot version (10) must equal maxCatalogVersion (5)"))
  }

  test("withMaxCatalogVersion: start timestamp boundary with matching snapshot version is valid") {
    val fileList = deltaFileStatuses(0L to 5L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 5)

    val builder = TableManager
      .loadCommitRange(
        dataPath.toString,
        CommitBoundary.atTimestamp(30, mockLatestSnapshot))
      .withMaxCatalogVersion(5)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 3) // timestamp 30 maps to version 3
    assert(commitRange.getEndVersion == 5)
  }

  test(
    "withMaxCatalogVersion: end timestamp boundary requires snapshot version = maxCatalogVersion") {
    val fileList = deltaFileStatuses(0L to 10L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 10)

    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxCatalogVersion(5)
        .withEndBoundary(CommitBoundary.atTimestamp(50, mockLatestSnapshot))
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("the provided snapshot version (10) must equal maxCatalogVersion (5)"))
  }

  test("withMaxCatalogVersion: end timestamp boundary with matching snapshot version is valid") {
    val fileList = deltaFileStatuses(0L to 5L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 5)

    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(5)
      .withEndBoundary(CommitBoundary.atTimestamp(30, mockLatestSnapshot))

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 3) // timestamp 30 maps to version 3
  }

  test("withMaxCatalogVersion: without end boundary, logData must end with maxCatalogVersion") {
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4))
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxCatalogVersion(5)
        .withLogData(logData.toList.asJava)
        .build(mockEngine())
    }.getMessage

    assert(exMsg.contains("the last logData version (4) must equal maxCatalogVersion (5)"))
  }

  test(
    "withMaxCatalogVersion: without end boundary, logData ending with maxCatalogVersion is valid") {
    val fileList = deltaFileStatuses(0L to 5L)
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4, 5))
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(5)
      .withLogData(logData.toList.asJava)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 5)
  }

  test("withMaxCatalogVersion: empty logData with maxCatalogVersion is valid") {
    val fileList = deltaFileStatuses(0L to 10L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(5)
      .withLogData(Collections.emptyList())

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 5)
  }

  test("withMaxCatalogVersion: with end boundary and logData is valid") {
    val fileList = deltaFileStatuses(0L to 10L)
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2, 3))
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxCatalogVersion(10)
      .withEndBoundary(CommitBoundary.atVersion(3))
      .withLogData(logData.toList.asJava)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 3)
  }

  //////////////////////////////////////////////
  // withLogData + endVersion validation tests
  //////////////////////////////////////////////

  test("withLogData: with endVersion, logData must cover the requested range") {
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2))
    val fileList = deltaFileStatuses(0L to 3L)
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withEndBoundary(CommitBoundary.atVersion(3))
        .withLogData(logData.toList.asJava)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("the last logData version (2) must be >= endVersion (3)"))
  }

  test("withLogData: with endVersion equal to last logData version is valid") {
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2, 3))
    val fileList = deltaFileStatuses(0L to 3L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withEndBoundary(CommitBoundary.atVersion(3))
      .withLogData(logData.toList.asJava)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 3)
  }

  test("withLogData: with endVersion less than last logData version is valid") {
    val logData = parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4, 5))
    val fileList = deltaFileStatuses(0L to 5L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withEndBoundary(CommitBoundary.atVersion(3))
      .withLogData(logData.toList.asJava)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 3)
  }

  ///////////////////////////////
  // withMaxEndVersion Tests
  ///////////////////////////////

  private def getDeltaFileVersions(commitRange: CommitRange): Seq[Long] = {
    commitRange.asInstanceOf[CommitRangeImpl].getDeltaFiles.asScala
      .map(fs => FileNames.getFileVersion(new Path(fs.getPath)))
      .toSeq
  }

  test("withMaxEndVersion: negative version throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxEndVersion(-1)
    }.getMessage

    assert(exMsg.contains("maxEndVersion must be >= 0"))
  }

  test("withMaxEndVersion: range ends at maxEndVersion when the table has later versions") {
    val fileList = deltaFileStatuses(0L to 10L)
    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(2))
      .withMaxEndVersion(5)
      .build(createMockFSListFromEngine(fileList))

    assert(commitRange.getStartVersion == 2)
    assert(commitRange.getEndVersion == 5)
    assert(!commitRange.getQueryEndBoundary.isPresent)
    assert(getDeltaFileVersions(commitRange) == (2L to 5L))
  }

  test("withMaxEndVersion: range ends at the latest version when the table ends before " +
    "maxEndVersion") {
    val fileList = deltaFileStatuses(0L to 3L)
    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(1))
      .withMaxEndVersion(10)
      .build(createMockFSListFromEngine(fileList))

    assert(commitRange.getStartVersion == 1)
    assert(commitRange.getEndVersion == 3)
    assert(!commitRange.getQueryEndBoundary.isPresent)
    assert(getDeltaFileVersions(commitRange) == (1L to 3L))
  }

  test("withMaxEndVersion: maxEndVersion equal to the start version gives a single version") {
    val fileList = deltaFileStatuses(0L to 10L)
    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(4))
      .withMaxEndVersion(4)
      .build(createMockFSListFromEngine(fileList))

    assert(commitRange.getStartVersion == 4)
    assert(commitRange.getEndVersion == 4)
    assert(getDeltaFileVersions(commitRange) == Seq(4L))
  }

  test("withMaxEndVersion: start version must be <= maxEndVersion") {
    val fileList = deltaFileStatuses(0L to 10L)
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(6))
        .withMaxEndVersion(5)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("startVersion (6) must be <= maxEndVersion (5)"))
  }

  test("withMaxEndVersion: cannot be combined with withEndBoundary") {
    val fileList = deltaFileStatuses(0L to 10L)
    val exMsg = intercept[IllegalArgumentException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withEndBoundary(CommitBoundary.atVersion(3))
        .withMaxEndVersion(5)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("withEndBoundary and withMaxEndVersion cannot be used together"))
  }

  test("withMaxEndVersion: start timestamp boundary") {
    val fileList = deltaFileStatuses(0L to 10L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 10)
    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atTimestamp(20, mockLatestSnapshot))
      .withMaxEndVersion(4)
      .build(createMockFSListFromEngine(fileList))

    assert(commitRange.getStartVersion == 2) // timestamp 20 maps to version 2
    assert(commitRange.getEndVersion == 4)
  }

  test("withMaxEndVersion: start timestamp boundary resolving after maxEndVersion fails") {
    val fileList = deltaFileStatuses(0L to 10L)
    val mockLatestSnapshot = getMockSnapshot(dataPath, 10)
    val exMsg = intercept[KernelException] {
      TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atTimestamp(70, mockLatestSnapshot))
        .withMaxEndVersion(5)
        .build(createMockFSListFromEngine(fileList))
    }.getMessage

    assert(exMsg.contains("startVersion=7 > endVersion=5"))
  }

  Seq(
    // (maxEndVersion, maxCatalogVersion, expectedEndVersion)
    (3L, 5L, 3L),
    (5L, 5L, 5L),
    (8L, 5L, 5L)).foreach { case (maxEndVersion, maxCatalogVersion, expectedEndVersion) =>
    test(s"withMaxEndVersion: with maxCatalogVersion, maxEndVersion=$maxEndVersion and " +
      s"maxCatalogVersion=$maxCatalogVersion ends at $expectedEndVersion") {
      val fileList = deltaFileStatuses(0L to 10L)
      val commitRange = TableManager
        .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
        .withMaxCatalogVersion(maxCatalogVersion)
        .withMaxEndVersion(maxEndVersion)
        .build(createMockFSListFromEngine(fileList))

      assert(commitRange.getStartVersion == 0)
      assert(commitRange.getEndVersion == expectedEndVersion)
      assert(getDeltaFileVersions(commitRange) == (0L to expectedEndVersion))
    }
  }

  test("withMaxEndVersion: the log is not listed past maxEndVersion") {
    val fileList = (0L to 1000L).flatMap(v => Seq(deltaFileStatus(v), checksumFileStatus(v)))
    var filesListed = 0
    val fileSystemClient = new BaseMockFileSystemClient {
      override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
        val files = listFromProvider(fileList)(filePath).iterator.map { fs =>
          filesListed += 1
          fs
        }
        toCloseableIterator(files.asJava)
      }

      override def resolvePath(path: String): String = path
    }

    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withMaxEndVersion(5)
      .build(mockEngine(fileSystemClient = fileSystemClient))

    assert(commitRange.getEndVersion == 5)
    // 2 files for each of the versions 0 to 5, then the checksum and the commit file of version 6.
    // Reading the commit file of version 6 stops the listing.
    assert(filesListed == 14)
  }

  test("withLogData: published and staged catalog commits are both accepted") {
    val logData = Seq[ParsedLogData](
      ParsedCatalogCommitData.forCatalogFileStatus(logPath.toString, 0, deltaFileStatus(0)),
      ParsedCatalogCommitData.forCatalogFileStatus(logPath.toString, 1, deltaFileStatus(1)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(2)))
    val fileList = deltaFileStatuses(0L to 1L)
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withEndBoundary(CommitBoundary.atVersion(2))
      .withLogData(logData.toList.asJava)

    val commitRange = builder.build(createMockFSListFromEngine(fileList))
    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 2)
  }

  test("withLogData: published catalog commits removed by metadata cleanup are not trusted") {
    // UC still reports v0 as a catalog commit, but 0.json has been removed by metadata cleanup
    val logData = Seq[ParsedLogData](
      ParsedCatalogCommitData.forCatalogFileStatus(logPath.toString, 0, deltaFileStatus(0)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(1)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(2)))
    val builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(0))
      .withEndBoundary(CommitBoundary.atVersion(2))
      .withLogData(logData.toList.asJava)

    intercept[StartVersionNotFoundException] {
      builder.build(createMockFSListFromEngine(deltaFileStatuses(1L to 2L)))
    }
  }

  ///////////////////////////////////////////////////////
  // Raw commit-file semantics (DeltaLog parity) tests //
  ///////////////////////////////////////////////////////

  private val startErrorParts = (start: Long, earliest: Long) =>
    (
      classOf[StartVersionNotFoundException],
      Seq(
        s"startVersion=$start",
        s"Earliest available version is $earliest"))

  private val gapErrorParts = (missing: Long) =>
    (
      classOf[InvalidTableException],
      Seq(
        "versions are not contiguous",
        s"Table version $missing is missing or unavailable for this log operation."))

  private def buildRawRange(
      files: Seq[FileStatus],
      start: Long,
      end: Option[Long],
      commitRangeValidation: Boolean,
      failOnDataLoss: Boolean = true,
      logData: Seq[ParsedLogData] = Seq.empty): CommitRange = {
    var builder = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(start))
      .withCommitRangeValidation(commitRangeValidation)
      .withFailOnDataLoss(failOnDataLoss)
    end.foreach(v => builder = builder.withEndBoundary(CommitBoundary.atVersion(v)))
    if (logData.nonEmpty) {
      builder = builder.withLogData(logData.asJava)
    }
    builder.build(createMockFSListFromEngine(files))
  }

  /** Drains getCommitFiles, returning yielded versions and the error that ended iteration. */
  private def drainCommitFiles(
      commitRange: CommitRange): (Seq[CommitFileStatus], Option[KernelException]) = {
    val iterator = commitRange.getCommitFiles
    try {
      val yielded = ArrayBuffer[CommitFileStatus]()
      var error: Option[KernelException] = None
      try {
        while (iterator.hasNext) {
          yielded += iterator.next()
        }
      } catch {
        case e: KernelException => error = Some(e)
      }
      // The iterator is fused: it never recovers after finishing or failing.
      assert(!iterator.hasNext)
      intercept[NoSuchElementException](iterator.next())
      (yielded.toSeq, error)
    } finally {
      iterator.close()
    }
  }

  private case class RawRangeCase(
      description: String,
      availableVersions: Seq[Long],
      start: Long,
      end: Option[Long],
      // Expected outcome with failOnDataLoss = true: yielded versions then an optional error
      strictVersions: Seq[Long],
      strictError: Option[(Class[_ <: KernelException], Seq[String])],
      // Expected versions with failOnDataLoss = false (never an error)
      lenientVersions: Seq[Long],
      expectedEnd: Long)

  private val rawRangeCases = Seq(
    // Requested range [5, 7]
    RawRangeCase(
      "[5,7] with 5,6,7",
      Seq(5L, 6L, 7L),
      5,
      Some(7),
      Seq(5L, 6L, 7L),
      None,
      Seq(5L, 6L, 7L),
      7),
    RawRangeCase(
      "[5,7] with 5,6,7 and later commits are never yielded",
      Seq(3L, 4L, 5L, 6L, 7L, 8L, 9L, 12L),
      5,
      Some(7),
      Seq(5L, 6L, 7L),
      None,
      Seq(5L, 6L, 7L),
      7),
    RawRangeCase(
      "[5,7] with 6,7 (missing start)",
      Seq(6L, 7L),
      5,
      Some(7),
      Seq.empty,
      Some(startErrorParts(5, 6)),
      Seq(6L, 7L),
      7),
    RawRangeCase(
      "[5,7] with 5,7 (interior gap)",
      Seq(5L, 7L),
      5,
      Some(7),
      Seq(5L),
      Some(gapErrorParts(6)),
      Seq(5L, 7L),
      7),
    RawRangeCase(
      "[5,7] with 5,6,8 (gap proven by a later commit)",
      Seq(5L, 6L, 8L),
      5,
      Some(7),
      Seq(5L, 6L),
      Some(gapErrorParts(7)),
      Seq(5L, 6L),
      7),
    RawRangeCase(
      "[5,7] with 5,6,10,11 (only the first later commit is inspected)",
      Seq(5L, 6L, 10L, 11L),
      5,
      Some(7),
      Seq(5L, 6L),
      Some(gapErrorParts(7)),
      Seq(5L, 6L),
      7),
    RawRangeCase(
      "[5,7] with 5,6 and no 7+ (a missing suffix is not proven)",
      Seq(5L, 6L),
      5,
      Some(7),
      Seq(5L, 6L),
      None,
      Seq(5L, 6L),
      7),
    RawRangeCase(
      "[5,7] with only 8 at/after 5 (missing start)",
      Seq(2L, 8L),
      5,
      Some(7),
      Seq.empty,
      Some(startErrorParts(5, 8)),
      Seq.empty,
      7),
    RawRangeCase(
      "[5,7] with no 5+",
      Seq(0L, 1L, 2L, 3L, 4L),
      5,
      Some(7),
      Seq.empty,
      None,
      Seq.empty,
      7),
    // Unbounded from 5
    RawRangeCase(
      "unbounded from 5 with no 5+",
      Seq(0L, 1L, 2L),
      5,
      None,
      Seq.empty,
      None,
      Seq.empty,
      5),
    RawRangeCase(
      "unbounded from 5 with 5,6,7",
      Seq(5L, 6L, 7L),
      5,
      None,
      Seq(5L, 6L, 7L),
      None,
      Seq(5L, 6L, 7L),
      7),
    // Reversed range [7, 5]
    RawRangeCase(
      "reversed [7,5] with 7 present",
      Seq(6L, 7L, 8L),
      7,
      Some(5),
      Seq.empty,
      None,
      Seq.empty,
      5),
    RawRangeCase(
      "reversed [7,5] with no 7+",
      Seq(3L, 4L, 5L, 6L),
      7,
      Some(5),
      Seq.empty,
      None,
      Seq.empty,
      5),
    RawRangeCase(
      "reversed [7,5] with 7 absent and 8 present (missing start)",
      Seq(5L, 8L),
      7,
      Some(5),
      Seq.empty,
      Some(startErrorParts(7, 8)),
      Seq.empty,
      5))

  for (testCase <- rawRangeCases; failOnDataLoss <- Seq(true, false)) {
    test(s"raw getCommitFiles ${testCase.description} - failOnDataLoss=$failOnDataLoss") {
      val files = deltaFileStatuses(testCase.availableVersions)
      // Building never fails; any data loss is only reported while iterating.
      val commitRange = buildRawRange(
        files,
        testCase.start,
        testCase.end,
        commitRangeValidation = false,
        failOnDataLoss = failOnDataLoss)
      assert(commitRange.getStartVersion == testCase.start)
      assert(commitRange.getEndVersion == testCase.expectedEnd)
      assert(commitRange.isEmpty == testCase.lenientVersions.isEmpty)

      val (yielded, error) = drainCommitFiles(commitRange)
      val expectedVersions =
        if (failOnDataLoss) testCase.strictVersions else testCase.lenientVersions
      assert(yielded.map(_.getVersion) == expectedVersions)
      // Only commits inside the requested range are ever yielded.
      yielded.foreach { commitFile =>
        assert(commitFile.getVersion >= testCase.start)
        assert(testCase.end.forall(commitFile.getVersion <= _))
      }
      val expectedError = if (failOnDataLoss) testCase.strictError else None
      assert(error.isDefined == expectedError.isDefined)
      expectedError.foreach { case (errorClass, messageParts) =>
        assert(errorClass.isInstance(error.get))
        messageParts.foreach(part => assert(error.get.getMessage.contains(part)))
      }
    }
  }

  test("raw getCommitFiles reports the error from hasNext, after yielding earlier commits") {
    val commitRange = buildRawRange(
      deltaFileStatuses(Seq(5L, 6L, 8L)),
      5,
      Some(7),
      commitRangeValidation = false)
    val iterator = commitRange.getCommitFiles
    assert(iterator.hasNext)
    assert(iterator.next().getVersion == 5)
    assert(iterator.next().getVersion == 6)
    intercept[InvalidTableException](iterator.hasNext)
    assert(!iterator.hasNext)
  }

  test("raw getCommitFiles close stops iteration") {
    val commitRange = buildRawRange(
      deltaFileStatuses(Seq(5L, 6L, 7L)),
      5,
      Some(7),
      commitRangeValidation = false)
    val iterator = commitRange.getCommitFiles
    assert(iterator.next().getVersion == 5)
    iterator.close()
    assert(!iterator.hasNext)
  }

  test("raw range listing stops after the first commit past the requested end") {
    val files = deltaFileStatuses(0L to 1000L)
    var filesListed = 0
    val fileSystemClient = new BaseMockFileSystemClient {
      override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
        val listed = listFromProvider(files)(filePath).iterator.map { fs =>
          filesListed += 1
          fs
        }
        toCloseableIterator(listed.asJava)
      }

      override def resolvePath(path: String): String = path
    }
    val commitRange = TableManager
      .loadCommitRange(dataPath.toString, CommitBoundary.atVersion(2))
      .withEndBoundary(CommitBoundary.atVersion(5))
      .withCommitRangeValidation(false)
      .build(mockEngine(fileSystemClient = fileSystemClient))

    assert(commitRange.getEndVersion == 5)
    assert(drainCommitFiles(commitRange)._1.map(_.getVersion) == (2L to 5L))
    // Versions 2 to 5 plus the first commit past the end (version 6)
    assert(filesListed == 5)
  }

  test("isEmpty is false for a validated range") {
    val commitRange = buildRawRange(
      deltaFileStatuses(Seq(5L, 6L, 7L)),
      5,
      Some(7),
      commitRangeValidation = true)
    assert(!commitRange.isEmpty)
  }

  test("validated ranges still reject reversed, empty and incomplete ranges") {
    // Reversed
    val reversed = intercept[IllegalArgumentException] {
      buildRawRange(deltaFileStatuses(Seq(7L, 8L)), 7, Some(5), commitRangeValidation = true)
    }
    assert(reversed.getMessage.contains("startVersion must be <= endVersion"))
    // Empty listing
    val empty = intercept[KernelException] {
      buildRawRange(deltaFileStatuses(0L to 4L), 5, Some(7), commitRangeValidation = true)
    }
    assert(empty.getMessage.contains("no log files found"))
    // Missing start
    intercept[StartVersionNotFoundException] {
      buildRawRange(deltaFileStatuses(Seq(6L, 7L)), 5, Some(7), commitRangeValidation = true)
    }
    // Missing end, even though nothing later exists
    val missingEnd = intercept[KernelException] {
      buildRawRange(deltaFileStatuses(Seq(5L, 6L)), 5, Some(7), commitRangeValidation = true)
    }
    assert(missingEnd.getMessage.contains("endVersion=7"))
    // Interior gap
    val gap = intercept[InvalidTableException] {
      buildRawRange(deltaFileStatuses(Seq(5L, 7L)), 5, Some(7), commitRangeValidation = true)
    }
    assert(gap.getMessage.contains("versions are not contiguous"))
  }

  test("validated ranges keep the commit file and action-read contracts") {
    val files = deltaFileStatuses(Seq(5L, 6L, 7L, 8L))
    val commitRange = buildRawRange(files, 5, Some(7), commitRangeValidation = true)
    assert(commitRange.getEndVersion == 7)
    assert(drainCommitFiles(commitRange)._1.map(_.getVersion) == Seq(5L, 6L, 7L))
    assert(getDeltaFileVersions(commitRange) == Seq(5L, 6L, 7L))
  }

  test("relaxed ranges, including empty ones, reject action iteration") {
    val commitRange = buildRawRange(
      deltaFileStatuses(Seq(0L, 1L)),
      5,
      Some(7),
      commitRangeValidation = false)
    assert(commitRange.isEmpty)
    intercept[KernelException](commitRange.getActions(null, null, null))
    intercept[KernelException](commitRange.getCommitActions(null, null, null))
  }

  // Catalog-managed tables

  private def stagedLogData(versions: Seq[Long]): Seq[ParsedLogData] =
    versions.map(v => ParsedCatalogCommitData.forFileStatus(stagedCommitFile(v)))

  private case class CatalogRawRangeCase(
      description: String,
      publishedVersions: Seq[Long],
      stagedVersions: Seq[Long],
      start: Long,
      end: Long,
      strictVersions: Seq[Long],
      strictError: Option[(Class[_ <: KernelException], Seq[String])],
      lenientVersions: Seq[Long])

  private val catalogRawRangeCases = Seq(
    CatalogRawRangeCase(
      "published 5,6 and staged 7,8 merge; the later staged commit is not yielded",
      Seq(5L, 6L),
      Seq(7L, 8L),
      5,
      7,
      Seq(5L, 6L, 7L),
      None,
      Seq(5L, 6L, 7L)),
    CatalogRawRangeCase(
      "staged commits after a published prefix with a hole before them",
      Seq(5L),
      Seq(7L, 8L),
      5,
      8,
      Seq(5L),
      Some(gapErrorParts(6)),
      Seq(5L, 7L, 8L)),
    CatalogRawRangeCase(
      "staged commit past the end proves a missing end version",
      Seq(5L, 6L),
      Seq(8L, 9L),
      5,
      7,
      Seq(5L, 6L),
      Some(gapErrorParts(7)),
      Seq(5L, 6L)),
    CatalogRawRangeCase(
      "staged commit right after the end does not prove anything",
      Seq(5L, 6L, 7L),
      Seq(8L),
      5,
      7,
      Seq(5L, 6L, 7L),
      None,
      Seq(5L, 6L, 7L)),
    CatalogRawRangeCase(
      "only staged commits past the end prove a missing start",
      Seq.empty,
      Seq(8L, 9L),
      5,
      7,
      Seq.empty,
      Some(startErrorParts(5, 8)),
      Seq.empty),
    CatalogRawRangeCase(
      "reversed range with a staged commit after the start",
      Seq.empty,
      Seq(8L, 9L),
      7,
      5,
      Seq.empty,
      Some(startErrorParts(7, 8)),
      Seq.empty),
    CatalogRawRangeCase(
      "reversed range whose start is the first staged commit",
      Seq.empty,
      Seq(7L, 8L),
      7,
      5,
      Seq.empty,
      None,
      Seq.empty))

  for (testCase <- catalogRawRangeCases; failOnDataLoss <- Seq(true, false)) {
    test(s"raw getCommitFiles with catalog commits: ${testCase.description} - " +
      s"failOnDataLoss=$failOnDataLoss") {
      val commitRange = buildRawRange(
        deltaFileStatuses(testCase.publishedVersions),
        testCase.start,
        Some(testCase.end),
        commitRangeValidation = false,
        failOnDataLoss = failOnDataLoss,
        logData = stagedLogData(testCase.stagedVersions))
      assert(commitRange.getEndVersion == testCase.end)
      assert(commitRange.isEmpty == testCase.lenientVersions.isEmpty)

      val (yielded, error) = drainCommitFiles(commitRange)
      val expectedVersions =
        if (failOnDataLoss) testCase.strictVersions else testCase.lenientVersions
      assert(yielded.map(_.getVersion) == expectedVersions)
      val expectedError = if (failOnDataLoss) testCase.strictError else None
      assert(error.isDefined == expectedError.isDefined)
      expectedError.foreach { case (errorClass, messageParts) =>
        assert(errorClass.isInstance(error.get))
        messageParts.foreach(part => assert(error.get.getMessage.contains(part)))
      }
    }
  }

  test("raw getCommitFiles prefers a staged commit over a published one for the same version") {
    val commitRange = buildRawRange(
      deltaFileStatuses(Seq(5L, 6L, 7L)),
      5,
      Some(7),
      commitRangeValidation = false,
      logData = stagedLogData(Seq(6L, 7L, 8L)))
    val (yielded, error) = drainCommitFiles(commitRange)
    assert(error.isEmpty)
    assert(yielded.map(_.getVersion) == Seq(5L, 6L, 7L))
    assert(!FileNames.isStagedDeltaFile(yielded(0).getFileStatus.getPath))
    assert(FileNames.isStagedDeltaFile(yielded(1).getFileStatus.getPath))
    assert(FileNames.isStagedDeltaFile(yielded(2).getFileStatus.getPath))
  }

  test("raw getCommitFiles ignores published catalog commits removed by metadata cleanup") {
    // The catalog still reports published v5, but 5.json was removed. The listing is
    // authoritative for leading published commits, so v5 is a missing start.
    val logData = Seq[ParsedLogData](
      ParsedCatalogCommitData.forCatalogFileStatus(logPath.toString, 5, deltaFileStatus(5)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(6)),
      ParsedCatalogCommitData.forFileStatus(stagedCommitFile(7)))
    val commitRange = buildRawRange(
      Seq.empty[FileStatus],
      5,
      Some(7),
      commitRangeValidation = false,
      logData = logData)
    val (yielded, error) = drainCommitFiles(commitRange)
    assert(yielded.isEmpty)
    assert(error.exists(_.isInstanceOf[StartVersionNotFoundException]))
  }
}
