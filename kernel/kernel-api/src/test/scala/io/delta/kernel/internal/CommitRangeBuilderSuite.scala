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
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.commitrange.{CommitRangeBuilderImpl, CommitRangeImpl}
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{MockFileSystemClientUtils, MockListFromFileSystemClient, MockSnapshotUtils}
import io.delta.kernel.test.MockSnapshotUtils.getMockSnapshot
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class CommitRangeBuilderSuite extends AnyFunSuite with MockFileSystemClientUtils {

  private def checkQueryBoundaries(
      commitRange: CommitRange,
      startVersion: Option[Long],
      endVersion: Option[Long],
      startTimestamp: Option[Long],
      endTimestamp: Option[Long]): Unit = {
    def assertBoundaryVersion(boundary: Optional[CommitBoundary], version: Long) = {
      assert(boundary.isPresent && boundary.get.isVersion && boundary.get.getVersion == version)
    }
    def assertBoundaryTimestamp(boundary: Optional[CommitBoundary], timestamp: Long) = {
      assert(
        boundary.isPresent && boundary.get.isTimestamp && boundary.get.getTimestamp == timestamp)
    }
    if (startVersion.nonEmpty) {
      assertBoundaryVersion(commitRange.getQueryStartBoundary, startVersion.get)
    } else if (startTimestamp.nonEmpty) {
      assertBoundaryTimestamp(commitRange.getQueryStartBoundary, startTimestamp.get)
    } else {
      assert(!commitRange.getQueryStartBoundary.isPresent)
    }
    if (endVersion.nonEmpty) {
      assertBoundaryVersion(commitRange.getQueryEndBoundary, endVersion.get)
    } else if (endTimestamp.nonEmpty) {
      assertBoundaryTimestamp(commitRange.getQueryEndBoundary, endTimestamp.get)
    } else {
      assert(!commitRange.getQueryEndBoundary.isPresent)
    }
  }

  private def buildCommitRange(
      fileList: Seq[FileStatus],
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None,
      startTimestamp: Option[Long] = None,
      endTimestamp: Option[Long] = None): CommitRange = {
    val latestVersion = fileList.map(fs => FileNames.getFileVersion(new Path(fs.getPath))).max

    var commitRangeBuilder = TableManager.loadCommitRange(dataPath.toString)
    startVersion.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withStartBoundary(CommitBoundary.atVersion(v))
    }
    endVersion.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withEndBoundary(CommitBoundary.atVersion(v))
    }
    startTimestamp.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withStartBoundary(
        CommitBoundary.atTimestamp(v, getMockSnapshot(dataPath, latestVersion)))
    }
    endTimestamp.foreach { v =>
      commitRangeBuilder = commitRangeBuilder.withEndBoundary(
        CommitBoundary.atTimestamp(v, getMockSnapshot(dataPath, latestVersion)))
    }
    val mockedEngine = mockEngine(fileSystemClient =
      new MockListFromFileSystemClient(listFromProvider(fileList)))
    commitRangeBuilder.build(mockedEngine)
  }

  private def checkCommitRange(
      fileList: Seq[FileStatus],
      expectedStartVersion: Long,
      expectedEndVersion: Long,
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None,
      startTimestamp: Option[Long] = None,
      endTimestamp: Option[Long] = None): Unit = {
    val commitRange = buildCommitRange(
      fileList,
      startVersion,
      endVersion,
      startTimestamp,
      endTimestamp)
    assert(commitRange.getStartVersion == expectedStartVersion)
    assert(commitRange.getEndVersion == expectedEndVersion)
    checkQueryBoundaries(commitRange, startVersion, endVersion, startTimestamp, endTimestamp)
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
   * Version-based boundary definition.
   * @param versionValue the version to use as boundary
   * @param expectsError whether we expect this def to inherently fail
   */
  private case class VersionBoundaryDef(
      versionValue: Long,
      expectsError: Boolean = false) extends BoundaryDef(versionValue, expectsError) {

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
      expectsError: Boolean = false) extends BoundaryDef(resolvedVersion, expectsError) {

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
      startBoundary: BoundaryDef,
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
        classOf[IllegalArgumentException],
        s"Resolved startVersion=${startBoundary.expectedVersion} > " +
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
      startBoundaries: Seq[BoundaryDef],
      endBoundaries: Seq[BoundaryDef]): Unit = {
    startBoundaries.foreach { startBound =>
      endBoundaries.foreach { endBound =>
        test(s"$description: build CommitRange with startBound=$startBound endBound=$endBound") {
          val expectedException = getExpectedException(startBound, endBound)
          if (expectedException.isDefined) {
            val e = intercept[Throwable] {
              buildCommitRange(
                fileList = fileStatuses,
                startVersion = startBound.version,
                endVersion = endBound.version,
                startTimestamp = startBound.timestamp,
                endTimestamp = endBound.timestamp)
            }
            assert(expectedException.get._1.isInstance(e))
            assert(e.getMessage.contains(expectedException.get._2))
          } else {
            checkCommitRange(
              fileList = fileStatuses,
              expectedStartVersion = startBound.expectedVersion,
              expectedEndVersion = endBound.expectedVersion,
              startVersion = startBound.version,
              endVersion = endBound.version,
              startTimestamp = startBound.timestamp,
              endTimestamp = endBound.timestamp)
          }
        }
      }
    }
  }

  // The below test cases mimic the cases in TableImplSuite for the timestamp-resolution
  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(0, 1)", // (version -> timestamp) = v0 -> 0, v1 -> 10
    fileStatuses = deltaFileStatuses(Seq(0L, 1L)),
    startBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
      TimestampBoundaryDef(0, resolvedVersion = 0L), // at v0
      TimestampBoundaryDef(5, resolvedVersion = 1), // between v0, v1
      TimestampBoundaryDef(10, resolvedVersion = 1), // at v1
      DefaultBoundaryDef(resolvedVersion = 0), // default to 0
      TimestampBoundaryDef(11, resolvedVersion = -1, expectsError = true), // after v1
      VersionBoundaryDef(2L, expectsError = true) // version DNE
    ),
    endBoundaries = Seq(
      VersionBoundaryDef(0L),
      VersionBoundaryDef(1L),
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
      DefaultBoundaryDef(resolvedVersion = 0, expectsError = true), // default to 0
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
      DefaultBoundaryDef(resolvedVersion = 0, expectsError = true), // default to 0
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

  // We don't support CCV2 tables yet so we don't support providing parsedLogData
  test("build - throws UnsupportedOperationException when logData is provided") {
    val builder = new CommitRangeBuilderImpl("/path/to/table")
    val mockLogData = Collections.singletonList(null.asInstanceOf[ParsedLogData])
    builder.withLogData(mockLogData)

    assertThrows[UnsupportedOperationException] {
      builder.build(mockEngine())
    }
  }
}
