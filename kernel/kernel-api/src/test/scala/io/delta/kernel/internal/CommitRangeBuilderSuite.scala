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
    commitRangeBuilder.build(mockEngine(fileSystemClient =
      new MockListFromFileSystemClient(listFromProvider(fileList))))
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

  private case class BoundaryDef(
      version: Option[Long] = None,
      timestamp: Option[Long] = None,
      expectedVersion: Long = -1,
      expectError: Boolean = false)

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
    description = "deltaFiles=(0, 1)",
    fileStatuses = deltaFileStatuses(Seq(0L, 1L)),
    startBoundaries = Seq(
      BoundaryDef(version = Some(0L), expectedVersion = 0L),
      BoundaryDef(version = Some(1L), expectedVersion = 1L),
      BoundaryDef(timestamp = Some(0), expectedVersion = 0L), // at v0
      BoundaryDef(timestamp = Some(5), expectedVersion = 1), // btw v0, v1
      BoundaryDef(timestamp = Some(10), expectedVersion = 1), // at v1
      BoundaryDef(expectedVersion = 0), // default to 0
      BoundaryDef(timestamp = Some(11), expectError = true), // after v1
      BoundaryDef(version = Some(2L), expectedVersion = 2L, expectError = true) // version DNE
    ),
    endBoundaries = Seq(
      BoundaryDef(version = Some(0L), expectedVersion = 0L),
      BoundaryDef(version = Some(1L), expectedVersion = 1L),
      BoundaryDef(timestamp = Some(0), expectedVersion = 0L), // at v0
      BoundaryDef(timestamp = Some(5), expectedVersion = 0), // btw v0, v1
      BoundaryDef(timestamp = Some(10), expectedVersion = 1), // at v1
      BoundaryDef(timestamp = Some(11), expectedVersion = 1), // after v1
      BoundaryDef(expectedVersion = 1), // default to latest
      BoundaryDef(version = Some(2L), expectedVersion = 2L, expectError = true) // version DNE
    ))

  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(10, 11, 12)",
    fileStatuses = deltaFileStatuses(Seq(10L, 11L, 12L)) ++
      singularCheckpointFileStatuses(Seq(10L)),
    startBoundaries = Seq(
      BoundaryDef(version = Some(10L), expectedVersion = 10L),
      BoundaryDef(version = Some(11L), expectedVersion = 11L),
      BoundaryDef(version = Some(12L), expectedVersion = 12L),
      BoundaryDef(timestamp = Some(99), expectedVersion = 10), // before v10
      BoundaryDef(timestamp = Some(100), expectedVersion = 10L), // at v10
      BoundaryDef(timestamp = Some(105), expectedVersion = 11L), // btw v10, v11
      BoundaryDef(timestamp = Some(110), expectedVersion = 11L), // at v11
      BoundaryDef(timestamp = Some(115), expectedVersion = 12L), // btw v11, v12
      BoundaryDef(timestamp = Some(120), expectedVersion = 12L), // at v12
      BoundaryDef(timestamp = Some(125), expectError = true), // after v12
      BoundaryDef(expectedVersion = 0, expectError = true), // default to 0
      BoundaryDef(version = Some(9L), expectedVersion = 9L, expectError = true), // version DNE
      BoundaryDef(version = Some(13L), expectedVersion = 13L, expectError = true) // version DNE
    ),
    endBoundaries = Seq(
      BoundaryDef(version = Some(10L), expectedVersion = 10L),
      BoundaryDef(version = Some(11L), expectedVersion = 11L),
      BoundaryDef(version = Some(12L), expectedVersion = 12L),
      BoundaryDef(timestamp = Some(100), expectedVersion = 10), // at v10
      BoundaryDef(timestamp = Some(105), expectedVersion = 10), // btw v10, v11
      BoundaryDef(timestamp = Some(110), expectedVersion = 11), // at v11
      BoundaryDef(timestamp = Some(115), expectedVersion = 11), // btw v11, v12
      BoundaryDef(timestamp = Some(120), expectedVersion = 12), // at v12
      BoundaryDef(timestamp = Some(125), expectedVersion = 12), // after v12
      BoundaryDef(expectedVersion = 12), // default to latest
      BoundaryDef(timestamp = Some(99), expectError = true), // before V10
      BoundaryDef(version = Some(9L), expectedVersion = 9L, expectError = true), // version DNE
      BoundaryDef(version = Some(13L), expectedVersion = 13L, expectError = true) // version DNE
    ))

  // Check case with only 1 delta file
  testStartAndEndBoundaryCombinations(
    description = "deltaFiles=(10)",
    fileStatuses = deltaFileStatuses(Seq(10L)) ++ singularCheckpointFileStatuses(Seq(10L)),
    startBoundaries = Seq(
      BoundaryDef(version = Some(10L), expectedVersion = 10L),
      BoundaryDef(timestamp = Some(99L), expectedVersion = 10L), // before v10
      BoundaryDef(timestamp = Some(100L), expectedVersion = 10L), // at v10
      BoundaryDef(expectedVersion = 0, expectError = true), // default to 0
      BoundaryDef(timestamp = Some(101L), expectError = true), // after v10
      BoundaryDef(version = Some(1L), expectedVersion = 1L, expectError = true) // version DNE
    ),
    endBoundaries = Seq(
      BoundaryDef(version = Some(10L), expectedVersion = 10L),
      BoundaryDef(timestamp = Some(100L), expectedVersion = 10L), // at v10
      BoundaryDef(timestamp = Some(101L), expectedVersion = 10L), // after v10
      BoundaryDef(expectedVersion = 10L), // default to latest
      BoundaryDef(timestamp = Some(99L), expectError = true), // before v10
      BoundaryDef(version = Some(1L), expectedVersion = 1L, expectError = true) // version DNE
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
