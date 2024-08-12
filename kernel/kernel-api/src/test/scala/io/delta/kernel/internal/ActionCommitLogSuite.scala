/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.exceptions.{InvalidTableException, KernelException, TableNotFoundException}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.internal.ActionCommitLog.{getCommitFilesForVersionRange, verifyDeltaVersions}
import io.delta.kernel.test.MockFileSystemClientUtils

class ActionCommitLogSuite extends AnyFunSuite with MockFileSystemClientUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // verifyDeltaVersions tests
  //////////////////////////////////////////////////////////////////////////////////

  def getCommitFiles(versions: Seq[Long]): java.util.List[FileStatus] = {
    versions
      .map(v => FileStatus.of(FileNames.deltaFile(logPath, v), 0, 0))
      .asJava
  }

  test("verifyDeltaVersions") {
    // Basic correct use case
    verifyDeltaVersions(
      getCommitFiles(Seq(1, 2, 3)),
      1,
      3,
      dataPath
    )
    // Only one version provided
    verifyDeltaVersions(
      getCommitFiles(Seq(1)),
      1,
      1,
      dataPath
    )
    // Non-continuous versions
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 3, 4)),
        1,
        4,
        dataPath
      )
    }
    // End-version or start-version not right
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 2, 3)),
        0,
        3,
        dataPath
      )
    }
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 2, 3)),
        1,
        4,
        dataPath
      )
    }
    // Empty versions
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq()),
        1,
        4,
        dataPath
      )
    }
    // Unsorted or duplicates (shouldn't be possible)
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 1, 2)),
        1,
        4,
        dataPath
      )
    }
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 4, 3, 2)),
        1,
        4,
        dataPath
      )
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // getCommitFilesForVersionRange tests
  //////////////////////////////////////////////////////////////////////////////////

  test("getCommitFilesForVersionRange: directory does not exist") {
    intercept[TableNotFoundException] {
      getCommitFilesForVersionRange(
        createMockFSListFromEngine(_ => throw new FileNotFoundException()),
        dataPath,
        0,
        1
      )
    }
  }

  def testGetCommitFilesExpectedError[T <: Throwable](
    testName: String,
    files: Seq[FileStatus],
    startVersion: Long = 1,
    endVersion: Long = 3,
    expectedErrorMessageContains: String,
  )(implicit classTag: ClassTag[T]): Unit = {
    test("getCommitFilesForVersionRange: " + testName) {
      val e = intercept[T] {
        getCommitFilesForVersionRange(
          createMockFSListFromEngine(files),
          dataPath,
          startVersion,
          endVersion
        )
      }
      assert(e.getMessage.contains(expectedErrorMessageContains))
    }
  }

  testGetCommitFilesExpectedError[KernelException](
    testName = "empty directory",
    files = Seq(),
    expectedErrorMessageContains = "no log files found in the requested version range"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "all versions less than startVersion",
    files = deltaFileStatuses(Seq(0)),
    expectedErrorMessageContains = "no log files found in the requested version range"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "all versions greater than endVersion",
    files = deltaFileStatuses(Seq(4, 5, 6)),
    expectedErrorMessageContains = "no log files found in the requested version range"
  )

  testGetCommitFilesExpectedError[InvalidTableException](
    testName = "missing log files",
    files = deltaFileStatuses(Seq(1, 3)),
    expectedErrorMessageContains = "versions are not continuous"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "start version not available",
    files = deltaFileStatuses(Seq(2, 3, 4, 5)),
    expectedErrorMessageContains = "no log file found for version 1"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "end version not available",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    expectedErrorMessageContains = "no log file found for version 3"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "invalid start version",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    startVersion = -1,
    expectedErrorMessageContains = "Invalid version range"
  )

  testGetCommitFilesExpectedError[KernelException](
    testName = "invalid end version",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    startVersion = 3,
    endVersion = 2,
    expectedErrorMessageContains = "Invalid version range"
  )

  def testGetCommitFiles(
    testName: String,
    files: Seq[FileStatus],
    startVersion: Long = 1,
    endVersion: Long = 3,
    expectedCommitFiles: Seq[FileStatus]
  ): Unit = {
    test("getCommitFilesForVersionRange: " + testName) {
      assert(
        getCommitFilesForVersionRange(
          createMockFSListFromEngine(files),
          dataPath,
          startVersion,
          endVersion
        ).asScala sameElements expectedCommitFiles
      )
    }
  }

  testGetCommitFiles(
    testName = "basic case",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3))
  )

  testGetCommitFiles(
    testName = "basic case with checkpoint file",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++ singularCheckpointFileStatuses(Seq(2)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3))
  )

  testGetCommitFiles(
    testName = "basic case with non-log files",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++
      deltaFileStatuses(Seq(2))
        .map(fs => FileStatus.of(fs.getPath + ".crc", fs.getSize, fs.getModificationTime)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3))
  )

  testGetCommitFiles(
    testName = "version range size 1",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)),
    startVersion = 0,
    endVersion = 0,
    expectedCommitFiles = deltaFileStatuses(Seq(0))
  )
}
