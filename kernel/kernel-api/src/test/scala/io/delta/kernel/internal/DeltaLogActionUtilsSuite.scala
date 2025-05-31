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
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.exceptions.{InvalidTableException, KernelException, TableNotFoundException}
import io.delta.kernel.internal.DeltaLogActionUtils.{getCommitFilesForVersionRange, listDeltaLogFilesAsIter, verifyDeltaVersions}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class DeltaLogActionUtilsSuite extends AnyFunSuite with MockFileSystemClientUtils {

  ///////////////////////////////
  // verifyDeltaVersions tests //
  ///////////////////////////////

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
      dataPath)
    // Only one version provided
    verifyDeltaVersions(
      getCommitFiles(Seq(1)),
      1,
      1,
      dataPath)
    // Non-contiguous versions
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 3, 4)),
        1,
        4,
        dataPath)
    }
    // End-version or start-version not right
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 2, 3)),
        0,
        3,
        dataPath)
    }
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 2, 3)),
        1,
        4,
        dataPath)
    }
    // Empty versions
    intercept[KernelException] {
      verifyDeltaVersions(
        getCommitFiles(Seq()),
        1,
        4,
        dataPath)
    }
    // Unsorted or duplicates (shouldn't be possible)
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 1, 2)),
        1,
        4,
        dataPath)
    }
    intercept[InvalidTableException] {
      verifyDeltaVersions(
        getCommitFiles(Seq(1, 4, 3, 2)),
        1,
        2,
        dataPath)
    }
  }

  /////////////////////////////////////////
  // getCommitFilesForVersionRange tests //
  /////////////////////////////////////////

  test("getCommitFilesForVersionRange: directory does not exist") {
    intercept[TableNotFoundException] {
      getCommitFilesForVersionRange(
        createMockFSListFromEngine(_ => throw new FileNotFoundException()),
        dataPath,
        0,
        1)
    }
  }

  def testGetCommitFilesExpectedError[T <: Throwable](
      testName: String,
      files: Seq[FileStatus],
      startVersion: Long = 1,
      endVersion: Long = 3,
      expectedErrorMessageContains: String)(implicit classTag: ClassTag[T]): Unit = {
    test("getCommitFilesForVersionRange: " + testName) {
      val e = intercept[T] {
        getCommitFilesForVersionRange(
          createMockFSListFromEngine(files),
          dataPath,
          startVersion,
          endVersion)
      }
      assert(e.getMessage.contains(expectedErrorMessageContains))
    }
  }

  testGetCommitFilesExpectedError[KernelException](
    testName = "empty directory",
    files = Seq(),
    expectedErrorMessageContains = "no log files found in the requested version range")

  testGetCommitFilesExpectedError[KernelException](
    testName = "all versions less than startVersion",
    files = deltaFileStatuses(Seq(0)),
    expectedErrorMessageContains = "no log files found in the requested version range")

  testGetCommitFilesExpectedError[KernelException](
    testName = "all versions greater than endVersion",
    files = deltaFileStatuses(Seq(4, 5, 6)),
    expectedErrorMessageContains = "no log files found in the requested version range")

  testGetCommitFilesExpectedError[InvalidTableException](
    testName = "missing log files",
    files = deltaFileStatuses(Seq(1, 3)),
    expectedErrorMessageContains = "versions are not contiguous")

  testGetCommitFilesExpectedError[KernelException](
    testName = "start version not available",
    files = deltaFileStatuses(Seq(2, 3, 4, 5)),
    expectedErrorMessageContains = "no log file found for version 1")

  testGetCommitFilesExpectedError[KernelException](
    testName = "end version not available",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    expectedErrorMessageContains = "no log file found for version 3")

  testGetCommitFilesExpectedError[KernelException](
    testName = "invalid start version",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    startVersion = -1,
    expectedErrorMessageContains = "Invalid version range")

  testGetCommitFilesExpectedError[KernelException](
    testName = "invalid end version",
    files = deltaFileStatuses(Seq(0, 1, 2)),
    startVersion = 3,
    endVersion = 2,
    expectedErrorMessageContains = "Invalid version range")

  def testGetCommitFiles(
      testName: String,
      files: Seq[FileStatus],
      startVersion: Long = 1,
      endVersion: Long = 3,
      expectedCommitFiles: Seq[FileStatus]): Unit = {
    test("getCommitFilesForVersionRange: " + testName) {
      assert(
        getCommitFilesForVersionRange(
          createMockFSListFromEngine(files),
          dataPath,
          startVersion,
          endVersion).asScala sameElements expectedCommitFiles)
    }
  }

  testGetCommitFiles(
    testName = "basic case",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3)))

  testGetCommitFiles(
    testName = "basic case with checkpoint file",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++ singularCheckpointFileStatuses(Seq(2)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3)))

  testGetCommitFiles(
    testName = "basic case with non-log files",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++
      deltaFileStatuses(Seq(2))
        .map(fs => FileStatus.of(fs.getPath + ".crc", fs.getSize, fs.getModificationTime)),
    expectedCommitFiles = deltaFileStatuses(Seq(1, 2, 3)))

  testGetCommitFiles(
    testName = "version range size 1",
    files = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)),
    startVersion = 0,
    endVersion = 0,
    expectedCommitFiles = deltaFileStatuses(Seq(0)))

  /////////////////////////////
  // listDeltaLogFiles tests //
  /////////////////////////////

  private val checkpointsAndDeltas = singularCheckpointFileStatuses(Seq(10)) ++
    deltaFileStatuses(Seq(10, 11, 12, 13, 14)) ++
    Seq(FileStatus.of(s"$logPath/00000000000000000014.crc", 0, 0)) ++
    multiCheckpointFileStatuses(Seq(14), 2) ++
    deltaFileStatuses(Seq(15, 16, 17)) ++
    v2CheckpointFileStatuses(Seq((17, false, 2)), "json").map(_._1)

  private def extractVersions(files: Seq[FileStatus]): Seq[Long] = {
    files.map(fs => FileNames.getFileVersion(new Path(fs.getPath)))
  }

  test("listDeltaLogFiles: no fileTypes provided") {
    intercept[IllegalArgumentException] {
      listDeltaLogFilesAsIter(
        createMockFSListFromEngine(deltaFileStatuses(Seq(1, 2, 3))),
        Collections.emptySet(), // No fileTypes provided!
        dataPath,
        1,
        Optional.empty(),
        false /* mustBeRecreatable */
      ).toInMemoryList
    }
  }

  test("listDeltaLogFiles: returns requested file type only") {
    val commitFiles = listDeltaLogFilesAsIter(
      createMockFSListFromEngine(checkpointsAndDeltas),
      Set(FileNames.DeltaLogFileType.COMMIT).asJava,
      dataPath,
      10,
      Optional.empty(),
      false /* mustBeRecreatable */
    ).toInMemoryList.asScala

    assert(commitFiles.forall(fs => FileNames.isCommitFile(fs.getPath)))
    assert(extractVersions(commitFiles.toSeq) == Seq(10, 11, 12, 13, 14, 15, 16, 17))

    val checkpointFiles = listDeltaLogFilesAsIter(
      createMockFSListFromEngine(checkpointsAndDeltas),
      Set(FileNames.DeltaLogFileType.CHECKPOINT).asJava,
      dataPath,
      10,
      Optional.empty(),
      false /* mustBeRecreatable */
    ).toInMemoryList.asScala

    assert(checkpointFiles.forall(fs => FileNames.isCheckpointFile(fs.getPath)))
    assert(extractVersions(checkpointFiles.toSeq) == Seq(10, 14, 14, 17))
  }

  test("listDeltaLogFiles: mustBeRecreatable") {
    val exMsg = intercept[KernelException] {
      listDeltaLogFilesAsIter(
        createMockFSListFromEngine(checkpointsAndDeltas),
        Set(FileNames.DeltaLogFileType.COMMIT, FileNames.DeltaLogFileType.CHECKPOINT).asJava,
        dataPath,
        0,
        Optional.of(4),
        true /* mustBeRecreatable */
      ).toInMemoryList
    }.getMessage
    assert(exMsg.contains("Cannot load table version 4 as the transaction log has been " +
      "truncated due to manual deletion or the log/checkpoint retention policy. The earliest " +
      "available version is 10"))
  }

  def testListWithCompactions(
      testName: String,
      files: Seq[FileStatus],
      startVersion: Long,
      endVersion: Optional[java.lang.Long],
      expectedListedFiles: Seq[FileStatus]): Unit = {
    test("testListWithCompactions: " + testName) {
      val listed = listDeltaLogFilesAsIter(
        createMockFSListFromEngine(files),
        Set(
          FileNames.DeltaLogFileType.COMMIT,
          FileNames.DeltaLogFileType.CHECKPOINT,
          FileNames.DeltaLogFileType.LOG_COMPACTION).asJava,
        dataPath,
        startVersion,
        endVersion,
        false /* mustBeRecreatable */ ).toInMemoryList.asScala
      assert(listed sameElements expectedListedFiles)
    }
  }

  testListWithCompactions(
    "compaction at start, no endVersion",
    files = deltaFileStatuses(0L to 4L) ++ compactedFileStatuses(Seq((0, 4))),
    startVersion = 0,
    endVersion = Optional.empty(),
    expectedListedFiles = compactedFileStatuses(Seq((0, 4))) ++ deltaFileStatuses(0L to 4L))

  testListWithCompactions(
    "compaction at end, no endVersion",
    files = deltaFileStatuses(0L to 4L) ++ compactedFileStatuses(Seq((3, 4))),
    startVersion = 0,
    endVersion = Optional.empty(),
    expectedListedFiles = deltaFileStatuses(0L to 2L) ++
      compactedFileStatuses(Seq((3, 4))) ++
      deltaFileStatuses(3L to 4L))

  testListWithCompactions(
    "compaction at end, with endVersion",
    files = deltaFileStatuses(0L to 4L) ++ compactedFileStatuses(Seq((3, 4))),
    startVersion = 0,
    endVersion = Optional.of(4),
    expectedListedFiles = deltaFileStatuses(0L to 2L) ++
      compactedFileStatuses(Seq((3, 4))) ++
      deltaFileStatuses(3L to 4L))

  testListWithCompactions(
    "compaction in middle, no endVersion",
    files = deltaFileStatuses(0L to 4L) ++ compactedFileStatuses(Seq((2, 4))),
    startVersion = 0,
    endVersion = Optional.empty(),
    expectedListedFiles = deltaFileStatuses(0L to 1L) ++
      compactedFileStatuses(Seq((2, 4))) ++
      deltaFileStatuses(2L to 4L))

  testListWithCompactions(
    "compaction over end, with endVersion",
    files = deltaFileStatuses(0L to 6L) ++ compactedFileStatuses(Seq((3, 7))),
    startVersion = 0,
    endVersion = Optional.of(5),
    expectedListedFiles = deltaFileStatuses(0L to 5L))

  testListWithCompactions(
    "compaction before start, no endVersion",
    files = deltaFileStatuses(0L to 6L) ++ compactedFileStatuses(Seq((2, 4))),
    startVersion = 3,
    endVersion = Optional.empty(),
    expectedListedFiles = deltaFileStatuses(3L to 6L))

  testListWithCompactions(
    "compaction before start, with endVersion",
    files = deltaFileStatuses(0L to 6L) ++ compactedFileStatuses(Seq((2, 4))),
    startVersion = 3,
    endVersion = Optional.of(5),
    expectedListedFiles = deltaFileStatuses(3L to 5L))

  testListWithCompactions(
    "multiple compactions, no endVersion",
    files = deltaFileStatuses(0L to 7L) ++ compactedFileStatuses(Seq((2, 4), (5, 7))),
    startVersion = 0,
    endVersion = Optional.empty(),
    expectedListedFiles = deltaFileStatuses(0L to 1L) ++
      compactedFileStatuses(Seq((2, 4))) ++
      deltaFileStatuses(2L to 4L) ++
      compactedFileStatuses(Seq((5, 7))) ++
      deltaFileStatuses(5L to 7L))

  testListWithCompactions(
    "multiple compactions, with endVersion, don't return second compaction",
    files = deltaFileStatuses(0L to 7L) ++ compactedFileStatuses(Seq((2, 4), (5, 7))),
    startVersion = 0,
    endVersion = Optional.of(6),
    expectedListedFiles = deltaFileStatuses(0L to 1L) ++
      compactedFileStatuses(Seq((2, 4))) ++
      deltaFileStatuses(2L to 4L) ++
      deltaFileStatuses(5L to 6L))

  testListWithCompactions(
    "multiple compactions, no endVersion, start after first compaction",
    files = deltaFileStatuses(0L to 7L) ++ compactedFileStatuses(Seq((2, 4), (5, 7))),
    startVersion = 3,
    endVersion = Optional.empty(),
    expectedListedFiles = deltaFileStatuses(3L to 4L) ++
      compactedFileStatuses(Seq((5, 7))) ++
      deltaFileStatuses(5L to 7L))

  testListWithCompactions(
    "multiple compactions, with endVersion, return no compactions",
    files = deltaFileStatuses(0L to 7L) ++ compactedFileStatuses(Seq((2, 4), (5, 7))),
    startVersion = 3,
    endVersion = Optional.of(6),
    expectedListedFiles = deltaFileStatuses(3L to 6L))
}
