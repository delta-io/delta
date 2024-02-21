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

import scala.reflect.ClassTag

import org.scalatest.funsuite.AnyFunSuite

import io.delta.kernel.internal.snapshot.DeltaHistoryManager
import io.delta.kernel.utils.FileStatus
import io.delta.kernel.{MockFileSystemClientUtils, TableNotFoundException}

class DeltaHistoryManagerSuite extends AnyFunSuite with MockFileSystemClientUtils {

  def checkGetActiveCommitAtTimestamp(
    fileList: Seq[FileStatus],
    timestamp: Long,
    expectedVersion: Long): Unit = {
    val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
      createMockTableClient(listFromFileList(fileList)),
      logPath,
      timestamp
    )
    assert(activeCommit == expectedVersion,
      s"Expected version $expectedVersion but got $activeCommit for timestamp=$timestamp")
  }

  def checkGetActiveCommitAtTimestampError[T <: Throwable](
    fileList: Seq[FileStatus],
    timestamp: Long,
    expectedErrorMessageContains: String)(implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockTableClient(listFromFileList(fileList)),
        logPath,
        timestamp
      )
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
      -1,
      DeltaErrors.timestampEarlierThanTableFirstCommitException(dataPath.toString, -1, 0).getMessage
    )
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      21,
      DeltaErrors.timestampLaterThanTableLastCommit(dataPath.toString, 21, 20, 2).getMessage
    )
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
      -1,
      DeltaErrors.timestampEarlierThanTableFirstCommitException(dataPath.toString, -1, 0).getMessage
    )
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      21,
      DeltaErrors.timestampLaterThanTableLastCommit(dataPath.toString, 21, 20, 2).getMessage
    )
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
      8,
      DeltaErrors.timestampEarlierThanTableFirstCommitException(dataPath.toString, 8, 20).getMessage
    )
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      31,
      DeltaErrors.timestampLaterThanTableLastCommit(dataPath.toString, 31, 30, 3).getMessage
    )
  }

  test("getActiveCommitAtTimestamp: truncated delta log only checkpoint version") {
    val deltaFiles = deltaFileStatuses(Seq(2L)) ++ singularCheckpointFileStatuses(Seq(2L))
    // Valid queries
    checkGetActiveCommitAtTimestamp(deltaFiles, 20, 2)
    // Invalid queries
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      8,
      DeltaErrors.timestampEarlierThanTableFirstCommitException(dataPath.toString, 8, 20).getMessage
    )
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      21,
      DeltaErrors.timestampLaterThanTableLastCommit(dataPath.toString, 21, 20, 2).getMessage
    )
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
      8,
      DeltaErrors.timestampEarlierThanTableFirstCommitException(dataPath.toString, 8, 20).getMessage
    )
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFiles,
      31,
      DeltaErrors.timestampLaterThanTableLastCommit(dataPath.toString, 31, 30, 3).getMessage
    )
  }

  test("getActiveCommitAtTimestamp: throws table not found exception") {
    // Non-existent path
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockTableClient(p => throw new FileNotFoundException(p)),
        logPath,
        0
      )
    )
    // Empty _delta_log directory
    intercept[TableNotFoundException](
      DeltaHistoryManager.getActiveCommitAtTimestamp(
        createMockTableClient(p => Seq()),
        logPath,
        0
      )
    )
  }

  // TODO: corrects commit timestamps for increasing commits (monotonizeCommitTimestamps)?
  //  (see test "getCommits should monotonize timestamps" in DeltaTimeTravelSuite)?

  test("getActiveCommitAtTimestamp: corrupt listings") {
    // No checkpoint or 000.json present
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFileStatuses(Seq(1L, 2L, 3L)),
      25,
      "No recreatable commits found"
    )
    // Must have corresponding delta file for a checkpoint
    checkGetActiveCommitAtTimestampError[RuntimeException](
      singularCheckpointFileStatuses(Seq(1L)) ++ deltaFileStatuses(Seq(2L, 3L)),
      25,
      "No recreatable commits found"
    )
    // No commit files at all (only checkpoint files)
    checkGetActiveCommitAtTimestampError[RuntimeException](
      singularCheckpointFileStatuses(Seq(1L)),
      25,
      "No commits found"
    )
    // No delta files
    checkGetActiveCommitAtTimestampError[RuntimeException](
      Seq("foo", "notdelta.parquet", "foo.json", "001.checkpoint.00foo0.parquet")
        .map(FileStatus.of(_, 10, 10)),
      25,
      "No delta files found in the directory"
    )
    // No complete checkpoint
    checkGetActiveCommitAtTimestampError[RuntimeException](
      deltaFileStatuses(Seq(2L, 3L)) ++ multiCheckpointFileStatuses(Seq(2L), 3).take(2),
      25,
      "No recreatable commits found"
    )
  }
}
