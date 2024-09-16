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
package io.delta.kernel.internal.snapshot

import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.snapshot.MetadataCleanup.cleanupExpiredLogs
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.test.{MockEngineUtils, MockFileSystemClientUtils, MockListFromDeleteFileSystemClient}
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class MetadataCleanupSuite extends AnyFunSuite with MockFileSystemClientUtils with MockEngineUtils {

  private val multiPartCheckpointPartsSize = 4;

  /** Case class containing the list of expected files in the deleted file list */
  case class DeletedFileList(
    deltaVersions: Seq[Long] = Seq.empty,
    classicCheckpointVersions: Seq[Long] = Seq.empty,
    multipartCheckpointVersions: Seq[Long] = Seq.empty,
    v2CheckpointVersions: Seq[(Long, Boolean, Int)] = Seq.empty) {

    def fileList(): Seq[String] = {
      (deltaFileStatuses(deltaVersions) ++
        singularCheckpointFileStatuses(classicCheckpointVersions) ++
        multiCheckpointFileStatuses(multipartCheckpointVersions, multiPartCheckpointPartsSize) ++
        v2CheckpointFileStatuses(v2CheckpointVersions, "parquet").map(_._1))
        .sortBy(_.getPath)
        .map(_.getPath)
    }
  }

  Seq(
    (
      // _deltalog directory contents - contains only delta files
      deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6)),
      Seq(
        (
          "no files should be deleted even some of them are expired",
          DeletedFileList(), // expected deleted files - none of them should be deleted
          70, // current time
          30 // retention period
        ),
        (
          "no files should be deleted as none of them are expired",
          DeletedFileList(), // expected deleted files - none of them should be deleted
          200, // current time
          200 // retention period
        )
      )
    ),
    (
      // _deltalog directory contains a combination of delta files and classic checkpoint files
      deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8)) ++
        singularCheckpointFileStatuses(Seq(3, 6)),
      Seq(
        (
          "classic checkpoint: delete expired delta files up to the checkpoint version," +
            "not all expired delta files are deleted",
          DeletedFileList(deltaVersions = Seq(0, 1, 2)),
          90, // current time
          40 // retention period
        ),
        (
          "classic checkpoint: expired delta files + expired checkpoint files should be deleted",
          DeletedFileList(
            deltaVersions = Seq(0, 1, 2, 3, 4, 5),
            classicCheckpointVersions = Seq(3)),
          100, // current time
          30 // retention period
        ),
        (
          "classic checkpoint: no delta/checkpoint files should be deleted as none expired",
          DeletedFileList(),
          200, // current time
          200 // retention period
        )
      )
    ),
    (
      // _deltalog directory contains a combination of delta files and multi-part checkpoint files
      deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8)) ++
        multiCheckpointFileStatuses(Seq(3, 6), numParts = multiPartCheckpointPartsSize),
      Seq(
        (
          "multi-part checkpoint: delete expired delta files up to the checkpoint version," +
            "not all expired delta files are deleted",
          DeletedFileList(deltaVersions = Seq(0, 1, 2)),
          90, // current time
          40 // retention period
        ),
        (
          "multi-part checkpoint: expired delta files + expired checkpoint files should be deleted",
          DeletedFileList(
            deltaVersions = Seq(0, 1, 2, 3, 4, 5),
            multipartCheckpointVersions = Seq(3)),
          100, // current time
          30 // retention period
        ),
        (
          "multi-part checkpoint: no delta/checkpoint files should be deleted as none expired",
          DeletedFileList(),
          200, // current time
          200 // retention period
        )
      )
    )
  ).foreach {
    case (logFiles, testCases) =>
      testCases.foreach {
        case (testName, expectedDeletedFiles, currentTime, retentionPeriod) =>
          test(s"metadataCleanup: $testName: $currentTime, $retentionPeriod") {
            val fsClient = mockFsClient(logFiles)

            cleanupExpiredLogs(
              mockEngine(fsClient),
              new ManualClock(currentTime),
              logPath,
              retentionPeriod
            )

            assert(fsClient.getDeleteCalls === expectedDeletedFiles.fileList())
          }
      }
  }

  def mockFsClient(logFiles: Seq[FileStatus]): MockListFromDeleteFileSystemClient = {
    new MockListFromDeleteFileSystemClient(logFiles)
  }

  def mockEngine(fsClient: MockListFromDeleteFileSystemClient): Engine = {
    mockEngine(fileSystemClient = fsClient)
  }
}
