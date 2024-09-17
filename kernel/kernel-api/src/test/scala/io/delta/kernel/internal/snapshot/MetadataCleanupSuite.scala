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

import io.delta.kernel.internal.snapshot.MetadataCleanup.cleanupExpiredLogs
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.test.{MockFileSystemClientUtils, MockListFromDeleteFileSystemClient}
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class MetadataCleanupSuite extends AnyFunSuite with MockFileSystemClientUtils {

  import MetadataCleanupSuite._

  /* ------------------- TESTS ------------------ */

  // Simple case where the Delta log directory contains only delta files and no checkpoint files
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
    ),
    (
      "no files should be deleted as none of them are expired",
      DeletedFileList(), // expected deleted files - none of them should be deleted
      200, // current time
      0 // retention period
    )
  ).foreach {
    case (testName, expectedDeletedFiles, currentTime, retentionPeriod) =>
      // _deltalog directory contents - contains only delta files
      val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6))
      testCleanup(testName, logFiles, expectedDeletedFiles, currentTime, retentionPeriod)
  }

  // with various checkpoint types
  Seq("classic", "multi-part", "v2", "hybrid").foreach { checkpointType =>
    // _deltalog directory contains a combination of delta files and checkpoint files

    val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) ++
      (checkpointType match {
        case "classic" =>
          singularCheckpointFileStatuses(Seq(3, 6, 9, 12))
        case "multi-part" =>
          multiCheckpointFileStatuses(Seq(3, 6, 9, 12), multiPartCheckpointPartsSize)
        case "v2" =>
          v2CPFileStatuses(Seq[Long](3, 6, 9, 12))
        case "hybrid" =>
          singularCheckpointFileStatuses(Seq(3)) ++
            multiCheckpointFileStatuses(Seq(6), numParts = multiPartCheckpointPartsSize) ++
            v2CPFileStatuses(Seq[Long](9)) ++
            singularCheckpointFileStatuses(Seq(12))
      })

    // test cases
    Seq(
      (
        "delete expired delta files up to the checkpoint version, " +
          "not all expired delta files are deleted",
        Seq(0L, 1L, 2L), // expDeletedDeltaVersions,
        Seq(), // expDeletedCheckpointVersions,
        130, // current time
        80 // retention period
      ),
      (
        "expired delta files + expired checkpoint should be deleted",
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions,
        Seq(3L), // expDeletedCheckpointVersions,
        130, // current time
        60 // retention period
      ),
      (
        "expired delta files + expired checkpoints should be deleted",
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), // expDeletedDeltaVersions,
        Seq(3L, 6L), // expDeletedCheckpointVersions,
        130, // current time
        40 // retention period
      ),
      (
        "all delta/checkpoint files should be except the last checkpoint file",
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L), // expDeletedDeltaVersions,
        Seq(3L, 6L, 9L), // expDeletedCheckpointVersions,
        130, // current time
        0 // retention period
      ),
      (
        "no delta/checkpoint files should be deleted as none expired",
        Seq(), // expDeletedDeltaVersions
        Seq(), // expDeletedDeltaVersions
        200, // current time
        200 // retention period
      )
    ).foreach {
      case (testName, expDeletedDeltaVersions, expDeletedCheckpointVersions,
      currentTime, retentionPeriod) =>

        val expectedDeletedFiles = DeletedFileList(
          deltaVersions = expDeletedDeltaVersions,
          classicCheckpointVersions = checkpointType match {
            case "classic" => expDeletedCheckpointVersions
            case "hybrid" => expDeletedCheckpointVersions.filter(Seq(3, 12).contains(_))
            case _ => Seq.empty
          },
          multipartCheckpointVersions = checkpointType match {
            case "multi-part" => expDeletedCheckpointVersions
            case "hybrid" => expDeletedCheckpointVersions.filter(_ == 6)
            case _ => Seq.empty
          },
          v2CheckpointVersions = checkpointType match {
            case "v2" => expDeletedCheckpointVersions
            case "hybrid" => expDeletedCheckpointVersions.filter(_ == 9)
            case _ => Seq.empty
          }
        )

        val testDesc = s"$checkpointType: $testName"
        testCleanup(testDesc, logFiles, expectedDeletedFiles, currentTime, retentionPeriod)
    }
  }

  test("first log entry is a checkpoint") {
    val logFiles = multiCheckpointFileStatuses(Seq(25), multiPartCheckpointPartsSize) ++
      singularCheckpointFileStatuses(Seq(29)) ++
      deltaFileStatuses(Seq(25, 26, 27, 28, 29, 30, 31, 32))

    Seq(
      (
        330, // current time
        50, // retention period
        DeletedFileList() // expected deleted files - none of them should be deleted
      ),
      (
        330, // current time
        30, // retention period
        DeletedFileList(
          deltaVersions = Seq(25, 26, 27, 28),
          multipartCheckpointVersions = Seq(25)
        )
      ),
      (
        330, // current time
        10, // retention period
        DeletedFileList(
          deltaVersions = Seq(25, 26, 27, 28),
          multipartCheckpointVersions = Seq(25)
        )
      )
    ).foreach {
      case (currentTime, retentionPeriod, expectedDeletedFiles) =>
        val fsClient = mockFsClient(logFiles)
        cleanupExpiredLogs(
          mockEngine(fsClient),
          new ManualClock(currentTime),
          logPath,
          retentionPeriod)

        assert(fsClient.getDeleteCalls.toSet === expectedDeletedFiles.fileList().toSet)
    }
  }

  /* ------------------- NEGATIVE TESTS ------------------ */
  test("metadataCleanup: invalid retention period") {
    val e = intercept[IllegalArgumentException] {
      cleanupExpiredLogs(
        mockEngine(mockFsClient(Seq.empty)),
        new ManualClock(100),
        logPath,
        -1 /* retentionPeriod */
      )
    }

    assert(e.getMessage.contains("Retention period must be non-negative"))
  }

  test("incomplete checkpoints should not be considered") {
    val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) ++
      multiCheckpointFileStatuses(Seq(3), multiPartCheckpointPartsSize)
        // delete the third part of the checkpoint
        .filterNot(_.getPath.contains(s"%010d.%010d".format(2, 4))) ++
      multiCheckpointFileStatuses(Seq(6), multiPartCheckpointPartsSize) ++
      v2CPFileStatuses(Seq(9))

    // test cases
    Seq(
      (
        Seq[Long](), // expDeletedDeltaVersions,
        Seq[Long](), // expDeletedCheckpointVersions,
        130, // current time
        80 // retention period
      ),
      (
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions,
        Seq(3L), // expDeletedCheckpointVersions,
        130, // current time
        60 // retention period
      ),
      (
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), // expDeletedDeltaVersions,
        Seq(3L, 6L), // expDeletedCheckpointVersions,
        130, // current time
        20 // retention period
      )
    ).foreach {
      case (expDeletedDeltaVersions, expDeletedCheckpointVersions,
      currentTime, retentionPeriod) =>

        val expectedDeletedFiles = deltaFileStatuses(expDeletedDeltaVersions) ++
          expDeletedCheckpointVersions.flatMap {
            case v@3 => multiCheckpointFileStatuses(Seq(v), multiPartCheckpointPartsSize)
              .filterNot(_.getPath.contains(s"%010d.%010d".format(2, 4)))
            case v@6 => multiCheckpointFileStatuses(Seq(v), multiPartCheckpointPartsSize)
            case v@9 => v2CPFileStatuses(Seq(v))
          }

        val fsClient = mockFsClient(logFiles)
        cleanupExpiredLogs(
          mockEngine(fsClient),
          new ManualClock(currentTime),
          logPath,
          retentionPeriod
        )

        assert(fsClient.getDeleteCalls.toSet === expectedDeletedFiles.map(_.getPath).toSet)
    }
  }

  /* ------------------- HELPER UTILITIES/CONSTANTS ------------------ */
  def testCleanup(
    testName: String,
    logFiles: Seq[FileStatus],
    expectedDeletedFiles: DeletedFileList,
    currentTime: Long,
    retentionPeriod: Long): Unit = {
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

object MetadataCleanupSuite extends MockFileSystemClientUtils {
  /* ------------------- HELPER UTILITIES/CONSTANTS ------------------ */
  private val multiPartCheckpointPartsSize = 4

  /** Case class containing the list of expected files in the deleted metadata log file list */
  case class DeletedFileList(
    deltaVersions: Seq[Long] = Seq.empty,
    classicCheckpointVersions: Seq[Long] = Seq.empty,
    multipartCheckpointVersions: Seq[Long] = Seq.empty,
    v2CheckpointVersions: Seq[Long] = Seq.empty) {

    def fileList(): Seq[String] = {
      (deltaFileStatuses(deltaVersions) ++
        singularCheckpointFileStatuses(classicCheckpointVersions) ++
        multiCheckpointFileStatuses(multipartCheckpointVersions, multiPartCheckpointPartsSize) ++
        v2CPFileStatuses(v2CheckpointVersions)
        ).sortBy(_.getPath).map(_.getPath)
    }
  }

  def mockFsClient(logFiles: Seq[FileStatus]): MockListFromDeleteFileSystemClient = {
    new MockListFromDeleteFileSystemClient(logFiles)
  }

  def v2CPFileStatuses(versions: Seq[Long]): Seq[FileStatus] = {
    // Replace the UUID with a standard UUID to make the test deterministic
    val standardUUID = "123e4567-e89b-12d3-a456-426614174000"
    val uuidPattern =
      "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r

    v2CheckpointFileStatuses(
      versions.map(v => (v, true, 20)), // to (version, useUUID, numSidecars)
      "parquet"
    ).map(_._1)
      .map(f => FileStatus.of(
        uuidPattern.replaceAllIn(f.getPath, standardUUID),
        f.getSize,
        f.getModificationTime))
  }
}
