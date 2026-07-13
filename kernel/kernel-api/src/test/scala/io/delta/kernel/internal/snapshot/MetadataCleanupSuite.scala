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

import java.util.Optional

import io.delta.kernel.internal.MockReadLastCheckpointFileJsonHandler
import io.delta.kernel.internal.snapshot.MetadataCleanup.cleanupExpiredLogs
import io.delta.kernel.internal.util.{FileNames, ManualClock}
import io.delta.kernel.test.{MockFileSystemClientUtils, MockListFromDeleteFileSystemClient}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for the metadata cleanup logic in the Delta log directory. It mocks the
 * `FileSystemClient` to test the cleanup logic for various combinations of delta files and
 * checkpoint files. Utility methods in `MockFileSystemClientUtils` are used to generate the
 * log file statuses which usually have modification time as the `version * 10`.
 *
 * The `_last_checkpoint` version must be provided to every test via `cleanupAndVerify`. The
 * new implementation short-circuits (returns 0 deletes) when `_last_checkpoint` is absent,
 * mirroring Delta Spark's `listExpiredDeltaLogs`.
 */
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
    )).foreach {
    case (testName, expectedDeletedFiles, currentTime, retentionPeriod) =>
      // _deltalog directory contents - contains only delta files, no checkpoint present
      val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6))
      test(s"metadataCleanup: $testName: $currentTime, $retentionPeriod") {
        // No _last_checkpoint: short-circuit, zero deletes regardless of file ages
        cleanupAndVerify(
          logFiles,
          expectedDeletedFiles.fileList(),
          currentTime,
          retentionPeriod,
          lastCheckpointVersion = Optional.empty())
      }
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
    // The BufferingLogDeletionIterator (mirroring Spark) flushes an entire inter-checkpoint run
    // only when the LAST file in that run satisfies shouldDeleteFile. This means more files may
    // be deleted than the old per-file cutoff check would have produced.
    Seq(
      (
        // cutoff=50: run v0-v2 flushed (last=v2 mtime=20<=50); run v3cp+v3-v5 also flushed
        // because last=v5 mtime=50<=50; run v6cp+v6-v8 NOT flushed (last=v8 mtime=80>50)
        "delete expired delta files up to the checkpoint version, " +
          "not all expired delta files are deleted",
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions
        Seq(3L), // expDeletedCheckpointVersions
        130, // current time
        80 // retention period
      ),
      (
        // cutoff=70: same as cutoff=50 - v5(mtime=50)<=70 still flushes v3-v5; v8(80)>70 stops
        "expired delta files + expired checkpoint should be deleted",
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions
        Seq(3L), // expDeletedCheckpointVersions
        130, // current time
        60 // retention period
      ),
      (
        // cutoff=90: v8(mtime=80)<=90 so run v6cp+v6-v8 also flushed; v11(110)>90 stops
        "expired delta files + expired checkpoints should be deleted",
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), // expDeletedDeltaVersions
        Seq(3L, 6L), // expDeletedCheckpointVersions
        130, // current time
        40 // retention period
      ),
      (
        "all delta/checkpoint files should be except the last checkpoint file",
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L), // expDeletedDeltaVersions
        Seq(3L, 6L, 9L), // expDeletedCheckpointVersions
        130, // current time
        0 // retention period
      ),
      (
        "no delta/checkpoint files should be deleted as none expired",
        Seq(), // expDeletedDeltaVersions
        Seq(), // expDeletedCheckpointVersions
        200, // current time
        200 // retention period
      )).foreach {
      case (
            testName,
            expDeletedDeltaVersions,
            expDeletedCheckpointVersions,
            currentTime,
            retentionPeriod) =>

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
          })

        test(s"metadataCleanup: $checkpointType: $testName: $currentTime, $retentionPeriod") {
          cleanupAndVerify(
            logFiles,
            expectedDeletedFiles.fileList(),
            currentTime,
            retentionPeriod,
            lastCheckpointVersion = Optional.of(12L))
        }
    }
  }

  test("first log entry is a checkpoint") {
    // When the very first file in the listing is a multi-part checkpoint part, init() places
    // that first part into maybeDeleteFiles instead of checkpointPartBuffer. The remaining
    // parts (2/4, 3/4, 4/4) are consumed by queueFilesInBuffer and go into checkpointPartBuffer,
    // which only ever accumulates 3 of the required 4 entries and therefore never triggers its
    // own flush. The multi-part v25 checkpoint is thus "split": part 1 sits in maybeDeleteFiles
    // while parts 2-4 are orphaned in checkpointPartBuffer and are never deleted.
    //
    // The staging buffer is eventually flushed when the classic v29 checkpoint is encountered.
    // At that point maybeDeleteFiles = [part1/4(v25), v25.json, v26.json, v27.json, v28.json].
    // The last file is v28.json (mtime=280, version=28 <= maxVersion=28).
    //   cutoff=280: 280 <= 280, shouldDeleteFile=true, all 5 files promoted.
    //   cutoff=300: 280 <= 300, same 5 files promoted.
    //   cutoff=320: 280 <= 320, same 5 files promoted.
    // In all three cases exactly the same 5 files are deleted; parts 2/4, 3/4, 4/4 of v25 are not.
    val logFiles = multiCheckpointFileStatuses(Seq(25), multiPartCheckpointPartsSize) ++
      singularCheckpointFileStatuses(Seq(29)) ++
      deltaFileStatuses(Seq(25, 26, 27, 28, 29, 30, 31, 32))

    // The 5 deleted files are: part 1/4 of the v25 multi-part checkpoint + deltas v25-v28.
    // Parts 2/4, 3/4, 4/4 of v25 are orphaned in checkpointPartBuffer and never deleted.
    val expectedDeleted = {
      val part1 = FileStatus.of(
        FileNames.multiPartCheckpointFile(logPath, 25, 1, multiPartCheckpointPartsSize).toString,
        25,
        250)
      (Seq(part1) ++ deltaFileStatuses(Seq(25, 26, 27, 28))).map(_.getPath)
    }

    Seq(
      (330, 50), // cutoff=280: v28.json mtime=280 <= 280, eligible
      (330, 30), // cutoff=300: v28.json mtime=280 <= 300, eligible
      (330, 10) // cutoff=320: v28.json mtime=280 <= 320, eligible
    ).foreach { case (currentTime, retentionPeriod) =>
      cleanupAndVerify(
        logFiles,
        expectedDeleted,
        currentTime,
        retentionPeriod,
        lastCheckpointVersion = Optional.of(29L))
    }
  }

  /* ------------------- NEGATIVE TESTS ------------------ */
  test("metadataCleanup: invalid retention period") {
    val e = intercept[IllegalArgumentException] {
      cleanupExpiredLogs(
        createMockFSAndJsonEngineForLastCheckpoint(Seq.empty, Optional.empty()),
        new ManualClock(100),
        dataPath,
        -1 /* retentionPeriod */
      )
    }

    assert(e.getMessage.contains("Retention period must be non-negative"))
  }

  test("incomplete checkpoints should not be considered") {
    // The v3 multi-part checkpoint is missing part 2/4.  The filter below removes the file
    // whose path contains "0000000002.0000000004", i.e. part 2/4.
    // Present parts for v3: 1/4, 3/4, 4/4 (3 of the required 4).
    //
    // Algorithm behaviour for the incomplete v3 checkpoint:
    //   checkpointPartBuffer[3][4] accumulates at most 3 entries and never reaches 4, so the
    //   "complete multi-part" branch is never triggered.  The 3 incomplete parts are therefore
    //   NEVER added to maybeDeleteFiles and are NEVER deleted, regardless of the cutoff.
    //
    // The complete v6 checkpoint (4/4 parts) does fire a flush when its 4th part arrives.
    // At that point maybeDeleteFiles = [v0, v1, v2, v3.json, v4, v5] (the commit files that
    // accumulated while the incomplete v3 parts were being buffered).
    //
    // Case 1 (cutoff=50):  last in maybeDelete = v5.json (mtime=50 <= 50, version=5<=8), PROMOTE.
    //   deltas v0-v5 deleted; no cp files deleted.
    // Case 2 (cutoff=70):  last = v5.json (mtime=50 <= 70), same result.
    //   deltas v0-v5 deleted; no cp files deleted.
    // Case 3 (cutoff=110): v6 cp flush promotes deltas v0-v5.
    //   Then maybeDelete = [4 v6 cp parts, v6.json, v7.json, v8.json].
    //   v9 V2 cp triggers flush: last=v8.json mtime=80 <= 110, version=8<=8, PROMOTE all 7.
    //   deltas v0-v8 + all 4 parts of v6 cp deleted (13 files);
    //   v3 incomplete parts never deleted.
    val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) ++
      multiCheckpointFileStatuses(Seq(3), multiPartCheckpointPartsSize)
        // Remove part 2/4 to make the v3 checkpoint incomplete.
        .filterNot(_.getPath.contains(s"%010d.%010d".format(2, 4))) ++
      multiCheckpointFileStatuses(Seq(6), multiPartCheckpointPartsSize) ++
      v2CPFileStatuses(Seq(9))

    Seq(
      (
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions
        Seq[Long](), // expDeletedCheckpointVersions - incomplete v3 parts are NEVER deleted
        130, // current time
        80 // retention period - cutoff=50
      ),
      (
        Seq(0L, 1L, 2L, 3L, 4L, 5L), // expDeletedDeltaVersions
        Seq[Long](), // expDeletedCheckpointVersions - same result as cutoff=50
        130, // current time
        60 // retention period - cutoff=70
      ),
      (
        Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), // expDeletedDeltaVersions
        Seq(6L), // only the complete v6 cp (4 parts); v3 incomplete parts still not deleted
        130, // current time
        20 // retention period - cutoff=110
      )).foreach {
      case (expDeletedDeltaVersions, expDeletedCheckpointVersions, currentTime, retentionPeriod) =>

        val expectedDeletedFiles = (deltaFileStatuses(expDeletedDeltaVersions) ++
          expDeletedCheckpointVersions.flatMap {
            case v @ 6 => multiCheckpointFileStatuses(Seq(v), multiPartCheckpointPartsSize)
            case v @ 9 => v2CPFileStatuses(Seq(v))
          }).map(_.getPath)

        cleanupAndVerify(
          logFiles,
          expectedDeletedFiles,
          currentTime,
          retentionPeriod,
          lastCheckpointVersion = Optional.of(9L))
    }
  }

  /* ------------------- _last_checkpoint short-circuit tests ------------------ */

  test("metadataCleanup: no _last_checkpoint present - no files deleted") {
    // Even with expired files present, no deletion occurs without a checkpoint.
    val logFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5))
    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = Seq.empty,
      currentTimeMillis = 1000,
      retentionPeriodMillis = 0,
      lastCheckpointVersion = Optional.empty())
  }

  test("metadataCleanup: maxVersion cap - files at or after last checkpoint version not deleted") {
    // _last_checkpoint at version 5 - maxVersion = 4.
    // All files have mtime = version * 10, fileCutOffTime = 1000.
    // Commits 0-4 are covered by the checkpoint at v5 and have mtime <= 1000 - deleted.
    // The checkpoint at v5 sits in the staging buffer after flush but is never re-flushed
    // (no subsequent checkpoint), so it is NOT deleted.
    // Commit at v6 is beyond maxVersion=4 so shouldDeleteFile returns false - not flushed.
    val logFiles =
      deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5, 6)) ++
        singularCheckpointFileStatuses(Seq(5))

    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = deltaFileStatuses(Seq(0, 1, 2, 3, 4)).map(_.getPath),
      currentTimeMillis = 1000,
      retentionPeriodMillis = 0,
      lastCheckpointVersion = Optional.of(5L))
  }

  /* ------------------- timestamp monotonicity tests ------------------ */

  test("metadataCleanup: clock skew - second file has same mtime as first") {
    // Version 0: mtime=10, version 1: mtime=10 (same, needs adjustment to 11),
    // version 2: mtime=20. Checkpoint at version 3: mtime=30.
    // Cutoff = 15, so with adjustment: v0 (mtime=10 <=15 ok), v1 (adjusted mtime=11 <=15 ok),
    // v2 (mtime=20 >15, loop stops before flush). Nothing deleted because the last file
    // in the buffer (v2) doesn't satisfy shouldDeleteFile, so flushBuffer does not promote.
    val logFiles = Seq(
      FileStatus.of(FileNames.deltaFile(logPath, 0).toString, 0, 10),
      FileStatus.of(FileNames.deltaFile(logPath, 1).toString, 0, 10), // same mtime as v0
      FileStatus.of(FileNames.deltaFile(logPath, 2).toString, 0, 20),
      FileStatus.of(FileNames.checkpointFileSingular(logPath, 3).toString, 3, 30))

    // Nothing deleted: v2's adjusted mtime (20) > cutoff (15), so flushBuffer sees
    // the last file in maybeDeleteFiles as non-deletable when the checkpoint is encountered.
    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = Seq.empty,
      currentTimeMillis = 15,
      retentionPeriodMillis = 0,
      lastCheckpointVersion = Optional.of(3L))
  }

  test("metadataCleanup: clock skew - all files before checkpoint have adjusted " +
    "mtimes within cutoff") {
    // Version 0: mtime=10, version 1: mtime=5 (skewed, adjusted to 11),
    // version 2: mtime=4 (skewed, adjusted to 12). Checkpoint at version 3: mtime=30.
    // Cutoff = 20. Adjusted mtimes: v0=10, v1=11, v2=12 - all <= 20. Checkpoint triggers
    // flushBuffer; last file in buffer is v2 with adjusted mtime 12 <= 20, all flushed.
    val logFiles = Seq(
      FileStatus.of(FileNames.deltaFile(logPath, 0).toString, 0, 10),
      FileStatus.of(FileNames.deltaFile(logPath, 1).toString, 0, 5),
      FileStatus.of(FileNames.deltaFile(logPath, 2).toString, 0, 4),
      FileStatus.of(FileNames.checkpointFileSingular(logPath, 3).toString, 3, 30))

    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = deltaFileStatuses(Seq(0, 1, 2)).map(_.getPath), // v0,v1,v2 deleted
      currentTimeMillis = 20,
      retentionPeriodMillis = 0,
      lastCheckpointVersion = Optional.of(3L))
  }

  /* ------------------- CRC file tests ------------------ */

  test("metadataCleanup: CRC files deleted alongside expired commit files") {
    // The iterator flushes an entire inter-checkpoint run only when the newest file in that run
    // is expired. To get v0-v3 deleted but v4-v5 retained, we need two checkpoints: one at v4
    // (covering v0-v3) and one at v6 (covering v4-v5). With cutoff=30, the run v0-v3 has its
    // newest member (v3, mtime=30) satisfying shouldDeleteFile, so it is flushed. The run v4-v5
    // has its newest member (v5, mtime=50 > 30) not satisfying shouldDeleteFile, so it is not.
    val logFiles =
      deltaFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++
        checksumFileStatuses(Seq(0, 1, 2, 3, 4, 5)) ++
        singularCheckpointFileStatuses(Seq(4, 6))

    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles =
        (deltaFileStatuses(Seq(0, 1, 2, 3)) ++
          checksumFileStatuses(Seq(0, 1, 2, 3))).map(_.getPath),
      currentTimeMillis = 50,
      retentionPeriodMillis = 20, // cutoff = 50 - 20 = 30, mtime = version * 10 so v0-v3 eligible
      lastCheckpointVersion = Optional.of(6L))
  }

  test("metadataCleanup: CRC files within retention window are not deleted") {
    // All files are within the retention window: mtime = version * 10, cutoff = 200 - 200 = 0.
    // No file has mtime <= 0, so nothing is deleted even though a checkpoint exists.
    val logFiles =
      deltaFileStatuses(Seq(0, 1, 2, 3)) ++
        checksumFileStatuses(Seq(0, 1, 2, 3)) ++
        singularCheckpointFileStatuses(Seq(4))

    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = Seq.empty,
      currentTimeMillis = 200,
      retentionPeriodMillis = 200,
      lastCheckpointVersion = Optional.of(4L))
  }

  test("metadataCleanup: orphaned CRC files (no corresponding commit) deleted when " +
    "expired and covered") {
    // CRC files at versions 0-2, no .json counterparts. Checkpoint at version 3 covers them.
    // All CRC files have mtime = version * 10 <= cutoff (100 - 0 = 100), so they are flushed
    // when the checkpoint is encountered and the last buffered file (v2.crc, mtime=20) passes
    // shouldDeleteFile.
    val logFiles =
      checksumFileStatuses(Seq(0, 1, 2)) ++
        singularCheckpointFileStatuses(Seq(3))

    cleanupAndVerify(
      logFiles,
      expectedDeletedFiles = checksumFileStatuses(Seq(0, 1, 2)).map(_.getPath),
      currentTimeMillis = 100,
      retentionPeriodMillis = 0,
      lastCheckpointVersion = Optional.of(3L))
  }

  /* ------------------- HELPER UTILITIES/CONSTANTS ------------------ */
  /**
   * Cleanup the metadata log files and verify the expected deleted files (both count and exact
   * paths). Uses [[MockListFromDeleteFileSystemClient]] so delete calls can be inspected, plus a
   * JSON handler that answers _last_checkpoint reads.
   *
   * @param logFiles List of log files in the _delta_log directory
   * @param expectedDeletedFiles List of expected deleted file paths
   * @param currentTimeMillis Current time in millis
   * @param retentionPeriodMillis Retention period in millis
   * @param lastCheckpointVersion The version to report in _last_checkpoint, or empty if absent
   */
  def cleanupAndVerify(
      logFiles: Seq[FileStatus],
      expectedDeletedFiles: Seq[String],
      currentTimeMillis: Long,
      retentionPeriodMillis: Long,
      lastCheckpointVersion: Optional[java.lang.Long]): Unit = {
    val fsClient = mockFsClient(logFiles)
    val engine = MetadataCleanupSuite.mockEngineWithFsAndLastCheckpoint(
      fsClient,
      lastCheckpointVersion)
    val resultDeletedCount = cleanupExpiredLogs(
      engine,
      new ManualClock(currentTimeMillis),
      dataPath,
      retentionPeriodMillis)

    assert(resultDeletedCount === expectedDeletedFiles.size)
    assert(fsClient.getDeleteCalls.toSet === expectedDeletedFiles.toSet)
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
      v2CheckpointVersions: Seq[Long] = Seq.empty,
      checksumVersions: Seq[Long] = Seq.empty) {

    def fileList(): Seq[String] = {
      (deltaFileStatuses(deltaVersions) ++
        singularCheckpointFileStatuses(classicCheckpointVersions) ++
        multiCheckpointFileStatuses(multipartCheckpointVersions, multiPartCheckpointPartsSize) ++
        v2CPFileStatuses(v2CheckpointVersions) ++
        checksumFileStatuses(checksumVersions)).sortBy(_.getPath).map(_.getPath)
    }
  }

  def mockFsClient(logFiles: Seq[FileStatus]): MockListFromDeleteFileSystemClient = {
    new MockListFromDeleteFileSystemClient(logFiles)
  }

  /**
   * Checksum file statuses where the timestamp = 10 * version
   * (matches deltaFileStatus convention)
   */
  def checksumFileStatuses(versions: Seq[Long]): Seq[FileStatus] = {
    versions.map(v =>
      FileStatus.of(FileNames.checksumFile(logPath, v).toString, 10, v * 10))
  }

  def v2CPFileStatuses(versions: Seq[Long]): Seq[FileStatus] = {
    // Replace the UUID with a standard UUID to make the test deterministic
    val standardUUID = "123e4567-e89b-12d3-a456-426614174000"
    val uuidPattern =
      "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r

    v2CheckpointFileStatuses(
      versions.map(v => (v, true, 20)), // to (version, useUUID, numSidecars)
      "parquet").map(_._1)
      .map(f =>
        FileStatus.of(
          uuidPattern.replaceAllIn(f.getPath, standardUUID),
          f.getSize,
          f.getModificationTime))
  }

  /**
   * Creates a mock engine that uses MockListFromDeleteFileSystemClient (so delete calls can be
   * inspected) and provides a _last_checkpoint JSON response.
   */
  def mockEngineWithFsAndLastCheckpoint(
      fsClient: MockListFromDeleteFileSystemClient,
      lastCheckpointVersion: Optional[java.lang.Long]): io.delta.kernel.engine.Engine = {
    mockEngine(
      fileSystemClient = fsClient,
      jsonHandler = if (lastCheckpointVersion.isPresent) {
        new MockReadLastCheckpointFileJsonHandler(
          s"$logPath/_last_checkpoint",
          lastCheckpointVersion.get())
      } else {
        null
      })
  }
}
