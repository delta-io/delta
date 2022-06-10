/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation, Snapshot}
import io.delta.standalone.actions.{AddFile => AddFileJ, JobInfo => JobInfoJ, Metadata => MetadataJ, NotebookInfo => NotebookInfoJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.exceptions.DeltaStandaloneException

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{ConversionUtils, FileNames}
import io.delta.standalone.internal.util.GoldenTableUtils._
import io.delta.standalone.internal.util.TestUtils._

/**
 * Instead of using Spark in this project to WRITE data and log files for tests, we have
 * io.delta.golden.GoldenTables do it instead. During tests, we then refer by name to specific
 * golden tables that that class is responsible for generating ahead of time. This allows us to
 * focus on READING only so that we may fully decouple from Spark and not have it as a dependency.
 *
 * See io.delta.golden.GoldenTables for documentation on how to ensure that the needed files have
 * been generated.
 */
abstract class DeltaLogSuiteBase extends FunSuite {

  val engineInfo = "test-engine-info"
  val manualUpdate = new Operation(Operation.Name.MANUAL_UPDATE)

  // We want to allow concrete child test suites to use their own "get all AddFiles" APIs.
  // e.g. snapshot.getAllFiles or snapshot.scan.getFiles
  //
  // Child test suites should create their own concrete `CustomAddFilesAccessor` class and then
  // override `createCustomAddFilesAccessor` to return a new instance of it.
  abstract class CustomAddFilesAccessor(snapshot: Snapshot) {
    def _getFiles(): java.util.List[AddFileJ]
  }

  implicit def createCustomAddFilesAccessor(snapshot: Snapshot): CustomAddFilesAccessor

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  // scalastyle:on funsuite
  test("checkpoint") {
    withLogForGoldenTable("checkpoint") { log =>
      assert(log.snapshot.getVersion == 14)
      assert(log.snapshot._getFiles().size == 1)
      log.snapshot._getFiles().hashCode()
    }
  }

  test("snapshot") {
    def getDirDataFiles(tablePath: String): Array[File] = {
      val correctTablePath =
        if (tablePath.startsWith("file:")) tablePath.stripPrefix("file:") else tablePath
      val dir = new File(correctTablePath)
      dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
    }

    def verifySnapshot(
        snapshot: Snapshot,
        expectedFiles: Array[File],
        expectedVersion: Int): Unit = {
      assert(snapshot.getVersion == expectedVersion)
      assert(snapshot._getFiles().size() == expectedFiles.length)
      assert(
        snapshot._getFiles().asScala.forall(f => expectedFiles.exists(_.getName == f.getPath)))
    }

    // Append data0
    var data0_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data0") { log =>
      data0_files = getDirDataFiles(log.getPath.toString) // data0 files
      verifySnapshot(log.snapshot(), data0_files, 0)
    }

    // Append data1
    var data0_data1_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data1") { log =>
      data0_data1_files = getDirDataFiles(log.getPath.toString) // data0 & data1 files
      verifySnapshot(log.snapshot(), data0_data1_files, 1)
    }

    // Overwrite with data2
    var data2_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data2") { log =>
      // we have overwritten files for data0 & data1; only data2 files should remain
      data2_files = getDirDataFiles(log.getPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_files, 2)
    }

    // Append data3
    withLogForGoldenTable("snapshot-data3") { log =>
      // we have overwritten files for data0 & data1; only data2 & data3 files should remain
      val data2_data3_files = getDirDataFiles(log.getPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_data3_files, 3)
    }

    // Delete data2 files
    withLogForGoldenTable("snapshot-data2-deleted") { log =>
      // we have overwritten files for data0 & data1, and deleted data2 files; only data3 files
      // should remain
      val data3_files = getDirDataFiles(log.getPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
        .filterNot(f => data2_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data3_files, 4)
    }

    // Repartition into 2 files
    withLogForGoldenTable("snapshot-repartitioned") { log =>
      assert(log.snapshot()._getFiles().size == 2)
      assert(log.snapshot().getVersion == 5)
    }

    // Vacuum
    withLogForGoldenTable("snapshot-vacuumed") { log =>
      // all remaining dir data files should be needed for current snapshot version
      // vacuum doesn't change the snapshot version
      verifySnapshot(log.snapshot(), getDirDataFiles(log.getPath.toString), 5)
    }
  }

  test("SC-8078: update deleted directory") {
    withGoldenTable("update-deleted-directory") { tablePath =>
      val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
      try {
        FileUtils.copyDirectory(new File(tablePath), tempDir)
        val log = DeltaLog.forTable(new Configuration(), tempDir.getCanonicalPath)
        FileUtils.deleteDirectory(tempDir)
        assert(log.update().getVersion == -1)
      } finally {
        // just in case
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  test("update shouldn't pick up delta files earlier than checkpoint") {
    withTempDir { tempDir =>
      val log1 = DeltaLog.forTable(new Configuration(), new Path(tempDir.getCanonicalPath))

      (1 to 5).foreach { i =>
        val txn = log1.startTransaction()
        val metadata = if (i == 1) Metadata() :: Nil else Nil
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile((i - 1).toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }

        val filesToCommit = (metadata ++ delete ++ file).map(ConversionUtils.convertAction)

        txn.commit(filesToCommit.asJava, manualUpdate, engineInfo)
      }

      // DeltaOSS performs `DeltaLog.clearCache()` here, but we can't
      val log2 = DeltaLogImpl.forTable(new Configuration(), new Path(tempDir.getCanonicalPath))

      (6 to 15).foreach { i =>
        val txn = log1.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete = RemoveFile((i - 1).toString, Some(System.currentTimeMillis()), true) :: Nil

        val filesToCommit = (delete ++ file).map(ConversionUtils.convertAction)

        txn.commit(filesToCommit.asJava, manualUpdate, engineInfo)
      }

      // Since log2 is a separate instance, it shouldn't be updated to version 15
      assert(log2.snapshot.getVersion == 4)
      val updateLog2 = log2.update()
      assert(updateLog2.getVersion == log1.snapshot.getVersion, "Did not update to correct version")

      val deltas = log2.snapshot.logSegment.deltas
      assert(deltas.length === 4, "Expected 4 files starting at version 11 to 14")
      val versions = deltas.map(f => FileNames.deltaVersion(f.getPath)).sorted
      assert(versions === Seq[Long](11, 12, 13, 14), "Received the wrong files for update")
    }
  }

  test("handle corrupted '_last_checkpoint' file") {
    withLogImplForWritableGoldenTable("corrupted-last-checkpoint") { log1 =>
      assert(log1.lastCheckpoint.isDefined)

      val lastCheckpoint = log1.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log1.LAST_CHECKPOINT.getFileSystem(log1.hadoopConf)
      fs.create(log1.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      val log2 = DeltaLogImpl.forTable(new Configuration(), new Path(log1.getPath.toString))

      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log1 ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.lastCheckpoint.get) === CheckpointInstance(lastCheckpoint))
    }
  }

  test("paths should be canonicalized - normal characters") {
    withLogForGoldenTable("canonicalized-paths-normal-a") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot._getFiles().size == 0)
    }

    withLogForGoldenTable("canonicalized-paths-normal-b") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot._getFiles().size == 0)
    }
  }

  test("paths should be canonicalized - special characters") {
    withLogForGoldenTable("canonicalized-paths-special-a") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot._getFiles().size == 0)
    }

    withLogForGoldenTable("canonicalized-paths-special-b") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot._getFiles().size == 0)
    }
  }

  test("do not relativize paths in RemoveFiles") {
    withTempDir { dir =>
      val log = DeltaLogImpl.forTable(new Configuration(), dir.getCanonicalPath)
      assert(new File(log.logPath.toUri).mkdirs())
      val path = new File(dir, "a/b/c").getCanonicalPath

      val removeFile = new RemoveFileJ(
        path,
        java.util.Optional.of(System.currentTimeMillis()),
        true, // dataChange
        false, // extendedFileMetadata
        null, // partitionValues
        java.util.Optional.of(0L), // size
        null // null
      )

      val metadata = MetadataJ.builder().build()
      val actions = java.util.Arrays.asList(removeFile, metadata)

      log.startTransaction().commit(actions, manualUpdate, engineInfo)

      val committedRemove = log.update().tombstonesScala
      assert(committedRemove.head.path === s"file://$path")
    }
  }

  test("delete and re-add the same file in different transactions") {
    withLogForGoldenTable("delete-re-add-same-file-different-transactions") { log =>
      assert(log.snapshot()._getFiles().size() == 2)

      assert(log.snapshot()._getFiles().asScala.map(_.getPath).toSet == Set("foo", "bar"))

      // We added two add files with the same path `foo`. The first should have been removed.
      // The second should remain, and should have a hard-coded modification time of 1700000000000L
      assert(log.snapshot()._getFiles().asScala.find(_.getPath == "foo").get
        .getModificationTime == 1700000000000L)
    }
  }

  test("error - versions not contiguous") {
    val ex = intercept[IllegalStateException] {
      withLogForGoldenTable("versions-not-contiguous") { _ => }
    }

    assert(ex.getMessage ===
      DeltaErrors.deltaVersionsNotContiguousException(Vector(0, 2)).getMessage)
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"state reconstruction without $action should fail") {
      val e = intercept[IllegalStateException] {
        // snapshot initialization triggers state reconstruction
        withLogForGoldenTable(s"deltalog-state-reconstruction-without-$action") { _ => }
      }
      assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 0).getMessage)
    }
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"state reconstruction from checkpoint with missing $action should fail") {
      val e = intercept[IllegalStateException] {
        val tblName = s"deltalog-state-reconstruction-from-checkpoint-missing-$action"
        // snapshot initialization triggers state reconstruction
        withLogForGoldenTable(tblName) { _ => }
      }
      assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 10).getMessage)
    }
  }

  test("table protocol version greater than client reader protocol version") {
    val e = intercept[DeltaErrors.InvalidProtocolVersionException] {
      withLogForGoldenTable("deltalog-invalid-protocol-version") { _ => }
    }

    assert(e.getMessage === new DeltaErrors.InvalidProtocolVersionException(Action.protocolVersion,
      Protocol(99)).getMessage)
  }

  test("get commit info") {
    // check all fields get deserialized properly
    withLogForGoldenTable("deltalog-commit-info") { log =>
      val ci = log.getCommitInfoAt(0)
      assert(ci.getVersion.get() == 0)
      assert(ci.getTimestamp == new Timestamp(1540415658000L))
      assert(ci.getUserId.get() == "user_0")
      assert(ci.getUserName.get() == "username_0")
      assert(ci.getOperation == "WRITE")
      assert(ci.getOperationParameters == Map("test" -> "test").asJava)
      assert(ci.getJobInfo.get() ==
        new JobInfoJ("job_id_0", "job_name_0", "run_id_0", "job_owner_0", "trigger_type_0"))
      assert(ci.getNotebookInfo.get() == new NotebookInfoJ("notebook_id_0"))
      assert(ci.getClusterId.get() == "cluster_id_0")
      assert(ci.getReadVersion.get() == -1)
      assert(ci.getIsolationLevel.get() == "default")
      assert(ci.getIsBlindAppend.get() == true)
      assert(ci.getOperationMetrics.get() == Map("test" -> "test").asJava)
      assert(ci.getUserMetadata.get() == "foo")
    }

    // use an actual spark transaction example
    withLogForGoldenTable("snapshot-vacuumed") { log =>
      // check that correct CommitInfo read
      (0 to 5).foreach { i =>
        val ci = log.getCommitInfoAt(i)

        assert(ci.getVersion.get() == i)
        if (i > 0) {
          assert(ci.getReadVersion.get() == i - 1)
        }
      }

      // test illegal version
      assertThrows[DeltaStandaloneException] {
        log.getCommitInfoAt(99)
      }
    }
  }

  test("getChanges - no data loss") {
    withLogForGoldenTable("deltalog-getChanges") { log =>
      val versionToActionsMap = Map(
        0L -> Seq("CommitInfo", "Protocol", "Metadata", "AddFile"),
        1L -> Seq("CommitInfo", "AddCDCFile", "RemoveFile"),
        2L -> Seq("CommitInfo", "Protocol", "SetTransaction")
      )

      def verifyChanges(startVersion: Int): Unit = {
        val versionLogs = log.getChanges(startVersion, false).asScala.toSeq

        assert(versionLogs.length == 3 - startVersion,
          s"getChanges($startVersion) skipped some versions")

        val versionsInOrder = new ListBuffer[Long]()

        for (versionLog <- versionLogs) {
          val version = versionLog.getVersion
          val actions = versionLog.getActions.asScala.map(_.getClass.getSimpleName)
          val expectedActions = versionToActionsMap(version)
          assert(expectedActions == actions,
            s"getChanges($startVersion) had incorrect actions at version $version.")

          versionsInOrder += version
        }

        // ensure that versions are seen in increasing order
        assert(versionsInOrder.toList == (startVersion to 2).map(_.toLong).toList)
      }

      // standard cases
      verifyChanges(0)
      verifyChanges(1)
      verifyChanges(2)

      // non-existant start version
      val versionLogsIter = log.getChanges(3, false)
      assert(!versionLogsIter.hasNext,
        "getChanges with a non-existant start version did not return an empty iterator")

      // negative start version
      assertThrows[IllegalArgumentException] {
        log.getChanges(-1, false)
      }
    }
  }

  test("getChanges - data loss") {
    withGoldenTable("deltalog-getChanges") { tablePath =>
      val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
      try {
        FileUtils.copyDirectory(new File(tablePath), tempDir)
        val log = DeltaLog.forTable(new Configuration(), tempDir.getCanonicalPath)

        // we delete 2 files so that the `DeltaErrors.failOnDataLossException` is thrown
        val logPath = new Path(log.getPath, "_delta_log")
        new File(new Path(logPath, "00000000000000000000.json").toUri).delete()
        new File(new Path(logPath, "00000000000000000001.json").toUri).delete()

        val versionLogs = log.getChanges(0, false).asScala.toSeq
        assert(versionLogs.length == 1)

        assertThrows[IllegalStateException] {
          val versionLogsIter = log.getChanges(0, true)
          while (versionLogsIter.hasNext) {
            versionLogsIter.next()
          }
        }
      } finally {
        // just in case
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  test("DeltaLog.tableExists") {
    withTempDir { dir =>

      val conf = new Configuration()
      val log = DeltaLog.forTable(conf, dir.getCanonicalPath)

      assert(!log.tableExists())

      log.startTransaction().commit(
        Seq(MetadataJ.builder().build()).asJava,
        new Operation(Operation.Name.CREATE_TABLE),
        "test"
      )
      assert(log.tableExists())
    }
  }

  test("getVersionBeforeOrAtTimestamp and getVersionAtOrAfterTimestamp") {
    // Note:
    // - all Xa test cases will test getVersionBeforeOrAtTimestamp
    // - all Xb test cases will test getVersionAtOrAfterTimestamp
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)

      // ========== case 0: delta table is empty ==========
      assert(log.getVersionBeforeOrAtTimestamp(System.currentTimeMillis()) == -1)
      assert(log.getVersionAtOrAfterTimestamp(System.currentTimeMillis()) == -1)

      // Setup part 1 of 2: create log files
      (0 to 2).foreach { i =>
        val files = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val metadata = if (i == 0) Metadata() :: Nil else Nil
        log.startTransaction().commit(
          (metadata ++ files).map(ConversionUtils.convertAction).asJava,
          manualUpdate, engineInfo
        )
      }

      // Setup part 2 of 2: edit lastModified times
      val logPath = new Path(dir.getCanonicalPath, "_delta_log")
      val logDir = new File(dir.getCanonicalPath, "_delta_log")
      // local file system truncates to seconds
      val nowEpochMs = System.currentTimeMillis() / 1000 * 1000

      val delta0 = FileNames.deltaFile(logPath, 0)
      val delta1 = FileNames.deltaFile(logPath, 1)
      val delta2 = FileNames.deltaFile(logPath, 2)

      new File(logDir, delta0.getName).setLastModified(1000)
      new File(logDir, delta1.getName).setLastModified(2000)
      new File(logDir, delta2.getName).setLastModified(3000)

      // ========== case 1: before first commit ==========
      // case 1a
      val e1 = intercept[IllegalArgumentException] {
        log.getVersionBeforeOrAtTimestamp(500)
      }.getMessage
      assert(e1.contains("is before the earliest version"))
      // case 1b
      assert(log.getVersionAtOrAfterTimestamp(500) == 0)

      // ========== case 2: at first commit ==========
      // case 2a
      assert(log.getVersionBeforeOrAtTimestamp(1000) == 0)
      // case 2b
      assert(log.getVersionAtOrAfterTimestamp(1000) == 0)

      // ========== case 3: between two normal commits ==========
      // case 3a
      assert(log.getVersionBeforeOrAtTimestamp(1500) == 0) // round down to v0
      // case 3b
      assert(log.getVersionAtOrAfterTimestamp(1500) == 1) // round up to v1

      // ========== case 4: at last commit ==========
      // case 4a
      assert(log.getVersionBeforeOrAtTimestamp(3000) == 2)
      // case 4b
      assert(log.getVersionAtOrAfterTimestamp(3000) == 2)

      // ========== case 5: after last commit ==========
      // case 5a
      assert(log.getVersionBeforeOrAtTimestamp(4000) == 2)
      // case 5b
      val e2 = intercept[IllegalArgumentException] {
        log.getVersionAtOrAfterTimestamp(4000)
      }.getMessage
      assert(e2.contains("is after the latest version"))
    }
  }

  test("getVersionBeforeOrAtTimestamp and getVersionAtOrAfterTimestamp - recoverability") {
    withTempDir { dir =>
      // local file system truncates to seconds
      val nowEpochMs = System.currentTimeMillis() / 1000 * 1000

      val logPath = new Path(dir.getCanonicalPath, "_delta_log")
      val logDir = new File(dir.getCanonicalPath, "_delta_log")

      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      (0 to 35).foreach { i =>
        val files = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val metadata = if (i == 0) Metadata() :: Nil else Nil
        log.startTransaction().commit(
          (metadata ++ files).map(ConversionUtils.convertAction).asJava,
          manualUpdate, engineInfo
        )
      }

      (0 to 35).foreach { i =>
        val delta = FileNames.deltaFile(logPath, i)
        val file = new File(logDir, delta.getName)
        val fs = logPath.getFileSystem(new Configuration())
        if (i >= 25) {
          file.setLastModified(nowEpochMs + i * 1000)
        } else {
          file.delete()
          assert(!fs.exists(delta))
        }
      }

      // A checkpoint exists at version 30, so all versions [30, 35] are recoverable.
      // Nonetheless, getVersionBeforeOrAtTimestamp and getVersionAtOrAfterTimestamp do not
      // require that the version is recoverable, so we should still be able to get back versions
      // [25-29]

      (25 to 34).foreach { i =>
        if (i == 25) {
          assertThrows[IllegalArgumentException] {
            log.getVersionBeforeOrAtTimestamp(nowEpochMs + i * 1000 - 1)
          }
        } else {
          assert(log.getVersionBeforeOrAtTimestamp(nowEpochMs + i * 1000 - 1) == i - 1)
        }

        assert(log.getVersionAtOrAfterTimestamp(nowEpochMs + i * 1000 - 1) == i)

        assert(log.getVersionBeforeOrAtTimestamp(nowEpochMs + i * 1000) == i)
        assert(log.getVersionAtOrAfterTimestamp(nowEpochMs + i * 1000) == i)

        assert(log.getVersionBeforeOrAtTimestamp(nowEpochMs + i * 1000 + 1) == i)

        if (i == 35) {
          log.getVersionAtOrAfterTimestamp(nowEpochMs + i * 1000 + 1)
        } else {
          assert(log.getVersionAtOrAfterTimestamp(nowEpochMs + i * 1000 + 1) == i + 1)
        }
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////
// Concrete Implementations
///////////////////////////////////////////////////////////////////////////

class StandardDeltaLogSuite extends DeltaLogSuiteBase {
  class StandardSnapshot(snapshot: Snapshot) extends CustomAddFilesAccessor(snapshot) {
    override def _getFiles(): java.util.List[AddFileJ] = snapshot.getAllFiles
  }

  override implicit def createCustomAddFilesAccessor(snapshot: Snapshot): CustomAddFilesAccessor = {
    new StandardSnapshot(snapshot)
  }
}

class MemoryOptimizedDeltaLogSuite extends DeltaLogSuiteBase {
  class MemoryOptimizedSnapshot(snapshot: Snapshot) extends CustomAddFilesAccessor(snapshot) {
    override def _getFiles(): java.util.List[AddFileJ] = {
      import io.delta.standalone.internal.util.Implicits._

      snapshot.scan().getFiles.toArray.toList.asJava
    }
  }

  override implicit def createCustomAddFilesAccessor(snapshot: Snapshot): CustomAddFilesAccessor = {
    new MemoryOptimizedSnapshot(snapshot)
  }
}
