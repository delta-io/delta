/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.io.File
import java.util.Optional

import scala.collection.JavaConverters._
import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration

import io.delta.kernel.types.{LongType, StructType}
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.Table

class LogReplaySuite extends AnyFunSuite with TestUtils {

  override lazy val defaultEngine = DefaultEngine.create(new Configuration() {{
    // Set the batch sizes to small so that we get to test the multiple batch scenarios.
    set("delta.kernel.default.parquet.reader.batch-size", "2");
    set("delta.kernel.default.json.reader.batch-size", "2");
  }})

  test("simple end to end with inserts and deletes and checkpoint") {
    val expectedValues = (0L until 5L) ++ (10L until 15L) ++ (20L until 25L) ++
      (30L until 35L) ++ (40L until 45L) ++ (50L to 65L)
    checkTable(
      path = goldenTablePath("basic-with-inserts-deletes-checkpoint"),
      expectedAnswer = expectedValues.map(TestRow(_)),
      expectedSchema = new StructType().add("id", LongType.LONG),
      expectedVersion = Some(13L)
    )
  }

  test("simple end to end with inserts and updates") {
    val expectedValues = (0 until 50).map((_, "N/A")) ++
      (50 until 100).map(x => (x, s"val=$x"))
    checkTable(
      path = goldenTablePath("basic-with-inserts-updates"),
      expectedAnswer = expectedValues.map(TestRow.fromTuple)
    )
  }

  test("simple end to end with inserts and merge") {
    val expectedValues = (10 until 50).map(x => (x, s"val=$x")) ++
      (50 until 100).map((_, "N/A")) ++ (100 until 150).map((_, "EXT"))
    checkTable(
      path = goldenTablePath("basic-with-inserts-merge"),
      expectedAnswer = expectedValues.map(TestRow.fromTuple)
    )
  }

  test("simple end to end with restore") {
    checkTable(
      path = goldenTablePath("basic-with-inserts-overwrite-restore"),
      expectedAnswer = (0L until 200L).map(TestRow(_)),
      expectedVersion = Some(3)
    )
  }

  test("end to end only checkpoint files") {
    val expectedValues = (5L until 10L) ++ (0L until 20L)
    checkTable(
      path = goldenTablePath("only-checkpoint-files"),
      expectedAnswer = expectedValues.map(TestRow(_))
    )
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"missing $action should fail") {
      val path = goldenTablePath(s"deltalog-state-reconstruction-without-$action")
      val e = intercept[IllegalStateException] {
        latestSnapshot(path).getSchema(defaultEngine)
      }
      assert(e.getMessage.contains(s"No $action found"))
    }
  }

  // TODO missing protocol should fail when missing from checkpoint
  //   GoldenTable("deltalog-state-reconstruction-from-checkpoint-missing-protocol")
  //   generation is broken and cannot be regenerated with a non-null schemaString until fixed
  Seq("metadata" /* , "protocol" */).foreach { action =>
    test(s"missing $action should fail missing from checkpoint") {
      val path = goldenTablePath(s"deltalog-state-reconstruction-from-checkpoint-missing-$action")
      val e = intercept[IllegalStateException] {
        latestSnapshot(path).getSchema(defaultEngine)
      }
      assert(e.getMessage.contains(s"No $action found"))
    }
  }

  test("fetches the latest protocol and metadata") {
    val path = goldenTablePath("log-replay-latest-metadata-protocol")
    val snapshot = latestSnapshot(path)
    val scanStateRow = snapshot.getScanBuilder(defaultEngine).build()
      .getScanState(defaultEngine)

    // schema is updated
    assert(ScanStateRow.getLogicalSchema(defaultEngine, scanStateRow)
      .fieldNames().asScala.toSet == Set("col1", "col2")
    )

    // check protocol version is upgraded
    val readerVersionOrd = scanStateRow.getSchema().indexOf("minReaderVersion")
    val writerVersionOrd = scanStateRow.getSchema().indexOf("minWriterVersion")
    assert(scanStateRow.getInt(readerVersionOrd) == 3 && scanStateRow.getInt(writerVersionOrd) == 7)
  }

  test("standalone DeltaLogSuite: 'checkpoint'") {
    val path = goldenTablePath("checkpoint")
    val snapshot = latestSnapshot(path)
    assert(snapshot.getVersion(defaultEngine) == 14)
    val scan = snapshot.getScanBuilder(defaultEngine).build()
    assert(collectScanFileRows(scan).length == 1)
  }

  test("standalone DeltaLogSuite: 'snapshot'") {
    def getDirDataFiles(tablePath: String): Array[File] = {
      val correctTablePath =
        if (tablePath.startsWith("file:")) tablePath.stripPrefix("file:") else tablePath
      val dir = new File(correctTablePath)
      dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
    }

    def verifySnapshotScanFiles(
      tablePath: String,
      expectedFiles: Array[File],
      expectedVersion: Int): Unit = {
      val snapshot = latestSnapshot(tablePath)
      assert(snapshot.getVersion(defaultEngine) == expectedVersion)
      val scanFileRows = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine).build())
      assert(scanFileRows.length == expectedFiles.length)
      val scanFilePaths = scanFileRows
        .map(InternalScanFileUtils.getAddFileStatus)
        .map(_.getPath)
        .map(new File(_).getName) // get the relative path to compare
      assert(scanFilePaths.toSet == expectedFiles.map(_.getName).toSet)
    }

    // Append data0
    var data0_files: Array[File] = Array.empty
    withGoldenTable("snapshot-data0") { tablePath =>
      data0_files = getDirDataFiles(tablePath) // data0 files
      verifySnapshotScanFiles(tablePath, data0_files, 0)
    }

    // Append data1
    var data0_data1_files: Array[File] = Array.empty
    withGoldenTable("snapshot-data1") { tablePath =>
      data0_data1_files = getDirDataFiles(tablePath) // data0 & data1 files
      verifySnapshotScanFiles(tablePath, data0_data1_files, 1)
    }

    // Overwrite with data2
    var data2_files: Array[File] = Array.empty
    withGoldenTable("snapshot-data2") { tablePath =>
      // we have overwritten files for data0 & data1; only data2 files should remain
      data2_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshotScanFiles(tablePath, data2_files, 2)
    }

    // Append data3
    withGoldenTable("snapshot-data3") { tablePath =>
      // we have overwritten files for data0 & data1; only data2 & data3 files should remain
      val data2_data3_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshotScanFiles(tablePath, data2_data3_files, 3)
    }

    // Delete data2 files
    withGoldenTable("snapshot-data2-deleted") { tablePath =>
      // we have overwritten files for data0 & data1, and deleted data2 files; only data3 files
      // should remain
      val data3_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
        .filterNot(f => data2_files.exists(_.getName == f.getName))
      verifySnapshotScanFiles(tablePath, data3_files, 4)
    }

    // Repartition into 2 files
    withGoldenTable("snapshot-repartitioned") { tablePath =>
      val snapshot = latestSnapshot(tablePath)
      assert(snapshot.getVersion(defaultEngine) == 5)
      val scanFileRows = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine).build())
      assert(scanFileRows.length == 2)
    }

    // Vacuum
    withGoldenTable("snapshot-vacuumed") { tablePath =>
      // all remaining dir data files should be needed for current snapshot version
      // vacuum doesn't change the snapshot version
      verifySnapshotScanFiles(tablePath, getDirDataFiles(tablePath), 5)
    }
  }

  test("DV cases with same path different DV keys") {
    val snapshot = latestSnapshot(goldenTablePath("log-replay-dv-key-cases"))
    val scanFileRows = collectScanFileRows(
      snapshot.getScanBuilder(defaultEngine).build()
    )
    assert(scanFileRows.length == 1) // there should only be 1 add file
    val dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFileRows.head)
    assert(dv.getCardinality == 3) // dv cardinality should be 3
  }

  test("special characters in path") {
    withGoldenTable("log-replay-special-characters-a") { path =>
      val snapshot = latestSnapshot(path)
      val scanFileRows = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine).build()
      )
      assert(scanFileRows.isEmpty)
    }
    withGoldenTable("log-replay-special-characters-b") { path =>
      val snapshot = latestSnapshot(path)
      val scanFileRows = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine).build()
      )
      assert(scanFileRows.length == 1)
      val addFileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRows.head)
      // get the relative path to compare
      assert(new File(addFileStatus.getPath).getName == "special p@#h")
    }
  }

  // TODO we need to canonicalize path during log replay see issue #2213
  ignore("path should be canonicalized - normal characters") {
    Seq("canonicalized-paths-normal-a", "canonicalized-paths-normal-b").foreach { path =>
      val snapshot = latestSnapshot(goldenTablePath(path))
      assert(snapshot.getVersion(defaultEngine) == 1)
      val scanFileRows = collectScanFileRows(snapshot.getScanBuilder(defaultEngine).build())
      assert(scanFileRows.isEmpty)
    }
  }

  ignore("path should be canonicalized - special characters") {
    Seq("canonicalized-paths-special-a", "canonicalized-paths-special-b").foreach { path =>
      val snapshot = latestSnapshot(goldenTablePath(path))
      assert(snapshot.getVersion(defaultEngine) == 1)
      val scanFileRows = collectScanFileRows(snapshot.getScanBuilder(defaultEngine).build())
      assert(scanFileRows.isEmpty)
    }
  }

  // from DeltaDataReaderSuite in standalone
  test("escaped chars sequences in path") {
    checkTable(
      path = goldenTablePath("data-reader-escaped-chars"),
      expectedAnswer = TestRow("foo1", "bar+%21") :: TestRow("foo2", "bar+%22") ::
        TestRow("foo3", "bar+%23") :: Nil
    )
  }

  test("delete and re-add same file in different transactions") {
    val path = goldenTablePath("delete-re-add-same-file-different-transactions")
    val snapshot = latestSnapshot(path)
    val scan = snapshot.getScanBuilder(defaultEngine).build()

    val foundFiles = collectScanFileRows(scan).map(InternalScanFileUtils.getAddFileStatus)

    assert(foundFiles.length == 2)
    assert(foundFiles.map(_.getPath.split('/').last).toSet == Set("foo", "bar"))

    // We added two add files with the same path `foo`. The first should have been removed.
    // The second should remain, and should have a hard-coded modification time of 1700000000000L
    assert(
      foundFiles.find(_.getPath.endsWith("foo")).exists(_.getModificationTime == 1700000000000L))
  }

  test("get the last transaction version for appID") {
    val unresolvedPath = goldenTablePath("deltalog-getChanges")
    val table = Table.forPath(defaultEngine, unresolvedPath)

    val snapshot = table.getLatestSnapshot(defaultEngine)
    assert(snapshot.isInstanceOf[SnapshotImpl])
    val snapshotImpl = snapshot.asInstanceOf[SnapshotImpl]

    assert(snapshotImpl.getLatestTransactionVersion(defaultEngine, "fakeAppId") === Optional.of(3L))
    assert(!snapshotImpl.getLatestTransactionVersion(defaultEngine, "nonExistentAppId").isPresent)
  }
}
