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
import java.nio.file.Files

import io.delta.kernel.Table
import io.delta.kernel.defaults.utils.{AbstractTestUtils, TestUtilsWithLegacyKernelAPIs, TestUtilsWithTableManagerAPIs}

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll

class LegacyLogReplayEngineMetricsSuite extends AbstractLogReplayEngineMetricsSuite
    with TestUtilsWithLegacyKernelAPIs {

  protected def loadPandMCheckMetricsForTable(
      table: Table,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = null,
      expChecksumReadSet: Seq[Long] = null,
      version: Long = -1): Unit = {
    engine.resetMetrics()

    version match {
      case -1 => table.getLatestSnapshot(engine)
      case ver => table.getSnapshotAsOfVersion(engine, ver)
    }

    assertMetrics(
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expChecksumReadSet = expChecksumReadSet)

  }

  // SnapshotHint tests only apply for the legacy APIs since in the new APIs there is no persistent
  // Table instance

  test("hint with no new commits, should read no files") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) {
        appendCommit(path)
      }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema()

      // A hint is now saved at v14
      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil)
    }
  }

  test("hint with no P or M updates") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema()

      // A hint is now saved at v14

      // Case: only one version change
      appendCommit(path) // v15
      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = Seq(15),
        expParquetVersionsRead = Nil)

      // A hint is now saved at v15

      // Case: several version changes
      for (_ <- 16 to 19) { appendCommit(path) }
      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = 19L to 16L by -1L,
        expParquetVersionsRead = Nil)

      // A hint is now saved at v19

      // Case: [delta-io/delta#2262] [Fix me!] Read the entire checkpoint at v20, even if v20.json
      // and v19 hint are available
      appendCommit(path) // v20
      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(20))
    }
  }

  test("hint with a P or M update") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 3) { appendCommit(path) }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema()

      // A hint is now saved at v3

      // v4 changes the metadata (schema)
      spark.range(10)
        .withColumn("col1", col("id"))
        .write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(path)

      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = Seq(4),
        expParquetVersionsRead = Nil)
      // a hint is now saved at v4

      // v5 changes the protocol (which also updates the metadata)
      spark.sql(s"""
                   |ALTER TABLE delta.`$path` SET TBLPROPERTIES (
                   |  'delta.minReaderVersion' = '2',
                   |  'delta.minWriterVersion' = '5',
                   |  'delta.columnMapping.mode' = 'name'
                   |)
                   |""".stripMargin)

      loadPandMCheckMetricsForTable(
        table,
        engine,
        expJsonVersionsRead = Seq(5),
        expParquetVersionsRead = Nil)
    }
  }

}

class LogReplayEngineMetricsSuite extends AbstractLogReplayEngineMetricsSuite
    with TestUtilsWithTableManagerAPIs {}

/**
 * Suite to test the engine metrics while replaying logs for getting the table protocol and
 * metadata (P&M) and scanning files. The metrics include how many files delta files, checkpoint
 * files read, size of checkpoint read set, and how many times `_last_checkpoint` is read etc.
 *
 * The goal is to test the behavior of calls to `readJsonFiles` and `readParquetFiles` that
 * Kernel makes. This calls determine the performance.
 */
trait AbstractLogReplayEngineMetricsSuite extends LogReplayBaseSuite with BeforeAndAfterAll {
  self: AbstractTestUtils =>

  // Disable writing checksums for this test suite
  // This test suite checks the files read when loading the P&M, however, with the crc optimization
  // if crc are available, crc will be the only files read.
  // We want to test the P&M loading when CRC are not available in the tests.
  // Tests for tables with available CRC are included using resource test tables (and thus are
  // unaffected by changing our confs for writes).
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, false)
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, true)
    } finally {
      super.afterAll()
    }
  }

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  def loadScanFilesCheckMetrics(
      engine: MetricsEngine,
      tablePath: String,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expLastCheckpointReadCalls: Option[Int] = None): Unit = {
    engine.resetMetrics()
    val scan = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      .getScanBuilder().build()
    // get all scan files and iterate through them to trigger the metrics collection
    val scanFiles = scan.getScanFiles(engine)
    while (scanFiles.hasNext) scanFiles.next()

    assertMetrics(
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expLastCheckpointReadCalls)
  }

  def checkpoint(path: String, actionsPerFile: Int): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> actionsPerFile.toString) {
      DeltaLog.forTable(spark, path).checkpoint()
    }
  }

  ///////////
  // Tests //
  ///////////

  test("no hint, no checkpoint, reads all files") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 9) { appendCommit(path) }

      loadPandMCheckMetrics(
        path,
        engine,
        expJsonVersionsRead = 9L to 0L by -1L,
        expParquetVersionsRead = Nil)
    }
  }

  test("no hint, existing checkpoint, reads all files up to that checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      loadPandMCheckMetrics(
        path,
        engine,
        expJsonVersionsRead = 14L to 11L by -1L,
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = Seq(1))
    }
  }

  test("no hint, existing checkpoint, newer P & M update, reads up to P & M commit") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 12) { appendCommit(path) }

      // v13 changes the protocol (which also updates the metadata)
      spark.sql(s"""
          |ALTER TABLE delta.`$path` SET TBLPROPERTIES (
          |  'delta.minReaderVersion' = '2',
          |  'delta.minWriterVersion' = '5',
          |  'delta.columnMapping.mode' = 'name'
          |)
          |""".stripMargin)

      for (_ <- 14 to 16) { appendCommit(path) }

      loadPandMCheckMetrics(
        path,
        engine,
        expJsonVersionsRead = 16L to 13L by -1L,
        expParquetVersionsRead = Nil)
    }
  }

  test("read a table with multi-part checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      // there should be one checkpoint file at version 10
      loadScanFilesCheckMetrics(
        engine,
        path,
        expJsonVersionsRead = 14L to 11L by -1L,
        expParquetVersionsRead = Seq(10),
        // we read the checkpoint twice: once for the P &M and once for the scan files
        expParquetReadSetSizes = Seq(1, 1))

      // create a multi-part checkpoint
      checkpoint(path, actionsPerFile = 2)

      // Reset metrics.
      engine.resetMetrics()

      // expect the Parquet read set to contain one request with size of 15
      loadScanFilesCheckMetrics(
        engine,
        path,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(14),
        // we read the checkpoint twice: once for the P &M and once for the scan files
        expParquetReadSetSizes = Seq(8, 8))
    }
  }

  Seq(true, false).foreach { deleteLastCheckpointMetadataFile =>
    test("ensure `_last_checkpoint` is tried to read only once when " +
      s"""${if (deleteLastCheckpointMetadataFile) "not exists" else "valid file exists"}""") {
      withTempDirAndMetricsEngine { (path, engine) =>
        for (_ <- 0 to 14) { appendCommit(path) }

        if (deleteLastCheckpointMetadataFile) {
          assert(Files.deleteIfExists(new File(path, "_delta_log/_last_checkpoint").toPath))
        }

        // there should be one checkpoint file at version 10
        loadScanFilesCheckMetrics(
          engine,
          path,
          expJsonVersionsRead = 14L to 11L by -1L,
          expParquetVersionsRead = Seq(10),
          // we read the checkpoint twice: once for the P &M and once for the scan files
          expParquetReadSetSizes = Seq(1, 1),
          // We try to read `_last_checkpoint` once. If it doesn't exist, we don't try reading
          // again. If it exists, we succeed reading in the first time
          expLastCheckpointReadCalls = Some(1))
      }
    }
  }
}
