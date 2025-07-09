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

import io.delta.kernel.Table
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.{Engine, ExpressionHandler, FileSystemClient}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.Checkpointer
import io.delta.kernel.internal.coordinatedcommits.CommitCoordinatorClientHandler
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import java.io.File
import java.nio.file.Files
import java.util
import java.util.Optional

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession

/**
 * Suite to test the engine metrics while replaying logs for getting the table protocol and
 * metadata (P&M) and scanning files. The metrics include how many files delta files, checkpoint
 * files read, size of checkpoint read set, and how many times `_last_checkpoint` is read etc.
 *
 * The goal is to test the behavior of calls to `readJsonFiles` and `readParquetFiles` that
 * Kernel makes. This calls determine the performance.
 */
class LogReplayEngineMetricsSuite extends AnyFunSuite with TestUtils {

  // Disable writing checksums for this test suite
  // This test suite checks the files read when loading the P&M, however, with the crc optimization
  // if crc are available, crc will be the only files read.
  // We want to test the P&M loading when CRC are not available in the tests.
  // Tests for tables with available CRC are included using resource test tables (and thus are
  // unaffected by changing our confs for writes).
  spark.conf.set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, false)

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  private def withTempDirAndMetricsEngine(f: (String, MetricsEngine) => Unit): Unit = {
    val engine = new MetricsEngine(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })
    withTempDir { dir => f(dir.getAbsolutePath, engine) }
  }

  private def loadPandMCheckMetrics(
      snapshotFetchCall: => StructType,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = null,
      expChecksumReadSet: Seq[Long] = null): Unit = {
    engine.resetMetrics()
    snapshotFetchCall

    assertMetrics(
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expChecksumReadSet = expChecksumReadSet)
  }

  private def loadScanFilesCheckMetrics(
      engine: MetricsEngine,
      table: Table,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expLastCheckpointReadCalls: Option[Int] = None): Unit = {
    engine.resetMetrics()
    val scan = table.getLatestSnapshot(engine).getScanBuilder(engine).build()
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

  def assertMetrics(
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = null,
      expLastCheckpointReadCalls: Option[Int] = None,
      expChecksumReadSet: Seq[Long] = null): Unit = {
    val actualJsonVersionsRead = engine.getJsonHandler.getVersionsRead
    val actualParquetVersionsRead = engine.getParquetHandler.getVersionsRead

    assert(
      actualJsonVersionsRead === expJsonVersionsRead, s"Expected to read json versions " +
        s"$expJsonVersionsRead but read $actualJsonVersionsRead"
    )
    assert(
      actualParquetVersionsRead === expParquetVersionsRead, s"Expected to read parquet " +
        s"versions $expParquetVersionsRead but read $actualParquetVersionsRead"
    )

    if (expParquetReadSetSizes != null) {
      val actualParquetReadSetSizes = engine.getParquetHandler.checkpointReadRequestSizes
      assert(
        actualParquetReadSetSizes === expParquetReadSetSizes, s"Expected parquet read set sizes " +
          s"$expParquetReadSetSizes but read $actualParquetReadSetSizes"
      )
    }

    expLastCheckpointReadCalls.foreach { expCalls =>
      val actualCalls = engine.getJsonHandler.getLastCheckpointMetadataReadCalls
      assert(actualCalls === expCalls,
        s"Expected to read last checkpoint metadata $expCalls times but read $actualCalls times")
    }

    if (expChecksumReadSet != null) {
      val actualChecksumReadSet = engine.getJsonHandler.checksumsRead
      assert(
        actualChecksumReadSet === expChecksumReadSet, s"Expected checksum read set " +
          s"$expChecksumReadSet but read $actualChecksumReadSet"
      )
    }
  }

  private def appendCommit(path: String): Unit =
    spark.range(10).write.format("delta").mode("append").save(path)

  private def checkpoint(path: String, actionsPerFile: Int): Unit = {
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

      val table = Table.forPath(engine, path)
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = 9L to 0L by -1L,
        expParquetVersionsRead = Nil
      )
    }
  }

  test("no hint, existing checkpoint, reads all files up to that checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(engine, path)
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
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

      val table = Table.forPath(engine, path)
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = 16L to 13L by -1L,
        expParquetVersionsRead = Nil)
    }
  }

  test("hint with no new commits, should read no files") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) {
        appendCommit(path)
      }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema(engine)

      // A hint is now saved at v14
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil)
    }
  }

  test("hint with no P or M updates") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema(engine)

      // A hint is now saved at v14

      // Case: only one version change
      appendCommit(path) // v15
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = Seq(15),
        expParquetVersionsRead = Nil)

      // A hint is now saved at v15

      // Case: several version changes
      for (_ <- 16 to 19) { appendCommit(path) }
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = 19L to 16L by -1L,
        expParquetVersionsRead = Nil)

      // A hint is now saved at v19

      // Case: [delta-io/delta#2262] [Fix me!] Read the entire checkpoint at v20, even if v20.json
      // and v19 hint are available
      appendCommit(path) // v20
      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(20))
    }
  }

  test("hint with a P or M update") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 3) { appendCommit(path) }

      val table = Table.forPath(engine, path)

      table.getLatestSnapshot(engine).getSchema(engine)

      // A hint is now saved at v3

      // v4 changes the metadata (schema)
      spark.range(10)
        .withColumn("col1", col("id"))
        .write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(path)

      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
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

      loadPandMCheckMetrics(
        table.getLatestSnapshot(engine).getSchema(engine),
        engine,
        expJsonVersionsRead = Seq(5),
        expParquetVersionsRead = Nil)
    }
  }

  test("read a table with multi-part checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      for (_ <- 0 to 14) { appendCommit(path) }

      // there should be one checkpoint file at version 10
      loadScanFilesCheckMetrics(
        engine,
        Table.forPath(engine, path),
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
        Table.forPath(engine, path),
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(14),
        // we read the checkpoint twice: once for the P &M and once for the scan files
        expParquetReadSetSizes = Seq(8, 8))
    }
  }

  Seq(true, false).foreach { deleteLastCheckpointMetadataFile =>
    test("ensure `_last_checkpoint` is tried to read only once when " +
      s"""${if (deleteLastCheckpointMetadataFile) "not exists" else "valid file exists"}""") {
      withTempDirAndMetricsEngine { (path, tc) =>
        for (_ <- 0 to 14) { appendCommit(path) }

        if (deleteLastCheckpointMetadataFile) {
          assert(Files.deleteIfExists(new File(path, "_delta_log/_last_checkpoint").toPath))
        }

        // there should be one checkpoint file at version 10
        loadScanFilesCheckMetrics(
          tc,
          Table.forPath(tc, path),
          expJsonVersionsRead = 14L to 11L by -1L,
          expParquetVersionsRead = Seq(10),
          // we read the checkpoint twice: once for the P &M and once for the scan files
          expParquetReadSetSizes = Seq(1, 1),
          // We try to read `_last_checkpoint` once. If it doesn't exist, we don't try reading
          // again. If it exists, we succeed reading in the first time
          expLastCheckpointReadCalls = Some(1)
        )
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for loading P & M  through checksums files                                            //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  Seq(-1L, 3L, 4L).foreach { version => // -1 means latest version
    test(s"checksum found at the read version: ${if (version == 1) "latest" else version}") {
      withTempDirAndMetricsEngine { (_, engine) =>
        val goldenTable = getTestResourceFilePath("stream_table_optimize")
        val table = Table.forPath(engine, goldenTable)

        loadPandMCheckMetrics(
          version match {
            case -1 => table.getLatestSnapshot(engine).getSchema(engine)
            case ver => table.getSnapshotAsOfVersion(engine, ver).getSchema(engine)
          },
          engine,
          // shouldn't need to read commit or checkpoint files as P&M are found through checksum
          expJsonVersionsRead = Nil,
          expParquetVersionsRead = Nil,
          expParquetReadSetSizes = Nil,
          expChecksumReadSet = Seq(if (version == -1) 6 else version))
      }
    }
  }

  test("checksum not found at the read version, but found at a previous version") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // copy the golden table with crc files to the temp dir delete the checksum files
      // for version 5 and 6
      copyTable(getTestResourceFilePath("stream_table_optimize"), path)
      Seq(5L, 6L).foreach { version =>
        val crcFile = new File(path, f"_delta_log/$version%020d.crc")
        assert(Files.deleteIfExists(crcFile.toPath))
      }

      loadPandMCheckMetrics(
        Table.forPath(engine, path)
          .getLatestSnapshot(engine).getSchema(engine),
        engine,
        // We find the checksum from crc at version 4, but still read commit files 5 and 6
        // to find the P&M which could have been updated in version 5 and 6.
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 6, then we do a listing of
        // last 100 crc files and read the latest one which is version 4 (as version 5 is deleted)
        expChecksumReadSet = Seq(6, 4))


      // now try to load version 3 and it should get P&M from checksum files only
      loadPandMCheckMetrics(
        Table.forPath(engine, path)
          .getSnapshotAsOfVersion(engine, 3 /* versionId */).getSchema(engine),
        engine,
        // We find the checksum from crc at version 3, so shouldn't read anything else
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(3))
    }
  }

  test("checksum not found at the read version, but found at a previous version; " +
    "checksum exists after version queried") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // copy the golden table with crc files to the temp dir delete the checksum files
      // for version 5
      copyTable(getTestResourceFilePath("stream_table_optimize"), path)
      val crcFile = new File(path, f"_delta_log/${5L}%020d.crc")
      assert(Files.deleteIfExists(crcFile.toPath))
      // Now table has CRC present for versions 0, 1, 2, 3, 4, 6 (missing for version 5)

      loadPandMCheckMetrics(
        Table.forPath(engine, path)
          .getSnapshotAsOfVersion(engine, 5 /* versionId */).getSchema(engine),
        engine,
        // We find the checksum from crc at version 4, but still read commit file 5
        // to find the P&M which could have been updated in version 5
        expJsonVersionsRead = Seq(5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 5, then we do a listing of
        // last 100 crc files and read the latest one which is version 4 (as version 5 is deleted)
        expChecksumReadSet = Seq(5, 4))
    }
  }

  test("checksum not found at the read version, but uses snapshot hint lower bound") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // copy the golden table with crc files to the temp dir delete the checksum files
      // for version 3 to 6
      copyTable(getTestResourceFilePath("stream_table_optimize"), path)
      (3 to 6).foreach { version =>
        val crcFile = new File(path, f"_delta_log/$version%020d.crc")
        assert(Files.deleteIfExists(crcFile.toPath))
      }

      val table = Table.forPath(engine, path)

      loadPandMCheckMetrics(
        table.getSnapshotAsOfVersion(engine, 4 /* versionId */).getSchema(engine),
        engine,
        // There are no checksum files for versions 4. Latest is at version 2.
        // We need to read the commit files 3 and 4 to get the P&M in addition the P&M from
        // checksum file at version 2
        expJsonVersionsRead = Seq(4, 3),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 4 which doesn't exists,
        // then we do a listing of last 100 crc files and read the latest
        // one which is version 2 (as version 3-6 are deleted)
        expChecksumReadSet = Seq(4, 2))
      // read version 4 which sets the snapshot P&M hint to 4

      // now try to load version 6 and we expect no checksums are read
      loadPandMCheckMetrics(
        table.getSnapshotAsOfVersion(engine, 6 /* versionId */).getSchema(engine),
        engine,
        // We have snapshot P&M hint at version 4, and no checksum after 2
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First we attempt to read at version 6, then we do a listing of last 100 crc files
        // bound by the snapshot hint which is at version 4 and we don't try to read checksums
        // beyond version 4
        expChecksumReadSet = Seq(6))
    }
  }
}

////////////////////
// Helper Classes //
////////////////////

/** An engine that records the Delta commit (.json) and checkpoint (.parquet) files read */
class MetricsEngine(config: Configuration) extends Engine {
  private val impl = DefaultEngine.create(config)
  private val jsonHandler = new MetricsJsonHandler(config)
  private val parquetHandler = new MetricsParquetHandler(config)

  def resetMetrics(): Unit = {
    jsonHandler.resetMetrics()
    parquetHandler.resetMetrics()
  }

  override def getExpressionHandler: ExpressionHandler = impl.getExpressionHandler

  override def getJsonHandler: MetricsJsonHandler = jsonHandler

  override def getFileSystemClient: FileSystemClient = impl.getFileSystemClient

  override def getParquetHandler: MetricsParquetHandler = parquetHandler
}

/**
 * Helper trait which wraps an underlying json/parquet read and collects the versions (e.g. 10.json,
 * 10.checkpoint.parquet) read
 */
trait FileReadMetrics { self: Object =>
  // number of times read is requested on `_last_checkpoint`
  private var lastCheckpointMetadataReadCalls = 0

  val checksumsRead = new ArrayBuffer[Long]() // versions of checksum files read

  private val versionsRead = ArrayBuffer[Long]()

  // Number of checkpoint files requested read in each readParquetFiles call
  val checkpointReadRequestSizes = new ArrayBuffer[Long]()

  private def updateVersionsRead(fileStatus: FileStatus): Unit = {
    val path = new Path(fileStatus.getPath)
    if (FileNames.isCommitFile(path.getName) || FileNames.isCheckpointFile(path.getName)) {
      val version = FileNames.getFileVersion(path)

      // We may split json/parquet reads, so don't record the same file multiple times
      if (!versionsRead.contains(version)) {
        versionsRead += version
      }
    } else if (Checkpointer.LAST_CHECKPOINT_FILE_NAME.equals(path.getName)) {
      lastCheckpointMetadataReadCalls += 1
    } else if (FileNames.isChecksumFile(path)) {
      checksumsRead += FileNames.getFileVersion(path)
    }
  }

  def getVersionsRead: Seq[Long] = versionsRead

  def getLastCheckpointMetadataReadCalls: Int = lastCheckpointMetadataReadCalls

  def resetMetrics(): Unit = {
    lastCheckpointMetadataReadCalls = 0
    versionsRead.clear()
    checkpointReadRequestSizes.clear()
    checksumsRead.clear()
  }

  def collectReadFiles(fileIter: CloseableIterator[FileStatus]): CloseableIterator[FileStatus] = {
    fileIter.map(file => {
      updateVersionsRead(file)
      file
    })
  }
}

/** A JsonHandler that collects metrics on the Delta commit (.json) files read */
class MetricsJsonHandler(config: Configuration)
    extends DefaultJsonHandler(config)
    with FileReadMetrics {

  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    super.readJsonFiles(collectReadFiles(fileIter), physicalSchema, predicate)
  }
}

/** A ParquetHandler that collects metrics on the Delta checkpoint (.parquet) files read */
class MetricsParquetHandler(config: Configuration)
    extends DefaultParquetHandler(config)
    with FileReadMetrics {

  override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    val fileReadSet = fileIter.toSeq
    checkpointReadRequestSizes += fileReadSet.size
    super.readParquetFiles(
      collectReadFiles(toCloseableIterator(fileReadSet.iterator)),
      physicalSchema,
      predicate)
  }
}
