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
import io.delta.kernel.Table
import io.delta.kernel.engine.{ExpressionHandler, FileSystemClient, Engine}
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.defaults.engine.{DefaultJsonHandler, DefaultParquetHandler, DefaultEngine}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

import java.util.Optional
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer

class LogReplayMetricsSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  private def withTempDirAndTableClient(f: (File, MetricsEngine) => Unit): Unit = {
    val tableClient = new MetricsEngine(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })
    withTempDir { dir => f(dir, tableClient) }
  }

  private def loadPandMCheckMetrics(
      tableClient: MetricsEngine,
      table: Table,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = Nil): Unit = {
    tableClient.resetMetrics()
    table.getLatestSnapshot(tableClient).getSchema(tableClient)

    assertMetrics(
      tableClient,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes)
  }

  private def loadScanFilesCheckMetrics(
      tableClient: MetricsEngine,
      table: Table,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long]): Unit = {
    tableClient.resetMetrics()
    val scan = table.getLatestSnapshot(tableClient).getScanBuilder(tableClient).build()
    // get all scan files and iterate through them to trigger the metrics collection
    val scanFiles = scan.getScanFiles(tableClient)
    while (scanFiles.hasNext) scanFiles.next()

    assertMetrics(
      tableClient,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes)
  }

  def assertMetrics(
      tableClient: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long]): Unit = {
    val actualJsonVersionsRead = tableClient.getJsonHandler.getVersionsRead
    val actualParquetVersionsRead = tableClient.getParquetHandler.getVersionsRead

    assert(
      actualJsonVersionsRead === expJsonVersionsRead, s"Expected to read json versions " +
        s"$expJsonVersionsRead but read $actualJsonVersionsRead"
    )
    assert(
      actualParquetVersionsRead === expParquetVersionsRead, s"Expected to read parquet " +
        s"versions $expParquetVersionsRead but read $actualParquetVersionsRead"
    )

    if (expParquetReadSetSizes.nonEmpty) {
      val actualParquetReadSetSizes = tableClient.getParquetHandler.checkpointReadRequestSizes
      assert(
        actualParquetReadSetSizes === expParquetReadSetSizes, s"Expected parquet read set sizes " +
          s"$expParquetReadSetSizes but read $actualParquetReadSetSizes"
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
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 9) { appendCommit(path) }

      val table = Table.forPath(tc, path)
      loadPandMCheckMetrics(tc, table, 9L to 0L by -1L, Nil)
    }
  }

  test("no hint, existing checkpoint, reads all files up to that checkpoint") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(tc, path)
      loadPandMCheckMetrics(tc, table, 14L to 11L by -1L, Seq(10), Seq(1))
    }
  }

  test("no hint, existing checkpoint, newer P & M update, reads up to P & M commit") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

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

      val table = Table.forPath(tc, path)
      loadPandMCheckMetrics(tc, table, 16L to 13L by -1L, Nil)
    }
  }

  test("hint with no new commits, should read no files") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) {
        appendCommit(path)
      }

      val table = Table.forPath(tc, path)

      table.getLatestSnapshot(tc).getSchema(tc)

      // A hint is now saved at v14

      loadPandMCheckMetrics(tc, table, Nil, Nil)
    }
  }

  test("hint with no P or M updates") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(tc, path)

      table.getLatestSnapshot(tc).getSchema(tc)

      // A hint is now saved at v14

      // Case: only one version change
      appendCommit(path) // v15
      loadPandMCheckMetrics(tc, table, Seq(15), Nil)

      // A hint is now saved at v15

      // Case: several version changes
      for (_ <- 16 to 19) { appendCommit(path) }
      loadPandMCheckMetrics(tc, table, 19L to 16L by -1L, Nil)

      // A hint is now saved at v19

      // Case: [delta-io/delta#2262] [Fix me!] Read the entire checkpoint at v20, even if v20.json
      // and v19 hint are available
      appendCommit(path) // v20
      loadPandMCheckMetrics(tc, table, Nil, Seq(20))
    }
  }

  test("hint with a P or M update") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 3) { appendCommit(path) }

      val table = Table.forPath(tc, path)

      table.getLatestSnapshot(tc).getSchema(tc)

      // A hint is now saved at v3

      // v4 changes the metadata (schema)
      spark.range(10)
        .withColumn("col1", col("id"))
        .write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(path)

      loadPandMCheckMetrics(tc, table, Seq(4), Nil)

      // a hint is now saved at v4

      // v5 changes the protocol (which also updates the metadata)
      spark.sql(s"""
          |ALTER TABLE delta.`$path` SET TBLPROPERTIES (
          |  'delta.minReaderVersion' = '2',
          |  'delta.minWriterVersion' = '5',
          |  'delta.columnMapping.mode' = 'name'
          |)
          |""".stripMargin)

      loadPandMCheckMetrics(tc, table, Seq(5), Nil)
    }
  }

  test("read a table with multi-part checkpoint") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) { appendCommit(path) }

      // there should be one checkpoint file at version 10
      loadScanFilesCheckMetrics(
        tc,
        Table.forPath(tc, path),
        expJsonVersionsRead = 14L to 11L by -1L,
        expParquetVersionsRead = Seq(10),
        // we read the checkpoint twice: once for the P &M and once for the scan files
        expParquetReadSetSizes = Seq(1, 1))

      // create a multi-part checkpoint
      checkpoint(path, actionsPerFile = 2)

      // Reset metrics.
      tc.resetMetrics()

      // expect the Parquet read set to contain one request with size of 15
      loadScanFilesCheckMetrics(
        tc,
        Table.forPath(tc, path),
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(14),
        // we read the checkpoint twice: once for the P &M and once for the scan files
        expParquetReadSetSizes = Seq(15, 15))
    }
  }
}

////////////////////
// Helper Classes //
////////////////////

/** A table client that records the Delta commit (.json) and checkpoint (.parquet) files read */
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
    }
  }

  def getVersionsRead: Seq[Long] = versionsRead

  def resetMetrics(): Unit = {
    versionsRead.clear()
    checkpointReadRequestSizes.clear()
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
