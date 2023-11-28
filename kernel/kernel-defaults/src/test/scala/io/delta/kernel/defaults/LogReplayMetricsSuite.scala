package io.delta.kernel.defaults

import java.io.File

import io.delta.kernel.Table
import io.delta.kernel.client.{ExpressionHandler, FileHandler, FileReadContext, FileSystemClient, TableClient}
import io.delta.kernel.data.FileDataReadResult
import io.delta.kernel.defaults.client.{DefaultJsonHandler, DefaultParquetHandler, DefaultTableClient}
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class LogReplayMetricsSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  private def withTempDirAndTableClient(f: (File, MetricsTableClient) => Unit): Unit = {
    val tableClient = new MetricsTableClient(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })
    withTempDir { dir => f(dir, tableClient) }
  }

  private def loadSnapshotAssertMetrics(
      tableClient: MetricsTableClient,
      table: Table,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long]): Unit = {
    tableClient.resetMetrics()
    table.getLatestSnapshot(tableClient).getSchema(tableClient)

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
  }

  private def appendCommit(path: String): Unit =
    spark.range(10).write.format("delta").mode("append").save(path)

  ///////////
  // Tests //
  ///////////

  test("no hint, no checkpoint, reads all files") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 9) { appendCommit(path) }

      val table = Table.forPath(tc, path)
      loadSnapshotAssertMetrics(tc, table, 9L to 0L by -1L, Nil)
    }
  }

  test("no hint, existing checkpoint, reads all files up to that checkpoint") {
    withTempDirAndTableClient { (dir, tc) =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) { appendCommit(path) }

      val table = Table.forPath(tc, path)
      loadSnapshotAssertMetrics(tc, table, 14L to 11L by -1L, Seq(10))
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
      loadSnapshotAssertMetrics(tc, table, 16L to 13L by -1L, Nil)
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

      loadSnapshotAssertMetrics(tc, table, Nil, Nil)
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
      loadSnapshotAssertMetrics(tc, table, Seq(15), Nil)

      // A hint is now saved at v15

      // Case: several version changes
      for (_ <- 16 to 19) { appendCommit(path) }
      loadSnapshotAssertMetrics(tc, table, 19L to 16L by -1L, Nil)

      // A hint is now saved at v19

      // Case: [delta-io/delta#2262] [Fix me!] Read the entire checkpoint at v20, even if v20.json
      // and v19 hint are available
      appendCommit(path) // v20
      loadSnapshotAssertMetrics(tc, table, Nil, Seq(20))
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

      loadSnapshotAssertMetrics(tc, table, Seq(4), Nil)

      // a hint is now saved at v4

      // v5 changes the protocol (which also updates the metadata)
      spark.sql(s"""
          |ALTER TABLE delta.`$path` SET TBLPROPERTIES (
          |  'delta.minReaderVersion' = '2',
          |  'delta.minWriterVersion' = '5',
          |  'delta.columnMapping.mode' = 'name'
          |)
          |""".stripMargin)

      loadSnapshotAssertMetrics(tc, table, Seq(5), Nil)
    }
  }
}

////////////////////
// Helper Classes //
////////////////////

/** A table client that records the Delta commit (.json) and checkpoint (.parquet) files read */
class MetricsTableClient(config: Configuration) extends TableClient {
  private val impl = DefaultTableClient.create(config)
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
trait FileReadMetrics { self: FileHandler =>
  private val versionsRead = scala.collection.mutable.ArrayBuffer[Long]()

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

  def resetMetrics(): Unit = versionsRead.clear()

  def readAndCollectMetrics(
      iter: CloseableIterator[FileDataReadResult]): CloseableIterator[FileDataReadResult] = {
    new CloseableIterator[FileDataReadResult] {
      override def close(): Unit = iter.close()

      override def hasNext: Boolean = iter.hasNext

      override def next(): FileDataReadResult = {
        val result = iter.next()
        val scanFile = result.getScanFileRow
        updateVersionsRead(InternalScanFileUtils.getAddFileStatus(scanFile))
        result
      }
    }
  }
}

/** A JsonHandler that collects metrics on the Delta commit (.json) files read */
class MetricsJsonHandler(config: Configuration)
    extends DefaultJsonHandler(config)
    with FileReadMetrics {

  override def readJsonFiles(
      fileIter: CloseableIterator[FileReadContext],
      physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
    readAndCollectMetrics(super.readJsonFiles(fileIter, physicalSchema))
  }
}

/** A ParquetHandler that collects metrics on the Delta checkpoint (.parquet) files read */
class MetricsParquetHandler(config: Configuration)
    extends DefaultParquetHandler(config)
    with FileReadMetrics {

  override def readParquetFiles(
      fileIter: CloseableIterator[FileReadContext],
      physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
    readAndCollectMetrics(super.readParquetFiles(fileIter, physicalSchema))
  }
}
