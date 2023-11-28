package io.delta.kernel.defaults

import io.delta.kernel.Table
import io.delta.kernel.client.{ExpressionHandler, FileReadContext, FileSystemClient, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.data.FileDataReadResult
import io.delta.kernel.defaults.client.{DefaultJsonHandler, DefaultParquetHandler, DefaultTableClient}
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class SnapshotManagerSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  val tableClient = new MetricsTableClient(new Configuration() {
    {
      // Set the batch sizes to small so that we get to test the multiple batch scenarios.
      set("delta.kernel.default.parquet.reader.batch-size", "2");
      set("delta.kernel.default.json.reader.batch-size", "2");
    }
  })

  protected override def beforeEach(): Unit = {
    super.beforeEach()

    tableClient.resetMetrics()
  }

  private def loadSnapshotAssertMetrics(
      table: Table,
      numJsonRead: Int,
      numParquetRead: Int): Unit = {
    tableClient.resetMetrics()
    table.getLatestSnapshot(tableClient).getSchema(tableClient)

    val actualNumJsonRead = tableClient.getJsonHandler.getNumDeltaFilesRead
    val actualNumParquetRead = tableClient.getParquetHandler.getNumCheckpointFilesRead
    assert(
      actualNumJsonRead === numJsonRead,
      s"Expected to read $numJsonRead json files, but read $actualNumJsonRead. Files read: " +
        s"${tableClient.getJsonHandler.getDeltaFilesRead.mkString("\n")}"
    )
    assert(
      actualNumParquetRead === numParquetRead,
      s"Expected to read $numParquetRead parquet files, but read $actualNumParquetRead. Files " +
          s"read: ${tableClient.getJsonHandler.getDeltaFilesRead.mkString("\n")}"
    )
  }

  test("snapshot hint: no hint, no checkpoint, reads all files") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 9) { // create 00.json to 09.json
        spark.range(10).write.format("delta").mode("append").save(path)
      }

      val table = Table.forPath(tableClient, path)
      loadSnapshotAssertMetrics(table, numJsonRead = 10, numParquetRead = 0)
    }
  }

  test("snapshot hint: no hint, existing checkpoint, reads all files up to that checkpoint") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      for (_ <- 0 to 14) { // create 00.json to 14.json; 10.checkpoint is auto created
        spark.range(10).write.format("delta").mode("append").save(path)
      }

      val table = Table.forPath(tableClient, path)
      loadSnapshotAssertMetrics(table, numJsonRead = 4, numParquetRead = 1)
    }
  }

  test("snapshot hint: hint with no P or M updates") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      def appendCommit(): Unit =
        spark.range(10).write.format("delta").mode("append").save(path)

      for (_ <- 0 to 14) { appendCommit() }

      val table = Table.forPath(tableClient, path)

      table.getLatestSnapshot(tableClient).getSchema(tableClient)

      // A hint is now saved at v14

      // Case: only one version change
      appendCommit() // v15
      loadSnapshotAssertMetrics(table, numJsonRead = 1, numParquetRead = 0)

      // A hint is now saved at v15

      // Case: several version changes
      for (_ <- 16 to 19) { appendCommit() }
      loadSnapshotAssertMetrics(table, numJsonRead = 4, numParquetRead = 0)

      // A hint is now saved at v19

      // Case: [delta-io/delta#2262] [Fix me!] Read the entire checkpoint at v20, even if v20.json
      // and v19 hint are available
      appendCommit() // v20
      loadSnapshotAssertMetrics(table, numJsonRead = 0, numParquetRead = 1)
    }
  }
}

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

trait FileReadMetrics {
  protected val filePathsRead = scala.collection.mutable.Set.empty[Path]

  def resetMetrics(): Unit = filePathsRead.clear()

  def readAndCollectMetrics(
      iter: CloseableIterator[FileDataReadResult]): CloseableIterator[FileDataReadResult] = {
    new CloseableIterator[FileDataReadResult] {
      override def close(): Unit = iter.close()

      override def hasNext: Boolean = iter.hasNext

      override def next(): FileDataReadResult = {
        val result = iter.next()
        val scanFile = result.getScanFileRow
        val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile)
        filePathsRead += new Path(fileStatus.getPath)
        result
      }
    }
  }
}

class MetricsJsonHandler(config: Configuration)
    extends DefaultJsonHandler(config)
    with FileReadMetrics {

  def getDeltaFilesRead: Seq[Path] =
    filePathsRead.filter(f => FileNames.isCommitFile(f.getName)).toSeq

  def getNumDeltaFilesRead: Int = getDeltaFilesRead.size

  override def readJsonFiles(
      fileIter: CloseableIterator[FileReadContext],
      physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
    readAndCollectMetrics(super.readJsonFiles(fileIter, physicalSchema))
  }
}

class MetricsParquetHandler(config: Configuration)
    extends DefaultParquetHandler(config)
    with FileReadMetrics {

  def getCheckpointFilesRead: Seq[Path] =
    filePathsRead.filter(f => FileNames.isCheckpointFile(f.getName)).toSeq

  def getNumCheckpointFilesRead: Int = getCheckpointFilesRead.size

  override def readParquetFiles(
      fileIter: CloseableIterator[FileReadContext],
      physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
    readAndCollectMetrics(super.readParquetFiles(fileIter, physicalSchema))
  }
}
