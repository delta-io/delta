import java.util.Optional

import io.delta.golden.GoldenTableUtils
import io.delta.kernel.Snapshot
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.test.AbstractTableManagerAdapter
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.internal.PaginatedScanImpl
import io.delta.kernel.internal.replay.{PageToken, PaginatedScanFilesIteratorImpl}

import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory};

class PaginatedScanSuite extends AnyFunSuite with TestUtils
    with ExpressionTestUtils with SQLHelper with DeltaTableWriteSuiteBase {

  private val logger = LoggerFactory.getLogger(classOf[PaginatedScanSuite])

  private object TestConstants {
    val DEFAULT_BATCH_SIZE = 5
  }

  val tableManager: AbstractTableManagerAdapter = getTableManagerAdapter

  // Custom engine with default batch size 5
  private val customEngine: DefaultEngine = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set(
      "delta.kernel.default.json.reader.batch-size",
      TestConstants.DEFAULT_BATCH_SIZE.toString)
    hadoopConf.set(
      "delta.kernel.default.parquet.reader.batch-size",
      TestConstants.DEFAULT_BATCH_SIZE.toString)
    DefaultEngine.create(hadoopConf)
  }

  private def createPaginatedScan(
      tablePath: String,
      tableVersion: Optional[Long],
      pageSize: Long,
      pageToken: Optional[io.delta.kernel.data.Row] = Optional.empty()): PaginatedScanImpl = {

    val resolvedTableAdapter = {
      if (tableVersion.isPresent) {
        tableManager.getResolvedTableAdapterAtVersion(customEngine, tablePath, tableVersion.get())
      } else {
        tableManager.getResolvedTableAdapterAtLatest(customEngine, tablePath)
      }
    }

    resolvedTableAdapter.getScanBuilder()
      .buildPaginated(pageSize, pageToken)
      .asInstanceOf[PaginatedScanImpl]
  }

  // Helper function to collect all batches from a paginated iterator for current page
  private def collectPaginatedBatches(
      paginatedIter: io.delta.kernel.PaginatedScanFilesIterator)
      : collection.mutable.Buffer[FilteredColumnarBatch] = {
    val batches = collection.mutable.Buffer[FilteredColumnarBatch]()
    while (paginatedIter.hasNext) {
      val batch = paginatedIter.next()
      batches += batch
    }
    batches
  }

  private def validatePageResults(
      batches: collection.mutable.Buffer[FilteredColumnarBatch],
      expectedFileCount: Int,
      expectedBatchCount: Int,
      testName: String): Unit = {
    assert(batches.nonEmpty)

    val fileCounts: Seq[Long] = batches.map(_.getPreComputedNumSelectedRows.get().toLong)
    val totalFileCountsReturned = fileCounts.sum

    assert(fileCounts.length == expectedBatchCount)
    assert(totalFileCountsReturned == expectedFileCount)

    logger.info(s"$testName: Total num batches returned in page one = ${fileCounts.length}")
    logger.info(s"$testName: Total num Parquet Files fetched in page one = " +
      s"$totalFileCountsReturned")
  }

  private def validatePageToken(
      paginatedIter: io.delta.kernel.PaginatedScanFilesIterator,
      expectedLogFilePath: String,
      expectedRowIndex: Long,
      testName: String): Unit = {
    val lastReadLogFilePath = PageToken.fromRow(paginatedIter.getCurrentPageToken)
      .getLastReadLogFilePath
    val lastReturnedRowIndex = PageToken.fromRow(paginatedIter.getCurrentPageToken)
      .getLastReturnedRowIndex

    assert(lastReadLogFilePath.endsWith(expectedLogFilePath))
    assert(lastReturnedRowIndex == expectedRowIndex)

    logger.info(s"$testName: New PageToken: lastReadLogFileName = $lastReadLogFilePath")
    logger.info(s"$testName: New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
  }

  private def runSinglePaginationTestCase(
      testName: String,
      tablePath: String,
      tableVersion: Optional[Long],
      pageSize: Long,
      expectedFileCount: Int,
      expectedBatchCount: Int,
      expectedLogFileName: String,
      expectedRowIndex: Int): Unit = {

    val paginatedScan = createPaginatedScan(tablePath, tableVersion, pageSize)

    val paginatedIter = paginatedScan.getScanFiles(customEngine)
    val returnedBatchesInPage = collectPaginatedBatches(paginatedIter)

    validatePageResults(returnedBatchesInPage, expectedFileCount, expectedBatchCount, testName)
    validatePageToken(paginatedIter, expectedLogFileName, expectedRowIndex, testName)

    paginatedIter.close()
  }

  private def runPaginationTestCasesForFirstPage(
      tablePath: String,
      tableVersion: Optional[Long],
      testCases: Seq[(String, Int, Int, Int, String, Int)]): Unit = {
    testCases.foreach {
      case (
            testName,
            pageSize,
            expectedFileCount,
            expectedBatchCount,
            expectedLogFileName,
            expectedRowIndex) =>
        runSinglePaginationTestCase(
          testName = testName,
          tablePath = tablePath,
          tableVersion = tableVersion,
          pageSize = pageSize,
          expectedFileCount = expectedFileCount,
          expectedBatchCount = expectedBatchCount,
          expectedLogFileName = expectedLogFileName,
          expectedRowIndex = expectedRowIndex)
    }
  }

  // TODO: test call hasNext() twice

  test("Read First Page: basic pagination with single JSON file") {
    withGoldenTable("kernel-pagination-oneJSON") { tablePath =>
      /**
       *  Log Segment List:
       *  00000000000000000000.json contains 13 rows, 10 AddFiles in total
       *  Non-AddFile rows: CommitInfo, Protocol, and Metadata
       *  AddFile rows: 10 AddFiles
       * Batch 1: 5 rows, 2 selected AddFiles (and 3 non AddFile rows)
       * Batch 2: 5 rows, 5 selected AddFiles
       * Batch 3: 3 rows, 3 selected AddFiles
       */

      // spark.range(0, 100, 1, 10).write.format("delta").save(tablePath)

      val JSONFile0 = "00000000000000000000.json"
      val testCases = Seq(
        (
          "Page size 1",
          1 /* page size */,
          2 /* expectedFileCount */,
          1 /* expectedBatchCount */,
          JSONFile0,
          4),
        ("Page size 2", 2, 2, 1, JSONFile0, 4),
        ("Page size 4", 4, 7, 2, JSONFile0, 9), // batch size go over page limit
        ("Page size 7", 7, 7, 2, JSONFile0, 9), // batch size is equal to page limit
        ("Page size 8", 8, 10, 3, JSONFile0, 12),
        ("Page size 20", 20, 10, 3, JSONFile0, 12) // page limit isn't reached
      )

      runPaginationTestCasesForFirstPage(tablePath, Optional.empty[Long](), testCases)

      // TODO: test all page requests
    }
  }

  test("Read First Page: basic pagination with multiple JSON files") {
    withGoldenTable("kernel-pagination-allJSONs") { tablePath =>
      /**
       * Log Segment List:
       * 00000000000000000000.json : 8 rows (5 AddFile row + 3 non-AddFile rows)
       * 00000000000000000001.json : 6 rows (5 AddFile row + 1 non-AddFile row)
       * 00000000000000000002.json : 6 rows (5 AddFile row + 1 non-AddFile row)
       *
       * 00000000000000000002.json contains 2 batches, 5 active AddFiles in total
       * Batch 1: 5 rows, 4 selected AddFiles
       * Batch 2: 1 rows, 1 selected AddFiles
       *
       * 00000000000000000001.json contains 2 batches, 5 active AddFiles in total
       * Batch 1: 5 rows, 4 selected AddFiles
       * Batch 2: 1 rows, 1 selected AddFiles
       *
       * 00000000000000000000.json contains 2 batches, 5 active AddFiles in total
       * Batch 1: 5 rows, 2 selected AddFiles
       * Batch 2: 3 rows, 3 selected AddFiles
       */

      /**
       * spark.range( 0, 50, 1, 5).write.format("delta").save(tablePath)
       * spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)
       * spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)
       */

      val JSONFile0 = "00000000000000000000.json"
      val JSONFile1 = "00000000000000000001.json"
      val JSONFile2 = "00000000000000000002.json"
      val testCases = Seq(
        (
          "Page size 1",
          1 /* page size */,
          4 /* expectedFileCount */,
          1 /* expectedBatchCount */,
          JSONFile2,
          4),
        ("Page size 4", 4, 4, 1, JSONFile2, 4),
        ("Page size 5", 5, 5, 2, JSONFile2, 5), // batch size is equal to page limit
        ("Page size 7", 7, 9, 3, JSONFile1, 4), // batch size is goes over page limit
        ("Page size 8", 8, 9, 3, JSONFile1, 4),
        ("Page size 18", 18, 15, 6, JSONFile0, 7))

      runPaginationTestCasesForFirstPage(tablePath, Optional.empty[Long](), testCases)

      // TODO: test all page requests
    }
  }

  test("Read First Page: basic pagination with one checkpoint file") {
    withGoldenTable("kernel-pagination-oneCheckpoint") { tablePath =>
      /**
       * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows in total
       * Batch 1: 5 rows, 5 selected AddFiles
       * Batch 2: 5 rows, 5 selected AddFiles
       * Batch 3: 5 rows, 5 selected AddFiles
       * Batch 4: 5 rows, 3 selected AddFiles
       * Batch 5: 4 rows, 4 selected AddFiles
       */
      val checkpoint10 = "00000000000000000010.checkpoint.parquet"

      val testCases = Seq(
        (
          "Page size 1",
          1 /* page size */,
          5 /* expectedFileCount */,
          1 /* expectedBatchCount */,
          checkpoint10,
          4 /* row index in page token*/ ),
        ("Page size 10", 10, 10, 2, checkpoint10, 9),
        ("Page size 12", 12, 15, 3, checkpoint10, 14), // batch size goes over limit
        ("Large Page size", 100, 22, 5, checkpoint10, 23) // page limit isn't reached
      )

      runPaginationTestCasesForFirstPage(tablePath, Optional.of(10L), testCases)

      // TODO: test all page requests
    }
  }

  test("Read First Page: basic pagination with one checkpoint file " +
    "and multiple JSON files") {
    withGoldenTable("kernel-pagination-oneCheckpoint") { tablePath =>
      /*
    for (i <- 0 until 10) {
      val mode = if (i == 0) "overwrite" else "append"
      spark.range(i * 10, (i + 1) * 10, 1, 2)
        .write.format("delta").mode(mode).save(tablePath)
    }

    // Force checkpoint creation
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    deltaLog.checkpoint()

    // Add more commits after checkpoint
    for (i <- 10 until 13) {
      spark.range(i * 10, (i + 1) * 10, 1, 2)
        .write.format("delta").mode("append").save(tablePath)
    }
       */

      /**
       * Log segment list:
       * 00000000000000000010.checkpoint.parquet
       * 00000000000000000011.json
       * 00000000000000000012.json
       *
       * 00000000000000000012.json contains 1 batch, 2 active AddFiles in total
       * Batch 1: 3 rows, 2 selected AddFiles
       *
       * 00000000000000000011.json contains 1 batch, 2 active AddFiles in total
       * Batch 1: 3 rows, 2 selected AddFiles
       *
       * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows in total
       * Batch 1: 5 rows, 5 selected AddFiles
       * Batch 2: 5 rows, 5 selected AddFiles
       * Batch 3: 5 rows, 5 selected AddFiles
       * Batch 4: 5 rows, 3 selected AddFiles
       * Batch 5: 4 rows, 4 selected AddFiles
       */

      val checkpoint10 = "00000000000000000010.checkpoint.parquet"
      val JSONFile11 = "00000000000000000011.json"
      val JSONFile12 = "00000000000000000012.json"

      val testCases = Seq(
        (
          "Page size 1",
          1 /* page size */,
          2 /* expectedFileCount */,
          1 /* expectedBatchCount */,
          JSONFile12,
          2 /* row index in page token*/ ),
        ("Page size 2", 2, 2, 1, JSONFile12, 2),
        ("Page size 3", 3, 4, 2, JSONFile11, 2),
        ("Page size 4", 4, 4, 2, JSONFile11, 2),
        ("Page size 8", 8, 9, 3, checkpoint10, 4),
        ("Page size 18", 18, 19, 5, checkpoint10, 14) // page limit isn't reached
      )

      runPaginationTestCasesForFirstPage(tablePath, Optional.empty[Long](), testCases)

      // TODO: test all page requests
    }
  }

  test("Read Page: basic pagination with multi-checkpoint files") {
    // TODO
  }

  test("Read Page: basic pagination with sidecar files") {
    // TODO
  }
}
