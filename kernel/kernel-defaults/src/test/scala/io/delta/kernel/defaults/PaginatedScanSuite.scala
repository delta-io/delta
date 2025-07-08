import java.util.Optional

import io.delta.golden.GoldenTableUtils
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.internal.PaginatedScanImpl
import io.delta.kernel.internal.replay.{PageToken, PaginatedScanFilesIteratorImpl}

import org.apache.spark.sql.delta.DeltaLog

import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory};

class PaginatedScanSuite extends AnyFunSuite with TestUtils
    with ExpressionTestUtils with SQLHelper with DeltaTableWriteSuiteBase {

  private val logger = LoggerFactory.getLogger(classOf[PaginatedScanSuite])

  // Constants to replace magic numbers
  private object TestConstants {
    val DEFAULT_BATCH_SIZE = 5
  }

  // Helper function to create a custom engine with default batch size
  private def createCustomEngine(): DefaultEngine = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("delta.kernel.default.json.reader.batch-size",
      TestConstants.DEFAULT_BATCH_SIZE.toString)
    hadoopConf.set("delta.kernel.default.parquet.reader.batch-size",
      TestConstants.DEFAULT_BATCH_SIZE.toString)
    DefaultEngine.create(hadoopConf)
  }

  // Helper function to create a paginated scan
  private def createPaginatedScan(
      snapshot: io.delta.kernel.Snapshot,
      pageSize: Long,
      pageToken: Optional[io.delta.kernel.data.Row] = Optional.empty()): PaginatedScanImpl = {
    snapshot.getScanBuilder
      .buildPaginated(pageSize, pageToken)
      .asInstanceOf[PaginatedScanImpl]
  }

  // Helper function to collect all batches from a paginated iterator for current page
  private def collectPaginatedBatches(
      paginatedIter: io.delta.kernel.PaginatedScanFilesIterator):
      collection.mutable.Buffer[FilteredColumnarBatch] = {
    val batches = collection.mutable.Buffer[FilteredColumnarBatch]()
    while (paginatedIter.hasNext) {
      val batch = paginatedIter.next()
      batches += batch
    }
    batches
  }

  // Helper function to validate current page results
  private def validatePageResults(
      batches: collection.mutable.Buffer[FilteredColumnarBatch],
      expectedFileCount: Long,
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

  // Helper function to validate page token
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

  // Helper function to run a complete pagination test
  private def runPaginationTest(
      testName: String,
      tablePath: String,
      pageSize: Long,
      expectedFileCount: Long,
      expectedBatchCount: Int,
      expectedLogFileName: String,
      expectedRowIndex: Long): Unit = {
    
    val snapshot = latestSnapshot(tablePath)
    val paginatedScan = createPaginatedScan(snapshot, pageSize)
    val customEngine = createCustomEngine()
    
    val paginatedIter = paginatedScan.getScanFiles(customEngine)
    val returnedBatchesInPage = collectPaginatedBatches(paginatedIter)
    
    validatePageResults(returnedBatchesInPage, expectedFileCount, expectedBatchCount, testName)
    validatePageToken(paginatedIter, expectedLogFileName, expectedRowIndex, testName)
    
    paginatedIter.close()
  }

  // TODO: test call hasNext() twice

  test("Read First Page: basic pagination with single JSON file") {
    /**
     *  Log Segment List:
     *  00000000000000000000.json contains 13 rows, 10 AddFiles in total
     *  Non-AddFile rows: CommitInfo, Protocol, and Metadata
     *  AddFile rows: 10 AddFiles
     * Batch 1: 5 rows, 2 selected AddFiles (and 3 non AddFile rows)
     * Batch 2: 5 rows, 5 selected AddFiles
     * Batch 3: 3 rows, 3 selected AddFiles
     */

    val tablePath = GoldenTableUtils.goldenTablePath("kernel-pagination-oneJSON")
    // spark.range(0, 100, 1, 10).write.format("delta").save(tablePath)

    val logFileName = "00000000000000000000.json"
    val testCases = Seq(
      ("Page size 1", 1 /* page size */, 2 /* expectedFileCount */, 1 /* expectedBatchCount */, logFileName , 4),
      ("Page size 2", 2, 2, 1, logFileName, 4),
      ("Page size 4", 4, 7, 2, logFileName, 9), // batch size go over page limit
      ("Page size 7", 7, 7, 2, logFileName, 9), // batch size is equal to page limit
      ("Page size 8", 8, 10, 3, logFileName, 12),
      ("Page size >10", 20, 10, 3, logFileName, 12) // page limit isn't reached
    )

    testCases.foreach { case (testName, pageSize, expectedFileCount, expectedBatchCount,
    expectedLogFileName, expectedRowIndex) =>
      runPaginationTest(
        testName = testName,
        tablePath = tablePath,
        pageSize = pageSize,
        expectedFileCount = expectedFileCount,
        expectedBatchCount = expectedBatchCount,
        expectedLogFileName = expectedLogFileName,
        expectedRowIndex = expectedRowIndex
      )
    }
  }

  test("Read First Page: basic pagination with multiple JSON files") {
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
    val tablePath = GoldenTableUtils.goldenTablePath("kernel-pagination-allJSONs")

    /**
    spark.range( 0, 50, 1, 5).write.format("delta").save(tablePath)

    spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)

    spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)
    */

    val JSONFile0 = "00000000000000000000.json"
    val JSONFile1 = "00000000000000000001.json"
    val JSONFile2 = "00000000000000000002.json"
    val testCases = Seq(
      ("Page size 1", 1 /* page size */, 4 /* expectedFileCount */, 1 /* expectedBatchCount */, JSONFile2 , 4),
      ("Page size 2", 4, 4, 1, JSONFile2, 4),
      ("Page size 5", 5, 5, 2, JSONFile2, 5), // batch size go over page limit
      ("Page size 7", 7, 9, 3, JSONFile1, 4), // batch size is equal to page limit
      ("Page size 8", 8, 9, 3, JSONFile1, 4),
      ("Page size > 15",18, 15, 6, JSONFile0, 7) // page limit isn't reached
    )

    testCases.foreach { case (testName, pageSize, expectedFileCount, expectedBatchCount,
    expectedLogFileName, expectedRowIndex) =>
      runPaginationTest(
        testName = testName,
        tablePath = tablePath,
        pageSize = pageSize,
        expectedFileCount = expectedFileCount,
        expectedBatchCount = expectedBatchCount,
        expectedLogFileName = expectedLogFileName,
        expectedRowIndex = expectedRowIndex
      )
    }
  }

  test("Read First Page: basic pagination with one checkpoint file" ) {
    val tablePath = GoldenTableUtils.goldenTablePath("kernel-pagination-oneCheckpoint")
    /**
    * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows in total
     * Batch 1: 5 rows, 5 selected AddFiles
     * Batch 2: 5 rows, 5 selected AddFiles
     * Batch 3: 5 rows, 5 selected AddFiles
     * Batch 4: 5 rows, 3 selected AddFiles
     * Batch 5: 4 rows, 4 selected AddFiles
     */

  }

  test("Read First Page: basic pagination with one checkpoint file " +
    "and multiple JSON files") {
    val tablePath = GoldenTableUtils.goldenTablePath("kernel-pagination-oneCheckpoint")

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
      ("Page size 1", 1 /* page size */, 2 /* expectedFileCount */, 1 /* expectedBatchCount */, JSONFile12 , 2 /* row index in page token*/),
      ("Page size 2", 2, 2, 1, JSONFile12, 2),
      ("Page size 5", 3, 4, 2, JSONFile11, 2), // batch size go over page limit
      ("Page size 7", 4, 4, 2, JSONFile11, 2), // batch size is equal to page limit
      ("Page size 8", 8, 9, 3, checkpoint10, 4),
      ("Page size 18",18, 19, 5, checkpoint10, 14) // page limit isn't reached
    )

    testCases.foreach { case (testName, pageSize, expectedFileCount, expectedBatchCount,
    expectedLogFileName, expectedRowIndex) =>
      runPaginationTest(
        testName = testName,
        tablePath = tablePath,
        pageSize = pageSize,
        expectedFileCount = expectedFileCount,
        expectedBatchCount = expectedBatchCount,
        expectedLogFileName = expectedLogFileName,
        expectedRowIndex = expectedRowIndex
      )
    }
  }

  test("Read First Page: basic pagination with multi-checkpoint files" ) {
    // TODO
  }
}
