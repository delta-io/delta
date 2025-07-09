/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import java.util.Optional

import io.delta.kernel.PaginatedScan
import io.delta.kernel.PaginatedScanFilesIterator
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.test.AbstractTableManagerAdapter
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.defaults.utils.TestUtilsWithTableManagerAPIs
import io.delta.kernel.internal.replay.{PageToken, PaginatedScanFilesIteratorImpl}

import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class PaginatedScanSuite extends AnyFunSuite with TestUtilsWithTableManagerAPIs
    with ExpressionTestUtils with SQLHelper with DeltaTableWriteSuiteBase {

  private val logger = LoggerFactory.getLogger(classOf[PaginatedScanSuite])
  val tableManager: AbstractTableManagerAdapter = getTableManagerAdapter

  /** Test case to check if the result of a single page request is expected */
  case class SinglePageRequestTestCase(
      testName: String,
      pageSize: Int,
      expectedFileCount: Int,
      expectedBatchCount: Int,
      expectedLogFileName: String,
      expectedRowIndex: Int)

  /** Custom engine with customized batch size */
  private val customEngine: DefaultEngine = {
    val BATCH_SIZE = 5
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("delta.kernel.default.json.reader.batch-size", BATCH_SIZE.toString)
    hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", BATCH_SIZE.toString)
    DefaultEngine.create(hadoopConf)
  }

  private def createPaginatedScan(
      tablePath: String,
      tableVersionOpt: Optional[Long],
      pageSize: Long,
      pageTokenOpt: Optional[Row] = Optional.empty()): PaginatedScan = {

    val resolvedTableAdapter = {
      if (tableVersionOpt.isPresent) {
        tableManager.getResolvedTableAdapterAtVersion(
          customEngine,
          tablePath,
          tableVersionOpt.get())
      } else {
        tableManager.getResolvedTableAdapterAtLatest(customEngine, tablePath)
      }
    }
    resolvedTableAdapter.getScanBuilder().buildPaginated(pageSize, pageTokenOpt)
  }

  private def collectPaginatedBatches(
      paginatedIter: PaginatedScanFilesIterator): Seq[FilteredColumnarBatch] = {
    val buffer = collection.mutable.Buffer[FilteredColumnarBatch]()
    while (paginatedIter.hasNext) {
      val batch = paginatedIter.next()
      buffer += batch
    }
    buffer
  }

  private def validatePageResults(
      batches: Seq[FilteredColumnarBatch],
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
      paginatedIter: PaginatedScanFilesIterator,
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
      tableVersionOpt: Optional[Long],
      pageSize: Long,
      expectedFileCount: Int,
      expectedBatchCount: Int,
      expectedLogFileName: String,
      expectedRowIndex: Int): Unit = {
    val paginatedScan = createPaginatedScan(
      tablePath = tablePath,
      tableVersionOpt = tableVersionOpt,
      pageSize = pageSize)

    val paginatedIter = paginatedScan.getScanFiles(customEngine)
    val returnedBatchesInPage = collectPaginatedBatches(paginatedIter)

    validatePageResults(returnedBatchesInPage, expectedFileCount, expectedBatchCount, testName)
    validatePageToken(paginatedIter, expectedLogFileName, expectedRowIndex, testName)

    paginatedIter.close()
  }

  private def runPaginationTestCasesForFirstPage(
      tablePath: String,
      tableVersionOpt: Optional[Long] = Optional.empty(),
      testCases: Seq[SinglePageRequestTestCase]): Unit = {
    testCases.foreach { testCase =>
      runSinglePaginationTestCase(
        testName = testCase.testName,
        tablePath = tablePath,
        tableVersionOpt = tableVersionOpt,
        pageSize = testCase.pageSize,
        expectedFileCount = testCase.expectedFileCount,
        expectedBatchCount = testCase.expectedBatchCount,
        expectedLogFileName = testCase.expectedLogFileName,
        expectedRowIndex = testCase.expectedRowIndex)
    }
  }

  // TODO: test call hasNext() twice

  test("Pagination with single JSON file") {
    withGoldenTable("kernel-pagination-all-jsons") { tablePath =>
      /**
       *  Log Segment List:
       *  00000000000000000000.json contains 2 batches, 5 active AddFiles in total
       *  Batch 1: 5 rows, 2 selected AddFiles
       *  Batch 2: 3 rows, 3 selected AddFiles
       */

      val JSONFile0 = "00000000000000000000.json"
      // Change page sizes to test various edge cases,
      // for example, batch size goes over page size limit
      val testCases = Seq(
        SinglePageRequestTestCase(
          "Page size 1",
          pageSize = 1,
          expectedFileCount = 2,
          expectedBatchCount = 1,
          expectedLogFileName = JSONFile0,
          expectedRowIndex = 4),
        SinglePageRequestTestCase("Page size 2", 2, 2, 1, JSONFile0, 4),
        SinglePageRequestTestCase("Page size 4", 4, 5, 2, JSONFile0, 7),
        SinglePageRequestTestCase("Page size 7", 7, 5, 2, JSONFile0, 7),
        SinglePageRequestTestCase("Page size 20", 20, 5, 2, JSONFile0, 7))

      runPaginationTestCasesForFirstPage(
        tablePath = tablePath,
        tableVersionOpt = Optional.of(0L),
        testCases = testCases)

      // TODO: test if page two returns correct page token
      // TODO: run page requests loop and compare to normal Scan
      //  also check no duplicate batch returned and numFileSkipped
    }
  }

  test("Pagination with multiple JSON files") {
    withGoldenTable("kernel-pagination-all-jsons") { tablePath =>
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
        SinglePageRequestTestCase(
          "Page size 1",
          pageSize = 1,
          expectedFileCount = 4,
          expectedBatchCount = 1,
          expectedLogFileName = JSONFile2,
          expectedRowIndex = 4),
        SinglePageRequestTestCase("Page size 4", 4, 4, 1, JSONFile2, 4),
        SinglePageRequestTestCase("Page size 5", 5, 5, 2, JSONFile2, 5),
        SinglePageRequestTestCase("Page size 7", 7, 9, 3, JSONFile1, 4),
        SinglePageRequestTestCase("Page size 8", 8, 9, 3, JSONFile1, 4),
        SinglePageRequestTestCase("Page size 18", 18, 15, 6, JSONFile0, 7))

      runPaginationTestCasesForFirstPage(tablePath = tablePath, testCases = testCases)

      // TODO: test all page requests
    }
  }

  test("Pagination with one checkpoint file") {
    withGoldenTable("kernel-pagination-single-checkpoint") { tablePath =>
      /**
       * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows
       * Batch 1: 5 rows, 5 selected AddFiles
       * Batch 2: 5 rows, 5 selected AddFiles
       * Batch 3: 5 rows, 5 selected AddFiles
       * Batch 4: 5 rows, 3 selected AddFiles
       * Batch 5: 4 rows, 4 selected AddFiles
       */
      val checkpoint10 = "00000000000000000010.checkpoint.parquet"

      val testCases = Seq(
        SinglePageRequestTestCase(
          "Page size 1",
          pageSize = 1,
          expectedFileCount = 5,
          expectedBatchCount = 1,
          expectedLogFileName = checkpoint10,
          expectedRowIndex = 4),
        SinglePageRequestTestCase("Page size 10", 10, 10, 2, checkpoint10, 9),
        SinglePageRequestTestCase("Page size 12", 12, 15, 3, checkpoint10, 14),
        SinglePageRequestTestCase("Large Page size", 100, 22, 5, checkpoint10, 23))

      runPaginationTestCasesForFirstPage(
        tablePath = tablePath,
        tableVersionOpt = Optional.of(10L),
        testCases = testCases)

      // TODO: test all page requests
    }
  }

  test("Pagination with one checkpoint file and multiple JSON files") {
    withGoldenTable("kernel-pagination-single-checkpoint") { tablePath =>
      /**
       * for (i <- 0 until 10) {
       * val mode = if (i == 0) "overwrite" else "append"
       * spark.range(i * 10, (i + 1) * 10, 1, 2)
       * .write.format("delta").mode(mode).save(tablePath)
       * }
       *
       * // Force checkpoint creation
       * val deltaLog = DeltaLog.forTable(spark, tablePath)
       * deltaLog.checkpoint()
       *
       * // Add more commits after checkpoint
       * for (i <- 10 until 13) {
       * spark.range(i * 10, (i + 1) * 10, 1, 2)
       * .write.format("delta").mode("append").save(tablePath)
       * }
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
       * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows
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
        SinglePageRequestTestCase(
          "Page size 1",
          pageSize = 1,
          expectedFileCount = 2,
          expectedBatchCount = 1,
          expectedLogFileName = JSONFile12,
          expectedRowIndex = 2),
        SinglePageRequestTestCase("Page size 2", 2, 2, 1, JSONFile12, 2),
        SinglePageRequestTestCase("Page size 3", 3, 4, 2, JSONFile11, 2),
        SinglePageRequestTestCase("Page size 4", 4, 4, 2, JSONFile11, 2),
        SinglePageRequestTestCase("Page size 8", 8, 9, 3, checkpoint10, 4),
        SinglePageRequestTestCase("Page size 18", 18, 19, 5, checkpoint10, 14))

      runPaginationTestCasesForFirstPage(tablePath = tablePath, testCases = testCases)

      // TODO: test all page requests
    }
  }

  test("Pagination with multi-checkpoint files") {
    // TODO
  }

  test("Pagination with sidecar files") {
    // TODO
  }
}
