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
      pageSize: Int,
      expFileCnt: Int,
      expBatchCnt: Int,
      expLogFile: String,
      expRowIdx: Int)

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

  // TODO: test call hasNext() twice

  // File name constants for better readability
  private val JSON_FILE_0 = "00000000000000000000.json"
  private val JSON_FILE_1 = "00000000000000000001.json"
  private val JSON_FILE_2 = "00000000000000000002.json"
  private val JSON_FILE_11 = "00000000000000000011.json"
  private val JSON_FILE_12 = "00000000000000000012.json"
  private val CHECKPOINT_FILE_10 = "00000000000000000010.checkpoint.parquet"

  // Single JSON file test cases
  val singleJsonTestCases = Seq(
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_0,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 2,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_0,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7),
    SinglePageRequestTestCase(
      pageSize = 7,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7),
    SinglePageRequestTestCase(
      pageSize = 20,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7))

  singleJsonTestCases.foreach { testCase =>
    test(s"Single JSON file - page size ${testCase.pageSize}") {
      withKernelStaticTable("kernel-pagination-all-jsons") { tablePath =>
        /**
         *  Log Segment List:
         *  00000000000000000000.json contains 2 batches, 5 active AddFiles in total
         *  Batch 1: 5 rows, 2 selected AddFiles
         *  Batch 2: 3 rows, 3 selected AddFiles
         */
        runSinglePaginationTestCase(
          testName = s"Single JSON file - page size ${testCase.pageSize}",
          tablePath = tablePath,
          tableVersionOpt = Optional.of(0L),
          pageSize = testCase.pageSize,
          expectedFileCount = testCase.expFileCnt,
          expectedBatchCount = testCase.expBatchCnt,
          expectedLogFileName = testCase.expLogFile,
          expectedRowIndex = testCase.expRowIdx)
      }
    }
  }

  // Multiple JSON files test cases
  val multipleJsonTestCases = Seq(
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 4,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_2,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 4,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_2,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 5,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_2,
      expRowIdx = 5),
    SinglePageRequestTestCase(
      pageSize = 7,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = JSON_FILE_1,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 8,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = JSON_FILE_1,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 18,
      expFileCnt = 15,
      expBatchCnt = 6,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7))

  multipleJsonTestCases.foreach { testCase =>
    test(s"Multiple JSON files - page size ${testCase.pageSize}") {
      withKernelStaticTable("kernel-pagination-all-jsons") { tablePath =>
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
        runSinglePaginationTestCase(
          testName = s"Multiple JSON files - page size ${testCase.pageSize}",
          tablePath = tablePath,
          tableVersionOpt = Optional.empty(),
          pageSize = testCase.pageSize,
          expectedFileCount = testCase.expFileCnt,
          expectedBatchCount = testCase.expBatchCnt,
          expectedLogFileName = testCase.expLogFile,
          expectedRowIndex = testCase.expRowIdx)
      }
    }
  }

  // Single checkpoint file test cases
  val singleCheckpointTestCases = Seq(
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 5,
      expBatchCnt = 1,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 10,
      expFileCnt = 10,
      expBatchCnt = 2,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 9),
    SinglePageRequestTestCase(
      pageSize = 12,
      expFileCnt = 15,
      expBatchCnt = 3,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 14),
    SinglePageRequestTestCase(
      pageSize = 100,
      expFileCnt = 22,
      expBatchCnt = 5,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 23))

  singleCheckpointTestCases.foreach { testCase =>
    test(s"Single checkpoint file - page size ${testCase.pageSize}") {
      withKernelStaticTable("kernel-pagination-single-checkpoint") { tablePath =>
        /**
         * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows
         * Batch 1: 5 rows, 5 selected AddFiles
         * Batch 2: 5 rows, 5 selected AddFiles
         * Batch 3: 5 rows, 5 selected AddFiles
         * Batch 4: 5 rows, 3 selected AddFiles
         * Batch 5: 4 rows, 4 selected AddFiles
         */
        runSinglePaginationTestCase(
          testName = s"Single checkpoint file - page size ${testCase.pageSize}",
          tablePath = tablePath,
          tableVersionOpt = Optional.of(10L),
          pageSize = testCase.pageSize,
          expectedFileCount = testCase.expFileCnt,
          expectedBatchCount = testCase.expBatchCnt,
          expectedLogFileName = testCase.expLogFile,
          expectedRowIndex = testCase.expRowIdx)
      }
    }
  }

  // Checkpoint file with multiple JSON files test cases
  val checkpointWithJsonTestCases = Seq(
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_12,
      expRowIdx = 2),
    SinglePageRequestTestCase(
      pageSize = 2,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_12,
      expRowIdx = 2),
    SinglePageRequestTestCase(
      pageSize = 3,
      expFileCnt = 4,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_11,
      expRowIdx = 2),
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 4,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_11,
      expRowIdx = 2),
    SinglePageRequestTestCase(
      pageSize = 8,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 4),
    SinglePageRequestTestCase(
      pageSize = 18,
      expFileCnt = 19,
      expBatchCnt = 5,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 14))

  checkpointWithJsonTestCases.foreach { testCase =>
    test(s"Checkpoint with JSON files - page size ${testCase.pageSize}") {
      withKernelStaticTable("kernel-pagination-single-checkpoint") { tablePath =>
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
        runSinglePaginationTestCase(
          testName = s"Checkpoint with JSON files - page size ${testCase.pageSize}",
          tablePath = tablePath,
          tableVersionOpt = Optional.empty(),
          pageSize = testCase.pageSize,
          expectedFileCount = testCase.expFileCnt,
          expectedBatchCount = testCase.expBatchCnt,
          expectedLogFileName = testCase.expLogFile,
          expectedRowIndex = testCase.expRowIdx)
      }
    }
  }

  test("Pagination with multi-checkpoint files") {
    // TODO
  }

  test("Pagination with sidecar files") {
    // TODO
  }
}
