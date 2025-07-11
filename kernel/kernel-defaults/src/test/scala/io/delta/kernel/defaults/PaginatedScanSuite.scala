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
import io.delta.kernel.ScanBuilder
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
import io.delta.kernel.utils.CloseableIterator

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

  /**
   * Custom engine with customized batch size. This engine will be used by
   *  all test cases. This number should not change, and it affects every single test.
   */
  private val customEngine: DefaultEngine = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
    hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", "5")
    DefaultEngine.create(hadoopConf)
  }

  // TODO: this can be a testUtil
  private def getScanBuilder(tablePath: String, tableVersionOpt: Optional[Long]): ScanBuilder = {
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
    resolvedTableAdapter.getScanBuilder()
  }

  private def createPaginatedScan(
      tablePath: String,
      tableVersionOpt: Optional[Long],
      pageSize: Long,
      pageTokenOpt: Optional[Row] = Optional.empty()): PaginatedScan = {
    getScanBuilder(tablePath, tableVersionOpt).buildPaginated(pageSize, pageTokenOpt)
  }

  private def collectPaginatedBatches(
      paginatedIter: CloseableIterator[FilteredColumnarBatch] ): Seq[FilteredColumnarBatch] = {
    val buffer = collection.mutable.Buffer[FilteredColumnarBatch]()
    while (paginatedIter.hasNext) {
      val batch = paginatedIter.next()
      buffer += batch
    }
    buffer
  }

  private def validateFirstPageResults(
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

  private def validateFirstPageToken(
      pageTokenRow: Row,
      expectedLogFileName: String,
      expectedRowIndex: Long,
      testName: String): Unit = {
    val lastReadLogFilePath = PageToken.fromRow(pageTokenRow).getLastReadLogFilePath
    val lastReturnedRowIndex = PageToken.fromRow(pageTokenRow).getLastReturnedRowIndex

    assert(lastReadLogFilePath.endsWith(expectedLogFileName))
    assert(lastReturnedRowIndex == expectedRowIndex)

    logger.info(s"$testName: New PageToken: lastReadLogFileName = $lastReadLogFilePath")
    logger.info(s"$testName: New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
  }

  // TODO: test call hasNext() twice
  private def doSinglePageRequest(
      tablePath: String,
      tableVersionOpt: Optional[Long],
      pageTokenOpt: Optional[Row] = Optional.empty(),
      pageSize: Long): (Optional[Row], Seq[FilteredColumnarBatch]) = {
    val paginatedScan = createPaginatedScan(
      tablePath = tablePath,
      tableVersionOpt = tableVersionOpt,
      pageSize = pageSize,
      pageTokenOpt = pageTokenOpt)
    val paginatedIter = paginatedScan.getScanFiles(customEngine)
    val returnedBatchesInPage = collectPaginatedBatches(paginatedIter)
    val nextPageToken = paginatedIter.getCurrentPageToken
    paginatedIter.close()

    assert(returnedBatchesInPage.nonEmpty)
    val fileCounts: Seq[Long] = returnedBatchesInPage.map(_.getPreComputedNumSelectedRows
      .get().toLong)
    val totalFileCountsReturned = fileCounts.sum

    System.out.println(s"number of batches = ${returnedBatchesInPage.length}")
    System.out.println(s"number of AddFiles = ${totalFileCountsReturned}")

    if(nextPageToken.isPresent) {
      val lastReadLogFilePath = PageToken.fromRow(nextPageToken.get).getLastReadLogFilePath
      val lastReturnedRowIndex = PageToken.fromRow(nextPageToken.get).getLastReturnedRowIndex

      System.out.println(s"New PageToken: lastReadLogFileName = $lastReadLogFilePath")
      System.out.println(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
    }

    (nextPageToken, returnedBatchesInPage)
  }

  private def runSingleTest(testCase: SinglePageRequestTestCase, tablePath: String,
                            tableVersionOpt: Optional[Long] = Optional.empty()) {
    var (pageTokenOpt, returnedBatchesInPage) = doSinglePageRequest(
      tablePath = tablePath,
      tableVersionOpt = tableVersionOpt,
      pageSize = testCase.pageSize)

    val testName = s"Single JSON file - page size ${testCase.pageSize}";
    validateFirstPageResults(
      returnedBatchesInPage,
      testCase.expFileCnt,
      testCase.expBatchCnt,
      testName)
    // TODO: test if kernel can return empty page token when scan is consumed
    if(pageTokenOpt.isPresent) {
      validateFirstPageToken(
        pageTokenOpt.get,
        testCase.expLogFile,
        testCase.expRowIdx,
        testName)
    }

    var allBatchesPaginationScan = returnedBatchesInPage
    while (pageTokenOpt.isPresent) {
      val (newPageTokenOpt, newReturnedBatchesInPage) = doSinglePageRequest(
        tablePath = tablePath,
        tableVersionOpt = tableVersionOpt,
        pageTokenOpt = pageTokenOpt,
        pageSize = testCase.pageSize)
      pageTokenOpt = newPageTokenOpt
      allBatchesPaginationScan ++= newReturnedBatchesInPage
    }

    val normalScan =
      getScanBuilder(tablePath = tablePath, tableVersionOpt = tableVersionOpt).build()

    val iter = normalScan.getScanFiles(customEngine)
    val allBatchesNormalScan = collectPaginatedBatches(iter)
    iter.close()

    // check no duplicate or missing batches in paginated scan
    assert(allBatchesNormalScan.size == allBatchesPaginationScan.size)
    for (i <- allBatchesNormalScan.indices) {
      val normalBatch = allBatchesNormalScan(i)
      val paginatedBatch = allBatchesPaginationScan(i)
      assert(normalBatch.getFilePath.equals(paginatedBatch.getFilePath))
      assert(normalBatch.getData.getSize == paginatedBatch.getData.getSize)
    }
  }

  // ==== Log File Name Variables ======
  private val JSON_FILE_0 = "00000000000000000000.json"
  private val JSON_FILE_1 = "00000000000000000001.json"
  private val JSON_FILE_2 = "00000000000000000002.json"
  private val JSON_FILE_11 = "00000000000000000011.json"
  private val JSON_FILE_12 = "00000000000000000012.json"
  private val CHECKPOINT_FILE_10 = "00000000000000000010.checkpoint.parquet"

  // ===== Single JSON file test cases =====
  /**
   *  Log Segment List:
   *  00000000000000000000.json contains 2 batches, 5 active AddFiles in total
   *
   *  Note: batch size is set to 5
   *  Batch 1: 5 rows, 2 selected AddFiles
   *  Batch 2: 3 rows, 3 selected AddFiles
   */
  Seq(
    // Kernel is asked to read the 1st page of size 1. Kernel reads the 1st
    // full batch, so returns 2 AddFiles and ends at the 5th row (index 4).
    // Note: Kernel should always return full batches, so return full batch one.
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_0,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 2. Kernel reads the 1st
    // full batch, so returns 2 AddFiles and ends at the 5th row (index 4)
    SinglePageRequestTestCase(
      pageSize = 2,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_0,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 4. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    // Note: Kernel should always return full batches, so return full 2 batches.
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7),
    // Kernel is asked to read the 1st page of size 5. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    SinglePageRequestTestCase(
      pageSize = 5,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7),
    // Kernel is asked to read the 1st page of size 20. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    // Note: page size won't be reached because there is only 5 data files in total.
    SinglePageRequestTestCase(
      pageSize = 20,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7)).foreach { testCase =>
    test(s"Single JSON file - page size ${testCase.pageSize}") {
      runSingleTest(
        testCase = testCase,
        tablePath = getTestResourceFilePath("kernel-pagination-all-jsons"),
        tableVersionOpt = Optional.of(0L))
    }
  }

  // ===== Multiple JSON files test cases =====
  /**
   * Log Segment List:
   * 00000000000000000000.json : 8 rows (5 AddFile row + 3 non-AddFile rows)
   * 00000000000000000001.json : 6 rows (5 AddFile row + 1 non-AddFile row)
   * 00000000000000000002.json : 6 rows (5 AddFile row + 1 non-AddFile row)
   *
   * Note: batch size is set to 5
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

  Seq(
    // Kernel is asked to read the 1st page of size 1. Kernel reads batch 1 in JSON_FILE_2,
    // so returns 4 AddFiles and ends at the 5th row (index 4) in JSON_FILE_2.
    // Note: Kernel should return full batches, so return full one batch (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 4,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_2,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 4. Kernel reads batch 1 in JSON_FILE_2,
    // so returns 4 AddFiles and ends at the 5th row (index 4) in JSON_FILE_2.
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 4,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_2,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 5. Kernel reads batch 1 and 2 in JSON_FILE_2,
    // so returns 5 AddFiles and ends at the 6th row (index 5) in JSON_FILE_2.
    SinglePageRequestTestCase(
      pageSize = 5,
      expFileCnt = 5,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_2,
      expRowIdx = 5),
    // Kernel is asked to read the 1st page of size 7. Kernel reads all batches in JSON_FILE_2,
    // batch 1 in JSON_FILE_1, so returns 9 AddFiles and ends at the 5th row (index 4)
    // in JSON_FILE_1.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 7,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = JSON_FILE_1,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 9. Kernel reads all batches in JSON_FILE_2,
    // batch 1 in JSON_FILE_1, so returns 9 AddFiles and ends at the 5th row (index 4)
    // in JSON_FILE_1.
    SinglePageRequestTestCase(
      pageSize = 9,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = JSON_FILE_1,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 18. Kernel reads all batches in JSON_FILE_2,
    // JSON_FILE_1 and SON_FILE_0, so returns 15 AddFiles and ends at the last row (index 7)
    // in JSON_FILE_0.
    // Note: page size won't be reached because there are 15 data files in total.
    SinglePageRequestTestCase(
      pageSize = 18,
      expFileCnt = 15,
      expBatchCnt = 6,
      expLogFile = JSON_FILE_0,
      expRowIdx = 7)).foreach { testCase =>
    test(s"Multiple JSON files - page size ${testCase.pageSize}") {
      runSingleTest(
        testCase = testCase,
        tablePath = getTestResourceFilePath("kernel-pagination-all-jsons"))
    }
  }

  // ===== Single checkpoint file test cases =====
  /**
   * Log Segment List:
   * 00000000000000000010.checkpoint.parquet contains 5 batches, 22 active AddFiles, 24 rows
   *
   * Note: batch size is set to 5
   *
   * Batch 1: 5 rows, 5 selected AddFiles
   * Batch 2: 5 rows, 5 selected AddFiles
   * Batch 3: 5 rows, 5 selected AddFiles
   * Batch 4: 5 rows, 3 selected AddFiles
   * Batch 5: 4 rows, 4 selected AddFiles
   */
  Seq(
    // Kernel is asked to read the 1st page of size 1. Kernel reads batch 1 in 10.checkpoint,
    // so returns 5 AddFiles and ends at the 5th row (index 4) in 10.checkpoint.
    // Note: Kernel should return full batches, so return one full batch (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 5,
      expBatchCnt = 1,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 10. Kernel reads 2 batches in 10.checkpoint,
    // so returns 10 AddFiles and ends at the 10th row (index 9) in 10.checkpoint.
    SinglePageRequestTestCase(
      pageSize = 10,
      expFileCnt = 10,
      expBatchCnt = 2,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 9),
    // Kernel is asked to read the 1st page of size 12. Kernel reads 3 batches in 10.checkpoint,
    // so returns 15 AddFiles and ends at the 15th row (index 14) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 12,
      expFileCnt = 15,
      expBatchCnt = 3,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 14),
    // Kernel is asked to read the 1st page of size 100. Kernel reads all 5 batches
    // in 10.checkpoint, so returns 22 AddFiles and ends at the 24th row (index 23)
    // in 10.checkpoint. Note: page size won't be reached in this test case.
    SinglePageRequestTestCase(
      pageSize = 100,
      expFileCnt = 22,
      expBatchCnt = 5,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 23)).foreach { testCase =>
    test(s"Single checkpoint file - page size ${testCase.pageSize}") {
      runSingleTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(10L),
        tablePath = getTestResourceFilePath("kernel-pagination-single-checkpoint"))
    }
  }

  // ===== Single checkpoint file and multiple JSON files test cases =====
  /**
   * Log segment list:
   * 00000000000000000010.checkpoint.parquet
   * 00000000000000000011.json
   * 00000000000000000012.json
   *
   * Note: batch size is set to 5
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
  Seq(
    // Kernel is asked to read the 1st page of size 1. Kernel reads 1 batches in 12.json,
    // so returns 2 AddFiles and ends at the 3rd row (index 2) in 10.checkpoint.
    // Note: Kernel should return full batches, so return one full batch (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 1,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_12,
      expRowIdx = 2),
    // Kernel is asked to read the 1st page of size 2. Kernel reads 1 batches in 12.json,
    // so returns 2 AddFiles and ends at the 3rd row (index 2) in 10.checkpoint.
    SinglePageRequestTestCase(
      pageSize = 2,
      expFileCnt = 2,
      expBatchCnt = 1,
      expLogFile = JSON_FILE_12,
      expRowIdx = 2),
    // Kernel is asked to read the 1st page of size 1. Kernel reads one batch in 12.json,
    // and one batch in 11.json, so returns 4 AddFiles and ends at the 3rd row (index 2) in 11.json.
    // Note: Kernel should return full batches, so return 2 full batches (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 3,
      expFileCnt = 4,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_11,
      expRowIdx = 2),
    // Kernel is asked to read the 1st page of size 4. Kernel reads one batch in 12.json,
    // and one batch in 11.json, so returns 4 AddFiles and ends at the 3rd row (index 2) in 11.json.
    SinglePageRequestTestCase(
      pageSize = 4,
      expFileCnt = 4,
      expBatchCnt = 2,
      expLogFile = JSON_FILE_11,
      expRowIdx = 2),
    // Kernel is asked to read the 1st page of size 8. Kernel reads one batch in 12.json,
    // one batch in 11.json, and one batch in 10.checkpoint, so returns 9 AddFiles and
    // ends at the 5th row (index 4) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 8,
      expFileCnt = 9,
      expBatchCnt = 3,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 4),
    // Kernel is asked to read the 1st page of size 18. Kernel reads one batch in 12.json,
    // one batch in 11.json, and 3 batches in 10.checkpoint, so returns 19 AddFiles and
    // ends at the 15th row (index 14) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 5 full batches (and go over page limit).
    SinglePageRequestTestCase(
      pageSize = 18,
      expFileCnt = 19,
      expBatchCnt = 5,
      expLogFile = CHECKPOINT_FILE_10,
      expRowIdx = 14)).foreach { testCase =>
    test(s"Single checkpoint and JSON files - page size ${testCase.pageSize}") {
      runSingleTest(
        testCase = testCase,
        tablePath = getTestResourceFilePath("kernel-pagination-single-checkpoint"))
    }
  }

  // TODO: test multi part checkpoint files
  /**
   * 00000000000000000009.checkpoint.0000000001.0000000004.parquet,
   * 00000000000000000009.checkpoint.0000000002.0000000004.parquet,
   * 00000000000000000009.checkpoint.0000000003.0000000004.parquet,
   * 00000000000000000009.checkpoint.0000000004.0000000004.parquet,
   * 00000000000000000010.checkpoint.parquet
   * JSON files: from 0 to 12
   * */
  /**
   * val tablePath = tempDir.getCanonicalPath
   *
   * // Create 10 commits to trigger checkpoint creation
   * for (i <- 0 until 10) {
   * val mode = if (i == 0) "overwrite" else "append"
   * // Create 4 files per commit = 40 total AddFile actions
   * spark.range(i * 40, (i + 1) * 40, 1, 4)
   * .write.format("delta").mode(mode).save(tablePath)
   * }
   *
   * // Force multi-part checkpoint creation (3-5 parts)
   * withSQLConf(
   * "spark.databricks.delta.checkpoint.partSize" -> "10" // 40 AddFiles ÷ 10 per part = 4 parts
   * ) {
   * val deltaLog = DeltaLog.forTable(spark, tablePath)
   * deltaLog.checkpoint()
   * }
  * */

}
