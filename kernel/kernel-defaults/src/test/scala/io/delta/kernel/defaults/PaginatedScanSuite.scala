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

import scala.jdk.CollectionConverters.asScalaBufferConverter

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
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.hook.LogCompactionHook
import io.delta.kernel.internal.replay.{PageToken, PaginatedScanFilesIteratorImpl}
import io.delta.kernel.utils.CloseableIterator

import org.apache.spark.sql.delta.DeltaLog

import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class PaginatedScanSuite extends AnyFunSuite with TestUtilsWithTableManagerAPIs
    with ExpressionTestUtils with SQLHelper with DeltaTableWriteSuiteBase {

  private val logger = LoggerFactory.getLogger(classOf[PaginatedScanSuite])
  val tableManager: AbstractTableManagerAdapter = getTableManagerAdapter

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

  // TODO: this can be a testUtil?
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

  case class FirstPageRequestTestContext(
      pageSize: Int,
      expScanFilesCnt: Int,
      expBatchCnt: Int,
      expLastReadLogFile: String,
      expLastReadRowIdx: Int)

  private def validateFirstPageResults(
      batches: Seq[FilteredColumnarBatch],
      expectedFileCount: Int,
      expectedBatchCount: Int): Unit = {
    assert(batches.nonEmpty)
    val fileCounts: Seq[Long] = batches.map(_.getPreComputedNumSelectedRows.get().toLong)
    val totalFileCountsReturned = fileCounts.sum

    assert(fileCounts.length == expectedBatchCount)
    assert(totalFileCountsReturned == expectedFileCount)

    logger.info(s"Total num batches returned in page one = ${fileCounts.length}")
    logger.info(s"Total num Parquet Files fetched in page one = " +
      s"$totalFileCountsReturned")
  }

  private def validateFirstPageToken(
      pageTokenRow: Row,
      expectedLogFileName: String,
      expectedRowIndex: Long): Unit = {
    val lastReadLogFilePath = PageToken.fromRow(pageTokenRow).getLastReadLogFilePath
    val lastReturnedRowIndex = PageToken.fromRow(pageTokenRow).getLastReturnedRowIndex

    assert(lastReadLogFilePath.endsWith(expectedLogFileName))
    assert(lastReturnedRowIndex == expectedRowIndex)

    logger.info(s"New PageToken: lastReadLogFileName = $lastReadLogFilePath")
    logger.info(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
  }

  /**
   * Executes a single paginated scan request.
   *
   * 1. Constructs a paginated scan using the provided page size and page token (optional).
   * 2. Collects scan results for the current page.
   * 3. Returns the results along with the next page token.
   */
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
    val returnedBatchesInPage = paginatedIter.toSeq
    val nextPageToken = paginatedIter.getCurrentPageToken

    assert(returnedBatchesInPage.nonEmpty)

    val fileCounts: Seq[Long] = returnedBatchesInPage.map(_.getPreComputedNumSelectedRows
      .get().toLong)
    val totalFileCountsReturned = fileCounts.sum

    logger.info(s"number of batches = ${returnedBatchesInPage.length}")
    logger.info(s"number of AddFiles = ${totalFileCountsReturned}")

    if (nextPageToken.isPresent) {
      val lastReadLogFilePath = PageToken.fromRow(nextPageToken.get).getLastReadLogFilePath
      val lastReturnedRowIndex = PageToken.fromRow(nextPageToken.get).getLastReturnedRowIndex

      logger.info(s"New PageToken: lastReadLogFileName = $lastReadLogFilePath")
      logger.info(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
    }

    (nextPageToken, returnedBatchesInPage)
  }

  /**
   * Simulates the client's behavior of reading a full scan in a paginated manner
   * with a given page size.
   *
   *  The client:
   * 1. Starts by requesting the first page (no page token).
   * 2. Receives a page of results along with a page token.
   * 3. Uses the page token to request the next page.
   * 4. Repeats until the returned page token is empty, indicating that all data has been consumed.
   */
  private def runCompletePaginationTest(
      testCase: FirstPageRequestTestContext,
      tablePath: String,
      tableVersionOpt: Optional[Long] = Optional.empty()): Unit = {

    // ============ Request the first page ==============
    var (pageTokenOpt, returnedBatchesInPage) = doSinglePageRequest(
      tablePath = tablePath,
      tableVersionOpt = tableVersionOpt,
      pageSize = testCase.pageSize)

    validateFirstPageResults(
      returnedBatchesInPage,
      testCase.expScanFilesCnt,
      testCase.expBatchCnt)

    // When the scan is exhausted, returned page token should be empty.
    if (pageTokenOpt.isPresent) {
      validateFirstPageToken(
        pageTokenOpt.get,
        testCase.expLastReadLogFile,
        testCase.expLastReadRowIdx)
    }

    // ============ Request following pages ==============
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
    val allBatchesNormalScan = iter.toSeq

    // check no duplicate or missing batches in paginated scan
    assert(allBatchesNormalScan.size == allBatchesPaginationScan.size)
    for (i <- allBatchesNormalScan.indices) {
      val normalBatch = allBatchesNormalScan(i)
      val paginatedBatch = allBatchesPaginationScan(i)
      assert(normalBatch.getFilePath.equals(paginatedBatch.getFilePath))
      assert(normalBatch.getData.getSize == paginatedBatch.getData.getSize)
    }
  }

  // ==== Test Paginated Iterator Behaviors ======
  // TODO: test call hasNext() twice
  test("Calling getCurrentPageToken() without calling next() should throw Exception") {
    // Request first page
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      // First commit: files 0-4 (5 files)
      spark.range(0, 50, 1, 5).write.format("delta").save(tablePath)

      val firstPageSize = 2L
      val firstPaginatedScan = createPaginatedScan(
        tablePath = tablePath,
        tableVersionOpt = Optional.empty(),
        pageSize = firstPageSize)
      val firstPaginatedIter = firstPaginatedScan.getScanFiles(customEngine)

      // throw exception
      var e = intercept[IllegalStateException] {
        firstPaginatedIter.getCurrentPageToken.get
      }
      assert(e.getMessage.contains("Can't call getCurrentPageToken()"))

      // throw exception
      e = intercept[IllegalStateException] {
        firstPaginatedIter.hasNext
        firstPaginatedIter.getCurrentPageToken.get
      }
      assert(e.getMessage.contains("Can't call getCurrentPageToken()"))

      firstPaginatedIter.close()
    }
  }

  test("getCurrentPageToken() is impacted only by next() calls, not hasNext() calls") {
    // Request first page
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      // First commit: files 0-4 (5 files)
      spark.range(0, 50, 1, 5).write.format("delta").save(tablePath)

      // Second commit: files 5-9 (5 more files)
      spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Third commit: files 10-14 (5 more files)
      spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)

      val firstPageSize = 2L
      val firstPaginatedScan = createPaginatedScan(
        tablePath = tablePath,
        tableVersionOpt = Optional.empty(),
        pageSize = firstPageSize)
      val firstPaginatedIter = firstPaginatedScan.getScanFiles(customEngine)
      if (firstPaginatedIter.hasNext) firstPaginatedIter.next()
      val expectedPageToken = firstPaginatedIter.getCurrentPageToken.get

      firstPaginatedIter.hasNext // call hsaNext() again, should not affect page token
      val pageToken = firstPaginatedIter.getCurrentPageToken.get

      assert(PageToken.fromRow(pageToken).equals(PageToken.fromRow(expectedPageToken)))
      firstPaginatedIter.close()
    }
  }

  // ===== Data Integrity test cases=====
  // TODO: test predicate changes
  /**
   * Test case to verify pagination behavior when log segment changes between page requests.
   */
  test("Throw exception when log segment changes between page requests") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      // First commit: files 0-4 (5 files)
      spark.range(0, 50, 1, 5).write.format("delta").save(tablePath)

      // Second commit: files 5-9 (5 more files)
      spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Third commit: files 10-14 (5 more files)
      spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Fourth commit: files 15-19 (5 more files)
      spark.range(150, 200, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Fifth commit: files 20-24 (5 more files)
      spark.range(200, 250, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Request first page
      val firstPageSize = 2L
      val firstPaginatedScan = createPaginatedScan(
        tablePath = tablePath,
        tableVersionOpt = Optional.empty(),
        pageSize = firstPageSize)
      val firstPaginatedIter = firstPaginatedScan.getScanFiles(customEngine)
      if (firstPaginatedIter.hasNext) firstPaginatedIter.next() // call next() once
      val firstPageToken = firstPaginatedIter.getCurrentPageToken.get
      firstPaginatedIter.close()

      // Perform log compaction for versions 0-2; log segment should change
      val dataPath = new Path(s"file:${tablePath}")
      val logPath = new Path(s"file:${tablePath}", "_delta_log")
      val compactionHook = new LogCompactionHook(dataPath, logPath, 0, 2, 0)
      compactionHook.threadSafeInvoke(customEngine)
      logger.info("Log compaction completed for versions 0-2")

      // Request second page
      val secondPageSize = 5L
      val e = intercept[IllegalArgumentException] {
        createPaginatedScan(
          tablePath = tablePath,
          tableVersionOpt = Optional.empty(),
          pageSize = secondPageSize,
          pageTokenOpt = Optional.of(firstPageToken))
      }
      assert(e.getMessage.contains("Invalid page token: token log segment"))
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
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 2,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 2. Kernel reads the 1st
    // full batch, so returns 2 AddFiles and ends at the 5th row (index 4)
    FirstPageRequestTestContext(
      pageSize = 2,
      expScanFilesCnt = 2,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 4. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    // Note: Kernel should always return full batches, so return full 2 batches.
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 5,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 7),
    // Kernel is asked to read the 1st page of size 5. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    FirstPageRequestTestContext(
      pageSize = 5,
      expScanFilesCnt = 5,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 7),
    // Kernel is asked to read the 1st page of size 20. Kernel reads batch 1 and
    // batch 2 in JSON_FILE_0, so returns 5 AddFiles and ends at the 8th row (index 7)
    // Note: page size won't be reached because there is only 5 data files in total.
    FirstPageRequestTestContext(
      pageSize = 20,
      expScanFilesCnt = 5,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 7)).foreach { testCase =>
    test(s"Single JSON file - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
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
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 4,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_2,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 4. Kernel reads batch 1 in JSON_FILE_2,
    // so returns 4 AddFiles and ends at the 5th row (index 4) in JSON_FILE_2.
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 4,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_2,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 5. Kernel reads batch 1 and 2 in JSON_FILE_2,
    // so returns 5 AddFiles and ends at the 6th row (index 5) in JSON_FILE_2.
    FirstPageRequestTestContext(
      pageSize = 5,
      expScanFilesCnt = 5,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_2,
      expLastReadRowIdx = 5),
    // Kernel is asked to read the 1st page of size 7. Kernel reads all batches in JSON_FILE_2,
    // batch 1 in JSON_FILE_1, so returns 9 AddFiles and ends at the 5th row (index 4)
    // in JSON_FILE_1.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    FirstPageRequestTestContext(
      pageSize = 7,
      expScanFilesCnt = 9,
      expBatchCnt = 3,
      expLastReadLogFile = JSON_FILE_1,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 9. Kernel reads all batches in JSON_FILE_2,
    // batch 1 in JSON_FILE_1, so returns 9 AddFiles and ends at the 5th row (index 4)
    // in JSON_FILE_1.
    FirstPageRequestTestContext(
      pageSize = 9,
      expScanFilesCnt = 9,
      expBatchCnt = 3,
      expLastReadLogFile = JSON_FILE_1,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 18. Kernel reads all batches in JSON_FILE_2,
    // JSON_FILE_1 and SON_FILE_0, so returns 15 AddFiles and ends at the last row (index 7)
    // in JSON_FILE_0.
    // Note: page size won't be reached because there are 15 data files in total.
    FirstPageRequestTestContext(
      pageSize = 18,
      expScanFilesCnt = 15,
      expBatchCnt = 6,
      expLastReadLogFile = JSON_FILE_0,
      expLastReadRowIdx = 7)).foreach { testCase =>
    test(s"Multiple JSON files - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
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
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 5,
      expBatchCnt = 1,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 10. Kernel reads 2 batches in 10.checkpoint,
    // so returns 10 AddFiles and ends at the 10th row (index 9) in 10.checkpoint.
    FirstPageRequestTestContext(
      pageSize = 10,
      expScanFilesCnt = 10,
      expBatchCnt = 2,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 9),
    // Kernel is asked to read the 1st page of size 12. Kernel reads 3 batches in 10.checkpoint,
    // so returns 15 AddFiles and ends at the 15th row (index 14) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    FirstPageRequestTestContext(
      pageSize = 12,
      expScanFilesCnt = 15,
      expBatchCnt = 3,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 14),
    // Kernel is asked to read the 1st page of size 100. Kernel reads all 5 batches
    // in 10.checkpoint, so returns 22 AddFiles and ends at the 24th row (index 23)
    // in 10.checkpoint. Note: page size won't be reached in this test case.
    FirstPageRequestTestContext(
      pageSize = 100,
      expScanFilesCnt = 22,
      expBatchCnt = 5,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 23)).foreach { testCase =>
    test(s"Single checkpoint file - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
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
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 2,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_12,
      expLastReadRowIdx = 2),
    // Kernel is asked to read the 1st page of size 2. Kernel reads 1 batches in 12.json,
    // so returns 2 AddFiles and ends at the 3rd row (index 2) in 10.checkpoint.
    FirstPageRequestTestContext(
      pageSize = 2,
      expScanFilesCnt = 2,
      expBatchCnt = 1,
      expLastReadLogFile = JSON_FILE_12,
      expLastReadRowIdx = 2),
    // Kernel is asked to read the 1st page of size 1. Kernel reads one batch in 12.json,
    // and one batch in 11.json, so returns 4 AddFiles and ends at the 3rd row (index 2) in 11.json.
    // Note: Kernel should return full batches, so return 2 full batches (and go over page limit).
    FirstPageRequestTestContext(
      pageSize = 3,
      expScanFilesCnt = 4,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_11,
      expLastReadRowIdx = 2),
    // Kernel is asked to read the 1st page of size 4. Kernel reads one batch in 12.json,
    // and one batch in 11.json, so returns 4 AddFiles and ends at the 3rd row (index 2) in 11.json.
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 4,
      expBatchCnt = 2,
      expLastReadLogFile = JSON_FILE_11,
      expLastReadRowIdx = 2),
    // Kernel is asked to read the 1st page of size 8. Kernel reads one batch in 12.json,
    // one batch in 11.json, and one batch in 10.checkpoint, so returns 9 AddFiles and
    // ends at the 5th row (index 4) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 3 full batches (and go over page limit).
    FirstPageRequestTestContext(
      pageSize = 8,
      expScanFilesCnt = 9,
      expBatchCnt = 3,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 4),
    // Kernel is asked to read the 1st page of size 18. Kernel reads one batch in 12.json,
    // one batch in 11.json, and 3 batches in 10.checkpoint, so returns 19 AddFiles and
    // ends at the 15th row (index 14) in 10.checkpoint.
    // Note: Kernel should return full batches, so return 5 full batches (and go over page limit).
    FirstPageRequestTestContext(
      pageSize = 18,
      expScanFilesCnt = 19,
      expBatchCnt = 5,
      expLastReadLogFile = CHECKPOINT_FILE_10,
      expLastReadRowIdx = 14)).foreach { testCase =>
    test(s"Single checkpoint and JSON files - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tablePath = getTestResourceFilePath("kernel-pagination-single-checkpoint"))
    }
  }

  // ===== Multi-part checkpoint files test cases =====
  /**
   * Log Segment List:
   * 00000000000000000000.checkpoint.0000000003.0000000003.parquet (6 AddFile)
   * Batch A: 5 AddFile, 5 rows
   * Batch B: 1 AddFile, 1 row
   * 00000000000000000000.checkpoint.0000000002.0000000003.parquet (7 AddFile)
   * Batch C: 5 AddFile, 5 row
   * Batch D: 2 AddFile, 2 row
   * 00000000000000000000.checkpoint.0000000001.0000000003.parquet (5 AddFile)
   * Batch E: 3 AddFile, 5 row
   * Batch F: 2 AddFile, 2 row
   */
  val MULTI_CHECKPOINT_FILE_0_1 = "00000000000000000000.checkpoint.0000000001.0000000003.parquet"
  val MULTI_CHECKPOINT_FILE_0_2 = "00000000000000000000.checkpoint.0000000002.0000000003.parquet"
  val MULTI_CHECKPOINT_FILE_0_3 = "00000000000000000000.checkpoint.0000000003.0000000003.parquet"

  // Note: Kernel should return full batches (and may go over page limit).
  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 5, /* 5 AddFiles in Batch A */
      expBatchCnt = 1, /* return only batch A */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_3, /* Batch A from checkpoint file 3 */
      expLastReadRowIdx = 4 /* Last Row index in Batch A is 4 in checkpoint file 3 */ ),
    FirstPageRequestTestContext(
      pageSize = 7,
      expScanFilesCnt = 11, /* 5 (batch A) + 1 (batch B) + 5 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_2, /* Batch C from checkpoint file 2 */
      expLastReadRowIdx = 4 /* Last Row index in Batch C is 4 in checkpoint file 2 */
    ),
    FirstPageRequestTestContext(
      pageSize = 13,
      expScanFilesCnt = 13, /* 5 (batch A) + 1 (batch B) + 5 (batch C) + 2 (batch D) */
      expBatchCnt = 4, /* return batch A, B, C, D */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_2, /* Batch D from checkpoint file 2 */
      expLastReadRowIdx = 6 /* Last Row index in Batch D is 6 in checkpoint file 3 */
    ),
    FirstPageRequestTestContext(
      pageSize = 100,
      expScanFilesCnt = 18, /* 5 (batch A) + 1 (batch B) + 5 (C) + 2 (D) + 3 (E) + 2 F) */
      expBatchCnt = 6, /* return batch A, B, C, D, E, F */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_1,
      expLastReadRowIdx = 1)).foreach { testCase =>
    test(s"Multi-part checkpoints - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(0L),
        tablePath = getTestResourceFilePath("kernel-pagination-multi-part-checkpoints"))
    }
  }

  // ===== Multi-part checkpoint files with JSON files test cases =====
  /**
   * Log Segment List:
   * 00000000000000000002.json (1 AddFile)
   * Batch A: 1 AddFile, 2 rows
   * 00000000000000000001.json (1 AddFile)
   * Batch B: 1 AddFile, 2 rows
   * 00000000000000000000.checkpoint.0000000003.0000000003.parquet (6 AddFile)
   * Batch C: 5 AddFile, 5 rows
   * Batch D: 1 AddFile, 1 row
   * 00000000000000000000.checkpoint.0000000002.0000000003.parquet (7 AddFile)
   * Batch E: 5 AddFile, 5 row
   * Batch F: 2 AddFile, 2 row
   * 00000000000000000000.checkpoint.0000000001.0000000003.parquet (5 AddFile)
   * Batch G: 3 AddFile, 5 row
   * Batch H: 2 AddFile, 2 row
   */
  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 1, /* 1 AddFile in Batch A */
      expBatchCnt = 1, /* return only batch A */
      expLastReadLogFile = JSON_FILE_2, /* Batch A from JSON file 2 */
      expLastReadRowIdx = 1
    ), /* Last Row index in Batch A is 1 in JSON file 2 */
    FirstPageRequestTestContext(
      pageSize = 2,
      expScanFilesCnt = 2, /* 1 (batch A) + 1 (batch B) */
      expBatchCnt = 2, /* return batch A, B */
      expLastReadLogFile = JSON_FILE_1, /* Batch B from JSON file 1 */
      expLastReadRowIdx = 1
    ), /* Last Row index in Batch B is 1 in JSON file 1 */
    FirstPageRequestTestContext(
      pageSize = 6,
      expScanFilesCnt = 7, /* 1 (batch A) + 1 (batch B) + 5 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_3, /* Batch C from checkpoint file 3 */
      expLastReadRowIdx = 4
    ), /* Last Row index in Batch C is 4 in checkpoint file 3 */
    FirstPageRequestTestContext(
      pageSize = 100,
      expScanFilesCnt =
        20, /* 1 (batch A) + 1 (batch B) + 6 (batch C+D) + 7 (batch E+F) + 5 (batch G+H) */
      expBatchCnt = 8, /* return all batches A through H */
      expLastReadLogFile = MULTI_CHECKPOINT_FILE_0_1, /* Batch H from checkpoint file 1 */
      expLastReadRowIdx = 1
    ) /* Last Row index in Batch H is 1 in checkpoint file 1 */
  ).foreach { testCase =>
    test(s"Multi-part checkpoints with jsons - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tablePath = getTestResourceFilePath("kernel-pagination-multi-part-checkpoints"))
    }
  }

  // ===== V2 parquet checkpoint with sidecar files test cases =====
  /**
   * 2.checkpoint.uuid.parquet:
   * - Batch A: 5 rows, 0 selected AddFiles
   * sidecar 1:
   * - Batch B: 3 rows, 3 selected AddFiles
   * sidecar 2:
   * - Batch C: 1 row, 1 selected AddFiles
   */

  val PARQUET_MANIFEST_SIDECAR_1 =
    "00000000000000000002.checkpoint.0000000001.0000000002." +
      "055454d8-329c-4e0e-864d-7f867075af33.parquet"
  val PARQUET_MANIFEST_SIDECAR_2 =
    "00000000000000000002.checkpoint.0000000002.0000000002." +
      "33321cc1-9c55-4d1f-8511-fafe6d2e1133.parquet"

  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 3, /* 0 (batch A) + 3 (batch B) */
      expBatchCnt = 2, /* return batch A, B */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_1, /* Batch B from sidecar 1 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch B is 2 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 3,
      expScanFilesCnt = 3, /* 0 (batch A) + 3 (batch B) */
      expBatchCnt = 2, /* return batch A, B */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_1, /* Batch B from sidecar 1 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch B is 2 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 4, /* 0 (batch A) + 3 (batch B) + 1 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_2, /* Batch C from sidecar 2 */
      expLastReadRowIdx = 0
    ), /* Last Row index in Batch C is 0 in sidecar 2 */
    FirstPageRequestTestContext(
      pageSize = 10000,
      expScanFilesCnt = 4, /* 0 (batch A) + 3 (batch B) + 1 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_2, /* Batch C from sidecar 2 */
      expLastReadRowIdx = 0
    ) /* Last Row index in Batch C is 0 in sidecar 2 */
  ).foreach { testCase =>
    test(s"V2 parquet checkpoints (and sidecars) - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(2L),
        tablePath = getTestResourceFilePath("kernel-pagination-v2-checkpoint-parquet"))
    }
  }

  // ===== V2 parquet checkpoint with sidecar files with jsons test cases =====
  /**
   * 00000000000000000003.json
   *  - Batch A: 2 rows, 1 selected AddFiles
   * 2.checkpoint.uuid.parquet:
   *  - Batch B: 5 rows, 0 selected AddFiles
   * sidecar 1:
   *  - Batch C: 3 rows, 3 selected AddFiles
   * sidecar 2:
   *  - Batch D: 1 row, 1 selected AddFiles
   */
  val JSON_FILE_3 = "00000000000000000003.json"
  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 1, /* 1 AddFile in Batch A */
      expBatchCnt = 1, /* return only batch A */
      expLastReadLogFile = JSON_FILE_3, /* Batch A from JSON file 3 */
      expLastReadRowIdx = 1
    ), /* Last Row index in Batch A is 1 in JSON file 3 */
    FirstPageRequestTestContext(
      pageSize = 2,
      expScanFilesCnt = 4, /* 1 (batch A) + 0 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_1, /* Batch C from sidecar 1 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch C is 2 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 3,
      expScanFilesCnt = 4, /* 1 (batch A) + 0 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_1, /* Batch C from sidecar 1 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch C is 2 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 4, /* 1 (batch A) + 0 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_1, /* Batch C from sidecar 1 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch C is 2 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 10000,
      expScanFilesCnt = 5, /* 1 (batch A) + 0 (batch B) + 3 (batch C) + 1 (batch D) */
      expBatchCnt = 4, /* return batch A, B, C, D */
      expLastReadLogFile = PARQUET_MANIFEST_SIDECAR_2, /* Batch D from sidecar 2 */
      expLastReadRowIdx = 0
    ) /* Last Row index in Batch D is 0 in sidecar 2 */
  ).foreach { testCase =>
    test(
      s"v2 parquet checkpoints (and sidecars) with json delta commit files - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(3L),
        tablePath = getTestResourceFilePath("kernel-pagination-v2-checkpoint-parquet"))
    }
  }

  // ===== V2 json checkpoint with sidecar files test cases =====
  /**
   * 2.checkpoint.uuid.json:
   *  - Batch A: 5 rows, 0 selected AddFiles
   * sidecar 1:
   *  - Batch B: 1 rows, 1 selected AddFiles
   * sidecar 2:
   *  - Batch C: 3 row, 3 selected AddFiles
   */
  val JSON_MANIFEST = "00000000000000000002.checkpoint." +
    "6374b053-df23-479b-b2cf-c9c550132b49.json"
  val JSON_MANIFEST_SIDECAR_1 =
    "00000000000000000002.checkpoint.0000000001.0000000002." +
      "bd1885fd-6ec0-4370-b0f5-43b5162fd4de.parquet"
  val JSON_MANIFEST_SIDECAR_2 =
    "00000000000000000002.checkpoint.0000000002.0000000002." +
      "0a8d73ee-aa83-49d0-9583-c99db75b89b2.parquet"

  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 1, /* 0 (batch A) + 1 (batch B) */
      expBatchCnt = 2, /* return batch A, B */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_1, /* Batch B from sidecar 1 */
      expLastReadRowIdx = 0
    ), /* Last Row index in Batch B is 0 in sidecar 1 */
    FirstPageRequestTestContext(
      pageSize = 2,
      expScanFilesCnt = 4, /* 0 (batch A) + 1 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch C from sidecar 2 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch C is 2 in sidecar 2 */
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 4, /* 0 (batch A) + 1 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch C from sidecar 2 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch C is 2 in sidecar 2 */
    FirstPageRequestTestContext(
      pageSize = 10000,
      expScanFilesCnt = 4, /* 0 (batch A) + 1 (batch B) + 3 (batch C) */
      expBatchCnt = 3, /* return batch A, B, C */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch C from sidecar 2 */
      expLastReadRowIdx = 0
    ) /* Last Row index in Batch C is 0 in sidecar 2 */
  ).foreach { testCase =>
    test(s"v2 json checkpoint (and sidecars) - page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(2L),
        tablePath = getTestResourceFilePath("kernel-pagination-v2-checkpoint-json"))
    }
  }

  // ===== V2 json checkpoint with sidecar files with jsons test cases =====
  /**
   * 00000000000000000003.json
   *  - Batch A: 2 rows, 1 selected AddFiles
   * 2.checkpoint.uuid.json:
   *  - Batch B: 5 rows, 0 selected AddFiles
   * sidecar 1:
   *  - Batch C: 1 rows, 1 selected AddFiles
   * sidecar 2:
   *  - Batch D: 3 row, 3 selected AddFiles
   */
  Seq(
    FirstPageRequestTestContext(
      pageSize = 1,
      expScanFilesCnt = 1, /* 1 AddFile in Batch A */
      expBatchCnt = 1, /* return only batch A */
      expLastReadLogFile = JSON_FILE_3, /* Batch A from JSON file 3 */
      expLastReadRowIdx = 1
    ), /* Last Row index in Batch A is 1 in JSON file 3 */
    FirstPageRequestTestContext(
      pageSize = 3,
      expScanFilesCnt = 5, /* 1 (batch A) + 0 (batch B) + 1 (batch C) + 3 (batch D) */
      expBatchCnt = 4, /* return batch A, B, C, D */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch D from sidecar 2 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch D is 2 in sidecar 2 */
    FirstPageRequestTestContext(
      pageSize = 4,
      expScanFilesCnt = 5, /* 1 (batch A) + 0 (batch B) + 1 (batch C) + 3 (batch D) */
      expBatchCnt = 4, /* return batch A, B, C, D */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch D from sidecar 2 */
      expLastReadRowIdx = 2
    ), /* Last Row index in Batch D is 2 in sidecar 2 */
    FirstPageRequestTestContext(
      pageSize = 10000,
      expScanFilesCnt = 5, /* 1 (batch A) + 0 (batch B) + 1 (batch C) + 3 (batch D) */
      expBatchCnt = 4, /* return batch A, B, C, D */
      expLastReadLogFile = JSON_MANIFEST_SIDECAR_2, /* Batch D from sidecar 2 */
      expLastReadRowIdx = 2
    ) /* Last Row index in Batch D is 2 in sidecar 2 */
  ).foreach { testCase =>
    test(s"v2 json checkpoint files (and sidecars) with json delta commit files - " +
      s"page size ${testCase.pageSize}") {
      runCompletePaginationTest(
        testCase = testCase,
        tableVersionOpt = Optional.of(3L),
        tablePath = getTestResourceFilePath("kernel-pagination-v2-checkpoint-json"))
    }
  }
  // TODO: tests for log compaction files
}
