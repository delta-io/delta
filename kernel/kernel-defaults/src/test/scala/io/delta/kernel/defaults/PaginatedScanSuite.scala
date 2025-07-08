import java.util.Optional

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
  // TODO: test call hasNext() twice

  test("Read First Page: basic pagination with single JSON file") {
    withTempDir { tempDir =>
      /**
       *   Creates a Delta table with 10 Parquet data files; each file contains 10 rows
       * * 00000000000000000000.json contains 10 AddFile actions — one per file
       */
      spark.range(0, 100, 1, 10).write.format("delta").save(tempDir.getCanonicalPath)

      val snapshot = latestSnapshot(tempDir.getCanonicalPath)
      val pageSize = 8 // TODO: change different page sizes
      val paginatedScan = snapshot.getScanBuilder
        .buildPaginated(pageSize, Optional.empty() /* page token opt */ )
        .asInstanceOf[PaginatedScanImpl]

      /**
       * Creates a custom engine with batch size 5;
       * 00000000000000000000.json contains 3 batches, 10 active AddFiles in total
       * Batch 1: 5 rows, 2 selected AddFiles
       * Batch 2: 5 rows, 5 selected AddFiles
       * Batch 3: 3 rows, 3 selected AddFiles
       */
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
      val customEngine = DefaultEngine.create(hadoopConf)

      val paginatedIter = paginatedScan.getScanFiles(customEngine)
      val firstPageFiles = collection.mutable.Buffer[FilteredColumnarBatch]()
      while (paginatedIter.hasNext) {
        val batch = paginatedIter.next()
        firstPageFiles += batch
      }
      assert(firstPageFiles.nonEmpty, "First page should contain some files")

      /** Verify we get 3 batches and 10 ScanFiles in total in the first page */
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalFileCountsReturned = fileCounts.sum
      assert(
        totalFileCountsReturned == 10,
        s"First page should contain 10 files, got $totalFileCountsReturned")

      assert(
        fileCounts.length == 3,
        s"First page should contain 3 batches, got ${fileCounts.length}")

      logger.info(s"Total num batches returned in page one = ${fileCounts.length}")
      logger.info(s"Total num Parquet Files fetched in page one = $totalFileCountsReturned")

      /** Verify we get correct page token for next page */
      val lastReadLogFileName = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReadLogFileName
      val lastReturnedRowIndex = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReturnedRowIndex

      assert(
        lastReadLogFileName.endsWith("00000000000000000000.json"),
        s"New Page Token: LastReadLogFilename should be 00000000000000000000.json, " +
          s"got $lastReadLogFileName")

      assert(
        lastReturnedRowIndex == 12,
        s"New Page Token:lastReadRowIndex should be 12, got $lastReturnedRowIndex")

      logger.info(s"New PageToken: lastReadLogFileName  = $lastReadLogFileName")
      logger.info(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")

      paginatedIter.close()
    }
  }

  test("Read First Page: basic pagination with multiple JSON files") {
    withTempDir { tempDir =>
      /**
       * Create multiple commits to generate multiple JSON files
       * First commit: files 0-4 (5 files)
       * Second commit: files 5-9 (5 more files)
       * Third commit: files 10-14 (5 more files)
       * This should create 3 JSON files:
       * 00000000000000000000.json, 00000000000000000001.json, 00000000000000000002.json
       */
      val tablePath = tempDir.getCanonicalPath
      spark.range(0, 50, 1, 5).write.format("delta").save(tablePath)
      spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)
      spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)

      val snapshot = latestSnapshot(tempDir.getCanonicalPath)
      val pageSize = 9 // TODO: change different page sizes
      val paginatedScan =
        snapshot.getScanBuilder.buildPaginated(pageSize, Optional.empty() /* page token opt */ )
          .asInstanceOf[PaginatedScanImpl]

      /**
       * Creates a custom engine with batch size 4;
       * 00000000000000000002.json contains 2 batches, 5 active AddFiles in total
       * Batch 1: 4 rows, 3 selected AddFiles
       * Batch 2: 2 rows, 2 selected AddFiles
       *
       * 00000000000000000001.json contains 2 batches, 5 active AddFiles in total
       * * Batch 1: 4 rows, 3 selected AddFiles
       * * Batch 2: 2 rows, 2 selected AddFiles
       *
       * 00000000000000000000.json contains 2 batches, 5 active AddFiles in total
       * * Batch 1: 4 rows, 3 selected AddFiles
       * * Batch 2: 2 rows, 2 selected AddFiles
       */
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("delta.kernel.default.json.reader.batch-size", "4")
      hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", "4")
      val customEngine = DefaultEngine.create(hadoopConf)

      val paginatedIter = paginatedScan.getScanFiles(customEngine)
      val firstPageFiles = collection.mutable.Buffer[FilteredColumnarBatch]()

      while (paginatedIter.hasNext) {
        val batch = paginatedIter.next()
        firstPageFiles += batch
      }
      assert(firstPageFiles.nonEmpty, "First page should contain some files")

      /** Verify we get 4 batches and 10 ScanFiles in total in the first page */
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalFileCountsReturned = fileCounts.sum
      assert(
        totalFileCountsReturned == 10,
        s"First page should contain 10 files, got $totalFileCountsReturned")

      assert(
        fileCounts.length == 4,
        s"First page should contain 3 batches, got ${fileCounts.length}")

      logger.info(s"Total num batches returned in page one = ${fileCounts.length}")
      logger.info(s"Total num Parquet Files fetched in page one = $totalFileCountsReturned")

      /** Verify we get correct page token for next page */
      val lastReadLogFileName = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReadLogFileName
      val lastReturnedRowIndex = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReturnedRowIndex

      assert(
        lastReadLogFileName.endsWith("00000000000000000001.json"),
        s"New Page Token: LastReadLogFilename should be 00000000000000000001.json," +
          s"got $lastReadLogFileName")

      assert(
        lastReturnedRowIndex == 5,
        s"New Page Token:lastReadRowIndex should be 5, got $lastReturnedRowIndex")

      logger.info(s"New PageToken: lastReadLogFileName  = $lastReadLogFileName")
      logger.info(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")

      paginatedIter.close()
    }
  }

  test("Read First Page: basic pagination with one checkpoint file " +
    "and multiple JSON files") {
    withTempDir { tempDir =>
      /**
       * This should create:
       * 00000000000000000010.checkpoint.parquet, 00000000000000000011.json, 00000000000000000012.json
       */
      val tablePath = tempDir.getCanonicalPath

      for (i <- 0 until 10) {
        val mode = if (i == 0) "overwrite" else "append"
        spark.range(i * 10, (i + 1) * 10, 1, 2)
          .write.format("delta").mode(mode).save(tablePath)
      }

      val deltaLog = DeltaLog.forTable(spark, tablePath)
      deltaLog.checkpoint()

      for (i <- 10 until 13) {
        spark.range(i * 10, (i + 1) * 10, 1, 2)
          .write.format("delta").mode("append").save(tablePath)
      }

      val snapshot = latestSnapshot(tablePath)
      val pageSize = 18 // TODO: test different page sizes
      val paginatedScan = snapshot.getScanBuilder().buildPaginated(
        pageSize,
        Optional.empty()).asInstanceOf[PaginatedScanImpl]

      /**
       * Creates a custom engine with batch size 5;
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
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
      hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", "5")
      val customEngine = DefaultEngine.create(hadoopConf)

      val paginatedIter = paginatedScan.getScanFiles(customEngine)
      val firstPageFiles = collection.mutable.Buffer[FilteredColumnarBatch]()

      while (paginatedIter.hasNext) {
        val batch = paginatedIter.next()
        firstPageFiles += batch
      }
      assert(firstPageFiles.nonEmpty, "First page should contain some files")

      /** Verify we get 5 batches and 19 ScanFiles in total in the first page */
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalFileCountsReturned = fileCounts.sum
      assert(
        totalFileCountsReturned == 19,
        s"First page should contain 19 files, got $totalFileCountsReturned")

      assert(
        fileCounts.length == 5,
        s"First page should contain 5 batches, got ${fileCounts.length}")

      logger.info(s"Total num batches returned in page one = ${fileCounts.length}")
      logger.info(s"Total num Parquet Files fetched in page one = $totalFileCountsReturned")

      /** Verify we get correct page token for next page */
      val lastReadLogFileName = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReadLogFileName
      val lastReturnedRowIndex = PageToken.fromRow(paginatedIter.getCurrentPageToken)
        .getLastReturnedRowIndex

      assert(
        lastReadLogFileName.endsWith("00000000000000000010.checkpoint.parquet"),
        s"New Page Token: LastReadLogFilename should be 00000000000000000010.checkpoint.parquet," +
          s"got $lastReadLogFileName")

      assert(
        lastReturnedRowIndex == 14,
        s"New Page Token:lastReadRowIndex should be 14, got $lastReturnedRowIndex")

      logger.info(s"New PageToken: lastReadLogFileName  = $lastReadLogFileName")
      logger.info(s"New PageToken: lastReadRowIndex = $lastReturnedRowIndex")
    }
  }
}
