import java.util.Optional

import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.internal.PaginatedScanImpl

import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite

class PaginatedScanSuite extends AnyFunSuite with TestUtils
    with ExpressionTestUtils with SQLHelper with DeltaTableWriteSuiteBase {
  //////////////////////////////////////////////////////////////////////////////////
  // Pagination tests for PaginatedScan
  //////////////////////////////////////////////////////////////////////////////////

  // TODO: test page size < JSON file size
  // TODO: test page size = JSON file size (page size = 10)
  // TODO: test call hasNext() twice
  test("getPaginatedScanFiles - basic pagination with single JSON file") {
    withTempDir { tempDir =>
      /**
       *       Creates a Delta table with 10 Parquet files
       *       Each file contains 10 rows
       *       _delta_log/00000000000000000000.json contains 10 AddFile actions — one per file
       *       parquet 0: 0 - 9
       *       parquet 1: 10 -19
       *       parquet 9: 90 -99
       */
      spark.range(0, 100, 1, 10).write.format("delta").save(tempDir.getCanonicalPath)

      val snapshot = latestSnapshot(tempDir.getCanonicalPath)
      // Try read first page (with size = 12)
      val paginatedScan = snapshot.getScanBuilder().buildPaginated(8, Optional.empty())
        .asInstanceOf[PaginatedScanImpl]

      if (paginatedScan != null) {
        System.out.println("has built a real paginated scan")
      }

      // Create a custom engine with batch size 5
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
      val customEngine = DefaultEngine.create(hadoopConf)
      System.out.println("has get engine lalala")

      val paginatedIter = paginatedScan.getScanFiles(customEngine)
      val firstPageFiles = collection.mutable.Buffer[FilteredColumnarBatch]()

      while (paginatedIter.hasNext) {
        val batch = paginatedIter.next()
        firstPageFiles += batch
      }
      assert(firstPageFiles.nonEmpty, "First page should contain some files")

      // Verify we got at most 12 AddFiles across all batches
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalFileCountsReturned = fileCounts.sum
      println(s"Num of batches returned in page = ${fileCounts.length}")

      //  val nextRowIndex = PpaginatedIter.getCurrentPageToken)
      //  val nextStartingFile = paginatedIter.getCurrentPageToken.getStartingFileName
      assert(
        totalFileCountsReturned <= 12,
        s"First page should contain at most 12 files, got $totalFileCountsReturned")

      // println(s"nextStartingFile = ${nextStartingFile.toString}")
      // println(s"nextRowIndex = ${nextRowIndex.toString}")
      println(s"Total Parquet Files fetched = ${totalFileCountsReturned.toString}")

      paginatedIter.close()
    }
  }

  test("getPaginatedScanFiles - basic pagination with multiple JSON files") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath

      // Create multiple commits to generate multiple JSON files
      // First commit: files 0-4 (5 files)
      spark.range(0, 50, 1, 5).write.format("delta").save(tablePath)

      // Second commit: files 5-9 (5 more files)
      spark.range(50, 100, 1, 5).write.format("delta").mode("append").save(tablePath)

      // Third commit: files 10-14 (5 more files)
      spark.range(100, 150, 1, 5).write.format("delta").mode("append").save(tablePath)
      // This should create: 00000000000000000000.json, 00000000000000000001.json, 00000000000000000002.json

      val snapshot = latestSnapshot(tempDir.getCanonicalPath)
      // Try read first page (with size = 9)
      val paginatedScan =
        snapshot.getScanBuilder().buildPaginated(
          9,
          Optional.empty()).asInstanceOf[PaginatedScanImpl]

      // Create a custom engine with batch size 4
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

      // Verify we got at most 10 AddFiles across all batches
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalAddFiles = fileCounts.sum
      println(s"Num of batches returned in page = ${fileCounts.length}")

      // Get pagination state info
      //      val paginatedAddFilesIter = paginatedIter.asInstanceOf[PaginatedAddFilesIterator]
      //  val lastRowIndex = paginatedAddFilesIter.getNewPageToken.getRowIndex()
      //  val nextStartingFile = paginatedAddFilesIter.getNewPageToken.getStartingFileName()

      //   println(s"lastRowIndex = ${lastRowIndex.toString}")
      //   println(s"nextStartingFile = ${nextStartingFile.toString}")
      println(s"totalAddFiles = ${totalAddFiles.toString}")

      assert(totalAddFiles <= 10, s"First page should contain at most 10 files, got $totalAddFiles")
      assert(totalAddFiles > 0, s"Should have some files, got $totalAddFiles")

      paginatedIter.close()

      // Verify that pagination spans multiple JSON files using a separate scan instance
      val verificationScan = snapshot.getScanBuilder().build()
      val allFiles = collectScanFileRows(verificationScan)
      val totalFilesInTable = allFiles.length
      assert(totalFilesInTable == 15, s"Should have 15 total files, got $totalFilesInTable")
    }
  }

  /*
  test("getPaginatedScanFiles - basic pagination with one checkpoint file " +
    "and multiple JSON files") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath

      // Create many commits to trigger checkpoint creation (checkpoints usually happen every 10 commits)
      // First, create 10 commits to trigger a checkpoint
      for (i <- 0 until 10) {
        val mode = if (i == 0) "overwrite" else "append"
        spark.range(i * 10, (i + 1) * 10, 1, 2)
          .write.format("delta").mode(mode).save(tablePath)
      }

      // Force checkpoint creation
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      deltaLog.checkpoint()

      // Add a few more commits after checkpoint to create additional JSON files
      for (i <- 10 until 13) {
        spark.range(i * 10, (i + 1) * 10, 1, 2)
          .write.format("delta").mode("append").save(tablePath)
      }

      // This should create: 00000000000000000010.checkpoint.parquet(checkpoint file), 00000000000000000011.json, 00000000000000000012.json
      val snapshot = latestSnapshot(tablePath)
      val paginatedScan = snapshot.getScanBuilder().buildPaginated(
        15,
        Optional.empty()).asInstanceOf[PaginatedScanImpl]

      // Create a custom engine with batch size 5
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
      hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", "5")
      val customEngine = DefaultEngine.create(hadoopConf)

      // Test pagination starting from the checkpoint (should be processed first)
      val paginatedIter = paginatedScan.getScanFiles(customEngine)
      val firstPageFiles = collection.mutable.Buffer[FilteredColumnarBatch]()

      while (paginatedIter.hasNext) {
        val batch = paginatedIter.next()
        firstPageFiles += batch
      }
      assert(firstPageFiles.nonEmpty, "First page should contain some files")

      // Verify we got at most 10 AddFiles across all batches
      val fileCounts: Seq[Long] = firstPageFiles.map(_.getPreComputedNumSelectedRows.get().toLong)
      val totalAddFiles = fileCounts.sum

      // Get pagination state info
      //      val paginatedAddFilesIter = paginatedIter.asInstanceOf[PaginatedAddFilesIterator]
      //      val lastRowIndex = paginatedAddFilesIter.getNewPageToken.getRowIndex()
      //      val nextStartingFile = paginatedAddFilesIter.getNewPageToken.getStartingFileName()

      //      println(s"lastRowIndex = ${lastRowIndex.toString}")
      //     println(s"nextStartingFile = ${nextStartingFile.toString}"
      //     println(s"totalAddFiles = ${totalAddFiles.toString}")

      assert(totalAddFiles == 19, s"First page should contain 19 files, got $totalAddFiles")
      assert(totalAddFiles > 0, s"Should have some files, got $totalAddFiles")

      paginatedIter.close()

      // Verify we have both checkpoint and JSON files in the log using a separate scan instance
      val verificationScan = snapshot.getScanBuilder().build()
      val allFiles = collectScanFileRows(verificationScan)
      val totalFilesInTable = allFiles.length
      assert(
        totalFilesInTable == 26,
        s"Should have 26 total files (13 commits * 2 files each), got $totalFilesInTable")
    }
  } */

}
