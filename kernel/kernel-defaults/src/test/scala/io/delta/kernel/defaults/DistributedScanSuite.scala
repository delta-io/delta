package io.delta.kernel.defaults

import java.util
import java.util.{List, Optional}

import scala.jdk.CollectionConverters._

import io.delta.kernel.ScanBuilder
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.test.AbstractTableManagerAdapter
import io.delta.kernel.defaults.utils.TestUtilsWithTableManagerAPIs
import io.delta.kernel.internal.replay.LogReplay

import org.apache.spark.sql.delta.DeltaLog

import org.scalatest.funsuite.AnyFunSuite

// test new Kernel API for distributed log replay
class distributedScanSuite extends AnyFunSuite with TestUtilsWithTableManagerAPIs {
  private val customEngine: DefaultEngine = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("delta.kernel.default.json.reader.batch-size", "5")
    hadoopConf.set("delta.kernel.default.parquet.reader.batch-size", "5")
    DefaultEngine.create(hadoopConf)
  }

  val tableManager: AbstractTableManagerAdapter = getTableManagerAdapter

  // TODO: this can be a testUtil?
  private def getScanBuilder(tablePath: String, tableVersionOpt: Optional[Long]): ScanBuilder = {
    val resolvedTableAdapter = {
      if (tableVersionOpt.isPresent) {
        tableManager.getResolvedTableAdapterAtVersion(
          defaultEngine,
          tablePath,
          tableVersionOpt.get())
      } else {
        tableManager.getResolvedTableAdapterAtLatest(defaultEngine, tablePath)
      }
    }
    resolvedTableAdapter.getScanBuilder()
  }

  private def runDistributedScanTest(
      tablePath: String,
      tableVersionOpt: Optional[Long] = Optional.empty()): Unit = {
    val scan =
      getScanBuilder(tablePath = tablePath, tableVersionOpt = tableVersionOpt).build()
    // ====== serial get Scan Files =====
    var lastTime = System.currentTimeMillis()
    // val allScanFiles = scan.getScanFiles(defaultEngine).toSeq
    var currentTime = System.currentTimeMillis()
    var duration = currentTime - lastTime
    System.out.println("Serial log replay time " + duration)
    lastTime = currentTime

    // ====== "distributed" get Scan Files =====
    val res = scan.getScanFilesFromJSON(defaultEngine)
    val tombstones = res._2
    // Convert Java list to Scala mutable buffer
    val allScanFilesDistributed = res._1.asScala

    val allCheckpoints = scan.getLogSegmentCheckpointFiles

    allCheckpoints.asScala.foreach { checkpoint =>
      {
        val t1: Long = System.currentTimeMillis
        val files =
          LogReplay.getAddFilesForOneCheckpoint(defaultEngine, tablePath, checkpoint, tombstones)
        allScanFilesDistributed ++= files.asScala

        val checkpointFileName: String = checkpoint.getPath
          .substring(checkpoint.getPath.lastIndexOf('/') + 1)
        val durationForOneFile: Long = System.currentTimeMillis - t1
        System.out.println(
          "Non-distributed getScanFiles for {} in {} ms",
          checkpointFileName,
          durationForOneFile)
      } // 15.8 ? average of num of AddFile in each commit?
    }

    currentTime = System.currentTimeMillis()
    duration = currentTime - lastTime
    System.out.println("Distributed log replay time " + duration)

//    assert(allScanFiles.size == allScanFilesDistributed.size)
//    for (i <- allScanFiles.indices) {
//      val normalBatch = allScanFiles(i)
//      val distributedBatch = allScanFilesDistributed(i)
//      // assert(normalBatch.getFilePath.equals(paginatedBatch.getFilePath))
////      System.out.println(
////        "batch " + normalBatch.getData.getSize + " " + distributedBatch.getData.getSize)
//      // assert(normalBatch.getData.getSize == distributedBatch.getData.getSize)
//    }
  }

  test("simple test") {
    withTempDir { tempDir =>
      val tablePath = "/home/ada.ma/delta/dsv2/src/test/resources/ada_1000_rows"
//      val tablePath = "/home/ada.ma/delta/kernel/kernel-defaults/src/test/" +
//        "resources/ada-multi-checkpoint"
      spark.range(0, 1800)
        .repartition(18) // 10 files = 10 AddFile actions
        .write.format("delta").mode("overwrite").save(tablePath)

//      // Force multi-part checkpoint creation with small part size
//      withSQLConf(
//        "spark.databricks.delta.checkpoint.partSize" -> "6" // 10 AddFiles → 3 checkpoint parts
//      ) {
//        val deltaLog = DeltaLog.forTable(spark, tablePath)
//        deltaLog.checkpoint() // multi-part checkpoint at version 0
//      }
//
//      // Commits 1 and 2: Add 1 file each
//      for (i <- 1 to 2) {
//        spark.range(i * 1000, i * 1000 + 100) // small data
//          .coalesce(1) // 1 file
//          .write.format("delta").mode("append").save(tablePath)
//      }
//
//      runDistributedScanTest(tablePath, Optional.empty())
    }
  }

  test("performance test: large table") {
    val tablePath = "/home/ada.ma/delta/dsv2/src/test/resources/ada_1M_rows"
    // table with 10,000 rows, 1300 checkpoint files
    runDistributedScanTest(tablePath, Optional.empty())
    // Serial log replay time 9367
    // "Distributed" log replay time 7682
  }

  test("create checkpoint files") {
    val tablePath = "/home/ada.ma/delta/dsv2/src/test/resources/ada_1M_rows"

    val numAddFiles = 10000
    spark.range(0, numAddFiles)
      .repartition(numAddFiles) // ensures each row goes to its own file → 10,000 AddFiles
      .write.format("delta").mode("append").save(tablePath)

    val partSize = 50000 // 10 checkpoint  * 50000 AddFiles
    withSQLConf("spark.databricks.delta.checkpoint.partSize" -> partSize.toString) {
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      deltaLog.checkpoint() // checkpoint at version 0
    }
  }

  test("create one-version Delta table with many checkpoint parts, 1 row per AddFile") {
    // val tablePath = "/home/ada.ma/delta/kernel/kernel-defaults/src/test/resources/" +
    "ada-5-checkpoint"

    val tablePath = "/home/ada.ma/delta/dsv2/src/test/resources/ada_100000_rows"

    // 20 checkpoint * 500 AddFile
    val numAddFiles = 10000
    val partSize = 2000

    // Generate 10,000 rows, each to its own file
    spark.range(0, numAddFiles)
      .repartition(numAddFiles) // ensures each row goes to its own file → 10,000 AddFiles
      .write.format("delta").mode("append").save(tablePath)

    withSQLConf("spark.databricks.delta.checkpoint.partSize" -> partSize.toString) {
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      deltaLog.checkpoint() // checkpoint at version 0
    }
  }
}
