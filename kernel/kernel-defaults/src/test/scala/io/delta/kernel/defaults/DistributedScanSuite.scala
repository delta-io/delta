package io.delta.kernel.defaults

import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.ScanBuilder
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
    val allScanFiles = scan.getScanFiles(defaultEngine).toSeq
    val res = scan.getScanFilesFromJSON(defaultEngine)
    val tombstones = res._2
    // Convert Java list to Scala mutable buffer
    val allScanFilesDistributed = res._1.asScala

    val allCheckpoints = scan.getLogSegmentCheckpointFiles
    allCheckpoints.asScala.foreach { checkpoint =>
      {
        val files =
          LogReplay.getAddFilesForOneCheckpoint(defaultEngine, tablePath, checkpoint, tombstones)
        allScanFilesDistributed ++= files.asScala
      }
    }

    assert(allScanFiles.size == allScanFilesDistributed.size)
    for (i <- allScanFiles.indices) {
      val normalBatch = allScanFiles(i)
      val distributedBatch = allScanFilesDistributed(i)
      // assert(normalBatch.getFilePath.equals(paginatedBatch.getFilePath))
      System.out.println(
        "batch " + normalBatch.getData.getSize + " " + distributedBatch.getData.getSize)
      // assert(normalBatch.getData.getSize == distributedBatch.getData.getSize)
    }
  }

  test("simple test") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
//      val tablePath = "/home/ada.ma/delta/kernel/kernel-defaults/src/test/" +
//        "resources/ada-multi-checkpoint"
      spark.range(0, 1800)
        .repartition(18) // 10 files = 10 AddFile actions
        .write.format("delta").mode("overwrite").save(tablePath)

      // Force multi-part checkpoint creation with small part size
      withSQLConf(
        "spark.databricks.delta.checkpoint.partSize" -> "6" // 10 AddFiles → 3 checkpoint parts
      ) {
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        deltaLog.checkpoint() // multi-part checkpoint at version 0
      }

      // Commits 1 and 2: Add 1 file each
      for (i <- 1 to 2) {
        spark.range(i * 1000, i * 1000 + 100) // small data
          .coalesce(1) // 1 file
          .write.format("delta").mode("append").save(tablePath)
      }

      runDistributedScanTest(tablePath, Optional.empty())
    }
  }
}
