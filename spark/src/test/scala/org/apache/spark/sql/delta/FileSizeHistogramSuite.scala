/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.util.UUID

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaTestUtils.countSparkJobs
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.stats.FileSizeHistogramUtils.compress
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class FileSizeHistogramSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    // enable the feature in tests
    super.sparkConf.set(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key, "true")
    super.sparkConf.set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
  }

  private def getTableName: String = "delta_tbl_" + UUID.randomUUID().toString.replace("-", "")

  /**
   * Runs a given operation sql and validates results and histograms
   */
  private def validateCommitStatsOnDeltaOperation(
      tableName: String,
      operationSql: String,
      expectedResult: Seq[Int],
      fileSizeDistribution: Seq[Int],
      addFilesDistribution: Seq[Int],
      removeFilesDistribution: Seq[Int]): Unit = {
    val usageLogs = Log4jUsageLogger.track {
      sql(operationSql)
    }

    val df = spark.read.format("delta").table(tableName)
    checkDatasetUnorderly(df.as[Int], expectedResult: _*)
    validateCommitStats(
      usageLogs,
      fileSizeDistribution,
      addFilesDistribution,
      removeFilesDistribution)
  }

  /**
   * Validates file size distribution histogram in CommitStat from the usage logs
   */
  private def validateCommitStats(
      usageLogs: Seq[UsageRecord],
      fileSizeDistribution: Seq[Int],
      addFilesDistribution: Seq[Int],
      removeFilesDistribution: Seq[Int]): Unit = {
    val commitStats = usageLogs
      .filter(_.metric === "tahoeEvent")
      .filter(_.tags.get("opType") === Some("delta.commit.stats"))
    assert(commitStats.length === 1)
    val commitStat = JsonUtils.fromJson[CommitStats](commitStats.head.blob)
    val actualFileSizeDistribution = commitStat.fileSizeHistogram.get.fileCounts.toSeq
    val actualAddFilesDistribution = commitStat.addFilesHistogram.get.fileCounts.toSeq
    val actualRemoveFilesDistribution = commitStat.removeFilesHistogram.get.fileCounts.toSeq
    assert(actualFileSizeDistribution === fileSizeDistribution)
    assert(actualAddFilesDistribution === addFilesDistribution)
    assert(actualRemoveFilesDistribution === removeFilesDistribution)

    // asserts that the totalBytes in each bucket lies in the range
    // [fileCountInBucket * low, fileCountInBucket * (high - 1)]
    def validateTotalBytesInHistogram(h: FileSizeHistogram): Unit = {
      h.sortedBinBoundaries.zip(h.fileCounts).zip(h.totalBytes).zipWithIndex.foreach {
        case (((low, fileCount), totalBytes), index) =>
          assert(totalBytes >= low * fileCount)
          if (index + 1 < h.sortedBinBoundaries.length) {
            val high = h.sortedBinBoundaries(index + 1)
            assert(totalBytes <= fileCount * (high - 1))
          }
      }
    }
    validateTotalBytesInHistogram(commitStat.fileSizeHistogram.get)
    validateTotalBytesInHistogram(commitStat.addFilesHistogram.get)
    validateTotalBytesInHistogram(commitStat.removeFilesHistogram.get)
  }

  /**
   * Validates that the file size distribution histogram is absent in the CommitStats
   * from the usage logs
   */
  private def validateHistogramAbsentInCommitStats(usageLogs: Seq[UsageRecord]): Unit = {
    val commitStats = usageLogs
      .filter(_.metric === "tahoeEvent")
      .filter(_.tags.get("opType") === Some("delta.commit.stats"))
    assert(commitStats.length === 1)
    val commitStat = JsonUtils.fromJson[CommitStats](commitStats.head.blob)
    assert(commitStat.fileSizeHistogram.isEmpty)
    assert(commitStat.addFilesHistogram.isEmpty)
    assert(commitStat.removeFilesHistogram.isEmpty)
  }

  /**
   * Returns the current snapshot for a given tableName
   */
  private def getSnapshotForTable(tableName: String): Snapshot =
    DeltaLog.forTable(spark, TableIdentifier(tableName)).snapshot

  /**
   * Validates the number of jobs when
   * [[DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED]] is set to true or false
   */
  private def assertEqualJobsWithHistogramEnabledAndDisabled(f: String => Unit): Unit = {
    val tableName = getTableName

    val jobCountWhenEnabled = countSparkJobs(sparkContext, {
      withSQLConf(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "true") {
        val usageLogs = Log4jUsageLogger.track {
          f(s"${tableName}_enabled")
        }
        val commitStatsUsageLogs = usageLogs.filter(
          _.tags.get("opType") === Some("delta.commit.stats"))
        commitStatsUsageLogs.foreach { commitStatsUsageLog =>
          val commitStats = JsonUtils.fromJson[CommitStats](commitStatsUsageLog.blob)
          assert(commitStats.fileSizeHistogram.nonEmpty)
          assert(commitStats.addFilesHistogram.nonEmpty)
          assert(commitStats.removeFilesHistogram.nonEmpty)
        }
      }
    })

    val jobCountWhenDisabled = countSparkJobs(sparkContext, {
      withSQLConf(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "false") {
        val usageLogs = Log4jUsageLogger.track {
          f(s"${tableName}_disabled")
        }
        val commitStatsUsageLogs = usageLogs.filter(
          _.tags.get("opType") === Some("delta.commit.stats"))
        commitStatsUsageLogs.foreach { commitStatsUsageLog =>
          val commitStats = JsonUtils.fromJson[CommitStats](commitStatsUsageLog.blob)
          assert(commitStats.fileSizeHistogram.isEmpty)
          assert(commitStats.addFilesHistogram.isEmpty)
          assert(commitStats.removeFilesHistogram.isEmpty)
        }
      }
    })

    assert(jobCountWhenEnabled === jobCountWhenDisabled)
  }

  test("file size histogram - equality") {
    val binBoundaries: IndexedSeq[Long] = IndexedSeq(0, 5, 15)
    val h1 = FileSizeHistogram.apply(binBoundaries)
    val h2 = FileSizeHistogram.apply(binBoundaries)
    val h3 = FileSizeHistogram.apply(binBoundaries)

    Seq(1, 4, 5, 6, 14, 15, 16, 17).foreach { size =>
      h1.insert(size)
      h2.insert(size)
      h3.insert(size)
    }
    h3.remove(16) // belongs to bin 3

    assert(h1.hashCode() == h2.hashCode())
    assert(h1 == h2)
    assert(h1.hashCode() != h3.hashCode())
    assert(h1 != h3)
  }

  test("file size histogram - insert") {
    val binBoundaries: IndexedSeq[Long] = IndexedSeq(0, 5, 15)
    val histogram = FileSizeHistogram.apply(binBoundaries)

    assert(histogram.sortedBinBoundaries === Seq(0, 5, 15))
    assert(histogram.fileCounts.toSeq === Seq(0, 0, 0))
    assert(histogram.totalBytes.toSeq === Seq(0, 0, 0))

    // Insert few elements and validate histogram
    histogram.insert(-1) // negative size file - should be ignored
    histogram.insert(0) // belongs to bin 1
    histogram.insert(1) // belongs to bin 1
    histogram.insert(4) // belongs to bin 1
    histogram.insert(5) // belongs to bin 2
    histogram.insert(6) // belongs to bin 2
    histogram.insert(14) // belongs to bin 2
    histogram.insert(15) // belongs to bin 3
    histogram.insert(16) // belongs to bin 3
    histogram.insert(100) // belongs to bin 3
    assert(histogram.sortedBinBoundaries === Seq(0, 5, 15))
    assert(histogram.fileCounts.toSeq === Seq(3, 3, 3))
    assert(histogram.totalBytes.toSeq === Seq(5, 25, 131))
  }

  test("file size histogram - compress") {
    val binBoundaries = IndexedSeq(0L, 2, 4, 8, 20, 40, 60, 100, 1000, 10000, 100000)
    val histogram = FileSizeHistogram.apply(binBoundaries)
    assert(histogram.sortedBinBoundaries.toSeq ===
      Seq(0, 2, 4, 8, 20, 40, 60, 100, 1000, 10000, 100000))
    assert(histogram.fileCounts.toSeq === Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    assert(histogram.totalBytes.toSeq === Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    // 3 consecutive bins are empty at the beginning, 3 are empty in the middle and 3 are
    // empty at the end.
    histogram.insert(8)
    histogram.insert(101)
    histogram.insert(9)

    assert(histogram.sortedBinBoundaries ===
      Seq(0, 2, 4, 8, 20, 40, 60, 100, 1000, 10000, 100000))
    assert(histogram.fileCounts.toSeq === Seq(0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0))
    assert(histogram.totalBytes.toSeq === Seq(0, 0, 0, 17, 0, 0, 0, 101, 0, 0, 0))

    val compressedHistogram = compress(histogram)
    assert(compressedHistogram.sortedBinBoundaries === Seq(0, 8, 20, 100, 1000))
    assert(compressedHistogram.fileCounts.toSeq === Seq(0, 2, 0, 1, 0))
    assert(compressedHistogram.totalBytes.toSeq === Seq(0, 17, 0, 101, 0))

    histogram.insert(5)
    histogram.insert(10)
    histogram.insert(21)
    histogram.insert(1001)

    // 2 consecutive bins are empty in the beginning, 2 are empty in the middle and 2 are
    // empty at the end.
    val compressedHistogram1 = compress(histogram)
    assert(compressedHistogram1.sortedBinBoundaries.toSeq ===
      Seq(0, 4, 8, 20, 40, 100, 1000, 10000))
    assert(compressedHistogram1.fileCounts.toSeq === Seq(0, 1, 3, 1, 0, 1, 1, 0))
    assert(compressedHistogram1.totalBytes.toSeq === Seq(0, 5, 27, 21, 0, 101, 1001, 0))

    // Insert data in first and last bins also
    histogram.insert(1)
    histogram.insert(1)
    histogram.insert(100001)
    val compressedHistogram2 = compress(histogram)
    assert(compressedHistogram2.sortedBinBoundaries.toSeq ===
      Seq(0, 2, 4, 8, 20, 40, 100, 1000, 10000, 100000))
    assert(compressedHistogram2.fileCounts.toSeq === Seq(2, 0, 1, 3, 1, 0, 1, 1, 0, 1))
    assert(compressedHistogram2.totalBytes.toSeq === Seq(2, 0, 5, 27, 21, 0, 101, 1001, 0, 100001))
  }

  test("test feature flag off") {
    withSQLConf(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "false") {
      val tableName = getTableName
      withTable(tableName) {
        Seq(1).toDF("value").write.format("delta").saveAsTable(tableName)

        // write to delta table
        val usageLogs = Log4jUsageLogger.track {
          Seq(1, 2).toDF("value").write
            .mode("append").format("delta").saveAsTable(tableName)
        }

        // no histogram in CommitStats
        validateHistogramAbsentInCommitStats(usageLogs)

        // no histogram from Snapshot API
        assert(getSnapshotForTable(tableName).fileSizeHistogram.isEmpty)
        DeltaLog.clearCache()
        assert(getSnapshotForTable(tableName).fileSizeHistogram.isEmpty)
      }
    }
  }

  test("histogram is maintained correctly after multiple appends") {
    val tableName = getTableName
    withTable(tableName) {
      // This test adds different integer ranges to the table to create new files and then
      // asserts that the files are added to correct bins. These ranges may need some tweaks
      // in future when something changes on how delta (or underlying file format i.e. parquet)
      // compresses/writes the files.

      // 2 small files
      sparkContext.parallelize(Seq(1, 2, 3), 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)
      assert(getSnapshotForTable(tableName).numOfFiles === 2)

      // 1 medium size file
      (1 to 2000).toDF("value").repartition(1)
        .write.format("delta").mode("append").saveAsTable(tableName)
      assert(getSnapshotForTable(tableName).numOfFiles === 3)

      // 2 medium+ size file
      (1 to 10000).toDF("value").repartition(2)
        .write.format("delta").mode("append").saveAsTable(tableName)
      assert(getSnapshotForTable(tableName).numOfFiles === 5)

      // 1 large size file
      (10000 to 20000).toDF("value").repartition(1)
        .write.format("delta").mode("append").saveAsTable(tableName)
      assert(getSnapshotForTable(tableName).numOfFiles === 6)

      val histogram = getSnapshotForTable(tableName).fileSizeHistogram.map(compress).get
      assert(histogram.fileCounts.toSeq === Seq(2, 1, 2, 1, 0))

      // add 2 more large file
      (10000 to 30000).toDF("value").repartition(2)
        .write.format("delta").mode("append").saveAsTable(tableName)
      val histogram2 = getSnapshotForTable(tableName).fileSizeHistogram.map(compress).get
      assert(histogram2.fileCounts.toSeq === Seq(2, 1, 2, 3, 0))
    }
  }

  test("histogram is re-calculated when files are removed") {
    val tableName = getTableName
    withTable(tableName) {
      // 2 small files
      sparkContext.parallelize(Seq(1L, 2L, 3L), 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)
      // 2 medium size file
      spark.range(1000, 5000, 1, 2).toDF("value")
        .write.format("delta").mode("append").saveAsTable(tableName)

      val histogram = getSnapshotForTable(tableName).fileSizeHistogram.map(compress).get
      assert(histogram.fileCounts.toSeq === Seq(2, 2, 0))

      sql(s"DELETE FROM $tableName WHERE value >= 1000 and value % 2 = 0")
      // we are deleting half the values of both medium sized files and so both the files
      // will come to the previous bin
      val histogram2 = getSnapshotForTable(tableName).fileSizeHistogram.map(compress).get
      assert(histogram2.fileCounts.toSeq === Seq(4, 0))
    }
  }

  test("check CommitStats when table is created and with insert overwrite") {
    val tableName = getTableName
    withTable(tableName) {
      val usageLogs = Log4jUsageLogger.track {
        sparkContext.parallelize(Seq(1, 2, 3), 2).toDF("value")
          .write.format("delta").saveAsTable(tableName)
      }
      validateCommitStats(
        usageLogs = usageLogs,
        fileSizeDistribution = Seq(2, 0),
        addFilesDistribution = Seq(2, 0),
        removeFilesDistribution = Seq(0))

      val usageLogsNew = Log4jUsageLogger.track {
        sparkContext.parallelize(Seq(1, 2, 3), 3).toDF("value")
          .write.mode("overwrite").format("delta").saveAsTable(tableName)
      }
      validateCommitStats(
        usageLogs = usageLogsNew,
        fileSizeDistribution = Seq(3, 0),
        addFilesDistribution = Seq(3, 0),
        removeFilesDistribution = Seq(2, 0))
    }
  }

  test("check CommitStats with updates") {
    val tableName = getTableName
    withTable(tableName) {
      sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6), 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)

      // new histogram should be same as old histogram as we just have updated integer values
      validateCommitStatsOnDeltaOperation(
        tableName = tableName,
        operationSql = s"UPDATE $tableName SET value = 2 WHERE value = 6",
        expectedResult = Seq(1, 2, 3, 4, 5, 2),
        fileSizeDistribution = Seq(2, 0),
        addFilesDistribution = Seq(1, 0),
        removeFilesDistribution = Seq(1, 0)
      )
    }
  }

  test("check CommitStats with deletes") {
    val tableName = getTableName
    withTable(tableName) {
      sparkContext.parallelize(1 to 30000, 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)

      validateCommitStatsOnDeltaOperation(
        tableName = tableName,
        operationSql = s"DELETE FROM $tableName WHERE value != 1 and value != 30000",
        expectedResult = Seq(1, 30000),
        fileSizeDistribution = Seq(2, 0),
        addFilesDistribution = Seq(2, 0),
        removeFilesDistribution = Seq(0, 2, 0)
      )
    }
  }

  test("check CommitStats with OPTIMIZE command") {
    val tableName = getTableName
    withTable(tableName) {
      sparkContext.parallelize(Seq(1, 2, 3, 4), 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)

      // Running optimize first time - two small files will be merged
      validateCommitStatsOnDeltaOperation(
        tableName = tableName,
        operationSql = s"OPTIMIZE $tableName",
        expectedResult = Seq(1, 2, 3, 4),
        fileSizeDistribution = Seq(1, 0),
        addFilesDistribution = Seq(1, 0),
        removeFilesDistribution = Seq(2, 0)
      )
    }
  }

  test("check CommitStats with CLONE command") {
    val tableName = getTableName
    val clonedTableName = s"${tableName}_cloned"
    withTable(tableName, clonedTableName) {
      sparkContext.parallelize(Seq(1, 2, 3, 4), 2).toDF("value")
        .write.format("delta").saveAsTable(tableName)

      validateCommitStatsOnDeltaOperation(
        tableName = clonedTableName,
        operationSql = s"CREATE TABLE $clonedTableName SHALLOW CLONE $tableName",
        expectedResult = Seq(1, 2, 3, 4),
        fileSizeDistribution = Seq(2, 0),
        addFilesDistribution = Seq(2, 0),
        removeFilesDistribution = Seq(0)
      )
    }
  }

  // The following set of tests validates that the histogram flow doesn't launch any extra jobs
  test("equal jobs - when table is created, followed by appends") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      withTable(tableName) {
        Seq(1, 2, 3).toDF("value").write
          .mode("overwrite").format("delta").saveAsTable(tableName)
        Seq(1, 2, 3).toDF("value").write
          .mode("append").format("delta").saveAsTable(tableName)
      }
    })
  }

  test("equal jobs - with updates") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      withTable(tableName) {
        Seq(1, 2, 3).toDF("value").write
          .mode("overwrite").format("delta").saveAsTable(tableName)
        sql(s"UPDATE $tableName SET value = 2 WHERE value > 1")
        val df = spark.read.format("delta").table(tableName)
        checkDatasetUnorderly(df.as[Int], 1, 2, 2)
      }
    })
  }

  test("equal jobs - with deletes") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      withTable(tableName) {
        Seq(1, 2, 3, 4).toDF("value").write
          .mode("overwrite").format("delta").saveAsTable(tableName)
        sql(s"DELETE FROM $tableName WHERE value > 2")
        val df = spark.read.format("delta").table(tableName)
        checkDatasetUnorderly(df.as[Int], 1, 2)
      }
    })
  }

  test("equal jobs - with optimize") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      withTable(tableName) {
        Seq(1, 2, 3, 4).toDF("value").write
          .mode("overwrite").format("delta").saveAsTable(tableName)
        sql(s"OPTIMIZE $tableName")
        val df = spark.read.format("delta").table(tableName)
        checkDatasetUnorderly(df.as[Int], 1, 2, 3, 4)
      }
    })
  }

  test("equal jobs - with convert to delta command") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      withTable(tableName) {
        Seq(1, 2, 3, 4).toDF("value").write
          .mode("overwrite").format("parquet").saveAsTable(tableName)
        sql(s"CONVERT TO DELTA $tableName")
        val df = spark.read.format("delta").table(tableName)
        checkDatasetUnorderly(df.as[Int], 1, 2, 3, 4)
      }
    })
  }

  test("equal jobs - with clone") {
    assertEqualJobsWithHistogramEnabledAndDisabled(tableName => {
      val clonedTableName = s"${tableName}_cloned"
      withTable(tableName, clonedTableName) {
        Seq(1, 2, 3, 4).toDF("value").write
          .mode("overwrite").format("delta").saveAsTable(tableName)
        sql(s"CREATE TABLE $clonedTableName SHALLOW CLONE $tableName")
        val df = spark.read.format("delta").table(clonedTableName)
        checkDatasetUnorderly(df.as[Int], 1, 2, 3, 4)
      }
    })
  }
}
