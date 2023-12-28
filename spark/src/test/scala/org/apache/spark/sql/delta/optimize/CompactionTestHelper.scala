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

package org.apache.spark.sql.delta.optimize

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SQLTestUtils

/**
 * A trait used by unit tests to trigger compaction over a table.
 */
private[delta] trait CompactionTestHelper extends QueryTest with SQLTestUtils {

  /**
   * Compact files under the given `tablePath` using AutoCompaction/OPTIMIZE and
   * returns the [[OptimizeMetrics]]
   */
  def compactAndGetMetrics(tablePath: String, where: String = ""): OptimizeMetrics

  /** config controlling the min file size required for compaction */
  val minFileSizeConf: String

  /** config controlling the target file size for compaction */
  val maxFileSizeConf: String

  /** Create `numFilePartitions` partitions and each partition has `numFilesPerPartition` files. */
  def createFilesToPartitions(
      numFilePartitions: Int, numFilesPerPartition: Int, dir: String)
      (implicit spark: SparkSession): Unit = {
    val totalNumFiles = numFilePartitions * numFilesPerPartition
    spark.range(start = 0, end = totalNumFiles, step = 1, numPartitions = totalNumFiles)
      .selectExpr(s"id % $numFilePartitions as c0", "id as c1")
      .write
      .format("delta")
      .partitionBy("c0")
      .mode("append")
      .save(dir)
  }

  /** Create `numFiles` files without any partition. */
  def createFilesWithoutPartitions(
      numFiles: Int, dir: String)(implicit spark: SparkSession): Unit = {
    spark.range(start = 0, end = numFiles, step = 1, numPartitions = numFiles)
      .selectExpr("id as c0", "id as c1", "id as c2")
      .write
      .format("delta")
      .mode("append")
      .save(dir)
  }
}

private[delta] trait CompactionTestHelperForOptimize extends CompactionTestHelper {

  override def compactAndGetMetrics(tablePath: String, where: String = ""): OptimizeMetrics = {
    import testImplicits._
    val whereClause = if (where != "") s"WHERE $where" else ""
    val res = spark.sql(s"OPTIMIZE tahoe.`$tablePath` $whereClause")
    val metrics: OptimizeMetrics = res.select($"metrics.*").as[OptimizeMetrics].head()
    metrics
  }

  override val minFileSizeConf: String = DELTA_OPTIMIZE_MIN_FILE_SIZE.key

  override val maxFileSizeConf: String = DELTA_OPTIMIZE_MAX_FILE_SIZE.key
}

private[delta] trait CompactionTestHelperForAutoCompaction extends CompactionTestHelper {

  override def compactAndGetMetrics(tablePath: String, where: String = ""): OptimizeMetrics = {
    // Set min num files to 2 - so that even if two small files are present in a partition, then
    // also they are compacted.
    var metrics: Seq[OptimizeMetrics] = Nil
    withSQLConf(DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "2") {
        metrics =
            AutoCompact.compact(
              spark,
              DeltaLog.forTable(spark, tablePath)
          )
    }
    metrics.head
  }

  override val minFileSizeConf: String = DELTA_AUTO_COMPACT_MIN_FILE_SIZE.key

  override val maxFileSizeConf: String = DELTA_AUTO_COMPACT_MAX_FILE_SIZE.key
}
