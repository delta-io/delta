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

import java.io.File

import org.apache.spark.sql.delta.commands.convert.ParquetTable
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

/**
 * Validates the CONVERT TO DELTA file-listing rebalance
 * (`spark.databricks.delta.convert.rebalanceFileListing`): listed files are spread across tasks by
 * file count before the footer reads, so one large partition directory is no longer a single
 * skewed task. Drives `ManualListingFileManifest` directly over a skewed layout and checks the
 * per-partition file distribution of `allFiles`, conf OFF vs ON.
 */
class ConvertToDeltaListingRebalanceSuite extends ConvertToDeltaSuiteBaseCommons {

  // recursiveListDirs uses defaultParallelism to partition; pin it so the OFF case (directory
  // distribution) is deterministic and the ON case fans out to a known width.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.default.parallelism", "8")

  override protected def convertToDelta(
      identifier: String,
      partitionSchema: Option[String] = None,
      collectStats: Boolean = true): Unit = {
    val stats = if (collectStats) "" else " NO STATISTICS"
    partitionSchema match {
      case Some(ps) => spark.sql(s"CONVERT TO DELTA $identifier PARTITIONED BY ($ps)$stats")
      case None => spark.sql(s"CONVERT TO DELTA $identifier$stats")
    }
  }

  // 60 one-row files: country=US has 45 (the dominant dir), country=CA has 15.
  private def writeSkewedParquet(dir: String): Unit = {
    withSQLConf("spark.sql.files.maxRecordsPerFile" -> "1") {
      spark.range(60)
        .withColumn("country", when(col("id") < 45, lit("US")).otherwise(lit("CA")))
        .withColumn("yyyy", lit("2024"))
        .withColumn("mm", lit("01"))
        .repartition(4)
        .write.mode("overwrite")
        .partitionBy("country", "yyyy", "mm")
        .parquet(dir)
    }
  }

  private def perPartitionFileCounts(dir: String, rebalance: Boolean): Seq[Int] = {
    withSQLConf(DeltaSQLConf.DELTA_CONVERT_REBALANCE_FILE_LISTING.key -> rebalance.toString) {
      // Go through ParquetTable so the manifest gets the Hadoop conf that
      // ParquetToSparkSchemaConverter needs (the parquet SQLConf booleans); it returns a
      // ManualListingFileManifest for an unpartitioned-catalog, non-metadata-log parquet path.
      val manifest = new ParquetTable(
        spark, dir, catalogTable = None, userPartitionSchema = None).fileManifest
      try {
        manifest.allFiles.rdd
          .mapPartitions(it => Iterator(it.size))
          .collect().toSeq.filter(_ > 0)
      } finally {
        manifest.close()
      }
    }
  }

  test("rebalance spreads footer-read work across tasks (no single skewed task)") {
    withTempDir { tmp =>
      val dir = new File(tmp, "src").getCanonicalPath
      writeSkewedParquet(dir)

      val off = perPartitionFileCounts(dir, rebalance = false)
      val on = perPartitionFileCounts(dir, rebalance = true)

      assert(off.sum == 60 && on.sum == 60, s"file counts must match: off=${off.sum} on=${on.sum}")
      // Without rebalance, the US directory (45 files) is a single skewed partition.
      assert(off.max >= 40, s"expected directory skew without rebalance, max=${off.max}")
      // With rebalance, no partition holds a large fraction of the files.
      assert(on.max <= 20, s"expected balanced partitions with rebalance, max=${on.max}")
      assert(on.max < off.max,
        s"rebalance must reduce the max partition: on=${on.max} off=${off.max}")
    }
  }
}
