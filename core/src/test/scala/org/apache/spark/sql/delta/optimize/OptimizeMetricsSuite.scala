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
import org.apache.spark.sql.delta.commands.optimize.{FileSizeStats, OptimizeMetrics}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables.DeltaTable

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.floor
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/** Tests that run optimize and verify the returned output (metrics) is expected. */
trait OptimizeMetricsSuiteBase extends QueryTest
    with SharedSparkSession  {

  import testImplicits._

  test("optimize metrics") {
    withTempDir { tempDir =>
      val skewedRightSeq =
        0.to(79).seq ++ 40.to(79).seq ++ 60.to(79).seq ++ 70.to(79).seq ++ 75.to(79).seq
      skewedRightSeq.toDF().withColumn("p", floor('value / 10)).repartition(4)
        .write.partitionBy("p").format("delta").save(tempDir.toString)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val startCount = deltaLog.snapshot.numOfFiles
      val startSizes = deltaLog.snapshot.allFiles.select('size).as[Long].collect()
      val res = spark.sql(s"OPTIMIZE delta.`${tempDir.toString}`")
      val metrics: OptimizeMetrics = res.select($"metrics.*").as[OptimizeMetrics].head()
      val finalSizes = deltaLog.snapshot.allFiles.select('size).collect().map(_.getLong(0))
      val finalNumFiles = deltaLog.snapshot.numOfFiles
      assert(metrics.numFilesAdded == finalNumFiles)
      assert(metrics.numFilesRemoved == startCount)
      assert(metrics.filesAdded.min.get == finalSizes.min)
      assert(metrics.filesAdded.max.get == finalSizes.max)
      assert(metrics.filesRemoved.max.get == startSizes.max)
      assert(metrics.filesRemoved.min.get == startSizes.min)
      assert(metrics.totalConsideredFiles == startCount)
      assert(metrics.totalFilesSkipped == 0)
    }
  }

  /**
   * Ensure public API for metrics persists
   */
  test("optimize command output schema") {

    val zOrderFileStatsSchema = StructType(Seq(
      StructField("num", LongType, nullable = false),
      StructField("size", LongType, nullable = false)
    ))

    val zOrderStatsSchema = StructType(Seq(
      StructField("strategyName", StringType, nullable = true),
      StructField("inputCubeFiles", zOrderFileStatsSchema, nullable = true),
      StructField("inputOtherFiles", zOrderFileStatsSchema, nullable = true),
      StructField("inputNumCubes", LongType, nullable = false),
      StructField("mergedFiles", zOrderFileStatsSchema, nullable = true),
      StructField("numOutputCubes", LongType, nullable = false),
      StructField("mergedNumCubes", LongType, nullable = true)
    ))
    val fileSizeMetricsSchema = StructType(Seq(
      StructField("min", LongType, nullable = true),
      StructField("max", LongType, nullable = true),
      StructField("avg", DoubleType, nullable = false),
      StructField("totalFiles", LongType, nullable = false),
      StructField("totalSize", LongType, nullable = false)
    ))

    val optimizeMetricsSchema = StructType(Seq(
      StructField("numFilesAdded", LongType, nullable = false),
      StructField("numFilesRemoved", LongType, nullable = false),
      StructField("filesAdded", fileSizeMetricsSchema, nullable = true),
      StructField("filesRemoved", fileSizeMetricsSchema, nullable = true),
      StructField("partitionsOptimized", LongType, nullable = false),
      StructField("zOrderStats", zOrderStatsSchema, nullable = true),
      StructField("numBatches", LongType, nullable = false),
      StructField("totalConsideredFiles", LongType, nullable = false),
      StructField("totalFilesSkipped", LongType, nullable = false),
      StructField("preserveInsertionOrder", BooleanType, nullable = false),
      StructField("numFilesSkippedToReduceWriteAmplification", LongType, nullable = false),
      StructField("numBytesSkippedToReduceWriteAmplification", LongType, nullable = false),
      StructField("startTimeMs", LongType, nullable = false),
      StructField("endTimeMs", LongType, nullable = false)
    ))
    val optimizeSchema = StructType(Seq(
      StructField("path", StringType, nullable = true),
      StructField("metrics", optimizeMetricsSchema, nullable = true)
    ))
    withTempDir { tempDir =>
      spark.range(0, 10).write.format("delta").save(tempDir.toString)
      val res = sql(s"OPTIMIZE delta.`${tempDir.toString}`")
      assert(res.schema == optimizeSchema)
    }
  }

  test("optimize operation metrics in Delta table history") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val sampleData =
          0.to(79).seq ++ 40.to(79).seq ++ 60.to(79).seq ++ 70.to(79).seq ++ 75.to(79).seq

        // partition the data and write to test table
        sampleData.toDF().withColumn("p", floor('value / 10)).repartition(4)
            .write.partitionBy("p").format("delta").save(tempDir.toString)

        spark.sql(s"OPTIMIZE delta.`${tempDir.toString}`") // run optimize on the table

        val actualOperationMetrics = DeltaTable.forPath(spark, tempDir.getAbsolutePath)
            .history(1)
            .select("operationMetrics")
            .take(1)
            .head
            .getMap(0)
            .asInstanceOf[Map[String, String]]

        // File sizes depend on the order of how they are merged (=> compression). In order to avoid
        // flaky test, just test that the metric exists.
        Seq(
          "numAddedFiles",
          "numAddedBytes",
          "numRemovedBytes",
          "numRemovedFiles",
          "numRemovedBytes",
          "minFileSize",
          "maxFileSize",
          "p25FileSize",
          "p50FileSize",
          "p75FileSize").foreach(metric => assert(actualOperationMetrics.get(metric).isDefined))
      }
    }
  }

  test("optimize metrics on idempotent operations") {
    val tblName = "tblName"
    withTable(tblName) {
      // Create Delta table
      spark.range(10).write.format("delta").saveAsTable(tblName)

      // First Optimize
      spark.sql(s"OPTIMIZE $tblName")

      // Second Optimize
      val res = spark.sql(s"OPTIMIZE $tblName")
      val actMetrics: OptimizeMetrics = res.select($"metrics.*").as[OptimizeMetrics].head()
      var preserveInsertionOrder = false

      val expMetrics = OptimizeMetrics(
        numFilesAdded = 0,
        numFilesRemoved = 0,
        filesAdded = FileSizeStats().toFileSizeMetrics,
        filesRemoved = FileSizeStats().toFileSizeMetrics,
        partitionsOptimized = 0,
        zOrderStats = None,
        numBatches = 0,
        totalConsideredFiles = 1,
        totalFilesSkipped = 1,
        preserveInsertionOrder = preserveInsertionOrder,
        startTimeMs = actMetrics.startTimeMs,
        endTimeMs = actMetrics.endTimeMs)

      assert(actMetrics === expMetrics)
    }
  }
}

class OptimizeMetricsSuite extends OptimizeMetricsSuiteBase
  with DeltaSQLCommandTest
