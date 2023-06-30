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

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.optimize.{FileSizeStats, OptimizeMetrics, ZOrderStats}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.tables.DeltaTable

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.floor
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/** Tests that run optimize and verify the returned output (metrics) is expected. */
trait OptimizeMetricsSuiteBase extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils {

  import testImplicits._

  test("optimize metrics") {
    withTempDir { tempDir =>
      val skewedRightSeq =
        0.to(79).seq ++ 40.to(79).seq ++ 60.to(79).seq ++ 70.to(79).seq ++ 75.to(79).seq
      skewedRightSeq.toDF().withColumn("p", floor('value / 10)).repartition(4)
        .write.partitionBy("p").format("delta").save(tempDir.toString)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val startCount = deltaLog.unsafeVolatileSnapshot.numOfFiles
      val startSizes = deltaLog.unsafeVolatileSnapshot.allFiles.select('size).as[Long].collect()
      val res = spark.sql(s"OPTIMIZE delta.`${tempDir.toString}`")
      val metrics: OptimizeMetrics = res.select($"metrics.*").as[OptimizeMetrics].head()
      val finalSizes = deltaLog.unsafeVolatileSnapshot.allFiles
        .select('size).collect().map(_.getLong(0))
      val finalNumFiles = deltaLog.unsafeVolatileSnapshot.numOfFiles
      assert(metrics.numFilesAdded == finalNumFiles)
      assert(metrics.numFilesRemoved == startCount)
      assert(metrics.filesAdded.min.get == finalSizes.min)
      assert(metrics.filesAdded.max.get == finalSizes.max)
      assert(metrics.filesAdded.totalSize == finalSizes.sum)
      assert(metrics.filesAdded.totalFiles == finalSizes.length)
      assert(metrics.filesRemoved.max.get == startSizes.max)
      assert(metrics.filesRemoved.min.get == startSizes.min)
      assert(metrics.filesRemoved.totalSize == startSizes.sum)
      assert(metrics.filesRemoved.totalFiles == startSizes.length)
      assert(metrics.totalConsideredFiles == startCount)
      assert(metrics.totalFilesSkipped == 0)
      assert(metrics.numTableColumns == 2)
      assert(metrics.numTableColumnsWithStats == 2)
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

    val parallelismMetricsSchema = StructType(Seq(
      StructField("maxClusterActiveParallelism", LongType, nullable = true),
      StructField("minClusterActiveParallelism", LongType, nullable = true),
      StructField("maxSessionActiveParallelism", LongType, nullable = true),
      StructField("minSessionActiveParallelism", LongType, nullable = true)
    ))
    val dvMetricsSchema = StructType(Seq(
      StructField("numDeletionVectorsRemoved", LongType, nullable = false),
      StructField("numDeletionVectorRowsRemoved", LongType, nullable = false)
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
      StructField("endTimeMs", LongType, nullable = false),
      StructField("totalClusterParallelism", LongType, nullable = false),
      StructField("totalScheduledTasks", LongType, nullable = false),
      StructField("autoCompactParallelismStats", parallelismMetricsSchema, nullable = true),
      StructField("deletionVectorStats", dvMetricsSchema, nullable = true),
      StructField("numTableColumns", LongType, nullable = false),
      StructField("numTableColumnsWithStats", LongType, nullable = false)
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

        val actualOperationMetricsAndName = DeltaTable.forPath(spark, tempDir.getAbsolutePath)
          .history(1)
          .select("operationMetrics", "operation")
          .head

        val actualOperationMetrics = actualOperationMetricsAndName
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
          "p75FileSize",
          "numDeletionVectorsRemoved"
        ).foreach(metric => assert(actualOperationMetrics.get(metric).isDefined))

        val operationName = actualOperationMetricsAndName(1).asInstanceOf[String]
        assert(operationName === DeltaOperations.OPTIMIZE_OPERATION_NAME)
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
        endTimeMs = actMetrics.endTimeMs,
        totalClusterParallelism = 2,
        totalScheduledTasks = 0,
        numTableColumns = 1,
        numTableColumnsWithStats = 1)

      assert(actMetrics === expMetrics)
    }
  }

  test("optimize metrics when certain table columns have no stats") {
    val tblName = "tblName"
    withTable(tblName) {
      // Create Delta table with 5 columns
      spark.range(10)
        .withColumn("col2", 'id * 2)
        .withColumn("col3", 'id * 3)
        .withColumn("col4", 'id * 4)
        .withColumn("col5", 'id * 5)
        .write.format("delta").saveAsTable(tblName)

      // Set to only collect data skipping stats on 3 columns
      spark.sql(s"""
                  |ALTER TABLE $tblName
                  |SET TBLPROPERTIES (
                  |  'delta.dataSkippingNumIndexedCols' = '3'
                  |)""".stripMargin)

      // Optimize
      val res = spark.sql(s"OPTIMIZE $tblName")
      val actMetrics: OptimizeMetrics = res.select($"metrics.*").as[OptimizeMetrics].head()

      // The table has 5 columns
      assert(actMetrics.numTableColumns == 5)
      // There are only 3 columns to collect stats because of the dataSkippingNumIndexedCols config
      assert(actMetrics.numTableColumnsWithStats == 3)
    }
  }


  test("optimize ZOrderBy operation metrics in Delta table history") {
    withSQLConf(
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        // create a partitioned table with each partition containing multiple files
        0.to(100).seq.toDF()
          .withColumn("col1", floor('value % 7))
          .withColumn("col2", floor('value % 27))
          .withColumn("p", floor('value % 10))
          .repartition(4).write.partitionBy("p").format("delta").save(tempDir.toString)

        val startSizes = DeltaLog.forTable(spark, tempDir)
          .unsafeVolatileSnapshot.allFiles.select('size).as[Long].collect().sorted

        spark.sql(s"OPTIMIZE delta.`${tempDir.toString}` ZORDER BY (col1, col2)").show()

        val finalSizes = DeltaLog.forTable(spark, tempDir)
          .unsafeVolatileSnapshot.allFiles.select('size).collect().map(_.getLong(0)).sorted

        val actualOperation = DeltaTable.forPath(spark, tempDir.getAbsolutePath).history(1)
          .select(
            "operationParameters.zOrderBy",
            "operationMetrics",
            "operation")
          .head

        // Verify ZOrder operation parameters
        val actualOpParameters = actualOperation.getString(0)
        assert(actualOpParameters === "[\"col1\",\"col2\"]")

        // Verify metrics records in commit log.
        val actualMetrics = actualOperation
          .getMap(1)
          .asInstanceOf[Map[String, String]]

        val expMetricsJson =
          s"""{
            |  "numRemovedFiles" : "37",
            |  "numAddedFiles" : "10",
            |  "numAddedBytes" : "${finalSizes.sum}",
            |  "numRemovedBytes" : "${startSizes.sum}",
            |  "minFileSize" : "${finalSizes.min}",
            |  "maxFileSize" : "${finalSizes.max}",
            |  "p25FileSize" : "${finalSizes(finalSizes.length / 4)}",
            |  "p50FileSize" : "${finalSizes(finalSizes.length / 2)}",
            |  "p75FileSize" : "${finalSizes(3 * finalSizes.length / 4)}",
            |  "numDeletionVectorsRemoved" : "0"
            |}""".stripMargin.trim

        val expMetrics = JsonUtils.fromJson[Map[String, String]](expMetricsJson)
        assert(actualMetrics === expMetrics)

        val operationName = actualOperation(2).asInstanceOf[String]
        assert(operationName === DeltaOperations.OPTIMIZE_OPERATION_NAME)
      }
    }
  }

  test("optimize ZOrderBy operation metrics in command output") {
    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "1000000") {
      withTempDir { tempDir =>
        // create a partitioned table with each partition containing multiple files
        0.to(100).seq.toDF()
          .withColumn("col1", floor('value % 7))
          .withColumn("col2", floor('value % 27))
          .withColumn("p", floor('value % 10))
          .repartition(4).write.partitionBy("p").format("delta").save(tempDir.toString)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val startCount = deltaLog.unsafeVolatileSnapshot.allFiles.count()
        val startSizes = deltaLog.unsafeVolatileSnapshot.allFiles.select('size).as[Long].collect()

        val result = spark.sql(s"OPTIMIZE delta.`${tempDir.toString}` ZORDER BY (col1, col2)")
        val metrics: OptimizeMetrics = result.select($"metrics.*").as[OptimizeMetrics].head()

        val finalSizes = deltaLog.unsafeVolatileSnapshot.allFiles
          .select('size).collect().map(_.getLong(0))
        val finalNumFiles = deltaLog.unsafeVolatileSnapshot.allFiles.collect().length

        assert(metrics.filesAdded.totalFiles === finalNumFiles)
        assert(metrics.filesRemoved.totalFiles === startCount)
        assert(metrics.filesAdded.min.get === finalSizes.min)
        assert(metrics.filesAdded.max.get === finalSizes.max)
        assert(metrics.filesRemoved.max.get === startSizes.max)
        assert(metrics.filesRemoved.min.get === startSizes.min)
        assert(metrics.totalFilesSkipped === 0)
        assert(metrics.totalConsideredFiles === metrics.numFilesRemoved)

        val expZOrderMetrics = s"""{
          |  "strategyName" : "all",
          |  "inputCubeFiles" : {
          |    "num" : 0,
          |    "size" : 0
          |  },
          |  "inputOtherFiles" : {
          |    "num" : $startCount,
          |    "size" : ${startSizes.sum}
          |  },
          |  "inputNumCubes" : 0,
          |  "mergedFiles" : {
          |    "num" : $startCount,
          |    "size" : ${startSizes.sum}
          |  },
          |  "numOutputCubes" : 10
          |}""".stripMargin

        assert(metrics.zOrderStats === Some(JsonUtils.fromJson[ZOrderStats](expZOrderMetrics)))
      }
    }
  }

  val optimizeCommands = Seq("optimize", "zorder", "purge")
  for (cmd <- optimizeCommands) {
    testWithDVs(s"deletion vector metrics - $cmd") {
      withTempDir { dirName =>
        // Create table with 100 files of 10 rows each.
        val numFiles = 100
        val path = dirName.getAbsolutePath
        spark.range(0, 1000, step = 1, numPartitions = numFiles)
          .write.format("delta").save(path)
        val tableName = s"delta.`$path`"
        val deltaTable = DeltaTable.forPath(spark, path)
        val deltaLog = DeltaLog.forTable(spark, path)

        var allFiles = deltaLog.unsafeVolatileSnapshot.allFiles.collect().toSeq
        // Delete two rows each from 5 files to create Deletion Vectors.
        val numFilesWithDVs = 5
        val numDeletedRows = numFilesWithDVs * 2
        allFiles.take(numFilesWithDVs).foreach(
          file => removeRowsFromFile(deltaLog, file, Seq(1, 5)))

        allFiles = deltaLog.unsafeVolatileSnapshot.allFiles.collect().toSeq
        assert(allFiles.size === numFiles)
        assert(allFiles.filter(_.deletionVector != null).size === numFilesWithDVs)

        var expOpName = DeltaOperations.OPTIMIZE_OPERATION_NAME
        val metrics: Seq[OptimizeMetrics] = cmd match {
          case "optimize" =>
            spark.sql(s"OPTIMIZE $tableName")
              .select("metrics.*").as[OptimizeMetrics].collect().toSeq
          case "zorder" =>
            spark.sql(s"OPTIMIZE $tableName ZORDER BY (id)")
              .select("metrics.*").as[OptimizeMetrics].collect().toSeq
          case "purge" =>
            expOpName = DeltaOperations.REORG_OPERATION_NAME
            spark.sql(s"REORG TABLE $tableName APPLY (PURGE)")
              .select("metrics.*").as[OptimizeMetrics].collect().toSeq
          case unknown => throw new IllegalArgumentException(s"Unknown command: $unknown")
        }

        // Check DV metrics in the result.
        assert(metrics.length === 1)
        val dvStats = metrics.head.deletionVectorStats
        assert(dvStats.get.numDeletionVectorsRemoved === numFilesWithDVs)
        assert(dvStats.get.numDeletionVectorRowsRemoved === numDeletedRows)

        // Check DV metrics in the Delta history.
        val opMetricsAndName = deltaTable.history.select("operationMetrics", "operation")
          .head

        val opMetrics = opMetricsAndName
          .getMap(0)
          .asInstanceOf[Map[String, String]]
        val dvMetrics = opMetrics.keys.filter(_.contains("DeletionVector"))
        assert(dvMetrics === Set("numDeletionVectorsRemoved"))
        assert(opMetrics("numDeletionVectorsRemoved") === numFilesWithDVs.toString)

        val operationName = opMetricsAndName(1).asInstanceOf[String]
        assert(operationName === expOpName)
      }
    }
  }
}

class OptimizeMetricsSuite extends OptimizeMetricsSuiteBase
  with DeltaSQLCommandTest
