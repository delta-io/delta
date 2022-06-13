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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaShufflePartitionsUtil
import org.apache.spark.sql.execution.CoalesceExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.REPARTITION_BY_NUM
import org.apache.spark.sql.test.SharedSparkSession

class DeltaAutoOptimizeSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  def writeData(
      numFiles: Int,
      dataPath: String,
      partitioned: Boolean = false,
      mode: String = "overwrite"): Unit = {
    val df = spark
      .range(50000)
      .map { _ =>
        (
          scala.util.Random.nextInt(10000000).toLong,
          scala.util.Random.nextInt(1000000000),
          scala.util.Random.nextInt(2))
      }
      .toDF("colA", "colB", "colC")
      .repartition(numFiles)
    if (partitioned) {
      df.write
        .partitionBy("colC")
        .mode(mode)
        .format("delta")
        .save(dataPath)
    } else {
      df.write
        .mode(mode)
        .format("delta")
        .save(dataPath)
    }
  }

  def checkTableVersionAndNumFiles(
      path: String,
      expectedVer: Long,
      expectedNumFiles: Long): Unit = {
    val dt = DeltaLog.forTable(spark, path)
    assert(dt.snapshot.version == expectedVer)
    assert(dt.snapshot.allFiles.count() == expectedNumFiles)
  }

  test("test adaptive config and OptimizeWrite enabled") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path = new Path(rootPath, "table1").toString

      var expectedTableVersion = -1
      writeData(20, path)
      expectedTableVersion += 1 // version should be 0.
      checkTableVersionAndNumFiles(path, expectedTableVersion, 20)

      withSQLConf("spark.sql.adaptive.enabled" -> "true",
        DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true") {
        writeData(20, path)
        expectedTableVersion += 1 // OptimizeWrite should be done with write transaction.
        checkTableVersionAndNumFiles(path, expectedTableVersion, 1)
      }
    }
  }

  test("test enabling OptimizeWrite") {
    val tableName = "optimizeWriteTestTable"
    val tableName2 = s"${tableName}2"
    withTable(tableName, tableName2) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString

        {
          var expectedTableVersion = -1
          writeData(20, path)
          expectedTableVersion += 1 // version should be 0.
          checkTableVersionAndNumFiles(path, expectedTableVersion, 20)

          withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true") {
            writeData(20, path)
            expectedTableVersion += 1 // optimize should be done with write transaction.
            checkTableVersionAndNumFiles(path, expectedTableVersion, 1)
          }
        }

        {
          // Test with default table properties.
          // Note that 0.6.1 does not support setting table properties using DDL.
          // E.g. CREATE/ALTER TABLE; no way to change the properties after it's created.
          var expectedTableVersion = -1
          // Test default delta table config
          val path2 = new Path(rootPath, "table2").toString
          withSQLConf(
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" -> "true") {
            writeData(20, path2)
            expectedTableVersion += 1
            checkTableVersionAndNumFiles(path2, expectedTableVersion, 1)
          }

          // Session config should be prior to table property.
          withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "false") {
            writeData(20, path2)
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path2, expectedTableVersion, 20)
          }

          withSQLConf(
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" -> "false") {
            // defaults config only applied at table creation.
            writeData(20, path2)
            expectedTableVersion += 1 // autoCompact should be triggered
            checkTableVersionAndNumFiles(path2, expectedTableVersion, 1)
          }
        }
      }
    }
  }

  test("test OptimizeWrite configs") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path = new Path(rootPath, "table1").toString

      withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true",
        "spark.sql.shuffle.partitions" -> "20",
        DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.key -> "10101") {
        // binSize is small, so won't coalesce partitions.
        writeData(30, path)
        checkTableVersionAndNumFiles(path, 0, 40)
      }

      withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true",
        "spark.sql.shuffle.partitions" -> "20",
        DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.key -> "101010") {
        // binSize is small, so won't coalesce partitions.
        writeData(30, path)
        checkTableVersionAndNumFiles(path, 1, 7)
      }
    }
  }

  test("test partitioned table with OptimizeWrite") {
    val tableName = "optimizeWriteTestTable"
    val tableName2 = s"${tableName}2"
    withTable(tableName, tableName2) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString
        writeData(20, path, partitioned = true)
        checkTableVersionAndNumFiles(path, 0, 40)

        withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true") {
          writeData(20, path, partitioned = true)
          checkTableVersionAndNumFiles(path, 1, 2)

          withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.key -> "101010") {
            writeData(20, path, partitioned = true)
            checkTableVersionAndNumFiles(path, 2, 4)
          }
        }
      }
    }
  }

  test("test OptimizeWrite with empty partition") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath

      withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true") {
        val path = new Path(rootPath, "table2").toString
        val tempPath = new Path(rootPath, "temp").toString
        new File(tempPath).mkdir()
        writeData(20, path)
        val df = spark.read.format("delta").load(path)

        // empty dataframe test
        val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
        emptyDF.write.mode("append").format("delta").save(path)

        // empty directory test
        val emptyDF2 = spark.read.schema(df.schema).parquet(tempPath)
        emptyDF2.write.mode("append").format("delta").save(path)
      }
    }
  }

  test("run optimize with OptimizeWrite") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path = new Path(rootPath, "table1").toString
      spark.range(100).repartition(50).write.format("delta").save(path)
      assert(spark.read.format("delta").load(path).inputFiles.length == 50)

      withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_ENABLED.key -> "true") {
        spark.range(100).repartition(50).write.mode("append").format("delta").save(path)
        assert(spark.read.format("delta").load(path).inputFiles.length == 51)

        withSQLConf(DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.key -> "2") {
          val dt = io.delta.tables.DeltaTable.forPath(path)
          dt.optimize().executeCompaction()
          // OptimizeWrite shouldn't been applied.
          assert(spark.read.format("delta").load(path).inputFiles.length == 1)
        }
      }
    }
  }

  test("test DeltaShufflePartitionsUtil.partitioningForRebalance") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath

      // Test partitioned data
      withSQLConf("spark.sql.shuffle.partitions" -> "100") {
        val path = new Path(rootPath, "table1").toString
        writeData(20, path, partitioned = true)
        val df = spark.read.format("delta").load(path)
        val dl = DeltaLog.forTable(spark, path)

        val partitioning = DeltaShufflePartitionsUtil.partitioningForRebalance(
          df.queryExecution.executedPlan.output,
          dl.snapshot.metadata.partitionSchema,
          spark.sessionState.conf.numShufflePartitions)

        assert(partitioning.isInstanceOf[HashPartitioning])
        assert(partitioning.asInstanceOf[HashPartitioning]
          .expressions.map(_.toString).head.contains("colC"))
        assert(partitioning.numPartitions == 100)
      }

      // Test non partitioned data
      withSQLConf("spark.sql.shuffle.partitions" -> "100") {
        val path = new Path(rootPath, "table2").toString
        writeData(20, path)
        val df = spark.read.format("delta").load(path)
        val dl = DeltaLog.forTable(spark, path)

        val partitioning = DeltaShufflePartitionsUtil.partitioningForRebalance(
          df.queryExecution.executedPlan.output,
          dl.snapshot.metadata.partitionSchema,
          spark.sessionState.conf.numShufflePartitions)

        assert(partitioning.isInstanceOf[RoundRobinPartitioning])
        assert(partitioning.numPartitions == 100)
      }
    }
  }

  test("test DeltaShufflePartitionsUtil.removeTopRepartition") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath

      withSQLConf("spark.sql.shuffle.partitions" -> "100",
        "spark.sql.codegen.whleStage" -> "false") {
        val path1 = new Path(rootPath, "table1").toString
        writeData(20, path1)
        val df = spark.read.format("delta").load(path1)

        withSQLConf("spark.sql.adaptive.enabled" -> "false") {
          {
            val repartitionDF = df.repartition(3)
            val plan = repartitionDF.queryExecution.executedPlan
            // Plan should have ShuffleExchangeExec.
            val isShuffle = plan match {
              case ShuffleExchangeExec(_, _, shuffleOrigin)
                if shuffleOrigin.equals(REPARTITION_BY_NUM) => true
              case _ => false
            }
            assert(isShuffle)

            val updatedPlan = DeltaShufflePartitionsUtil.removeTopRepartition(plan)
            // ShuffleExchangeExec should be removed.
            assert(updatedPlan.equals(plan.children.head))
          }

          {
            val coalesceDF = df.coalesce(3)
            val plan = coalesceDF.queryExecution.executedPlan
            // Plan should have CoalesceExec.
            assert(plan.isInstanceOf[CoalesceExec])

            val updatedPlan = DeltaShufflePartitionsUtil.removeTopRepartition(plan)
            // CoalesceExec should be removed.
            assert(updatedPlan.equals(plan.children.head))
          }
        }

        // Test with AdaptiveSparkPlanExec
        withSQLConf("spark.sql.adaptive.enabled" -> "true") {
          val repartitionDF = df.repartition(3)
          val plan = repartitionDF.queryExecution.executedPlan
          // Plan should have ShuffleExchangeExec.
          val inputPlan = plan.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
          val isShuffle = inputPlan match {
            case ShuffleExchangeExec(_, _, shuffleOrigin)
              if shuffleOrigin.equals(REPARTITION_BY_NUM) => true
            case _ => false
          }
          assert(isShuffle)

          val updatedPlan = DeltaShufflePartitionsUtil.removeTopRepartition(plan)
          // ShuffleExchangeExec should be removed.
          assert(updatedPlan.asInstanceOf[AdaptiveSparkPlanExec].inputPlan.equals(
            inputPlan.children.head))
        }
      }
    }
  }

  test("test DeltaShufflePartitionsUtil.splitSizeListByTargetSize") {
    val targetSize = 100
    val smallPartitionFactor = 0.5
    val mergedPartitionFactor = 1.2

    // merge the small partitions at the beginning/end
    val sizeList1 = Seq[Long](15, 90, 15, 15, 15, 90, 15)
    assert(DeltaShufflePartitionsUtil.splitSizeListByTargetSize(sizeList1, targetSize,
      smallPartitionFactor, mergedPartitionFactor).toSeq ==
      Seq(0, 5))

    // merge the small partitions in the middle
    val sizeList2 = Seq[Long](30, 15, 90, 10, 90, 15, 30)
    assert(DeltaShufflePartitionsUtil.splitSizeListByTargetSize(sizeList2, targetSize,
      smallPartitionFactor, mergedPartitionFactor).toSeq ==
      Seq(0, 4))

    // merge small partitions if the partition itself is smaller than
    // targetSize * SMALL_PARTITION_FACTOR
    val sizeList3 = Seq[Long](15, 1000, 15, 1000)
    assert(DeltaShufflePartitionsUtil.splitSizeListByTargetSize(sizeList3, targetSize,
      smallPartitionFactor, mergedPartitionFactor).toSeq ==
      Seq(0, 3))

    // merge small partitions if the combined size is smaller than
    // targetSize * MERGED_PARTITION_FACTOR
    val sizeList4 = Seq[Long](35, 75, 90, 20, 35, 25, 35)
    assert(DeltaShufflePartitionsUtil.splitSizeListByTargetSize(sizeList4, targetSize,
      smallPartitionFactor, mergedPartitionFactor).toSeq ==
      Seq(0, 2, 3))

    val sizeList5 = Seq[Long](99, 19, 19, 99, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19)
    assert(DeltaShufflePartitionsUtil.splitSizeListByTargetSize(sizeList5, targetSize,
      smallPartitionFactor, mergedPartitionFactor).toSeq ==
      Seq(0, 3, 4, 9))

  }
}
