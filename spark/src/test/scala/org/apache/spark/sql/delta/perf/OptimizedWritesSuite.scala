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

package org.apache.spark.sql.delta.perf

import java.io.File

import scala.language.implicitConversions

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions, DeltaTestUtils}
import org.apache.spark.sql.delta.CommitStats
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}

abstract class OptimizedWritesSuiteBase extends QueryTest
  with SharedSparkSession {

  import testImplicits._

  protected def writeTest(testName: String)(f: String => Unit): Unit = {
    test(testName) {
      withTempDir { dir =>
        withSQLConf(DeltaConfigs.OPTIMIZE_WRITE.defaultTablePropertyKey -> "true") {
          f(dir.getCanonicalPath)
        }
      }
    }
  }

  protected def checkResult(df: DataFrame, numFileCheck: Long => Boolean, dir: String): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dir)
    val files = snapshot.numOfFiles
    assert(numFileCheck(files), s"file check failed: received $files")

    checkAnswer(
      spark.read.format("delta").load(dir),
      df
    )
  }

  protected implicit def fileToPathString(dir: File): String = dir.getCanonicalPath

  writeTest("non-partitioned write - table config") { dir =>
    val df = spark.range(0, 100, 1, 4).toDF()
    df.write.format("delta").save(dir)
    checkResult(
      df,
      numFileCheck = _ === 1,
      dir)
  }

  test("non-partitioned write - table config compatibility") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      // When table property is not set, we use session conf value.
      // Writes 1 file instead of 4 when OW is enabled
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        val df = spark.range(0, 100, 1, 4).toDF()
        val commitStats = Log4jUsageLogger.track {
          df.write.format("delta").mode("append").save(dir)
        }.filter(_.tags.get("opType") === Some("delta.commit.stats"))
        assert(commitStats.length >= 1)
        checkResult(
          df,
          numFileCheck = _ === 1,
          dir)
      }
    }

    // When table property "delta.autoOptimize.optimizeWrite" is set, it takes precedence over
    // session conf.
    for {
      sqlConf <- DeltaTestUtils.BOOLEAN_DOMAIN
      tableProperty <- DeltaTestUtils.BOOLEAN_DOMAIN
    } {
      withTempDir { tempDir =>
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> sqlConf.toString) {
          val dir = tempDir.getCanonicalPath
          // Write one file to be able to set tblproperties
          spark.range(10).coalesce(1).write.format("delta")
            .mode("append").save(dir)

          // We're setting the table property explicitly and hence, OW should be controlled
          // only by the table property, not the config.
          sql(s"ALTER TABLE delta.`$dir` SET TBLPROPERTIES" +
            s" (delta.autoOptimize.optimizeWrite = ${tableProperty.toString})")

          val df = spark.range(0, 100, 1, 4).toDF()
          // OW adds one file vs non-OW adds 4 files
          val expectedNumberOfFiles = if (tableProperty) 2 else 5
          df.write.format("delta").mode("append").save(dir)
          checkResult(
            df.union(spark.range(10).toDF()),
            numFileCheck = _ === expectedNumberOfFiles,
            dir)
        }
      }
    }
  }

  test("non-partitioned write - data frame config") {
    withTempDir { dir =>
      val df = spark.range(0, 100, 1, 4).toDF()
      df.write.format("delta")
        .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "true").save(dir)
      checkResult(
        df,
        numFileCheck = _ === 1,
        dir)
    }
  }

  writeTest("non-partitioned write - data frame config trumps table config") { dir =>
    val df = spark.range(0, 100, 1, 4).toDF()
    df.write.format("delta").option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "false").save(dir)
    checkResult(
      df,
      numFileCheck = _ === 4,
      dir)
  }

  writeTest("partitioned write - table config") { dir =>
    val df = spark.range(0, 100, 1, 4)
      .withColumn("part", 'id % 5)

    df.write.partitionBy("part").format("delta").save(dir)
    checkResult(
      df,
      numFileCheck = _ <= 5,
      dir)
  }

  test("partitioned write - data frame config") {
    withTempDir { dir =>
      val df = spark.range(0, 100, 1, 4)
        .withColumn("part", 'id % 5)

      df.write.partitionBy("part").option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "true")
        .format("delta").save(dir)

      checkResult(
        df,
        numFileCheck = _ <= 5,
        dir)
    }
  }

  writeTest("partitioned write - data frame config trumps table config") { dir =>
    val df = spark.range(0, 100, 1, 4)
      .withColumn("part", 'id % 5)

    df.write.partitionBy("part").format("delta")
      .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "false").save(dir)

    checkResult(
      df,
      numFileCheck = _ === 20,
      dir)
  }

  writeTest("multi-partitions - table config") { dir =>
    val df = spark.range(0, 100, 1, 4)
      .withColumn("part", 'id % 5)
      .withColumn("part2", ('id / 20).cast("int"))

    df.write.partitionBy("part", "part2").format("delta").save(dir)

    checkResult(
      df,
      numFileCheck = _ <= 25,
      dir)
  }

  test("multi-partitions - data frame config") {
    withTempDir { dir =>
      val df = spark.range(0, 100, 1, 4)
        .withColumn("part", 'id % 5)
        .withColumn("part2", ('id / 20).cast("int"))

      df.write.partitionBy("part", "part2")
        .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "true").format("delta").save(dir)

      checkResult(
        df,
        numFileCheck = _ <= 25,
        dir)
    }
  }

  test("optimized writes used if enabled when a stream starts") {
    withTempDir { f =>
      // Write some data into the table so it already exists
      Seq(1).toDF().write.format("delta").save(f)

      // Use optimized writes just when starting the stream
      val inputData = MemoryStream[Int]

      val df = inputData.toDF().repartition(10)
      var stream: StreamingQuery = null

      // Start the stream with optimized writes enabled, and then reset the conf
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        val checkpoint = new File(f, "checkpoint").getCanonicalPath
        stream = df.writeStream.format("delta").option("checkpointLocation", checkpoint).start(f)
      }
      try {
        inputData.addData(1 to 100)
        stream.processAllAvailable()
      } finally {
        stream.stop()
      }

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, f)
      assert(snapshot.numOfFiles == 2, "Optimized writes were not used")
    }
  }

  writeTest("multi-partitions - data frame config trumps table config") { dir =>
    val df = spark.range(0, 100, 1, 4)
      .withColumn("part", 'id % 5)
      .withColumn("part2", ('id / 20).cast("int"))

    df.write.partitionBy("part", "part2")
      .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "false").format("delta").save(dir)

    checkResult(
      df,
      numFileCheck = _ > 25,
      dir)
  }

  writeTest("optimize should not leverage optimized writes") { dir =>
    val df = spark.range(0, 10, 1, 2)

    val logs1 = Log4jUsageLogger.track {
      df.write.format("delta").mode("append").save(dir)
      df.write.format("delta").mode("append").save(dir)
    }.filter(_.metric == "tahoeEvent")

    assert(logs1.count(_.tags.get("opType") === Some("delta.optimizeWrite.planned")) === 2)

    val logs2 = Log4jUsageLogger.track {
      sql(s"optimize delta.`$dir`")
    }.filter(_.metric == "tahoeEvent")

    assert(logs2.count(_.tags.get("opType") === Some("delta.optimizeWrite.planned")) === 0)
  }

  writeTest("map task with more partitions than target shuffle blocks - non-partitioned") { dir =>
    val df = spark.range(0, 20, 1, 4)

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "2") {
      df.write.format("delta").mode("append").save(dir)
    }

    checkResult(
      df.toDF(),
      numFileCheck = _ === 1,
      dir)
  }

  writeTest("map task with more partitions than target shuffle blocks - partitioned") { dir =>
    val df = spark.range(0, 20, 1, 4).withColumn("part", 'id % 5)

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "2") {
      df.write.format("delta").partitionBy("part").mode("append").save(dir)
    }

    checkResult(
      df,
      numFileCheck = _ === 5,
      dir)
  }

  writeTest("zero partition dataframe write") { dir =>
    val df = spark.range(0, 20, 1, 4).withColumn("part", 'id % 5)
    df.write.format("delta").partitionBy("part").mode("append").save(dir)
    val schema = new StructType().add("id", LongType).add("part", LongType)

    spark.createDataFrame(sparkContext.emptyRDD[Row], schema).write.format("delta")
      .partitionBy("part").mode("append").save(dir)

    checkResult(
      df,
      numFileCheck = _ === 5,
      dir)
  }

  test("OptimizedWriterBlocks is not serializable") {
    assert(!new OptimizedWriterBlocks(Array.empty).isInstanceOf[Serializable],
      "The blocks should not be serializable so that they don't get shipped to executors.")
  }

  writeTest("single partition dataframe write") { dir =>
    val df = spark.range(0, 20).repartition(1).withColumn("part", 'id % 5)
    val logs1 = Log4jUsageLogger.track {
      df.write.format("delta").partitionBy("part").mode("append").save(dir)
    }.filter(_.metric == "tahoeEvent")

    // doesn't use optimized writes
    assert(logs1.count(_.tags.get("opType") === Some("delta.optimizeWrite.planned")) === 0)

    checkResult(
      df,
      numFileCheck = _ === 5,
      dir)
  }

  writeTest("do not create tons of shuffle partitions during optimized writes") { dir =>
    // 50M shuffle blocks would've led to 25M shuffle partitions
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "50000000") {
      val df = spark.range(0, 20).repartition(2).withColumn("part", 'id % 5)
      val logs1 = Log4jUsageLogger.track {
        df.write.format("delta").partitionBy("part").mode("append").save(dir)
      }.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType") === Some("delta.optimizeWrite.planned"))

      assert(logs1.length === 1)
      val blob = JsonUtils.fromJson[Map[String, Any]](logs1.head.blob)
      assert(blob("outputPartitions") === 5)
      assert(blob("originalPartitions") === 2)
      assert(blob("numShuffleBlocks") === 50000000)
      assert(blob("shufflePartitions") ===
        spark.conf.get(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))

      checkResult(
        df,
        numFileCheck = _ === 5,
        dir)
    }
  }
}

class OptimizedWritesSuite extends OptimizedWritesSuiteBase with DeltaSQLCommandTest {}
