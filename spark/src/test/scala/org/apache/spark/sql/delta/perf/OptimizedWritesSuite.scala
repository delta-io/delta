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
import org.apache.spark.sql.delta.test.shims.StreamingTestShims.MemoryStream
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.lit
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

  // Expected "outputPartitions" (number of bins) logged for the write in
  // "do not create tons of shuffle partitions during optimized writes": one bin per reducer
  // in the block-fetcher path; suites running under a non-default shuffle manager pack the
  // small reducers into a single bin and override this.
  protected def expectedLoggedOutputPartitions: Int = 5

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

    // Test order of precedence between table property "delta.autoOptimize.optimizeWrite" and
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

          sql(s"ALTER TABLE delta.`$dir` SET TBLPROPERTIES" +
            s" (delta.autoOptimize.optimizeWrite = ${tableProperty.toString})")

          val df = spark.range(0, 100, 1, 4).toDF()
          // OW adds one file vs non-OW adds 4 files
          val expectedNumberOfFiles = if (sqlConf) 2 else 5
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
      assert(blob("outputPartitions") === expectedLoggedOutputPartitions)
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

  // useShuffleManager=true path tests
  //
  // When useShuffleManager=true, DeltaOptimizedWriterExec reads shuffle output via
  // ShuffleManager.getReader() instead of ShuffleBlockFetcherIterator. This path is
  // designed to be compatible with remote shuffle services (e.g. Celeborn, Uniffle),
  // whose implementations live in separate repositories; no RSS is present here.
  //
  // Data is fetched at (reducer, contiguous map-index range) granularity -- the same API
  // AQE uses to split skewed partitions. computeBins() packs small reducers together as
  // atomic units and splits large reducers into map-index chunks of up to binSize, so the
  // write stays parallel and files stay near the target size even when all rows hash to a
  // single reducer (one partition value, or an unpartitioned table).
  //
  // Note on bin-size choice in these tests: "1b" is parsed by bytesConf(MiB) as 0 MiB (integer
  // division 1 / 1048576 = 0). maxBinSize = 0 classifies every non-empty reducer as "large"
  // and makes every map block its own chunk, forcing the splitting machinery to engage
  // without needing a multi-GB test dataset.
  // maxShufflePartitions is capped at 20 to keep bin/file counts manageable.

  writeTest("useShuffleManager=true - oversized partition value is split across multiple files") {
    dir =>
      // 4 input partitions, ALL rows share the same Delta partition value "x", so every row
      // hashes to ONE reducer. With bin size forced to 0 that reducer must be split into one
      // map-range chunk per map block -- multiple bins -- multiple files -- instead of a
      // single task writing one giant file. checkResult calls checkAnswer, so this also
      // verifies the split does not duplicate or drop rows.
      val df = spark.range(0, 200, 1, 4).withColumn("part", lit("x"))

      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER.key -> "true",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "20",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {

        df.write.partitionBy("part").format("delta").save(dir)
      }

      checkResult(df, numFileCheck = _ > 1, dir)
  }

  writeTest("useShuffleManager=true - unpartitioned write does not collapse into one file") {
    dir =>
      // Unpartitioned: partitionColumns = Seq() -- HashPartitioning(Seq(), N) is a constant
      // -- all rows land in one reducer regardless of data size. Map-range splitting breaks
      // that reducer into multiple bins -- multiple files; checkAnswer verifies no row
      // duplication or loss.
      val df = spark.range(0, 200, 1, 4).toDF()

      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER.key -> "true",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "20",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {

        df.write.format("delta").save(dir)
      }

      checkResult(df, numFileCheck = _ > 1, dir)
  }

  writeTest("useShuffleManager=true - no data loss or duplication at default bin size") { dir =>
    // Primary correctness requirement: bin packing and map-range splitting must not add or
    // drop rows. Uses default bin size so this tests a realistic code path, not the "0-bin"
    // edge case (here the small per-value reducers get packed together into shared bins).
    val df = spark.range(0, 1000, 1, 8).withColumn("part", 'id % 3)

    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER.key -> "true") {

      df.write.partitionBy("part").format("delta").save(dir)
    }

    // checkResult calls checkAnswer -- verifies exact row-level equality (no dups, no loss).
    checkResult(df, numFileCheck = _ >= 3, dir)
  }

  writeTest("useShuffleManager=false still coalesces unpartitioned write to one file") { dir =>
    // Local-shuffle path unchanged: ShuffleBlockFetcherIterator can split individual blocks
    // across bins, so 4 input partitions coalesce into 1 output file.
    val df = spark.range(0, 100, 1, 4).toDF()

    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER.key -> "false") {

      df.write.format("delta").save(dir)
    }

    checkResult(df, numFileCheck = _ === 1, dir)
  }
}

class OptimizedWritesSuite extends OptimizedWritesSuiteBase with DeltaSQLCommandTest {}
