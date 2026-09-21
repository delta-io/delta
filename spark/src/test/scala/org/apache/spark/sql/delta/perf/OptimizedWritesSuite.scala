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

  // planReducerRanges is the range planner for the shuffleManager path. These exercise it
  // directly, which covers edge cases the end-to-end tests cannot reach cheaply.

  private def ranges(
      blocks: Seq[(Long, Int)], binSize: Long, numMappers: Int): Seq[ShuffleMapRange] =
    DeltaOptimizedWriterExec.planReducerRanges(7, blocks, binSize, numMappers).map(_._1)

  test("planReducerRanges - a reducer that fits in one task reads the whole partition") {
    val r = ranges(Seq((10L, 0), (10L, 1)), binSize = 1024, numMappers = 4)
    assert(r === Seq(ShuffleMapRange(7, 0, Int.MaxValue)))
  }

  test("planReducerRanges - Int.MaxValue is only ever paired with start 0") {
    val blocks = Seq((100L, 0), (100L, 1), (100L, 2), (100L, 3))
    for (binSize <- Seq(1L, 50L, 100L, 150L, 400L, 10000L)) {
      ranges(blocks, binSize, numMappers = 4).foreach { r =>
        assert(r.endMapIndex != Int.MaxValue || r.startMapIndex == 0,
          s"bounded range $r must not end at Int.MaxValue: a remote shuffle service " +
            "ignores the start index on the whole-partition path")
      }
    }
  }

  test("planReducerRanges - split chunks cover [0, numMappers) with no gaps or overlaps") {
    val blocks = Seq((100L, 0), (100L, 1), (100L, 3), (100L, 7))
    val r = ranges(blocks, binSize = 150, numMappers = 8).sortBy(_.startMapIndex)
    assert(r.length > 1, "expected the reducer to be split")
    assert(r.head.startMapIndex === 0)
    assert(r.last.endMapIndex === 8, "the last chunk must end at the map count")
    r.sliding(2).foreach {
      case Seq(a, b) => assert(a.endMapIndex === b.startMapIndex, s"gap or overlap: $a then $b")
      case _ =>
    }
  }

  test("planReducerRanges - a single oversized map block is one whole-partition read") {
    val r = ranges(Seq((10000L, 2)), binSize = 100, numMappers = 4)
    assert(r === Seq(ShuffleMapRange(7, 0, Int.MaxValue)))
  }

  test("planReducerRanges - a reducer exactly at binSize is not split") {
    val r = ranges(Seq((50L, 0), (50L, 1)), binSize = 100, numMappers = 4)
    assert(r === Seq(ShuffleMapRange(7, 0, Int.MaxValue)))
  }

  test("planReducerRanges - no blocks plans no tasks") {
    assert(ranges(Seq.empty, binSize = 100, numMappers = 4).isEmpty)
  }

  test("OptimizedWriterBlocks is not serializable") {
    assert(!new OptimizedWriterBlocks(BlockBins(Array.empty)).isInstanceOf[Serializable],
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

  // shuffleReadMode=shuffleManager path tests
  //
  // With shuffleReadMode=shuffleManager, DeltaOptimizedWriterExec reads shuffle output via
  // ShuffleManager.getReader() instead of ShuffleBlockFetcherIterator. This path is
  // designed to be compatible with remote shuffle services (e.g. Celeborn, Uniffle),
  // whose implementations live in separate repositories; no RSS is present here.
  //
  // Data is fetched at (reducer, contiguous map-index range) granularity, the same API
  // AQE uses to split skewed partitions. Each write task reads one reducer; a reducer
  // larger than binSize is split into contiguous map-index chunks, so the write stays
  // parallel even when all rows hash to a single reducer (one partition value, or an
  // unpartitioned table).
  //
  // Note on bin-size choice in these tests: "1b" is parsed by bytesConf(MiB) as 0 MiB (integer
  // division 1 / 1048576 = 0). maxBinSize = 0 classifies every non-empty reducer as "large"
  // and makes every map block its own chunk, forcing the splitting machinery to engage
  // without needing a multi-GB test dataset.
  // maxShufflePartitions is capped at 20 to keep bin/file counts manageable.

  writeTest("shuffleManager path - oversized partition value splits across files") {
    dir =>
      // 4 input partitions, ALL rows share the same Delta partition value "x", so every row
      // hashes to ONE reducer. With bin size forced to 0 that reducer must be split into one
      // map-range chunk per map block, so one reducer yields several write tasks and
      // several files instead of a single task writing one giant file. checkResult calls
      // checkAnswer, so this also verifies the split does not duplicate or drop rows.
      val df = spark.range(0, 200, 1, 4).withColumn("part", lit("x"))

      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "shuffleManager",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "20",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {

        df.write.partitionBy("part").format("delta").save(dir)
      }

      checkResult(df, numFileCheck = _ > 1, dir)
  }

  writeTest("shuffleManager path - unpartitioned write does not collapse") {
    dir =>
      // Unpartitioned: partitionColumns = Seq(), so HashPartitioning(Seq(), N) is a constant
      // and all rows land in one reducer regardless of data size. Map-range splitting breaks
      // that reducer into several write tasks and several files; checkAnswer verifies no
      // row duplication or loss.
      val df = spark.range(0, 200, 1, 4).toDF()

      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "shuffleManager",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "20",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {

        df.write.format("delta").save(dir)
      }

      checkResult(df, numFileCheck = _ > 1, dir)
  }

  writeTest("shuffleManager path - no data loss at default bin size") { dir =>
    // Primary correctness requirement: map-range splitting must not add or drop rows. Uses
    // the default bin size so this exercises a realistic plan rather than the "0-bin" edge
    // case; here each partition value is small enough to be read as a whole partition.
    val df = spark.range(0, 1000, 1, 8).withColumn("part", 'id % 3)

    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "shuffleManager") {

      df.write.partitionBy("part").format("delta").save(dir)
    }

    // checkResult calls checkAnswer, which verifies exact row-level equality.
    checkResult(df, numFileCheck = _ >= 3, dir)
  }

  /**
   * Whether this suite's shuffle manager exposes its blocks as local files, which the
   * blockFetcher path requires. False for a suite running under a manager that stands in for
   * a remote shuffle service.
   */
  protected def blocksAddressableAsFiles: Boolean = true

  if (blocksAddressableAsFiles) {
    test("auto keeps Spark's built-in shuffle on the block-fetcher path") {
      // The central compatibility claim: a user on Spark's own shuffle manager sees no
      // change in planning. Gated, because a suite running under a non-default manager is
      // expected to resolve the other way.
      withTempDir { dir =>
        val path = dir.getCanonicalPath + "/tbl"
        val df = spark.range(0, 100, 1, 4).withColumn("part", 'id % 5)
        val events = withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "auto") {
          Log4jUsageLogger.track {
            df.write.partitionBy("part").format("delta").save(path)
          }.filter(_.metric == "tahoeEvent")
            .filter(_.tags.get("opType") === Some("delta.optimizeWrite.planned"))
        }
        assert(events.length === 1)
        val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
        assert(blob("useShuffleManager") === false,
          "auto must keep Spark's built-in shuffle on the existing read path")
      }
    }

    writeTest("blockFetcher path - unpartitioned write coalesces to one file") { dir =>
      // blockFetcher path: an unpartitioned write smaller than binSize lands in one file.
      val df = spark.range(0, 100, 1, 4).toDF()

      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "blockFetcher") {

        df.write.format("delta").save(dir)
      }

      checkResult(df, numFileCheck = _ === 1, dir)
    }
  }
}

class OptimizedWritesSuite extends OptimizedWritesSuiteBase with DeltaSQLCommandTest {}
