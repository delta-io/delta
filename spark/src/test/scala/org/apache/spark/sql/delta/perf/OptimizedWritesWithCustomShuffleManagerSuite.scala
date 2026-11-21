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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage._

/**
 * A shuffle manager that delegates to [[SortShuffleManager]] but tracks calls to `getReader`.
 * This simulates the behavior of external shuffle managers (e.g., Celeborn, Uniffle, Membrain)
 * that override `getReader` to provide their own shuffle read implementation.
 *
 * The test verifies that Delta's optimized writer goes through `ShuffleManager.getReader()`
 * rather than directly constructing a `ShuffleBlockFetcherIterator`, which would bypass
 * external shuffle managers.
 */
class TrackingShuffleManager(conf: SparkConf) extends ShuffleManager {

  private val delegate = new SortShuffleManager(conf)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    delegate.registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    delegate.getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    TrackingShuffleManager.getReaderCallCount.incrementAndGet()
    delegate.getReader(handle, startMapIndex, endMapIndex,
      startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    delegate.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    delegate.shuffleBlockResolver
  }

  override def stop(): Unit = {
    delegate.stop()
  }
}

object TrackingShuffleManager {
  /** Tracks the number of times `getReader` is called. */
  val getReaderCallCount = new AtomicInteger(0)

  def resetCounters(): Unit = {
    getReaderCallCount.set(0)
  }
}

/**
 * A custom [[OptimizedWriteShuffleHelper]] that delegates to [[DefaultOptimizedWriteShuffleHelper]]
 * but tracks whether it was invoked, verifying the config-based injection path.
 */
class TrackingShuffleHelper extends DefaultOptimizedWriteShuffleHelper {
  override def computeBins(
      shuffleStats: Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      maxBinSize: Long): Seq[Seq[BlockId]] = {
    TrackingShuffleHelper.computeBinsCallCount.incrementAndGet()
    super.computeBins(shuffleStats, maxBinSize)
  }
}

object TrackingShuffleHelper {
  val computeBinsCallCount = new AtomicInteger(0)

  def resetCounters(): Unit = {
    computeBinsCallCount.set(0)
  }
}

/**
 * Tests that Delta's optimized writer correctly delegates to the configured ShuffleManager
 * for shuffle reads when a non-default shuffle manager is configured, ensuring compatibility
 * with external shuffle managers.
 *
 * This is a regression test for the issue where `OptimizedWriterShuffleReader` directly
 * created `ShuffleBlockFetcherIterator` with `blockManager.blockStoreClient`, bypassing
 * external shuffle managers entirely. See:
 * - https://github.com/delta-io/delta/issues/4343
 * - https://github.com/apache/uniffle/issues/2409
 */
class OptimizedWritesWithCustomShuffleManagerSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", classOf[TrackingShuffleManager].getName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    TrackingShuffleManager.resetCounters()
  }

  private def checkResult(df: DataFrame, dir: String): Unit = {
    checkAnswer(
      spark.read.format("delta").load(dir),
      df
    )
  }

  test("detects non-default shuffle manager") {
    assert(!SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager],
      "TrackingShuffleManager should be active, not SortShuffleManager")
    assert(SparkEnv.get.shuffleManager.isInstanceOf[TrackingShuffleManager])
  }

  test("optimized write delegates to ShuffleManager.getReader - non-partitioned") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleManager.resetCounters()

      val df = spark.range(0, 100, 1, 4).toDF()
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.format("delta").save(path)
      }

      // Verify data correctness
      checkResult(df, path)

      // Verify that getReader was called (meaning we went through the ShuffleManager)
      assert(TrackingShuffleManager.getReaderCallCount.get() > 0,
        "Optimized write should delegate shuffle reads to ShuffleManager.getReader() " +
          "when a non-default shuffle manager is configured")
    }
  }

  test("optimized write delegates to ShuffleManager.getReader - partitioned") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleManager.resetCounters()

      val df = spark.range(0, 100, 1, 4).withColumn("part", $"id" % 5)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.partitionBy("part").format("delta").save(path)
      }

      checkResult(df, path)

      assert(TrackingShuffleManager.getReaderCallCount.get() > 0,
        "Optimized write should delegate shuffle reads to ShuffleManager.getReader() " +
          "when a non-default shuffle manager is configured")
    }
  }

  test("optimized write produces correct file count with custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      val df = spark.range(0, 100, 1, 4).withColumn("part", $"id" % 5)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.partitionBy("part").format("delta").save(path)
      }

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, path)
      assert(snapshot.numOfFiles <= 5,
        s"Expected at most 5 files (one per partition), got ${snapshot.numOfFiles}")

      checkResult(df, path)
    }
  }

  test("optimized write with multi-partition columns and custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleManager.resetCounters()

      val df = spark.range(0, 100, 1, 4)
        .withColumn("part", $"id" % 5)
        .withColumn("part2", ($"id" / 20).cast("int"))

      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.partitionBy("part", "part2").format("delta").save(path)
      }

      checkResult(df, path)

      assert(TrackingShuffleManager.getReaderCallCount.get() > 0,
        "Optimized write should delegate shuffle reads to ShuffleManager.getReader() " +
          "when a non-default shuffle manager is configured")

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, path)
      assert(snapshot.numOfFiles <= 25,
        s"Expected at most 25 files, got ${snapshot.numOfFiles}")
    }
  }

  test("optimized write with small bin size and custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleManager.resetCounters()

      // Use a very small bin size to force multiple bins
      val df = spark.range(0, 200, 1, 8).withColumn("part", $"id" % 3)
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1") {
        df.write.partitionBy("part").format("delta").save(path)
      }

      checkResult(df, path)

      assert(TrackingShuffleManager.getReaderCallCount.get() > 0,
        "Optimized write should delegate shuffle reads to ShuffleManager.getReader() " +
          "when a non-default shuffle manager is configured")
    }
  }

  test("single partition dataframe skips optimized write with custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleManager.resetCounters()

      // Single partition should bypass optimized write entirely
      val df = spark.range(0, 20).repartition(1).withColumn("part", $"id" % 5)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.partitionBy("part").format("delta").save(path)
      }

      checkResult(df, path)
    }
  }

  test("append mode with custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      val df1 = spark.range(0, 50, 1, 4).withColumn("part", $"id" % 5)
      val df2 = spark.range(50, 100, 1, 4).withColumn("part", $"id" % 5)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df1.write.partitionBy("part").format("delta").save(path)
        df2.write.partitionBy("part").format("delta").mode("append").save(path)
      }

      checkResult(df1.union(df2), path)
    }
  }

  test("zero row dataframe with custom shuffle manager") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      // First write some data so the table exists
      val df = spark.range(0, 20, 1, 4).withColumn("part", $"id" % 5)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        df.write.partitionBy("part").format("delta").save(path)
      }

      // Then write an empty dataframe
      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types.{LongType, StructType}
      val schema = new StructType().add("id", LongType).add("part", LongType)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        spark.createDataFrame(sparkContext.emptyRDD[Row], schema).write.format("delta")
          .partitionBy("part").mode("append").save(path)
      }

      // Data should be unchanged
      checkResult(df, path)
    }
  }

  test("custom shuffle helper loaded via config") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      TrackingShuffleHelper.resetCounters()

      val df = spark.range(0, 100, 1, 4).withColumn("part", $"id" % 5)
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_HELPER.key ->
          classOf[TrackingShuffleHelper].getName) {
        df.write.partitionBy("part").format("delta").save(path)
      }

      checkResult(df, path)

      assert(TrackingShuffleHelper.computeBinsCallCount.get() > 0,
        "Custom shuffle helper's computeBins should have been called")
    }
  }

  test("invalid shuffle helper class name throws exception with config context") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(0, 100, 1, 4).toDF()

      val ex = intercept[SparkException] {
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_HELPER.key ->
            "com.nonexistent.ShuffleHelper") {
          df.write.format("delta").save(path)
        }
      }
      assert(ex.getMessage.contains("com.nonexistent.ShuffleHelper"),
        "Error should mention the invalid class name")
      assert(ex.getMessage.contains(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_HELPER.key),
        "Error should mention the config key")
    }
  }
}
