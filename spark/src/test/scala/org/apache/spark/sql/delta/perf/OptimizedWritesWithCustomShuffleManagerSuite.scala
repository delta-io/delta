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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.functions.lit

/**
 * A [[ShuffleManager]] that delegates everything to [[SortShuffleManager]] but records every
 * getReader() call. Because it is not a SortShuffleManager, DeltaOptimizedWriterExec
 * auto-detects it as a non-default shuffle manager and takes the ShuffleManager.getReader()
 * read path -- the same code path a remote shuffle service (e.g. Celeborn, Uniffle) would
 * take -- while data is still served from local shuffle files.
 */
class RecordingDelegatingShuffleManager(conf: SparkConf) extends ShuffleManager {
  private val delegate = new SortShuffleManager(conf)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle =
    delegate.registerShuffle(shuffleId, dependency)

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    delegate.getWriter(handle, mapId, context, metrics)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    RecordingDelegatingShuffleManager.recordedReads.add(
      (startMapIndex, endMapIndex, startPartition, endPartition))
    delegate.getReader(
      handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean =
    delegate.unregisterShuffle(shuffleId)

  override def shuffleBlockResolver: ShuffleBlockResolver = delegate.shuffleBlockResolver

  override def stop(): Unit = delegate.stop()
}

object RecordingDelegatingShuffleManager {
  /**
   * (startMapIndex, endMapIndex, startPartition, endPartition) of every getReader() call.
   * Tests run in local mode, so executor-side calls land in this JVM.
   */
  val recordedReads = new ConcurrentLinkedQueue[(Int, Int, Int, Int)]()

  /**
   * Reads issued with a bounded map-index range: DeltaOptimizedWriterExec's reads. Generic
   * ShuffledRowRDD reads (e.g. Delta state reconstruction) pass endMapIndex = Int.MaxValue.
   */
  def boundedReads: Seq[(Int, Int, Int, Int)] =
    recordedReads.asScala.toSeq.filter(_._2 != Int.MaxValue).distinct

  def reset(): Unit = recordedReads.clear()
}

/**
 * Runs the full optimized-writes suite under a non-default ShuffleManager -- exercising the
 * auto-detected ShuffleManager.getReader() path end to end -- plus targeted tests for the
 * detection logic and the map-range read behavior.
 */
class OptimizedWritesWithCustomShuffleManagerSuite extends OptimizedWritesSuiteBase
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      "spark.shuffle.manager", classOf[RecordingDelegatingShuffleManager].getName)

  // Under the getReader() path the 5 small per-partition-value reducers are bin-packed into
  // a single bin, vs one bin per reducer in the block-fetcher path.
  override protected def expectedLoggedOutputPartitions: Int = 1

  writeTest("auto-detection - non-default shuffle manager uses getReader() without any conf") {
    dir =>
      RecordingDelegatingShuffleManager.reset()
      val df = spark.range(0, 100, 1, 4).withColumn("part", 'id % 5)

      // DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER is unset: detection alone must route the
      // read through ShuffleManager.getReader().
      val events = Log4jUsageLogger.track {
        df.write.partitionBy("part").format("delta").save(dir)
      }.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType") === Some("delta.optimizeWrite.planned"))

      assert(RecordingDelegatingShuffleManager.boundedReads.nonEmpty,
        "expected the optimized write to read shuffle data through ShuffleManager.getReader()")

      // The planned event must report the detected read path.
      assert(events.length === 1)
      val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
      assert(blob("useShuffleManager") === true)

      checkResult(df, numFileCheck = _ <= 5, dir)
  }

  writeTest("auto-detection - explicit useShuffleManager=false overrides detection") { dir =>
    val df = spark.range(0, 100, 1, 4).withColumn("part", 'id % 5)

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_USE_SHUFFLE_MANAGER.key -> "false") {
      RecordingDelegatingShuffleManager.reset()
      df.write.partitionBy("part").format("delta").save(dir)
      assert(RecordingDelegatingShuffleManager.boundedReads.isEmpty,
        "expected the block-fetcher path: no bounded-map-range getReader() calls")
    }

    checkResult(df, numFileCheck = _ <= 5, dir)
  }

  writeTest("map-range reads target single reducers with disjoint ranges") { dir =>
    // All rows share one partition value -> one reducer. Bin size 0 forces one map-range
    // chunk per map block, so the reducer must be read via several disjoint single-reducer
    // ranges rather than one whole-partition read.
    val df = spark.range(0, 200, 1, 4).withColumn("part", lit("x"))

    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "20",
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {
      RecordingDelegatingShuffleManager.reset()
      df.write.partitionBy("part").format("delta").save(dir)
    }

    val reads = RecordingDelegatingShuffleManager.boundedReads
    assert(reads.nonEmpty, "expected map-range getReader() calls")
    assert(reads.forall { case (_, _, startPartition, endPartition) =>
      endPartition == startPartition + 1
    }, s"every read should target exactly one reducer, got: $reads")

    reads.groupBy(_._3).foreach { case (reducerId, readsForReducer) =>
      val sorted = readsForReducer.sortBy(_._1)
      sorted.sliding(2).foreach {
        case Seq((_, prevEnd, _, _), (curStart, _, _, _)) =>
          assert(prevEnd <= curStart, s"map ranges for reducer $reducerId overlap: $sorted")
        case _ => // single range for this reducer
      }
    }

    // The single oversized reducer must have been split into multiple map ranges.
    assert(reads.map(_._3).distinct.size < reads.size,
      s"expected at least one reducer to be split into multiple map ranges, got: $reads")

    checkResult(df, numFileCheck = _ > 1, dir)
  }
}
