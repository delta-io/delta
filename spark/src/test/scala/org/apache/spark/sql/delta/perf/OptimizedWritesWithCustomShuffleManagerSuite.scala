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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.jdk.CollectionConverters._

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.shuffle._
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.sql.functions.lit

/**
 * A [[ShuffleManager]] that behaves like a remote shuffle service, by extending
 * [[NoBlockAccessShuffleManager]], and additionally records every getReader() call and can
 * inject a fetch failure. Because it is not a SortShuffleManager, DeltaOptimizedWriterExec
 * auto-detects it as a non-default shuffle manager and takes the getReader() read path.
 */
class RecordingDelegatingShuffleManager(conf: SparkConf)
  extends NoBlockAccessShuffleManager(conf) {

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // Int.MaxValue is the "whole partition, not a range read" signal. A remote shuffle
    // service taking that path does not consult the start index, so pairing it with a
    // non-zero start silently re-reads the whole partition. Spark's own reader honours the
    // start, so only an explicit check pins the rule down here.
    require(
      !(startMapIndex > 0 && endMapIndex == Int.MaxValue),
      s"a range read starting at $startMapIndex must not end at Int.MaxValue")
    RecordingDelegatingShuffleManager.recordedReads.add(
      (startMapIndex, endMapIndex, startPartition, endPartition))

    val boundedRead = startMapIndex > 0 || endMapIndex != Int.MaxValue
    if (boundedRead &&
        RecordingDelegatingShuffleManager.failNextBoundedRead.compareAndSet(true, false)) {
      // Fail this read the way a lost shuffle block would, naming a map output that really
      // is inside the range we were asked for, so Spark takes its normal recovery route.
      RecordingDelegatingShuffleManager.injectedFailures.incrementAndGet()
      val shuffleId = handle.shuffleId
      val (bmId, blocks) =
        SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, startPartition).next()
      val inRange = blocks.find { case (_, _, mapIndex) =>
        mapIndex >= startMapIndex && (endMapIndex == Int.MaxValue || mapIndex < endMapIndex)
      }
      val (blockId, _, mapIndex) = inRange.getOrElse(blocks.head)
      throw new FetchFailedException(
        bmId,
        shuffleId,
        blockId.asInstanceOf[ShuffleBlockId].mapId,
        mapIndex,
        startPartition,
        "injected for the stage-retry test")
    }

    super.getReader(
      handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }
}

object RecordingDelegatingShuffleManager {
  /**
   * (startMapIndex, endMapIndex, startPartition, endPartition) of every getReader() call.
   * Tests run in local mode, so executor-side calls land in this JVM.
   */
  val recordedReads = new ConcurrentLinkedQueue[(Int, Int, Int, Int)]()

  def reset(): Unit = {
    recordedReads.clear()
    failNextBoundedRead.set(false)
    injectedFailures.set(0)
  }

  /** Set by a test to make the next bounded-range read raise a fetch failure, once. */
  val failNextBoundedRead = new AtomicBoolean(false)

  /** How many injected failures actually fired, so a test cannot pass without one. */
  val injectedFailures = new AtomicInteger(0)

}

/**
 * Runs the full optimized-writes suite under a non-default ShuffleManager, exercising the
 * auto-detected ShuffleManager.getReader() path end to end, plus targeted tests for the
 * detection logic and the map-range read behavior.
 */
class OptimizedWritesWithCustomShuffleManagerSuite extends OptimizedWritesSuiteBase
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      "spark.shuffle.manager", classOf[RecordingDelegatingShuffleManager].getName)

  writeTest("auto-detection - non-default shuffle manager uses getReader() without any conf") {
    dir =>
      RecordingDelegatingShuffleManager.reset()
      val df = spark.range(0, 100, 1, 4).withColumn("part", 'id % 5)

      // shuffleReadMode is left at "auto": detection alone must route the
      // read through ShuffleManager.getReader().
      val events = Log4jUsageLogger.track {
        df.write.partitionBy("part").format("delta").save(dir)
      }.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType") === Some("delta.optimizeWrite.planned"))

      // The planned event records which read path was chosen. Shuffle-manager call counts
      // are not a reliable discriminator here: reducers that fit in one bin are read as
      // whole partitions, and Delta's own log replay also reads through the shuffle manager.
      assert(events.length === 1)
      val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
      assert(blob("useShuffleManager") === true)

      checkResult(df, numFileCheck = _ <= 5, dir)
  }

  // The blockFetcher path reads shuffle blocks directly, which this manager refuses, as a
  // remote shuffle service does. Tests that need that path are skipped here.
  override protected def blocksAddressableAsFiles: Boolean = false

  // Guards the rest of this suite. Every other test here is only meaningful while the test
  // shuffle manager actually behaves like a remote shuffle service, i.e. refuses direct
  // block reads. If that ever stops being true the suite would still pass while testing
  // nothing, so assert the refusal directly by forcing the path that needs block access.
  // Forcing it also shows an explicit setting overrides auto-detection.
  writeTest("the test shuffle manager still refuses direct block reads") { dir =>
    val df = spark.range(0, 100, 1, 4).withColumn("part", 'id % 5)

    val e = intercept[Exception] {
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "blockFetcher") {
        df.write.partitionBy("part").format("delta").save(dir)
      }
    }
    assert(
      Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null)
        .exists(t => Option(t.getMessage).exists(_.contains("direct block fetch"))),
      s"expected a direct-block-fetch refusal, got: $e")
  }

  writeTest("a stage retry after a fetch failure does not lose or duplicate rows") { dir =>
    // The writer plans a bounded map-index range on the driver and resolves block locations at
    // read time, so a retried attempt must be able to reuse the same plan. Force one fetch
    // failure on a bounded read and check the write still lands exactly once.
    val df = spark.range(0, 2000, 1, 8).withColumn("part", lit("x"))

    val attempts = new ConcurrentLinkedQueue[Int]()
    val listener = new SparkListener {
      override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit =
        attempts.add(e.stageInfo.attemptNumber())
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      RecordingDelegatingShuffleManager.reset()
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE.key -> "shuffleManager",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE.key -> "1b") {
        // Bin size 1b splits the single reducer, so the reads that retry are the bounded ones.
        RecordingDelegatingShuffleManager.failNextBoundedRead.set(true)
        df.write.partitionBy("part").format("delta").save(dir)
      }
    } finally {
      // Listener events are delivered on another thread, so save() can return before the
      // retried stage's event arrives. Drain the bus before removing the listener.
      spark.sparkContext.listenerBus.waitUntilEmpty()
      spark.sparkContext.removeSparkListener(listener)
    }

    // Both guards matter: without them this passes even if nothing was injected or retried.
    assert(RecordingDelegatingShuffleManager.injectedFailures.get() === 1,
      "expected exactly one injected fetch failure")
    assert(attempts.asScala.exists(_ > 0),
      s"expected a retried stage attempt, saw attempts ${attempts.asScala.toSeq.distinct}")

    val back = spark.read.format("delta").load(dir)
    assert(back.count() === 2000, "row count after the retry")
    assert(back.select("id").distinct().count() === 2000, "distinct ids after the retry")
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

    // Not de-duplicated: reading the same range twice is the failure this test exists to
    // catch. No retry is injected here, so a repeated range really is a duplicate read.
    val reads = RecordingDelegatingShuffleManager.recordedReads.asScala.toSeq
      .filter(_._2 != Int.MaxValue)
    assert(reads.nonEmpty, "expected map-range getReader() calls")
    assert(reads.forall { case (_, _, startPartition, endPartition) =>
      endPartition == startPartition + 1
    }, s"every read should target exactly one reducer, got: $reads")

    reads.groupBy(_._3).foreach { case (reducerId, readsForReducer) =>
      val sorted = readsForReducer.sortBy(_._1)
      assert(sorted.head._1 === 0, s"reducer $reducerId must start at map index 0: $sorted")
      sorted.sliding(2).foreach {
        case Seq((_, prevEnd, _, _), (curStart, _, _, _)) =>
          assert(prevEnd === curStart,
            s"map ranges for reducer $reducerId must tile without gaps or overlap: $sorted")
        case _ => // single range for this reducer
      }
    }

    // The single oversized reducer must have been split into multiple map ranges.
    assert(reads.map(_._3).distinct.size < reads.size,
      s"expected at least one reducer to be split into multiple map ranges, got: $reads")

    checkResult(df, numFileCheck = _ > 1, dir)
  }
}
