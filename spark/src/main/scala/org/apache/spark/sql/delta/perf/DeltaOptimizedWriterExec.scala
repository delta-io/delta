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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.BinPackingUtils

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.storage._
import org.apache.spark.util.ThreadUtils


/**
 * An execution node which shuffles data to a target output of `DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS`
 * blocks, hash partitioned on the table partition columns. We group all blocks by their
 * reducer_id's and bin-pack into `DELTA_OPTIMIZE_WRITE_BIN_SIZE` bins. Then we launch a Spark task
 * per bin to write out a single file for each bin.
 *
 * @param child The execution plan
 * @param partitionColumns The partition columns of the table. Used for hash partitioning the write
 * @param deltaLog The DeltaLog for the table. Used for logging only
 */
case class DeltaOptimizedWriterExec(
    child: SparkPlan,
    partitionColumns: Seq[String],
    @transient deltaLog: DeltaLog
  ) extends UnaryExecNode with DeltaLogging {

  override def output: Seq[Attribute] = child.output

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  private lazy val childNumPartitions = child.execute().getNumPartitions

  private lazy val numPartitions: Int = {
    val targetShuffleBlocks = getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS)
    math.min(
      math.max(targetShuffleBlocks / childNumPartitions, 1),
      getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))
  }

  @transient private var cachedShuffleRDD: ShuffledRowRDD = _

  @transient private lazy val mapTracker = SparkEnv.get.mapOutputTracker

  /** Creates a ShuffledRowRDD for facilitating the shuffle in the map side. */
  private def getShuffleRDD: ShuffledRowRDD = {
    if (cachedShuffleRDD == null) {
      val resolver = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      val saltedPartitioning = HashPartitioning(
        partitionColumns.map(p => output.find(o => resolver(p, o.name)).getOrElse(
          throw DeltaErrors.failedFindPartitionColumnInOutputPlan(p))),
        numPartitions)

      val shuffledRDD =
        ShuffleExchangeExec(saltedPartitioning, child).execute().asInstanceOf[ShuffledRowRDD]

      cachedShuffleRDD = shuffledRDD
    }
    cachedShuffleRDD
  }

  private def computeBins(): Array[List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])]] = {
    // Get all shuffle information
    val shuffleStats = getShuffleStats()

    // Group by blockId instead of block manager
    val blockInfo = shuffleStats.flatMap { case (bmId, blocks) =>
      blocks.map { case (blockId, size, index) =>
        (blockId, (bmId, size, index))
      }
    }.toMap

    val maxBinSize =
      ByteUnit.BYTE.convertFrom(getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE), ByteUnit.MiB)

    val bins = shuffleStats.toSeq.flatMap(_._2).groupBy(_._1.asInstanceOf[ShuffleBlockId].reduceId)
      .flatMap { case (_, blocks) =>
        BinPackingUtils.binPackBySize[(BlockId, Long, Int), BlockId](
          blocks,
          _._2, // size
          _._1, // blockId
          maxBinSize)
      }

    bins
      .map { bin =>
        var binSize = 0L
        val blockLocations =
          new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long, Int)]]()
        for (blockId <- bin) {
          val (bmId, size, index) = blockInfo(blockId)
          binSize += size
          val blocksAtBM = blockLocations.getOrElseUpdate(
            bmId, new ArrayBuffer[(BlockId, Long, Int)]())
          blocksAtBM.append((blockId, size, index))
        }
        (binSize, blockLocations.toList)
      }
      .toArray
      .sortBy(_._1)(Ordering[Long].reverse) // submit largest blocks first
      .map(_._2)
  }

  /** Performs the shuffle before the write, so that we can bin-pack output data. */
  private def getShuffleStats(): Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val dep = getShuffleRDD.dependency
    // Gets the shuffle output stats
    def getStats() = mapTracker.getMapSizesByExecutorId(
      dep.shuffleId, 0, Int.MaxValue, 0, numPartitions).toArray

    // Executes the shuffle map stage in case we are missing output stats
    def awaitShuffleMapStage(): Unit = {
      assert(dep != null, "Shuffle dependency should not be null")
      // hack to materialize the shuffle files in a fault tolerant way
      ThreadUtils.awaitResult(sparkContext.submitMapStage(dep), Duration.Inf)
    }

    try {
      val res = getStats()
      if (res.isEmpty) awaitShuffleMapStage()
      getStats()
    } catch {
      case e: FetchFailedException =>
        logWarning("Failed to fetch shuffle blocks for the optimized writer. Retrying", e)
        awaitShuffleMapStage()
        getStats()
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    // Single partitioned tasks can simply be written
    if (childNumPartitions <= 1) return child.execute()

    val shuffledRDD = getShuffleRDD

    val partitions = computeBins()

    recordDeltaEvent(deltaLog,
      "delta.optimizeWrite.planned",
      data = Map(
        "originalPartitions" -> childNumPartitions,
        "outputPartitions" -> partitions.length,
        "shufflePartitions" -> numPartitions,
        "numShuffleBlocks" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS),
        "binSize" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE),
        "maxShufflePartitions" ->
          getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS)
      )
    )

    new DeltaOptimizedWriterRDD(
      sparkContext,
      shuffledRDD.dependency,
      readMetrics,
      new OptimizedWriterBlocks(partitions))
  }

  private def getConf[T](entry: ConfigEntry[T]): T = {
    conf.getConf(entry)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaOptimizedWriterExec =
    copy(child = newChild)
}

/**
 * A wrapper class to make the blocks non-serializable. If we serialize the blocks and send them to
 * the executors, it may cause memory problems.
 * NOTE!!!: By wrapping the Array in a non-serializable class we enforce that the field needs to
 *          be transient, and gives us extra security against a developer making a mistake.
 */
class OptimizedWriterBlocks(
    val bins: Array[List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])]])

/**
 * A specialized implementation similar to `ShuffledRowRDD`, where a partition reads a prepared
 * set of shuffle blocks.
 */
private class DeltaOptimizedWriterRDD(
    @transient sparkContext: SparkContext,
    var dep: ShuffleDependency[Int, _, InternalRow],
    metrics: Map[String, SQLMetric],
    @transient blocks: OptimizedWriterBlocks)
  extends RDD[InternalRow](sparkContext, Seq(dep)) with DeltaLogging {

  override def getPartitions: Array[Partition] = Array.tabulate(blocks.bins.length) { i =>
    ShuffleBlockRDDPartition(i, blocks.bins(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    val blocks = if (context.stageAttemptNumber() > 0) {
      // We lost shuffle blocks, so we need to now get new manager addresses
      val executorTracker = SparkEnv.get.mapOutputTracker
      val oldBlockLocations = split.asInstanceOf[ShuffleBlockRDDPartition].blocks

      // assumes we bin-pack by reducerId
      val reducerId = oldBlockLocations.head._2.head._1.asInstanceOf[ShuffleBlockId].reduceId
      // Get block addresses
      val newLocations = executorTracker.getMapSizesByExecutorId(dep.shuffleId, reducerId)
        .flatMap { case (bmId, newBlocks) =>
          newBlocks.map { blockInfo =>
            (blockInfo._3, (bmId, blockInfo))
          }
        }.toMap

      val blockLocations = new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long, Int)]]()
      oldBlockLocations.foreach { case (_, oldBlocks) =>
        oldBlocks.foreach { oldBlock =>
          val (bmId, blockInfo) = newLocations(oldBlock._3)
          val blocksAtBM = blockLocations.getOrElseUpdate(bmId,
            new ArrayBuffer[(BlockId, Long, Int)]())
          blocksAtBM.append(blockInfo)
        }
      }

      blockLocations.iterator
    } else {
      split.asInstanceOf[ShuffleBlockRDDPartition].blocks.iterator
    }

    val reader = new OptimizedWriterShuffleReader(
      dep,
      context,
      blocks,
      sqlMetricsReporter)
    reader.read().map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dep = null
  }
}

/** The list of blocks that need to be read by a partition of the ShuffleBlockRDD. */
private case class ShuffleBlockRDDPartition(
    index: Int,
    blocks: List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])]) extends Partition

/** A simplified implementation of the `BlockStoreShuffleReader` for reading shuffle blocks. */
private class OptimizedWriterShuffleReader(
    dep: ShuffleDependency[Int, _, InternalRow],
    context: TaskContext,
    blocks: Iterator[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])],
    readMetrics: ShuffleReadMetricsReporter) extends ShuffleReader[Int, InternalRow] {

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[Int, InternalRow]] = {
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.blockStoreClient,
      SparkEnv.get.blockManager,
      SparkEnv.get.mapOutputTracker,
      blocks,
      SparkEnv.get.serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt.useExtraMemory", false),
      SparkEnv.get.conf.getBoolean("spark.shuffle.checksum.enabled", true),
      SparkEnv.get.conf.get("spark.shuffle.checksum.algorithm", "ADLER32"),
      readMetrics,
      false)

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (_, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }.asInstanceOf[Iterator[Product2[Int, InternalRow]]]

    new InterruptibleIterator[Product2[Int, InternalRow]](context, recordIter)
  }
}
