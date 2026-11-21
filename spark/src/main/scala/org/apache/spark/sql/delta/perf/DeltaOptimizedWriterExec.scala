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
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.BinPackingUtils

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark._
import org.apache.spark.internal.MDC
import org.apache.spark.internal.config
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
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

  private lazy val useShuffleManager: Boolean =
    getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE) match {
      case "blockfetcher" => false
      case "shufflemanager" => true
      case _ =>
        // A shuffle manager that is not Spark's own cannot be assumed to serve direct block
        // fetches through ShuffleBlockFetcherIterator. getReader() is correct for any
        // shuffle manager, so this errs towards the portable path; the cost is that a
        // local-disk custom manager also takes it. Both paths cap bins at binSize, so that
        // changes which blocks share an output file but not how large one can get.
        !SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
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

  /**
   * Plans the write tasks. The block-fetcher path addresses individual shuffle blocks; the
   * getReader() path addresses one reducer over a half-open map-index range.
   */
  private def computeBins(): OptimizedWritePlan = {
    // Get all shuffle information
    val shuffleStats = getShuffleStats()

    val maxBinSize =
      ByteUnit.BYTE.convertFrom(getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE), ByteUnit.MiB)

    // Group blocks by reducer and calculate total size per reducer
    val reducerGroups = shuffleStats.toSeq.flatMap(_._2)
      .groupBy(_._1.asInstanceOf[ShuffleBlockId].reduceId)
      .map { case (reducerId, blocks) =>
        (reducerId, blocks, blocks.map(_._2).sum)
      }.toSeq

    if (useShuffleManager) {
      // getReader() serves data at (reducer, contiguous map-index range) granularity, never
      // as arbitrary subsets of blocks, so each task reads exactly one reducer. The shuffle
      // hashes on the table's partition columns, so every partition value lands in exactly
      // one reducer, even though a reducer can hold several values. Packing reducers into
      // one task therefore cannot reduce the output file count, only write parallelism.
      val (largeReducers, smallReducers) = reducerGroups.partition(_._3 >= maxBinSize)

      // A reducer larger than the bin size is split into contiguous map-index chunks of up
      // to maxBinSize, which keeps the write parallel even when every row hashes to one
      // reducer, as happens for a single partition value or an unpartitioned table.
      val numMappers = getShuffleRDD.dependency.rdd.getNumPartitions
      val largeRanges = largeReducers.flatMap { case (reducerId, blocks, _) =>
        DeltaOptimizedWriterExec.planReducerRanges(
          reducerId, blocks.map(b => (b._2, b._3)).toSeq, maxBinSize, numMappers)
      }

      // A reducer that fits in one task is read as a whole partition. A bounded range can
      // make a remote shuffle service (Celeborn, for example) sort the partition file by map
      // id before serving it, which rewrites the whole file. Int.MaxValue is the end index
      // Spark's own whole-partition reads pass, and what such a service keys off to skip
      // that sort.
      val smallRanges = smallReducers.map { case (reducerId, _, size) =>
        (ShuffleMapRange(reducerId, 0, Int.MaxValue), size)
      }

      MapRanges((largeRanges ++ smallRanges)
        .sortBy(-_._2) // submit the largest tasks first
        .map(_._1)
        .toArray)
    } else {
      // ShuffleBlockFetcherIterator addresses individual blocks, so this path packs a
      // reducer's blocks in size order. Only it needs block locations keyed by block id,
      // which is one entry per (map task x shuffle partition), so build it here rather
      // than for every plan.
      val blockInfo = shuffleStats.flatMap { case (bmId, blocks) =>
        blocks.map { case (blockId, size, index) =>
          (blockId, (bmId, size, index))
        }
      }.toMap

      val bins = reducerGroups.flatMap { case (_, blocks, _) =>
        BinPackingUtils.binPackBySize[(BlockId, Long, Int), BlockId](
          blocks,
          _._2, // size
          _._1, // blockId
          maxBinSize)
      }

      BlockBins(bins
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
        .map(_._2))
    }
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
        logWarning(log"Failed to fetch shuffle blocks for the optimized writer. Retrying", e)
        awaitShuffleMapStage()
        getStats()
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    // Single partitioned tasks can simply be written
    if (childNumPartitions <= 1) return child.execute()

    val shuffledRDD = getShuffleRDD

    val plan = computeBins()
    val numTasks = plan match {
      case BlockBins(bins) => bins.length
      case MapRanges(ranges) => ranges.length
    }

    if (useShuffleManager) {
      logInfo(log"Optimized write: reading shuffle data via ShuffleManager.getReader(), " +
        log"planned ${MDC(DeltaLogKeys.NUM_TASKS, numTasks)} write tasks over " +
        log"${MDC(DeltaLogKeys.NUM_PARTITIONS, numPartitions)} shuffle partitions")
    } else {
      logInfo(log"Optimized write: reading shuffle data via ShuffleBlockFetcherIterator, " +
        log"planned ${MDC(DeltaLogKeys.NUM_TASKS, numTasks)} write tasks over " +
        log"${MDC(DeltaLogKeys.NUM_PARTITIONS, numPartitions)} shuffle partitions")
    }

    recordDeltaEvent(deltaLog,
      "delta.optimizeWrite.planned",
      data = Map(
        "originalPartitions" -> childNumPartitions,
        "outputPartitions" -> numTasks,
        "shufflePartitions" -> numPartitions,
        "numShuffleBlocks" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS),
        "binSize" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE),
        "maxShufflePartitions" ->
          getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS),
        "shuffleReadMode" ->
          getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_READ_MODE),
        "useShuffleManager" -> useShuffleManager
      )
    )

    new DeltaOptimizedWriterRDD(
      sparkContext,
      shuffledRDD.dependency,
      readMetrics,
      new OptimizedWriterBlocks(plan))
  }

  private def getConf[T](entry: ConfigEntry[T]): T = {
    conf.getConf(entry)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaOptimizedWriterExec =
    copy(child = newChild)
}

object DeltaOptimizedWriterExec {

  /**
   * Splits one reducer into contiguous map-index ranges of at most `maxBinSize` bytes,
   * returning one range per write task with the bytes it covers.
   *
   * The ranges cover `[0, numMappers)` with no gaps or overlaps, so a map index the
   * statistics do not mention is still inside some range and simply contributes nothing.
   * That keeps the plan correct for a shuffle manager whose statistics are coarser than
   * Spark's own.
   *
   * A reducer that fits in one task is expressed as `(0, Int.MaxValue)`. That is the
   * "whole partition, not a range read" signal: a remote shuffle service serves the file
   * as-is instead of sorting it by map id first. It is only ever used with start 0,
   * because a service that takes the non-range path does not consult the start index.
   */
  private[delta] def planReducerRanges(
      reducerId: Int,
      blocks: Seq[(Long, Int)],
      maxBinSize: Long,
      numMappers: Int): Seq[(ShuffleMapRange, Long)] = {
    if (blocks.isEmpty) return Seq.empty

    val chunks = ArrayBuffer(ArrayBuffer[(Long, Int)]())
    var chunkSize = 0L
    blocks.sortBy(_._2).foreach { block =>
      if (chunks.last.nonEmpty && chunkSize + block._1 > maxBinSize) {
        chunks += ArrayBuffer[(Long, Int)]()
        chunkSize = 0L
      }
      chunks.last += block
      chunkSize += block._1
    }

    if (chunks.length == 1) {
      Seq((ShuffleMapRange(reducerId, 0, Int.MaxValue), chunks.head.map(_._1).sum))
    } else {
      val ends = chunks.map(_.map(_._2).max + 1)
      chunks.zipWithIndex.map { case (chunk, i) =>
        val start = if (i == 0) 0 else ends(i - 1)
        val end = if (i == chunks.length - 1) numMappers else ends(i)
        (ShuffleMapRange(reducerId, start, end), chunk.map(_._1).sum)
      }.toSeq
    }
  }
}

/**
 * A wrapper class to make the write plan non-serializable. If we serialize the plan and send it
 * to the executors, it may cause memory problems.
 * NOTE!!!: By wrapping it in a non-serializable class we enforce that the field needs to be
 *          transient, and gives us extra security against a developer making a mistake.
 */
class OptimizedWriterBlocks(val plan: OptimizedWritePlan)

/** One write task on the getReader() path: a reducer over a half-open map-index range. */
private[delta] case class ShuffleMapRange(reducerId: Int, startMapIndex: Int, endMapIndex: Int)

/** How the optimized writer addresses the shuffle data for each write task. */
private[delta] sealed trait OptimizedWritePlan
private[delta] case class BlockBins(
    bins: Array[List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])]])
  extends OptimizedWritePlan
private[delta] case class MapRanges(ranges: Array[ShuffleMapRange]) extends OptimizedWritePlan

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

  override def getPartitions: Array[Partition] = blocks.plan match {
    case BlockBins(bins) => Array.tabulate(bins.length)(i => ShuffleBlockRDDPartition(i, bins(i)))
    case MapRanges(ranges) => Array.tabulate(ranges.length)(i => MapRangeRDDPartition(i, ranges(i)))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    val records = split match {
      case MapRangeRDDPartition(_, range) =>
        // getReader() resolves block locations when it runs, so a stage retry needs no
        // refresh here: the range holds only (reducerId, mapIndex), both of which are
        // stable across attempts. A map output with no data for this reducer may fall
        // inside the range and simply contributes nothing.
        val reader = SparkEnv.get.shuffleManager.getReader(
          dep.shuffleHandle,
          range.startMapIndex,
          range.endMapIndex,
          range.reducerId,
          range.reducerId + 1,
          context,
          sqlMetricsReporter)
        new InterruptibleIterator[Product2[Int, InternalRow]](
          context, reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]])

      case partition: ShuffleBlockRDDPartition =>
        val blocks = if (context.stageAttemptNumber() > 0) {
          // We lost shuffle blocks, so we need to now get new manager addresses
          val executorTracker = SparkEnv.get.mapOutputTracker
          val oldBlockLocations = partition.blocks

          // assumes we bin-pack by reducerId: on the block-fetcher path every bin holds
          // exactly one reducer
          val reducerId = oldBlockLocations.head._2.head._1.asInstanceOf[ShuffleBlockId].reduceId
          // Get block addresses
          val newLocations = executorTracker.getMapSizesByExecutorId(dep.shuffleId, reducerId)
            .flatMap { case (bmId, newBlocks) =>
              newBlocks.map { blockInfo =>
                (blockInfo._3, (bmId, blockInfo))
              }
            }.toMap

          val blockLocations =
            new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long, Int)]]()
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
          partition.blocks.iterator
        }

        new OptimizedWriterShuffleReader(dep, context, blocks, sqlMetricsReporter).read()

      case other =>
        throw new IllegalStateException(s"Unexpected partition type ${other.getClass.getName}")
    }

    records.map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dep = null
  }
}

/** The reducer and map-index range read by a partition on the getReader() path. */
private case class MapRangeRDDPartition(index: Int, range: ShuffleMapRange) extends Partition

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
