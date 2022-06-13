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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RoundRobinPartitioning, UnknownPartitioning}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaShufflePartitionsUtil
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{MapOutputStatistics, ShuffleDependency}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class OptimizeWriteExchangeExec(
    partitioning: Partitioning,
    override val child: SparkPlan) extends Exchange {

  // Use 140% of target file size hint config considering parquet compression.
  // Still the result file can be smaller/larger than the config due to data skew or
  // variable compression ratio for each data type.
  final val PARQUET_COMPRESSION_RATIO = 1.4

  // Dummy partitioning because:
  // 1) The exact output partitioning is determined at query runtime
  // 2) optimizeWrite is always placed right after the top node(DeltaInvariantChecker),
  //    there is no parent plan to refer to outputPartitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(partitioning.numPartitions)

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }


  @transient lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      partitioning,
      serializer,
      writeMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // Collect execution statistics, these will be used to adjust/decide how to split files
    val stats = ThreadUtils.awaitResult(mapOutputStatisticsFuture, Duration.Inf)
    if (stats == null) {
      new ShuffledRowRDD(shuffleDependency, readMetrics)
    } else {
      try {
        val partitionSpecs = Some(rebalancePartitions(stats))
        new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs.get.toArray)
      } catch {
        case e: Throwable =>
          logWarning("Failed to apply OptimizeWrite.", e)
          new ShuffledRowRDD(shuffleDependency, readMetrics)
      }
    }
  }

  private def rebalancePartitions(stats: MapOutputStatistics): Seq[ShufflePartitionSpec] = {
    val binSize = Option(sparkContext.getLocalProperty(
      DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.key))
      .map(_.toLong)
      .getOrElse(DeltaSQLConf.OPTIMIZE_WRITE_BIN_SIZE.defaultValue.get)
    val smallPartitionFactor = Option(sparkContext.getLocalProperty(
      DeltaSQLConf.OPTIMIZE_WRITE_SMALL_PARTITION_FACTOR.key))
      .map(_.toDouble)
      .getOrElse(DeltaSQLConf.OPTIMIZE_WRITE_SMALL_PARTITION_FACTOR.defaultValue.get)
    val mergedPartitionFactor = Option(sparkContext.getLocalProperty(
      DeltaSQLConf.OPTIMIZE_WRITE_MERGED_PARTITION_FACTOR.key))
      .map(_.toDouble)
      .getOrElse(DeltaSQLConf.OPTIMIZE_WRITE_MERGED_PARTITION_FACTOR.defaultValue.get)
    val bytesByPartitionId = stats.bytesByPartitionId
    val targetPartitionSize = (binSize * PARQUET_COMPRESSION_RATIO).toLong

    val splitPartitions = if (partitioning.isInstanceOf[RoundRobinPartitioning]) {
      DeltaShufflePartitionsUtil.splitSizeListByTargetSize(
        bytesByPartitionId,
        targetPartitionSize,
        smallPartitionFactor,
        mergedPartitionFactor)
    } else {
      // For partitioned data, do not coalesce small partitions as it will hurt parallelism.
      // Eg. a partition containing 100 partition keys => a task will write 100 files.
      Seq.range(0, bytesByPartitionId.length).toArray
    }

    def optimizeSkewedPartition(reduceIndex: Int): Seq[ShufflePartitionSpec] = {
      val partitionSize = bytesByPartitionId(reduceIndex)
      if (partitionSize > targetPartitionSize) {
        val shuffleId = shuffleDependency.shuffleId
        val newPartitionSpec = DeltaShufflePartitionsUtil.createSkewPartitionSpecs(
          shuffleId,
          reduceIndex,
          targetPartitionSize,
          smallPartitionFactor,
          mergedPartitionFactor)

        if (newPartitionSpec.isEmpty) {
          CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
        } else {
          logDebug(s"[OptimizeWrite] Partition $reduceIndex is skew, " +
            s"split it into ${newPartitionSpec.get.size} parts.")
          newPartitionSpec.get
        }
      } else if (partitionSize > 0) {
        CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
      } else {
        Nil
      }
    }

    // Transform the partitions to the ranges.
    // e.g. [0, 3, 6, 7, 10] -> [[0, 3), [3, 6), [6, 7), [7, 10)]
    (splitPartitions :+ stats.bytesByPartitionId.length).sliding(2).flatMap { k =>
      if (k.head == k.last - 1) {
        // If not a merged partition, split it if needed.
        optimizeSkewedPartition(k.head)
      } else {
        CoalescedPartitionSpec(k.head, k.last) :: Nil
      }
    }.toList
  }

  override protected def withNewChildInternal(newChild: SparkPlan): OptimizeWriteExchangeExec = {
    copy(child = newChild)
  }
}
