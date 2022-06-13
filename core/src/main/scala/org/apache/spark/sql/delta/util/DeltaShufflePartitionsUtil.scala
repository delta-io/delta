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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}
import org.apache.spark.sql.execution.{CoalesceExec, PartialReducerPartitionSpec, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{MapOutputTrackerMaster, SparkEnv}

import scala.collection.mutable.ArrayBuffer

object DeltaShufflePartitionsUtil {

  // scalastyle:off line.size.limit
  /**
   * Splits the skewed partition based on the map size and the target partition size
   * after split, and create a list of `PartialMapperPartitionSpec`. Returns None if can't split.
   *
   * The function is copied from Spark 3.2:
   *   https://github.com/apache/spark/blob/v3.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/ShufflePartitionsUtil.scala#L376
   * EDIT: Configurable smallPartitionFactor and mergedPartitionFactor.
   */
  // scalastyle:on
  def createSkewPartitionSpecs(
      shuffleId: Int,
      reducerId: Int,
      targetSize: Long,
      smallPartitionFactor: Double,
      mergedPartitionFactor: Double): Option[Seq[PartialReducerPartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    if (mapPartitionSizes.exists(_ < 0)) return None
    val mapStartIndices = splitSizeListByTargetSize(
      mapPartitionSizes,
      targetSize,
      smallPartitionFactor,
      mergedPartitionFactor)
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        val dataSize = startMapIndex.until(endMapIndex).map(mapPartitionSizes(_)).sum
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Given a list of size, return an array of indices to split the list into multiple partitions,
   * so that the size sum of each partition is close to the target size. Each index indicates the
   * start of a partition.
   *
   * The function is copied from Spark 3.2:
   *   https://github.com/apache/spark/blob/v3.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/ShufflePartitionsUtil.scala#L319
   * EDIT: Configurable smallPartitionFactor and mergedPartitionFactor.
   */
  // scalastyle:on
  // Visible for testing
  private[sql] def splitSizeListByTargetSize(
    sizes: Seq[Long],
    targetSize: Long,
    smallPartitionFactor: Double,
    mergedPartitionFactor: Double): Array[Int] = {
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var currentPartitionSize = 0L
    var lastPartitionSize = -1L

    def tryMergePartitions() = {
      // When we are going to start a new partition, it's possible that the current partition or
      // the previous partition is very small and it's better to merge the current partition into
      // the previous partition.
      val shouldMergePartitions = lastPartitionSize > -1 &&
        ((currentPartitionSize + lastPartitionSize) < targetSize * mergedPartitionFactor ||
          (currentPartitionSize < targetSize * smallPartitionFactor ||
            lastPartitionSize < targetSize * smallPartitionFactor))
      if (shouldMergePartitions) {
        // We decide to merge the current partition into the previous one, so the start index of
        // the current partition should be removed.
        partitionStartIndices.remove(partitionStartIndices.length - 1)
        lastPartitionSize += currentPartitionSize
      } else {
        lastPartitionSize = currentPartitionSize
      }
    }

    while (i < sizes.length) {
      // If including the next size in the current partition exceeds the target size, package the
      // current partition and start a new partition.
      if (i > 0 && currentPartitionSize + sizes(i) > targetSize) {
        tryMergePartitions()
        partitionStartIndices += i
        currentPartitionSize = sizes(i)
      } else {
        currentPartitionSize += sizes(i)
      }
      i += 1
    }
    tryMergePartitions()
    partitionStartIndices.toArray
  }

  // scalastyle:off line.size.limit
  /**
   * Get the map size of the specific shuffle and reduce ID. Note that, some map outputs can be
   * missing due to issues like executor lost. The size will be -1 for missing map outputs and the
   * caller side should take care of it.
   *
   * The function is copied from Spark 3.2:
   *   https://github.com/apache/spark/blob/v3.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/ShufflePartitionsUtil.scala#L365
   */
  // scalastyle:on
  private[sql] def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).withMapStatuses(_.map { stat =>
      if (stat == null) -1 else stat.getSizeForBlock(partitionId)
    })
  }

  private[sql] def removeTopRepartition(plan: SparkPlan): SparkPlan = {
    plan match {
      case p@AdaptiveSparkPlanExec(inputPlan: ShuffleExchangeExec, _, _, _, _)
        if !inputPlan.shuffleOrigin.equals(ENSURE_REQUIREMENTS) =>
        p.copy(inputPlan = inputPlan.child)
      case ShuffleExchangeExec(_, child, shuffleOrigin)
        if !shuffleOrigin.equals(ENSURE_REQUIREMENTS) =>
        child
      case AdaptiveSparkPlanExec(inputPlan: CoalesceExec, _, _, _, _) =>
        inputPlan.child
      case CoalesceExec(_, child) =>
        child
      case _ =>
        plan
    }
  }

  private[sql] def partitioningForRebalance(
      outputColumns: Seq[Attribute],
      partitionSchema: StructType,
      numShufflePartitions: Int): Partitioning = {
    if (partitionSchema.fields.isEmpty) {
      RoundRobinPartitioning(numShufflePartitions)
    } else {
      val partitionColumnsExpr = partitionSchema.fields.map { f =>
        outputColumns.find(c => c.name.equals(f.name)).get
      }
      HashPartitioning(partitionColumnsExpr, numShufflePartitions)
    }
  }

}
