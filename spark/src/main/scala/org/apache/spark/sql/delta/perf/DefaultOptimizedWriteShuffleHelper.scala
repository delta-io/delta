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

import org.apache.spark.sql.delta.util.BinPackingUtils

import org.apache.spark.storage._

/**
 * Default implementation of [[OptimizedWriteShuffleHelper]] for external shuffle managers.
 *
 * Bin-packs whole reducers as atomic units, since `ShuffleManager.getReader()` reads entire
 * reducer partitions and splitting a reducer across bins would cause data duplication.
 */
class DefaultOptimizedWriteShuffleHelper extends OptimizedWriteShuffleHelper {

  override def computeBins(
      shuffleStats: Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      maxBinSize: Long): Seq[Seq[BlockId]] = {
    val blocksByReducer = shuffleStats.toSeq.flatMap(_._2)
      .groupBy(_._1.asInstanceOf[ShuffleBlockId].reduceId)

    case class ReducerData(reducerId: Int, totalSize: Long,
        blocks: Seq[(BlockId, Long, Int)])
    val reducerDataSeq = blocksByReducer.map { case (reducerId, blocks) =>
      ReducerData(reducerId, blocks.map(_._2).sum, blocks)
    }.toSeq

    BinPackingUtils.binPackBySize[ReducerData, ReducerData](
      reducerDataSeq,
      _.totalSize,
      identity,
      maxBinSize).map(_.flatMap(_.blocks.map(_._1)))
  }
}
