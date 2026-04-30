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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage._

/**
 * :: DeveloperApi ::
 * Extension point for external shuffle services (e.g., Celeborn, Uniffle, Membrain) to customize
 * how Delta's optimized writer computes bin-packing.
 *
 * The default implementation bin-packs whole reducers as atomic units. External shuffle services
 * can provide their own implementation via
 * `spark.databricks.delta.optimizeWrite.shuffleHelper` to leverage service-specific optimizations
 * such as locality-aware bin-packing.
 *
 * Shuffle reads on executors are always delegated to `ShuffleManager.getReader()`, which is
 * Spark's standard extension point for custom shuffle read behavior. This trait only controls
 * the driver-side bin-packing strategy.
 *
 * Implementations must have a no-arg constructor.
 */
@DeveloperApi
trait OptimizedWriteShuffleHelper {

  /**
   * Bin-pack shuffle blocks into groups, where each group will be written as a single output file.
   * Called on the driver.
   *
   * Each bin must contain complete reducers (all blocks for a given reducer ID must be in the
   * same bin). Splitting a reducer across bins will cause data duplication because
   * `ShuffleManager.getReader()` reads entire reducer partitions.
   *
   * @param shuffleStats the shuffle output statistics: (BlockManagerId, blocks) pairs where
   *                     each block is (BlockId, size, mapIndex)
   * @param maxBinSize the maximum size in bytes for each bin
   * @return bins of block IDs, where each bin becomes one output file
   */
  def computeBins(
      shuffleStats: Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      maxBinSize: Long): Seq[Seq[BlockId]]
}
