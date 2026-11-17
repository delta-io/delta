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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.AdaptiveMetadataTableFeature
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.stats.DeltaStatistics.{TIGHT_BOUNDS, TIGHT_STATS}


/**
 * Helpers for the two file-level tightness flags of Delta file statistics.
 *
 *  - [[TIGHT_BOUNDS]] (legacy): whether the stats are tight or wide.
 *  - [[TIGHT_STATS]]: the same, for the stats of tables with
 *    [[AdaptiveMetadataTableFeature]] (`adaptiveMetadata`), whose writers emit it instead
 *    of `tightBounds`. Tables with the preview AMT feature keep `tightBounds`.
 *
 * Older files keep `tightBounds`, and a file converted through an AMT checkpoint may carry both.
 * `tightStats = true` implies `tightBounds = true`, so a file's effective `tightBounds` is
 * `coalesce(tightBounds, tightStats)`.
 *
 * Stats collected from scratch, by a writer or by a stats recomputation such as ANALYZE (from the
 * rows or from the Parquet footers), get the flag of the table: `tightStats` where the table's
 * stats schema has it, and `tightBounds` otherwise. A rewrite that only changes the tightness of
 * a file's existing stats (such as deletion-vector widening) changes the flags those stats already
 * have instead. Existing stats without `tightStats` must not gain it, because a follow-up change
 * makes `tightStats` also mean that timestamp stats keep microsecond precision, which legacy stats
 * lack.
 */
object StatsTightness {

  /** Whether the stats schema of a table with `protocol` has a [[TIGHT_STATS]] field. */
  def tightStatsSupported(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(AdaptiveMetadataTableFeature)

}
