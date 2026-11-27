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
import org.apache.spark.sql.delta.actions.{AddFile, Protocol}
import org.apache.spark.sql.delta.amt.AMTUtils
import org.apache.spark.sql.delta.stats.DeltaStatistics.{TIGHT_BOUNDS, TIGHT_STATS}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode

import org.apache.spark.SparkException
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.coalesce

/**
 * Helpers for the two file-level tightness flags of Delta file statistics.
 *
 *  - [[TIGHT_BOUNDS]] (legacy): whether the stats are tight or wide.
 *  - [[TIGHT_STATS]]: the same, for the stats of tables with
 *    [[AdaptiveMetadataTableFeature]] (`adaptiveMetadata`), whose writers emit it instead
 *    of `tightBounds`. Tables with the preview AMT feature keep `tightBounds`.
 *
 * Older files keep `tightBounds`. A `true` flag means the stats are tight and a `false` flag that
 * they may be wide. A file's effective `tightBounds` is `coalesce(tightBounds, tightStats)`.
 *
 * Stats collected from scratch, by a writer or by a stats recomputation such as ANALYZE (from the
 * rows or from the Parquet footers), get the flag of the table: `tightStats` where the table's
 * stats schema has it, and `tightBounds` otherwise. A rewrite that only changes the tightness of
 * a file's existing stats (such as deletion-vector widening) changes the flags those stats already
 * have instead, and widening sets all of them to false, so a `true` flag is never stale. Existing
 * stats without `tightStats` must not gain it, because a follow-up change makes `tightStats` also
 * mean that timestamp stats keep microsecond precision, which legacy stats lack.
 */
object StatsTightness {

  /** The two tightness flags, in the order writers put them in the stats JSON. */
  val TIGHTNESS_FLAGS: Seq[String] = Seq(TIGHT_BOUNDS, TIGHT_STATS)

  /** Whether the stats schema of a table with `protocol` has a [[TIGHT_STATS]] field. */
  def tightStatsSupported(protocol: Protocol): Boolean =
    AMTUtils.amtSupported(protocol, isPreviewCompatible = false)

  /** A file's effective tight-bounds flag; see the object comment. */
  def effectiveTightBounds(tightBounds: Column, tightStats: Column): Column =
    coalesce(tightBounds, tightStats)

  /**
   * Whether the parsed stats JSON `node` is tight: its effective tight-bounds flag (see the object
   * comment), or true if it has neither flag, since an absent `tightBounds` means tight.
   */
  def isTight(node: JsonNode): Boolean =
    flag(node, TIGHT_BOUNDS).orElse(flag(node, TIGHT_STATS)).forall(_.asBoolean(true))

  /**
   * Throws if the stats of `add`, committed to a table with `protocol`, break a rule of the object
   * comment: only tables whose stats schema has `tightStats` may have files with it, and widening
   * sets every flag to false, so the writers here never leave `tightStats = true` next to
   * `tightBounds = false`. Commits run this check in tests. Stats that aren't valid JSON are left
   * to the checks that read them.
   */
  def checkTightnessFlags(add: AddFile, protocol: Protocol): Unit = {
    // Stats without a `tightStats` key can't break either rule, so skip parsing them.
    if (add.stats == null || !add.stats.contains(TIGHT_STATS)) return
    val node = try JsonUtils.mapper.readTree(add.stats) catch {
      case _: JsonProcessingException => return
    }
    if (node == null || !node.isObject || flag(node, TIGHT_STATS).isEmpty) return
    // Readers of other tables ignore `tightStats`, so a file with only that flag would read as
    // tight there.
    if (!tightStatsSupported(protocol)) {
      throw SparkException.internalError(
        s"AddFile '${add.path}' has $TIGHT_STATS, but its table doesn't support " +
          s"${AdaptiveMetadataTableFeature.name}: ${add.stats}")
    }
    val tightStats = flag(node, TIGHT_STATS).exists(_.asBoolean(false))
    val wideBounds = flag(node, TIGHT_BOUNDS).exists(!_.asBoolean(true))
    if (tightStats && wideBounds) {
      throw SparkException.internalError(
        s"AddFile '${add.path}' has $TIGHT_STATS = true but $TIGHT_BOUNDS = false: a rewrite " +
          s"that widened its stats must set $TIGHT_STATS to false too: ${add.stats}")
    }
  }

  private def flag(node: JsonNode, name: String): Option[JsonNode] =
    Option(node.get(name)).filterNot(_.isNull)
}
