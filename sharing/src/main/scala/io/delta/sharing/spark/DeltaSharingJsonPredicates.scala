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

package io.delta.sharing.spark

import io.delta.sharing.client.util.{ConfUtils, JsonUtils}
import io.delta.sharing.filters.{AndOp, BaseOp, OpConverter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

/**
 * Converts Catalyst filter expressions into the JSON predicate hint the Delta Sharing server uses
 * for server-side file skipping. Extracted from the logic previously inlined in
 * [[DeltaSharingFileIndex]] so it can be shared: the V1 path calls [[convert]] directly.
 *
 * The hint is best-effort: the server is not required to apply it, so callers must still evaluate
 * every filter after the scan. Consequently any conversion failure (an unsupported filter, a bad
 * expression) degrades to [[None]] rather than erroring -- a missing hint only forgoes server-side
 * skipping, never correctness.
 */
object DeltaSharingJsonPredicates extends Logging {

  /**
   * Convert partition and data filter expressions to a json predicate:
   *   - the whole conversion is off unless `jsonPredicateHints.enabled` (default on);
   *   - data filters are converted only under `jsonPredicateV2Hints.enabled`.
   * Partition and data predicates are AND-ed. A conversion error on either side drops just that
   * side (logged, not thrown).
   *
   * @param partitionFilters filters referencing only partition columns.
   * @param dataFilters      filters referencing at least one non-partition column.
   * @param conf             session conf supplying the two feature gates.
   * @return the serialized json predicate, or None when disabled or nothing converted.
   */
  def convert(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      conf: SQLConf): Option[String] = {
    if (!ConfUtils.jsonPredicatesEnabled(conf)) {
      return None
    }

    // Convert the partition filters.
    val partitionOp = try {
      OpConverter.convert(partitionFilters)
    } catch {
      case e: Exception =>
        logError("Error while converting partition filters: " + e)
        None
    }

    // If V2 predicates are enabled, also convert the data filters.
    val dataOp = try {
      if (ConfUtils.jsonPredicatesV2Enabled(conf)) {
        logInfo("Converting data filters")
        OpConverter.convert(dataFilters)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError("Error while converting data filters: " + e)
        None
    }

    // Combine partition and data filters using an AND operation.
    val combinedOp = if (partitionOp.isDefined && dataOp.isDefined) {
      Some(AndOp(Seq(partitionOp.get, dataOp.get)))
    } else if (partitionOp.isDefined) {
      partitionOp
    } else {
      dataOp
    }
    logInfo("Using combined predicate: " + combinedOp)

    if (combinedOp.isDefined) {
      Some(JsonUtils.toJson[BaseOp](combinedOp.get))
    } else {
      None
    }
  }
}
