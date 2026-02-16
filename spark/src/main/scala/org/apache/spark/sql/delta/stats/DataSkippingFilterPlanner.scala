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

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Plans data-filter usage for data skipping:
 *   - builds stats-based skipping filters
 *   - optionally rewrites unused filters as partition-like
 *   - returns used/unused filter sets and the final merged skipping predicate
 */
private[delta] trait DataSkippingFilterPlanner {
  def plan(filters: Seq[Expression]): DataSkippingFilterPlanner.Result
}

private[delta] object DataSkippingFilterPlanner {
  case class Result(
      ineligibleFilters: Seq[Expression],
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      skippingFilters: Seq[(Expression, Option[DataSkippingPredicate])],
      partitionLikeFilters: Seq[(Expression, DataSkippingPredicate)],
      unusedFilters: Seq[(Expression, Option[DataSkippingPredicate])],
      finalSkippingFilter: DataSkippingPredicate)
}

/**
 * Default implementation shared by V1 and V2.
 */
private[delta] class DefaultDataSkippingFilterPlanner(
    useStats: Boolean,
    isIneligible: Expression => Boolean,
    isPartitionOnly: Expression => Boolean,
    truePredicate: DataSkippingPredicate,
    buildDataFilter: Expression => Option[DataSkippingPredicate],
    canRewriteAsPartitionLike: Boolean,
    rewriteAsPartitionLike: Expression => Option[DataSkippingPredicate])
  extends DataSkippingFilterPlanner {
  import DataSkippingFilterPlanner._

  override def plan(filters: Seq[Expression]): Result = {
    val (ineligibleFilters, eligibleFilters) = filters.partition(isIneligible)
    val (partitionFilters, dataFilters) = eligibleFilters.partition(isPartitionOnly)

    var (skippingFilters, unusedFilters) = if (useStats) {
      dataFilters
        .map(f => (f, buildDataFilter(f)))
        .partition(_._2.isDefined)
    } else {
      (Nil, dataFilters.map(f => (f, None: Option[DataSkippingPredicate])))
    }

    val partitionLikeFilters = if (canRewriteAsPartitionLike && unusedFilters.nonEmpty) {
      val (rewrittenUsedFilters, rewrittenUnusedFilters) = unusedFilters
        .map { case (expr, _) =>
          (expr, rewriteAsPartitionLike(expr))
        }
        .partition(_._2.isDefined)
      skippingFilters = skippingFilters ++ rewrittenUsedFilters
      unusedFilters = rewrittenUnusedFilters
      rewrittenUsedFilters.map { case (orig, rewrittenOpt) => (orig, rewrittenOpt.get) }
    } else {
      Nil
    }

    val finalSkippingFilter = skippingFilters
      .map(_._2.get)
      .reduceOption((skip1, skip2) => DataSkippingPredicate(
        // Fold filters into a conjunction, while unioning referenced stats.
        skip1.expr && skip2.expr, skip1.referencedStats ++ skip2.referencedStats))
      .getOrElse(truePredicate)

    Result(
      ineligibleFilters = ineligibleFilters,
      partitionFilters = partitionFilters,
      dataFilters = dataFilters,
      skippingFilters = skippingFilters,
      partitionLikeFilters = partitionLikeFilters,
      unusedFilters = unusedFilters,
      finalSkippingFilter = finalSkippingFilter)
  }
}

