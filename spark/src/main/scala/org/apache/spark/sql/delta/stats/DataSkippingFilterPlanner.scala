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

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaTableUtils.{containsSubquery, isPredicatePartitionColumnsOnly}
import org.apache.spark.sql.delta.RowCommitVersion.MetadataAttribute
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import org.apache.spark.sql.types.StructType

/**
 * Plans data-filter usage for data skipping.
 *
 * Owns the full filter classification and rewrite pipeline:
 *   1. Split filters into ineligible / eligible
 *   2. Split eligible into partition / data
 *   3. Build stats-based skipping predicates via [[DataFiltersBuilder]]
 *   4. Optionally rewrite unused filters as partition-like
 *   5. Merge into final skipping predicate
 *
 * V1 and V2 share [[DefaultDataSkippingFilterPlanner]]; differences are
 * captured through the injected [[DataFiltersBuilder]] instance.
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
 *
 * All filter-related logic is managed here. The caller (DataSkippingReader)
 * only needs to pass raw filters and consume the [[Result]].
 *
 * The planner internally constructs a [[DataFiltersBuilder]] from the
 * injected dependencies -- the caller never touches the builder directly.
 *
 * @param spark              SparkSession (for config access)
 * @param dataSkippingType   The type of data skipping being performed
 * @param getStatsColumnOpt  Resolves a StatsColumn to a Column expression
 * @param constructNotNullFilter Builds IsNotNull skipping predicate
 * @param partitionColumns   Partition column names for split classification
 * @param useStats           Whether data skipping is enabled
 * @param clusteringColumns  Clustering column names (for partition-like rewrite)
 * @param protocol           Table protocol (for partition-like eligibility)
 * @param numOfFilesIfKnown  Number of files (for partition-like threshold)
 */
private[delta] class DefaultDataSkippingFilterPlanner(
    spark: SparkSession,
    dataSkippingType: DeltaDataSkippingType,
    getStatsColumnOpt: StatsColumn => Option[Column],
    constructNotNullFilter: (StatsProvider, Seq[String]) => Option[DataSkippingPredicate],
    partitionColumns: Seq[String],
    useStats: Boolean,
    clusteringColumns: Seq[String] = Nil,
    protocol: Option[org.apache.spark.sql.delta.actions.Protocol] = None,
    numOfFilesIfKnown: Option[Long] = None)
  extends DataSkippingFilterPlanner {
  import DataSkippingFilterPlanner._

  private val truePredicate = DataSkippingPredicate(
    org.apache.spark.sql.Column(TrueLiteral))

  private lazy val builder: DataFiltersBuilder = new DataFiltersBuilder(
    spark = spark,
    dataSkippingType = dataSkippingType,
    getStatsColumnOpt = getStatsColumnOpt,
    constructNotNullFilter = constructNotNullFilter,
    limitPartitionLikeFiltersToClusteringColumns =
      spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_CLUSTERING_COLUMNS_ONLY),
    additionalPartitionLikeFilterSupportedExpressions =
      spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ADDITIONAL_SUPPORTED_EXPRESSIONS)
        .toSet.flatMap((exprs: String) => exprs.split(",")))

  override def plan(filters: Seq[Expression]): Result = {
    // Step 1: Split ineligible (subquery, non-deterministic, metadata)
    val (ineligibleFilters, eligibleFilters) = filters.partition { f =>
      containsSubquery(f) || !f.deterministic || f.exists {
        case MetadataAttribute(_) => true
        case _ => false
      }
    }

    // Step 2: Split partition vs data
    val (partitionFilters, dataFilters) = eligibleFilters.partition(
      isPredicatePartitionColumnsOnly(_, partitionColumns, spark))

    // Step 3: Build stats-based skipping predicates
    var (skippingFilters, unusedFilters) = if (useStats) {
      dataFilters
        .map(f => (f, builder(f)))
        .partition(_._2.isDefined)
    } else {
      (Nil, dataFilters.map(f => (f, None: Option[DataSkippingPredicate])))
    }

    // Step 4: Partition-like rewrite for clustered tables
    val canRewriteAsPartitionLike =
      spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ENABLED) &&
        protocol.exists(ClusteredTableUtils.isSupported) &&
        numOfFilesIfKnown.exists(_ >=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_THRESHOLD)) &&
        unusedFilters.nonEmpty

    val partitionLikeFilters = if (canRewriteAsPartitionLike) {
      val (rewrittenUsedFilters, rewrittenUnusedFilters) = unusedFilters
        .map { case (expr, _) =>
          (expr, builder.rewriteDataFiltersAsPartitionLike(clusteringColumns, expr))
        }
        .partition(_._2.isDefined)
      skippingFilters = skippingFilters ++ rewrittenUsedFilters
      unusedFilters = rewrittenUnusedFilters
      rewrittenUsedFilters.map { case (orig, rewrittenOpt) => (orig, rewrittenOpt.get) }
    } else {
      Nil
    }

    // Step 5: Merge into final skipping predicate
    val finalSkippingFilter = skippingFilters
      .map(_._2.get)
      .reduceOption((skip1, skip2) => DataSkippingPredicate(
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
