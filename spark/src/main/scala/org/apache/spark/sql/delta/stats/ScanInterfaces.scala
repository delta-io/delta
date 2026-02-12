/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.{DeltaLogFileIndex, Snapshot, StateReconstructionUtils}
import org.apache.spark.sql.delta.actions.{AddFile, Protocol, SingleAction}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataFiltersBuilderUtils.ScanPipelineResult
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

// ===================== Data Source Interface + Default =====================

/**
 * Provides raw and parsed AddFile DataFrames for scan planning.
 *
 * Implementations own the full pipeline:
 *   raw source -> state reconstruction -> extract add files -> parse stats -> cache
 *
 * [[DefaultDataSource]] is the shared implementation used by both V1 and V2.
 */
private[delta] trait ScanDataSource {

  /** Flat AddFile rows with stats as a JSON string. */
  def allAddFiles(): DataFrame

  /**
   * AddFile rows with parsed stats columns, optionally cached.
   * This is the primary API consumed by [[DefaultScanPlanner]].
   */
  def withParsedStats: DataFrame
}

/**
 * Shared [[ScanDataSource]] implementation used by both V1 and V2.
 *
 * Owns the full pipeline from raw log actions to cached AddFile
 * DataFrames with parsed statistics:
 *
 *  1. State reconstruction: loadActions -> canonicalize -> repartition -> replay
 *  2. Cache full state (optional, V1 only)
 *  3. Extract AddFile rows
 *  4. Parse stats JSON into struct columns
 *  5. Cache parsed stats DF (optional, V1 only)
 *
 * V1/V2 differences are captured through constructor parameters:
 *
 *  - V1: passes `Snapshot.loadActions`, `getCanonicalPathUdf`,
 *    retention timestamps, and cache factories from [[StateCache]].
 *  - V2: passes `DistributedLogReplayHelper.loadActions`,
 *    `callUDF("canonicalizePath", _)`, no retention, no caching.
 *
 * V1's `Snapshot` owns a `DefaultDataSource` instance and delegates
 * `stateDS`, `stateDF`, `allFiles` to it. This eliminates the
 * separate `stateReconstruction` / `cachedState` chain in `Snapshot`.
 *
 * @param loadActions            Supplier for the union of checkpoint + delta
 *                               log files as a DataFrame. Must include
 *                               `commitVersion` and `add_stats_to_use` columns.
 * @param numPartitions          Number of Spark partitions for state
 *                               reconstruction (V1: config, V2: passed in)
 * @param canonicalizeUdf        UDF to canonicalize file paths
 *                               (V1: deltaLog.getCanonicalPathUdf,
 *                                V2: callUDF("canonicalizePath", _))
 * @param statsSchema            Schema for parsing the stats JSON column
 * @param minFileRetentionTimestamp
 *                               Tombstone expiry threshold (V1: from config,
 *                               V2: None -- keep all)
 * @param minSetTransactionRetentionTimestamp
 *                               Transaction expiry threshold (V1: from config,
 *                               V2: None)
 * @param stateCacheFactory      Optional factory to cache the full state
 *                               Dataset[SingleAction]. V1 passes
 *                               `ds => cacheDS(ds, name).getDS`;
 *                               V2 passes None.
 * @param statsParseCacheFactory Optional factory to cache the parsed stats
 *                               DataFrame. V1 passes
 *                               `df => cacheDS(df, name).getDS`;
 *                               V2 passes None.
 */
private[delta] class DefaultDataSource(
    loadActions: () => DataFrame,
    numPartitions: Int,
    canonicalizeUdf: Column => Column,
    statsSchema: StructType,
    minFileRetentionTimestamp: Option[Long] = None,
    minSetTransactionRetentionTimestamp: Option[Long] = None,
    stateCacheFactory: Option[Dataset[SingleAction] => Dataset[SingleAction]] = None,
    statsParseCacheFactory: Option[DataFrame => DataFrame] = None
) extends ScanDataSource {

  /**
   * Full state after log replay, optionally cached.
   *
   * Pipeline: loadActions() -> canonicalize paths -> repartition by path
   *   -> sort by version -> InMemoryLogReplay per partition -> optional cache
   *
   * For V1 this replaces `Snapshot.stateReconstruction` + `cachedState`.
   * For V2 this replaces `DistributedLogReplayHelper.stateReconstructionV2`.
   */
  lazy val stateDS: Dataset[SingleAction] = {
    val reconstructed = StateReconstructionUtils.canonicalizeRepartitionAndReconstruct(
      loadActions(),
      numPartitions,
      canonicalizeUdf,
      DeltaLogFileIndex.COMMIT_VERSION_COLUMN,
      Snapshot.ADD_STATS_TO_USE_COL_NAME)
    val replayed = StateReconstructionUtils.replayPerPartition(
      reconstructed,
      minFileRetentionTimestamp,
      minSetTransactionRetentionTimestamp)
    stateCacheFactory.map(_(replayed)).getOrElse(replayed)
  }

  /** DataFrame view of the full state. */
  lazy val stateDF: DataFrame = stateDS.toDF()

  /**
   * Extract flat AddFile rows from the reconstructed state.
   * Equivalent to V1's `Snapshot.allFilesViaStateReconstruction`.
   */
  override lazy val allAddFiles: DataFrame =
    stateDS.where("add IS NOT NULL")
      .select(col("add").as[AddFile])
      .toDF

  /**
   * AddFile rows with parsed statistics, optionally cached.
   *
   * Pipeline: allAddFiles -> from_json(stats, statsSchema) -> optional cache
   *
   * For V1 this replaces the old `withStatsInternal0` + `withStatsCache` chain.
   * For V2 this replaces `prepareV2DataSource` + inline stats parsing.
   */
  override lazy val withParsedStats: DataFrame = {
    val parsed = DataFiltersBuilderUtils.withParsedStats(allAddFiles, statsSchema)
    statsParseCacheFactory.map(_(parsed)).getOrElse(parsed)
  }
}

// ===================== Result Types =====================

/**
 * Result of predicate building, including both the pipeline result and
 * metadata about which filters were classified into each category.
 * This metadata is needed to construct [[DeltaScan]] with accurate filter sets.
 *
 * Shared between V1 and V2 connectors.
 */
private[delta] case class PredicateBuilderResult(
    pipelineResult: ScanPipelineResult,
    partitionFilterExprs: Seq[Expression],
    usedDataFilters: Seq[(Expression, Option[DataSkippingPredicate])],
    partitionLikeFilters: Seq[(Expression, DataSkippingPredicate)],
    unusedFilterExprs: Seq[(Expression, Option[DataSkippingPredicate])])

// ===================== Trait 1: ScanPredicateBuilder =====================

/**
 * Shared interface for building and applying predicate-based filtering
 * to an AddFile DataFrame.
 *
 * Implementations handle:
 *  1. Split filters into partition vs data
 *  2. Build data skipping predicates from data filters
 *  3. Rewrite partition filters to `partitionValues.*`
 *  4. (Optional) Partition-like rewrite for clustered tables
 *  5. Execute the scan pipeline with size accumulators
 */
private[delta] trait DeltaScanPredicateBuilder {

  /**
   * Apply partition pruning + data skipping to an AddFile DataFrame
   * with parsed stats.
   *
   * @param withStatsDF  Flat AddFile DataFrame with parsed stats struct
   * @param filters      Resolved, eligible filter Expressions
   * @return ScanPipelineResult (filtered DF + accumulators)
   */
  def apply(
      withStatsDF: DataFrame,
      filters: Seq[Expression]
  ): ScanPipelineResult
}

// ===================== Trait 2: ScanPlanner =====================

/**
 * Shared interface for orchestrating a complete scan pipeline.
 *
 * Responsibilities:
 *  1. Expose the parsed + cached AddFile DF via [[withParsedStats]]
 *  2. Delegate to [[DeltaScanPredicateBuilder]] for filtering
 */
private[delta] trait DeltaScanPlanner {

  /**
   * Returns the AddFile DataFrame with parsed stats (and optionally cached).
   * Delegates to the underlying [[ScanDataSource]].
   */
  def withParsedStats: DataFrame

  /**
   * Plan a scan: use [[withParsedStats]] DF, call PredicateBuilder, optional limit.
   *
   * @param filters            Resolved, eligible filter Expressions
   * @param predicateBuilder   The predicate builder for this scan
   * @param limit              Optional row limit for LIMIT pushdown
   * @return ScanPipelineResult (filtered DF + accumulators)
   */
  def plan(
      filters: Seq[Expression],
      predicateBuilder: DefaultScanPredicateBuilder,
      limit: Option[Long] = None
  ): ScanPipelineResult
}

// ===================== DefaultScanPredicateBuilder =====================

/**
 * Default shared implementation of [[DeltaScanPredicateBuilder]].
 *
 * Works for both V1 and V2 connectors. V1/V2 differences are captured
 * as constructor parameters -- no subclasses needed.
 *
 * '''Injected adapter functions (3):'''
 *  - `getStatColumn`: V1 uses column mapping (physical names),
 *    V2 uses simple paths (`col("stats.MIN.x")`).
 *  - `buildDataFilters`: V1 passes `DataFiltersBuilder.apply`;
 *    V2 passes `SharedDataFiltersBuilder.apply`.
 *  - `rewriteAsPartitionLike`: V1 passes
 *    `DataFiltersBuilder.rewriteDataFiltersAsPartitionLike`;
 *    V2 passes None (for now).
 *
 * '''Simple value parameters:'''
 *  - `numRecordsCol`, `partitionSchema`, `useStats`, `protocol`,
 *    `numOfFilesIfKnown`, `clusteringColumns`.
 *
 * '''Built-in logic:'''
 *  - Filter splitting (partition vs data)
 *  - Partition filter rewriting to `partitionValues.*`
 *  - Partition-like rewrite decision (config + protocol + threshold)
 *  - Scan pipeline execution with accumulators
 *
 * @param spark                  SparkSession
 * @param getStatColumn          Resolves StatsColumn to Column expression
 * @param numRecordsCol          Column for stats.numRecords
 * @param partitionSchema        Partition schema for filter rewriting
 * @param buildDataFilters       Converts Expression to DataSkippingPredicate
 * @param useStats               Whether data skipping is enabled
 * @param protocol               Table protocol (for partition-like eligibility)
 * @param numOfFilesIfKnown      Number of files (for partition-like threshold)
 * @param clusteringColumns      Clustering column names (for partition-like)
 * @param rewriteAsPartitionLike Rewrites an expression as partition-like
 */
private[delta] class DefaultScanPredicateBuilder(
    spark: SparkSession,
    getStatColumn: StatsColumn => Option[Column],
    numRecordsCol: Column,
    partitionSchema: StructType,
    buildDataFilters: Expression => Option[DataSkippingPredicate],
    useStats: Boolean = true,
    protocol: Option[Protocol] = None,
    numOfFilesIfKnown: Option[Long] = None,
    clusteringColumns: Seq[String] = Nil,
    rewriteAsPartitionLike: Option[
      (Seq[String], Expression) => Option[DataSkippingPredicate]] = None
) extends DeltaScanPredicateBuilder {

  override def apply(
      withStatsDF: DataFrame,
      filters: Seq[Expression]
  ): ScanPipelineResult = {
    applyWithMetadata(withStatsDF, filters).pipelineResult
  }

  /**
   * Full pipeline with metadata tracking.
   *
   * Performs:
   *  1. Split filters into partition vs data
   *  2. Build data skipping predicates
   *  3. Partition-like rewrite (built-in decision, injected rewrite fn)
   *  4. Rewrite partition filters to `partitionValues.*`
   *  5. Execute the shared scan pipeline with accumulators
   *
   * @param withStatsDF  Flat AddFile DataFrame with parsed stats
   * @param filters      All eligible filter expressions
   * @return PredicateBuilderResult with pipeline result + filter metadata
   */
  def applyWithMetadata(
      withStatsDF: DataFrame,
      filters: Seq[Expression]
  ): PredicateBuilderResult = {
    val partitionColumns = partitionSchema.fieldNames.toSeq
    val (partitionFilterExprs, dataFilterExprs) =
      DataFiltersBuilderUtils.splitFilters(filters, partitionColumns, spark)

    // Step 1: Build data skipping predicates
    var (skippingFilters, unusedFilters) = if (useStats) {
      dataFilterExprs.map(f => (f, buildDataFilters(f))).partition(_._2.isDefined)
    } else {
      (Nil, dataFilterExprs.map(f => (f, None: Option[DataSkippingPredicate])))
    }

    // Step 2: Partition-like rewrite (built-in decision logic)
    val shouldRewriteAsPartitionLike =
      spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ENABLED) &&
        protocol.exists(ClusteredTableUtils.isSupported) &&
        numOfFilesIfKnown.exists(_ >=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_THRESHOLD)) &&
        unusedFilters.nonEmpty &&
        rewriteAsPartitionLike.isDefined

    val partitionLikeFilters = if (shouldRewriteAsPartitionLike) {
      val rewriteFn = rewriteAsPartitionLike.get
      val (rewrittenUsedFilters, rewrittenUnusedFilters) =
        unusedFilters
          .map { case (expr, _) =>
            (expr, rewriteFn(clusteringColumns, expr))
          }
          .partition(_._2.isDefined)
      skippingFilters = skippingFilters ++ rewrittenUsedFilters
      unusedFilters = rewrittenUnusedFilters
      rewrittenUsedFilters.map { case (orig, rewrittenOpt) => (orig, rewrittenOpt.get) }
    } else {
      Nil
    }

    // Step 3: Combine all skipping predicates
    val finalSkippingFilters = skippingFilters
      .map(_._2.get)
      .reduceOption((skip1, skip2) => DataSkippingPredicate(
        skip1.expr && skip2.expr, skip1.referencedStats ++ skip2.referencedStats))
      .getOrElse(DataSkippingPredicate(lit(true)))

    // Step 4: Rewrite partition filters to partitionValues.*
    val rewrittenPartFilters = DataFiltersBuilderUtils.rewritePartitionFilters(
      partitionSchema,
      spark.sessionState.conf.resolver,
      partitionFilterExprs)
    val partFilterCol =
      DataFiltersBuilderUtils.buildPartitionFilterColumn(rewrittenPartFilters)

    // Step 5: Execute the shared scan pipeline
    val pipelineResult = DataFiltersBuilderUtils.executeScanPipeline(
      withStatsDF, partFilterCol, finalSkippingFilters,
      getStatColumn, numRecordsCol, spark)

    PredicateBuilderResult(
      pipelineResult,
      partitionFilterExprs,
      skippingFilters,
      partitionLikeFilters,
      unusedFilters)
  }
}

// ===================== DefaultScanPlanner =====================

/**
 * Default shared implementation of [[DeltaScanPlanner]].
 *
 * Works for both V1 and V2 connectors. Delegates to [[ScanDataSource]]
 * for the data pipeline (state reconstruction, stats parsing, caching).
 *
 * @param dataSource  Provides parsed + cached AddFile DataFrames
 */
private[delta] class DefaultScanPlanner(
    dataSource: ScanDataSource
) extends DeltaScanPlanner {

  /**
   * Parsed (and optionally cached) AddFile DataFrame.
   * Delegates to [[ScanDataSource.withParsedStats]].
   */
  override def withParsedStats: DataFrame = dataSource.withParsedStats

  override def plan(
      filters: Seq[Expression],
      predicateBuilder: DefaultScanPredicateBuilder,
      limit: Option[Long] = None
  ): ScanPipelineResult = {
    planWithMetadata(filters, predicateBuilder).pipelineResult
  }

  /**
   * Plan with full metadata, returning [[PredicateBuilderResult]].
   *
   * @param filters            Resolved, eligible filter Expressions
   * @param predicateBuilder   The predicate builder for this scan
   * @return PredicateBuilderResult with pipeline result + filter metadata
   */
  def planWithMetadata(
      filters: Seq[Expression],
      predicateBuilder: DefaultScanPredicateBuilder
  ): PredicateBuilderResult = {
    predicateBuilder.applyWithMetadata(withParsedStats, filters)
  }
}
