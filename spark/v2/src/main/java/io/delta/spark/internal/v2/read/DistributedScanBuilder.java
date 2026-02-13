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
package io.delta.spark.internal.v2.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.delta.stats.DataFiltersBuilderUtils;
import org.apache.spark.sql.delta.stats.DataFiltersBuilderV2;
import org.apache.spark.sql.delta.stats.DefaultScanPlanner;
import org.apache.spark.sql.delta.stats.DefaultScanPredicateBuilder;
import org.apache.spark.sql.delta.stats.DeltaStateProvider;
import org.apache.spark.sql.sources.Filter;

/**
 * Kernel-compatible {@link ScanBuilder} for batch reads.
 *
 * <p>Follows the Builder pattern (Effective Java Item 2): collects configuration incrementally via
 * fluent setters, then orchestrates the scan at {@link #build()} time using the shared {@link
 * DefaultScanPlanner} and {@link DefaultScanPredicateBuilder} (shared with V1).
 *
 * <p>V2-specific concerns handled at build time:
 *
 * <ul>
 *   <li>Data source: distributed log replay via {@link DistributedLogReplayHelper}
 *   <li>Envelope: unwrap/re-wrap {@code {add: struct}}, stats JSON parse/serialize
 *   <li>Filter conversion: Spark {@code Filter[]} to resolved Catalyst Expressions
 * </ul>
 *
 * <p>For streaming, use {@link DistributedScan} directly with a pre-built DataFrame.
 */
public class DistributedScanBuilder implements ScanBuilder {

  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;

  // Mutable builder state -- set via fluent methods before build()
  private StructType readSchema;
  private Filter[] pushdownFilters;
  private org.apache.spark.sql.types.StructType partitionSchema;

  /** Creates a ScanBuilder for batch reads. */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = requireNonNull(spark, "spark");
    this.snapshot = requireNonNull(snapshot, "snapshot");
    this.numPartitions = numPartitions;
    this.readSchema = snapshot.getSchema();
    this.pushdownFilters = new Filter[0];
    this.partitionSchema = new org.apache.spark.sql.types.StructType();
  }

  /**
   * Set pushdown filters (partition + data) for the scan to apply.
   *
   * @param filters all pushable Spark filters (non-null)
   * @param partitionSchema partition schema for rewriting (non-null)
   * @return this builder for chaining
   */
  public DistributedScanBuilder withPushdownFilters(
      Filter[] filters, org.apache.spark.sql.types.StructType partitionSchema) {
    this.pushdownFilters = requireNonNull(filters, "filters").clone(); // Item 50
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema");
    return this;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    // Phase 1: Kernel Predicates not used for data skipping.
    // Filtering is done via DefaultScanPlanner through withPushdownFilters().
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    this.readSchema = requireNonNull(readSchema, "readSchema");
    return this;
  }

  /**
   * Builds the scan using the shared {@link DefaultScanPlanner}:
   *
   * <ol>
   *   <li>Create V2 {@link DeltaStateProvider}: loadActions -> state reconstruction -> extract add
   *       -> parse stats (full pipeline, same class as V1)
   *   <li>Create shared {@link DefaultScanPlanner} backed by the data source
   *   <li>Create shared {@link DefaultScanPredicateBuilder} with V2-specific stat column paths
   *   <li>Convert Spark {@code Filter[]} to resolved Catalyst Expressions
   *   <li>Call {@code planner.plan(expressions, predicateBuilder)} -- shared pipeline
   *   <li>V2 post-processing: serialize stats JSON + re-wrap as {add: struct}
   * </ol>
   */
  @Override
  public Scan build() {
    // Step 1: V2 data source (full pipeline: loadActions -> reconstruct -> extract -> parse stats)
    DeltaStateProvider v2StateProvider =
        DataFiltersBuilderV2.createDataSource(
            () -> DistributedLogReplayHelper.loadActionsV2(spark, snapshot),
            numPartitions,
            snapshot);

    // Step 2: Shared planner backed by the data source
    DefaultScanPlanner planner = DataFiltersBuilderV2.createPlanner(v2StateProvider);

    // Step 3: Shared predicate builder (V2-specific stat column paths injected)
    DefaultScanPredicateBuilder predicateBuilder =
        DataFiltersBuilderV2.createPredicateBuilder(snapshot, spark, partitionSchema);

    // Step 4: Convert Filter[] -> resolved Catalyst Expressions
    org.apache.spark.sql.types.StructType tableSchema =
        DataFiltersBuilderV2.getSparkTableSchema(snapshot);
    scala.collection.immutable.Seq<Expression> filterExprs =
        DataFiltersBuilderV2.filtersToResolvedExpressions(pushdownFilters, tableSchema).toList();

    // Step 5: Execute shared pipeline (partition pruning + data skipping + accumulators)
    DataFiltersBuilderUtils.ScanPipelineResult result =
        planner.plan(filterExprs, predicateBuilder, scala.Option.empty());

    // Step 6: V2 post-processing (serialize stats + re-wrap as {add: struct})
    Dataset<org.apache.spark.sql.Row> plannedDF =
        DataFiltersBuilderV2.postProcessV2(result.filteredDF());

    // DistributedScan wraps ScanExecutor + Kernel delegate
    return new DistributedScan(plannedDF, snapshot, readSchema);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed log replay");
  }
}
