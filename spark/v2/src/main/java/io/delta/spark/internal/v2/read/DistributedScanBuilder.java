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
import org.apache.spark.sql.sources.Filter;

/**
 * Kernel-compatible {@link ScanBuilder} for batch reads, orchestrating the 3-component architecture
 * via interfaces.
 *
 * <p>Follows the Builder pattern (Effective Java Item 2): collects configuration incrementally via
 * fluent setters, then delegates to the three components at {@link #build()} time:
 *
 * <ol>
 *   <li>{@link ScanPlanner} (Component 1) — reads log segments, builds DataFrame, applies
 *       predicates.
 *   <li>{@link ScanPredicateBuilder} (Component 2) — rewrites pushdown filters into partition
 *       pruning and data skipping expressions. Used internally by the Planner.
 *   <li>{@link ScanExecutor} (Component 3) — executes the planned DataFrame and returns Kernel
 *       rows.
 * </ol>
 *
 * <p>For streaming, use {@link DistributedScan} directly with a pre-built DataFrame — the Planner
 * and PredicateBuilder are not needed since streaming constructs its own sorted DataFrame.
 */
public class DistributedScanBuilder implements ScanBuilder {

  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;

  // Mutable builder state — set via fluent methods before build()
  private StructType readSchema;
  private Filter[] pushdownFilters;
  private org.apache.spark.sql.types.StructType partitionSchema;

  /**
   * Creates a ScanBuilder for batch reads. The {@link ScanPlanner} will read log segments and build
   * the DataFrame at {@link #build()} time.
   */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = requireNonNull(spark, "spark");
    this.snapshot = requireNonNull(snapshot, "snapshot");
    this.numPartitions = numPartitions;
    this.readSchema = snapshot.getSchema();
    this.pushdownFilters = new Filter[0];
    this.partitionSchema = new org.apache.spark.sql.types.StructType();
  }

  /**
   * Set pushdown filters (partition + data) for the Planner to apply.
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
    // Filtering is done via Planner + PredicateBuilder through withPushdownFilters().
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    this.readSchema = requireNonNull(readSchema, "readSchema");
    return this;
  }

  /**
   * Builds the scan by orchestrating the 3 components:
   *
   * <ol>
   *   <li>Planner reads log segments and applies predicates (via PredicateBuilder internally)
   *   <li>DistributedScan wraps ScanExecutor for Kernel consumption
   * </ol>
   */
  @Override
  public Scan build() {
    // Component 1: Planner (via interface) — log replay + predicate application
    ScanPlanner planner = DistributedScanPlanner.create(spark, snapshot, numPartitions);
    Dataset<org.apache.spark.sql.Row> plannedDF = planner.plan(pushdownFilters, partitionSchema);

    // DistributedScan wraps ScanExecutor (Component 3) + Kernel delegate
    return new DistributedScan(plannedDF, snapshot, readSchema);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed log replay");
  }
}
