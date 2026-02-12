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

import io.delta.kernel.Snapshot;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * Distributed implementation of {@link ScanPlanner}.
 *
 * <p>Reads log segments from {@code _delta_log/} via {@link DistributedLogReplayHelper}, applies
 * distributed state reconstruction (shared with V1 via {@code StateReconstructionUtils}), then
 * delegates predicate rewriting to a {@link ScanPredicateBuilder}.
 *
 * <h3>Effective Java design (Items 1, 17, 19, 49, 64):</h3>
 *
 * <ul>
 *   <li>Implements {@link ScanPlanner} interface (Item 64).
 *   <li>Immutable — all fields {@code final}; no setters (Item 17).
 *   <li>Static factory {@link #create} instead of public constructor (Item 1).
 *   <li>{@code final} class — prohibits sub-classing (Item 19).
 * </ul>
 */
public final class DistributedScanPlanner implements ScanPlanner {

  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;

  /**
   * Creates a new DistributedScanPlanner (Item 1: static factory).
   *
   * @param spark active Spark session (non-null)
   * @param snapshot kernel snapshot providing log segment and table schema (non-null)
   * @param numPartitions number of Spark partitions for distributed log replay
   * @return immutable planner instance
   * @throws NullPointerException if spark or snapshot is null
   */
  public static DistributedScanPlanner create(
      SparkSession spark, Snapshot snapshot, int numPartitions) {
    return new DistributedScanPlanner(spark, snapshot, numPartitions);
  }

  private DistributedScanPlanner(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = requireNonNull(spark, "spark"); // Item 49
    this.snapshot = requireNonNull(snapshot, "snapshot");
    this.numPartitions = numPartitions;
  }

  /**
   * Plans a batch scan: reads log segments, then applies pushdown predicates.
   *
   * <ol>
   *   <li>Reads log segments and applies state reconstruction (distributed log replay)
   *   <li>Creates a {@link DeltaScanPredicateBuilder} to rewrite filters
   *   <li>Applies the rewritten predicates to the DataFrame
   * </ol>
   */
  @Override
  public Dataset<Row> plan(Filter[] pushdownFilters, StructType partitionSchema) {
    requireNonNull(pushdownFilters, "pushdownFilters"); // Item 49
    requireNonNull(partitionSchema, "partitionSchema");

    // Step 1: Read log segments -> build DataFrame via distributed log replay
    Dataset<Row> logReplayDF =
        DistributedLogReplayHelper.stateReconstructionV2(spark, snapshot, numPartitions);

    // Step 2+3: Rewrite predicates and apply to plan
    ScanPredicateBuilder predicateBuilder =
        DeltaScanPredicateBuilder.of(snapshot, spark, partitionSchema);
    return predicateBuilder.apply(logReplayDF, pushdownFilters);
  }

  @Override
  public String toString() {
    return String.format(
        "DistributedScanPlanner[snapshot=%s, numPartitions=%d]", snapshot.getPath(), numPartitions);
  }
}
