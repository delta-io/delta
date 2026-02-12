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
import org.apache.spark.sql.delta.stats.DataFiltersBuilderV2;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * Default implementation of {@link ScanPredicateBuilder}.
 *
 * <p>Converts pushdown predicates plus the kernel's stats provider into partition pruning and data
 * skipping Column expressions, then applies them to the Planner's DataFrame via {@code
 * df.filter()}.
 *
 * <h3>Responsibilities:</h3>
 *
 * <ul>
 *   <li>Split incoming filters into partition filters and data filters
 *   <li>Rewrite partition filters to reference {@code partitionValues.<colName>}
 *   <li>Convert data filters to stats-based predicates via V1's {@code constructDataFilters}
 *   <li>Apply both as DataFrame filters, including stats JSON parsing and {@code
 *       verifyStatsForFilter}
 * </ul>
 *
 * <h3>Composition (Effective Java Item 18):</h3>
 *
 * <p>This class composes the internal Scala bridge {@link DataFiltersBuilderV2} for operations
 * requiring Catalyst expression manipulation (pattern matching, {@code transformUp}, etc.). The
 * Java class owns the public contract; the Scala bridge is an implementation detail.
 *
 * <h3>Effective Java design (Items 1, 17, 18, 19, 49, 64):</h3>
 *
 * <ul>
 *   <li>Implements {@link ScanPredicateBuilder} interface (Item 64).
 *   <li>Immutable — all fields {@code final} (Item 17).
 *   <li>Static factory {@link #of} instead of public constructor (Item 1).
 *   <li>{@code final} class — prohibits sub-classing (Item 19).
 *   <li>Composition — delegates to Scala shared code (Item 18).
 * </ul>
 */
public final class DeltaScanPredicateBuilder implements ScanPredicateBuilder {

  private final Snapshot snapshot;
  private final SparkSession spark;
  private final StructType partitionSchema;

  /**
   * Creates a new DeltaScanPredicateBuilder (Item 1: static factory).
   *
   * @param snapshot kernel snapshot providing table schema for data skipping (non-null)
   * @param spark active Spark session (non-null)
   * @param partitionSchema partition schema for partition filter rewriting (non-null)
   * @return immutable predicate builder instance
   * @throws NullPointerException if any argument is null
   */
  public static DeltaScanPredicateBuilder of(
      Snapshot snapshot, SparkSession spark, StructType partitionSchema) {
    return new DeltaScanPredicateBuilder(snapshot, spark, partitionSchema);
  }

  private DeltaScanPredicateBuilder(
      Snapshot snapshot, SparkSession spark, StructType partitionSchema) {
    this.snapshot = requireNonNull(snapshot, "snapshot"); // Item 49
    this.spark = requireNonNull(spark, "spark");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema");
  }

  @Override
  public Dataset<Row> apply(Dataset<Row> wrappedDF, Filter[] filters) {
    requireNonNull(wrappedDF, "wrappedDF"); // Item 49
    requireNonNull(filters, "filters");

    if (filters.length == 0) {
      return wrappedDF;
    }

    // Item 50: defensive copy of mutable array parameter
    Filter[] filtersCopy = filters.clone();

    // Item 18: composition — delegate to Scala bridge for Catalyst manipulation.
    return DataFiltersBuilderV2.applyAllFilters(
        wrappedDF, filtersCopy, partitionSchema, snapshot, spark);
  }

  @Override
  public String toString() {
    return String.format(
        "DeltaScanPredicateBuilder[partitionSchema=%s]", partitionSchema.simpleString());
  }
}
