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
 * Kernel-compatible ScanBuilder that uses distributed DataFrame-based log replay.
 *
 * <p><b>Phase 1 Architecture (aligned with 1DD):</b>
 *
 * <ul>
 *   <li><b>Log Reading</b>: Spark DataFrames read from _delta_log/ with Kernel providing LogSegment
 *   <li><b>Log Replay</b>: Distributed dedup via repartition + sortWithinPartitions + window (V1
 *       pattern)
 *   <li><b>Data Skipping</b>: V1 DataFiltersBuilderUtils rewrites filters → df.filter() on parsed
 *       stats
 *   <li><b>Output</b>: Kernel ColumnarBatch via SparkRow wrapper at collection boundary
 * </ul>
 *
 * <p>Data skipping is purely DataFrame-based — no OpaquePredicate, no row-level callback. Filters
 * are passed as raw Spark {@link Filter} objects and applied by {@link
 * DataFiltersBuilderV2.applyDataSkipping} (which delegates to shared {@code
 * DataFiltersBuilderUtils}).
 */
public class DistributedScanBuilder implements ScanBuilder {
  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;
  private Dataset<org.apache.spark.sql.Row> dataFrame;
  private StructType readSchema;
  private boolean maintainOrdering;

  // Phase 1: Raw Spark filters for data skipping — converted to predicates by shared utils
  private Filter[] dataSkippingFilters;

  /**
   * Create a new DistributedScanBuilder.
   *
   * @param spark Spark session
   * @param snapshot Delta snapshot
   * @param numPartitions Number of partitions for distributed processing
   */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = spark;
    this.snapshot = snapshot;
    this.numPartitions = numPartitions;
    this.dataFrame =
        DistributedLogReplayHelper.stateReconstructionV2(spark, snapshot, numPartitions);
    this.readSchema = snapshot.getSchema();
    this.maintainOrdering = false;
    this.dataSkippingFilters = new Filter[0];
  }

  /**
   * Create a new DistributedScanBuilder with a custom DataFrame. Useful for streaming where we need
   * pre-sorted DataFrames.
   *
   * @param spark Spark session
   * @param snapshot Delta snapshot
   * @param numPartitions Number of partitions for distributed processing
   * @param customDataFrame Pre-computed DataFrame with "add" struct
   */
  public DistributedScanBuilder(
      SparkSession spark,
      Snapshot snapshot,
      int numPartitions,
      Dataset<org.apache.spark.sql.Row> customDataFrame) {
    this.spark = spark;
    this.snapshot = snapshot;
    this.numPartitions = numPartitions;
    this.dataFrame = customDataFrame;
    this.readSchema = snapshot.getSchema();
    this.maintainOrdering = false;
    this.dataSkippingFilters = new Filter[0];
  }

  /**
   * Enable ordering preservation for streaming. When called, the scan will maintain the DataFrame's
   * sort order. This is essential for streaming initial snapshot where files must be processed in
   * order.
   *
   * @return this builder for chaining
   */
  public DistributedScanBuilder withSortKey() {
    this.maintainOrdering = true;
    return this;
  }

  /**
   * Set data skipping filters to be applied on the log replay DataFrame.
   *
   * <p>The raw Spark Filters will be converted to data skipping predicates by the shared {@code
   * DataFiltersBuilderUtils} pipeline (stats schema construction, JSON parsing,
   * verifyStatsForFilter — all reused from V1).
   *
   * @param filters Spark data filters for data skipping
   * @return this builder for chaining
   */
  public DistributedScanBuilder withDataSkippingFilters(Filter[] filters) {
    // TODO: how to get rid of Predicate predicate constraint
    this.dataSkippingFilters = filters;
    return this;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    // Phase 1: Kernel Predicates are not used for data skipping.
    // Data skipping is done via df.filter() through withDataSkippingFilters().
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    this.readSchema = readSchema;
    return this;
  }

  @Override
  public Scan build() {
    return new DistributedScan(
        spark, dataFrame, snapshot, readSchema, maintainOrdering, dataSkippingFilters);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed log replay");
  }
}
