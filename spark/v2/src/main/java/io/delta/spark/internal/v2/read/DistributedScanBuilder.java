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

/**
 * Kernel-compatible ScanBuilder that uses distributed DataFrame-based log replay.
 *
 * <p>This implementation satisfies the requirement to use Kernel APIs while leveraging Spark
 * DataFrames for distributed processing.
 */
public class DistributedScanBuilder implements ScanBuilder {
  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;
  private Dataset<org.apache.spark.sql.Row> dataFrame;
  private StructType readSchema;
  private boolean maintainOrdering; // Whether to preserve DataFrame order

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

    // Initialize DataFrame with distributed log replay
    this.dataFrame =
        DistributedLogReplayHelper.stateReconstructionV2(spark, snapshot, numPartitions);

    // Start with full schema
    this.readSchema = snapshot.getSchema();
    this.maintainOrdering = false; // Default: no ordering guarantee
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
    this.maintainOrdering = false; // Default: no ordering guarantee
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

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    // For now, filtering is already done in DistributedLogReplayHelper
    // TODO: Add additional filtering here if needed
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    this.readSchema = readSchema;
    // Schema projection will be handled when converting to PartitionedFiles
    return this;
  }

  @Override
  public Scan build() {
    return new DistributedScan(spark, dataFrame, snapshot, readSchema, maintainOrdering);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    // Paginated scan not yet supported in distributed mode
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed log replay");
  }
}
