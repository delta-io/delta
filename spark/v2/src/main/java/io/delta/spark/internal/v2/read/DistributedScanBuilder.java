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
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.FileStatus;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.stats.DistributedScanHelper;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Kernel-compatible {@link ScanBuilder} that performs Spark-level data skipping by orchestrating V1
 * shared components on a distributed DataFrame.
 *
 * <p><b>Pipeline:</b>
 *
 * <ol>
 *   <li>Extract {@link LogSegment} from Kernel {@link SnapshotImpl} (checkpoint + delta paths)
 *   <li>Load raw actions as Spark DataFrame via {@link DistributedScanHelper#loadActionsFromPaths}
 *   <li>Delegate to V1 shared pipeline via {@link DistributedScanHelper#executeFilteredScanAsDF}:
 *       <ul>
 *         <li>{@code DefaultStateProvider} — state reconstruction + stats parsing
 *         <li>{@code StatsColumnResolver} — stats column resolution
 *         <li>{@code DefaultDataSkippingFilterPlanner} — filter planning
 *         <li>DataFrame-level filter application (no collection)
 *       </ul>
 *   <li>Wrap the filtered DataFrame in a {@link DistributedScan} (zero-copy)
 * </ol>
 *
 * @see DistributedScan the Scan produced by this builder (zero-copy Spark Row → Kernel Row)
 * @see DistributedScanHelper V1 Scala helper that orchestrates the pipeline
 */
public class DistributedScanBuilder implements ScanBuilder {

  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;

  // Mutable builder state — set via fluent methods before build()
  private io.delta.kernel.types.StructType readSchema;
  private Filter[] pushdownFilters;
  private StructType sparkPartitionSchema;

  /**
   * Creates a ScanBuilder for distributed batch reads.
   *
   * @param spark SparkSession
   * @param snapshot Kernel snapshot (must be SnapshotImpl to access LogSegment)
   * @param numPartitions Number of Spark partitions for state reconstruction
   */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = requireNonNull(spark, "spark");
    this.snapshot = requireNonNull(snapshot, "snapshot");
    this.numPartitions = numPartitions;
    this.readSchema = snapshot.getSchema();
    this.pushdownFilters = new Filter[0];
    this.sparkPartitionSchema = new StructType();
  }

  /**
   * Sets the pushdown filters (partition + data) for the scan.
   *
   * @param filters all pushable Spark filters (non-null)
   * @param partitionSchema partition schema for filter rewriting (non-null)
   * @return this builder for chaining
   */
  public DistributedScanBuilder withPushdownFilters(Filter[] filters, StructType partitionSchema) {
    this.pushdownFilters = requireNonNull(filters, "filters").clone();
    this.sparkPartitionSchema = requireNonNull(partitionSchema, "partitionSchema");
    return this;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    // Kernel predicates are not used for distributed data skipping.
    // Filtering is done via the V1 pipeline through withPushdownFilters().
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(io.delta.kernel.types.StructType readSchema) {
    this.readSchema = requireNonNull(readSchema, "readSchema");
    return this;
  }

  /**
   * Builds the scan using the V1 shared pipeline, returning a zero-copy {@link DistributedScan}
   * backed by a filtered DataFrame (not collected).
   */
  @Override
  public Scan build() {
    // Step 0: Cast to SnapshotImpl to access LogSegment
    if (!(snapshot instanceof SnapshotImpl)) {
      throw new IllegalArgumentException("Snapshot must be SnapshotImpl to access LogSegment");
    }
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    LogSegment logSegment = snapshotImpl.getLogSegment();

    // Step 1: Extract file paths from LogSegment
    String[] checkpointPaths =
        logSegment.getCheckpoints().stream().map(FileStatus::getPath).toArray(String[]::new);

    long checkpointVersion = logSegment.getCheckpointVersionOpt().orElse(-1L);

    List<FileStatus> deltas = logSegment.getDeltas();
    String[] deltaPaths = deltas.stream().map(FileStatus::getPath).toArray(String[]::new);

    long[] deltaVersions =
        deltas.stream()
            .mapToLong(fs -> DistributedScanHelper.extractDeltaVersion(fs.getPath()))
            .toArray();

    // Step 2: Register canonicalizePath UDF
    DistributedScanHelper.registerCanonicalizeUdf(spark);

    // Step 3: Convert Kernel schemas to Spark schemas
    StructType sparkTableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());

    List<String> partitionColumnNames = snapshot.getPartitionColumnNames();

    // Build partition schema from table schema + partition column names
    StructType partSchema = buildPartitionSchema(sparkTableSchema, partitionColumnNames);

    // Step 4: Execute V1 shared pipeline → returns DataFrame with {add: struct<...>}
    Dataset<org.apache.spark.sql.Row> plannedDF =
        DistributedScanHelper.executeFilteredScanAsDF(
            spark,
            () ->
                DistributedScanHelper.loadActionsFromPaths(
                    spark, checkpointPaths, checkpointVersion, deltaPaths, deltaVersions),
            numPartitions,
            sparkTableSchema,
            partitionColumnNames,
            partSchema,
            pushdownFilters);

    // Step 5: Wrap in DistributedScan (zero-copy: DataFrame → Kernel rows)
    return new DistributedScan(plannedDF, snapshot, readSchema);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed data skipping");
  }

  // ========================
  // Private helpers
  // ========================

  /** Builds a partition schema from the full table schema and partition column names. */
  private StructType buildPartitionSchema(
      StructType tableSchema, List<String> partitionColumnNames) {
    if (partitionColumnNames.isEmpty()) {
      return new StructType();
    }

    Set<String> partColSet =
        partitionColumnNames.stream().map(String::toLowerCase).collect(Collectors.toSet());

    List<StructField> partFields = new ArrayList<>();
    for (StructField field : tableSchema.fields()) {
      if (partColSet.contains(field.name().toLowerCase())) {
        partFields.add(field);
      }
    }

    // Preserve partition column ordering
    List<StructField> orderedFields = new ArrayList<>();
    for (String partCol : partitionColumnNames) {
      for (StructField field : partFields) {
        if (field.name().equalsIgnoreCase(partCol)) {
          orderedFields.add(field);
          break;
        }
      }
    }

    return new StructType(orderedFields.toArray(new StructField[0]));
  }
}
