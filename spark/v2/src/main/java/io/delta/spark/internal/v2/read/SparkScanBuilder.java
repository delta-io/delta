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
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.ExpressionUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation for the V2 Delta connector.
 *
 * <p><b>Phase 1 Architecture (aligned with 1DD):</b> Data skipping is performed entirely via
 * DataFrame operations (df.filter), reusing V1's DataFiltersBuilder logic through shared {@code
 * DataFiltersBuilderUtils}. No OpaquePredicate or Kernel-level callbacks are used.
 *
 * <p>Controlled by {@code spark.databricks.delta.v2.distributedScan.enabled} (default: true):
 *
 * <ul>
 *   <li><b>true</b> (distributed): DistributedScanBuilder -> DistributedScanPlanner ->
 *       DeltaScanPredicateBuilder -> DataFiltersBuilderUtils -> DistributedScan (wraps
 *       SparkRowScanExecutor)
 *   <li><b>false</b> (local): Kernel-native ScanBuilder -> Kernel Scan (single-driver log replay)
 * </ul>
 */
public class SparkScanBuilder
    implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {

  // Single ScanBuilder field — either DistributedScanBuilder or Kernel-native.
  // Use instanceof when distributed-specific API (withPushdownFilters) is needed.
  private io.delta.kernel.ScanBuilder scanBuilder;

  private final Snapshot initialSnapshot;
  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final CaseInsensitiveStringMap options;
  private final Set<String> partitionColumnSet;
  private StructType requiredDataSchema;
  // pushedKernelPredicates: Predicates that have been pushed down to the Delta Kernel for
  // evaluation.
  // pushedSparkFilters: The same pushed predicates, but represented using Spark's {@link Filter}
  // API (needed because Spark operates on Filter objects while the Kernel uses Predicate)
  private Predicate[] pushedKernelPredicates;
  private Filter[] pushedSparkFilters;
  private Filter[] dataFilters;
  private final SparkSession spark;

  /**
   * Creates a SparkScanBuilder with the given snapshot and configuration.
   *
   * @param spark the Spark session
   * @param tableName the name of the table
   * @param initialSnapshot Snapshot created during connector setup
   * @param snapshotManager the snapshot manager for this table
   * @param dataSchema the data schema (non-partition columns)
   * @param partitionSchema the partition schema
   * @param options scan options
   */
  public SparkScanBuilder(
      SparkSession spark,
      String tableName,
      io.delta.kernel.Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      StructType partitionSchema,
      CaseInsensitiveStringMap options) {
    this.spark = requireNonNull(spark, "spark is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.options = requireNonNull(options, "options is null");
    this.requiredDataSchema = this.dataSchema;
    this.partitionColumnSet =
        Arrays.stream(this.partitionSchema.fields())
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    this.pushedKernelPredicates = new Predicate[0];
    this.dataFilters = new Filter[0];

    // Config: distributed vs local (Kernel-native) scan
    boolean distributedEnabled =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf.V2_DISTRIBUTED_SCAN_ENABLED());

    if (distributedEnabled) {
      int numPartitions = getNumPartitionsFromOptions(options);
      this.scanBuilder = new DistributedScanBuilder(spark, initialSnapshot, numPartitions);
    } else {
      this.scanBuilder = initialSnapshot.getScanBuilder();
    }
  }

  private int getNumPartitionsFromOptions(CaseInsensitiveStringMap options) {
    return Integer.parseInt(options.getOrDefault("numPartitions", "50"));
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    requireNonNull(requiredSchema, "requiredSchema is null");
    this.requiredDataSchema =
        new StructType(
            Arrays.stream(requiredSchema.fields())
                .filter(f -> !partitionColumnSet.contains(f.name().toLowerCase(Locale.ROOT)))
                .toArray(StructField[]::new));
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> kernelSupportedFilters = new ArrayList<>();
    List<Predicate> convertedKernelPredicates = new ArrayList<>();
    List<Filter> dataFilterList = new ArrayList<>();
    List<Filter> postScanFilters = new ArrayList<>();

    for (Filter filter : filters) {
      ExpressionUtils.FilterClassificationResult classification =
          ExpressionUtils.classifyFilter(filter, partitionColumnSet);
      // Collect kernel predicates if supported
      if (classification.isKernelSupported) {
        convertedKernelPredicates.add(classification.kernelPredicate.get());
        if (!classification.isPartialConversion) {
          kernelSupportedFilters.add(filter);
        }
      }

      // Collect data filters
      if (classification.isDataFilter) {
        dataFilterList.add(filter);
      }

      // Collect post-scan filters
      if (!classification.isKernelSupported
          || classification.isPartialConversion
          || classification.isDataFilter) {
        postScanFilters.add(filter);
      }
    }

    this.pushedSparkFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedKernelPredicates = convertedKernelPredicates.toArray(new Predicate[0]);

    if (scanBuilder instanceof DistributedScanBuilder) {
      // Distributed path: pass ALL pushable filters (partition + data) to
      // DistributedScanBuilder. Both partition pruning and data skipping are
      // applied on the distributed log replay DataFrame.
      List<Filter> allPushableFilters = new ArrayList<>();
      allPushableFilters.addAll(dataFilterList);
      // Also include pure partition filters for partition pruning on the DataFrame
      for (Filter filter : filters) {
        String[] refs = filter.references();
        boolean isPurePartitionFilter =
            refs != null
                && refs.length > 0
                && Arrays.stream(refs)
                    .allMatch(col -> partitionColumnSet.contains(col.toLowerCase(Locale.ROOT)));
        if (isPurePartitionFilter && !dataFilterList.contains(filter)) {
          allPushableFilters.add(filter);
        }
      }

      if (!allPushableFilters.isEmpty()) {
        ((DistributedScanBuilder) scanBuilder)
            .withPushdownFilters(allPushableFilters.toArray(new Filter[0]), partitionSchema);
      }
    } else {
      // Local (Kernel-native) path: push Kernel predicates to the native ScanBuilder
      for (Predicate p : this.pushedKernelPredicates) {
        this.scanBuilder = this.scanBuilder.withFilter(p);
      }
    }

    this.dataFilters = dataFilterList.toArray(new Filter[0]);
    return postScanFilters.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return this.pushedSparkFilters;
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    // Apply schema projection and build — works for both distributed and local paths
    io.delta.kernel.Scan kernelScan =
        scanBuilder
            .withReadSchema(
                io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
                    requiredDataSchema))
            .build();

    return new SparkScan(
        snapshotManager,
        initialSnapshot,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        pushedKernelPredicates,
        dataFilters,
        kernelScan,
        options);
  }

  CaseInsensitiveStringMap getOptions() {
    return options;
  }

  StructType getDataSchema() {
    return dataSchema;
  }

  StructType getPartitionSchema() {
    return partitionSchema;
  }

  /** Helper to merge data and partition fields into a single field array. */
  private StructField[] mergeFields(StructField[] dataFields, StructField[] partitionFields) {
    StructField[] merged = new StructField[dataFields.length + partitionFields.length];
    System.arraycopy(dataFields, 0, merged, 0, dataFields.length);
    System.arraycopy(partitionFields, 0, merged, dataFields.length, partitionFields.length);
    return merged;
  }
}
