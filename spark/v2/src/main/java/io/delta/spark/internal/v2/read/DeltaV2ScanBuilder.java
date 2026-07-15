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
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.ExpressionUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Package-private scan builder implementation for Delta's Spark DataSource V2 read path.
 *
 * <p>This class must remain package-private so callers outside {@code v2.read} depend only on
 * Spark's public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
class DeltaV2ScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit {

  private io.delta.kernel.ScanBuilder kernelScanBuilder;
  private final Snapshot initialSnapshot;
  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final StructType tableSchema;
  private final Optional<Statistics> catalogStats;
  private final CaseInsensitiveStringMap options;
  private final Set<String> partitionColumnSet;
  private StructType requiredDataSchema;
  // pushedKernelPredicates: Predicates that have been pushed down to the Delta Kernel for
  // evaluation.
  // pushedSparkFilters: The same pushed predicates, but represented using Spark’s {@link Filter}
  // API (needed because Spark operates on Filter objects while the Kernel uses Predicate)
  private Predicate[] pushedKernelPredicates;
  private Filter[] pushedSparkFilters;
  private Filter[] dataFilters;
  // Tracks whether any filter still needs to be applied after the scan.
  private boolean hasPOstScanResidualFilters = false;
  private OptionalInt pushedLimit = OptionalInt.empty();

  /**
   * Creates a DeltaV2ScanBuilder with the given snapshot and configuration.
   *
   * @param tableName the name of the table
   * @param initialSnapshot Snapshot created during connector setup
   * @param snapshotManager the snapshot manager for this table
   * @param dataSchema the data schema (non-partition columns)
   * @param partitionSchema the partition schema
   * @param tableSchema the full table schema (all columns) for filter type alignment
   * @param catalogStats optional V2 Statistics converted from catalog stats
   * @param options scan options
   */
  public DeltaV2ScanBuilder(
      String tableName,
      io.delta.kernel.Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      StructType partitionSchema,
      StructType tableSchema,
      Optional<Statistics> catalogStats,
      CaseInsensitiveStringMap options) {
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.kernelScanBuilder = initialSnapshot.getScanBuilder();
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
    this.catalogStats = requireNonNull(catalogStats, "catalogStats is null");
    this.options = requireNonNull(options, "options is null");
    this.requiredDataSchema = this.dataSchema;
    this.partitionColumnSet =
        Arrays.stream(this.partitionSchema.fields())
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    this.pushedKernelPredicates = new Predicate[0];
    this.dataFilters = new Filter[0];
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    requireNonNull(requiredSchema, "requiredSchema is null");
    // CDC columns are injected later by CDCReadFunction, so strip them here.
    this.requiredDataSchema =
        new StructType(
            Arrays.stream(requiredSchema.fields())
                .filter(
                    f -> {
                      String name = f.name().toLowerCase(Locale.ROOT);
                      return !partitionColumnSet.contains(name)
                          && !CDCSchemaContext.isCDCColumn(name);
                    })
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
          ExpressionUtils.classifyFilter(filter, partitionColumnSet, tableSchema);
      // Collect kernel predicates if supported
      if (classification.isKernelSupported) {
        convertedKernelPredicates.add(classification.kernelPredicate.get());
        if (!classification.isPartialConversion) {
          // Add filter to kernelSupportedFilters if it is fully converted
          // TODO: add partially converted Spark filter as well
          // right now we only have the partially converted kernel predicate
          kernelSupportedFilters.add(filter);
        }
      }

      // Collect data filters
      if (classification.isDataFilter) {
        dataFilterList.add(filter);
      }

      // Collect post-scan filters
      // Filters with the following characteristics need to be evaluated after delta kernel scan:
      // 1. filters that are not supported by delta kernel, thus kernel cannot apply them during
      // scan
      // 2. filters that are not fully converted to kernel predicates, thus the unconverted part
      // needs to be evaluated after scan
      // 3. filters that are data filters, as kernel only evaluate data filter based on min/max
      // stats, thus need to be evaluated with actual data after scan
      //
      // Fully converted partition filters are used to prune partitions during scan. Only the
      // partitions that satisfy the filters will be scanned, so no need for post-scan evaluation.
      if (!classification.isKernelSupported
          || classification.isPartialConversion
          || classification.isDataFilter) {
        postScanFilters.add(filter);
      }
    }

    this.pushedSparkFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedKernelPredicates = convertedKernelPredicates.toArray(new Predicate[0]);
    if (this.pushedKernelPredicates.length > 0) {
      Optional<Predicate> kernelAnd = Arrays.stream(this.pushedKernelPredicates).reduce(And::new);
      this.kernelScanBuilder = this.kernelScanBuilder.withFilter(kernelAnd.get());
    }
    this.dataFilters = dataFilterList.toArray(new Filter[0]);
    Filter[] postScan = postScanFilters.toArray(new Filter[0]);
    // ScanBuilder mutations can be cumulative, so a later pushFilters call should not make an
    // earlier residual safe to ignore.
    this.hasPOstScanResidualFilters |= postScan.length > 0;
    return postScan;
  }

  @Override
  public Filter[] pushedFilters() {
    return this.pushedSparkFilters;
  }

  /**
   * Accepts a LIMIT hint from Spark's optimizer.
   *
   * <p>Always returns {@code true}: the connector treats the limit as a best-effort hint, using
   * per-file {@code numRecords} statistics to stop adding files to the scan plan once enough rows
   * have been accumulated (see {@link DeltaV2Scan}). Because pruning happens at file granularity,
   * the planned scan may still return more rows than requested (for example, a single file with
   * 1,000 rows for LIMIT 5), so {@link #isPartiallyPushed()} is left at its default of {@code true}
   * and Spark keeps its limit operators as a backstop.
   *
   * @param limit the row limit requested by Spark which must be non-negative.
   */
  @Override
  public boolean pushLimit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("Pushed limit must be non-negative, but got: " + limit);
    }
    this.pushedLimit = OptionalInt.of(limit);
    return true;
  }

  // isPartiallyPushed() intentionally uses the interface default (true). Because pruning happens at
  // file granularity, the scan may produce more rows than requested, so Spark must reapply LIMIT.

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    // Spark's V2ScanRelationPushDown only pushes a limit when no post-scan residual remains. It
    // matches PhysicalOperation(_, Nil, sHolder). Retain this defensive check for direct callers
    // that may invoke the ScanBuilder methods in a different order.
    OptionalInt effectiveLimit =
        hasPOstScanResidualFilters ? OptionalInt.empty() : this.pushedLimit;

    return new DeltaV2Scan(
        snapshotManager,
        initialSnapshot,
        tableSchema,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        pushedKernelPredicates,
        dataFilters,
        kernelScanBuilder.build(),
        catalogStats,
        options,
        effectiveLimit);
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

  OptionalInt getPushedLimit() {
    return pushedLimit;
  }
}
