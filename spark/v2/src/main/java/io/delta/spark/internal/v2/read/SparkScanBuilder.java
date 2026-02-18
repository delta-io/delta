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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.ExpressionUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.delta.stats.DistributedScanHelper;
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class SparkScanBuilder
    implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownCatalystFilters {

  private io.delta.kernel.ScanBuilder kernelScanBuilder;
  private final Snapshot initialSnapshot;
  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final CaseInsensitiveStringMap options;
  private final Set<String> partitionColumnSet;
  private StructType requiredDataSchema;
  // pushedKernelPredicates: Predicates that have been pushed down to the Delta Kernel for
  // evaluation.
  // pushedSparkFilters: The same pushed predicates, represented as V1 Filter for SparkScan.
  private Predicate[] pushedKernelPredicates;
  private Filter[] pushedSparkFilters;
  private Filter[] dataFilters;

  // All Catalyst expressions received from Spark's optimizer (for distributed data-skipping path).
  private Expression[] allCatalystExpressions;

  /**
   * Creates a SparkScanBuilder with the given snapshot and configuration.
   *
   * @param tableName the name of the table
   * @param initialSnapshot Snapshot created during connector setup
   * @param snapshotManager the snapshot manager for this table
   * @param dataSchema the data schema (non-partition columns)
   * @param partitionSchema the partition schema
   * @param options scan options
   */
  public SparkScanBuilder(
      String tableName,
      io.delta.kernel.Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      StructType partitionSchema,
      CaseInsensitiveStringMap options) {
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.kernelScanBuilder = initialSnapshot.getScanBuilder();
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
    this.pushedSparkFilters = new Filter[0];
    this.dataFilters = new Filter[0];
    this.allCatalystExpressions = new Expression[0];
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

  /**
   * Receives resolved Catalyst expressions directly from Spark's optimizer via {@link
   * SupportsPushDownCatalystFilters}. This avoids the lossy {@code Expression → sources.Filter →
   * Expression} round-trip.
   *
   * <ul>
   *   <li>Stores raw expressions for the distributed data-skipping path.
   *   <li>Translates each expression to a V1 {@link Filter} (via Spark's {@code
   *       DataSourceStrategy.translateFilter}) for kernel-native classification.
   * </ul>
   */
  @Override
  public scala.collection.immutable.Seq<Expression> pushFilters(
      scala.collection.immutable.Seq<Expression> filters) {
    List<Expression> exprList =
        new ArrayList<>(scala.jdk.javaapi.CollectionConverters.asJava(filters));

    // Store all catalyst expressions for the distributed path
    this.allCatalystExpressions = exprList.toArray(new Expression[0]);

    List<Filter> kernelSupportedFilters = new ArrayList<>();
    List<Predicate> convertedKernelPredicates = new ArrayList<>();
    List<Filter> dataFilterList = new ArrayList<>();
    List<Expression> postScanExpressions = new ArrayList<>();

    for (Expression expr : exprList) {
      // Convert Catalyst expression → V1 Filter using Spark's DataSourceStrategy
      Optional<Filter> filterOpt = DistributedScanHelper.catalystToFilter(expr);

      if (!filterOpt.isPresent()) {
        // Untranslatable expression — must evaluate after scan
        postScanExpressions.add(expr);
        continue;
      }

      Filter filter = filterOpt.get();
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

      // Post-scan: unsupported / partial / data filters
      if (!classification.isKernelSupported
          || classification.isPartialConversion
          || classification.isDataFilter) {
        postScanExpressions.add(expr);
      }
    }

    this.pushedSparkFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedKernelPredicates = convertedKernelPredicates.toArray(new Predicate[0]);
    if (this.pushedKernelPredicates.length > 0) {
      Optional<Predicate> kernelAnd = Arrays.stream(this.pushedKernelPredicates).reduce(And::new);
      this.kernelScanBuilder = this.kernelScanBuilder.withFilter(kernelAnd.get());
    }
    this.dataFilters = dataFilterList.toArray(new Filter[0]);
    return scala.jdk.javaapi.CollectionConverters.asScala(postScanExpressions).toList();
  }

  /**
   * Returns the pushed-down filters as V2 {@link
   * org.apache.spark.sql.connector.expressions.filter.Predicate}s (required by {@link
   * SupportsPushDownCatalystFilters}).
   */
  @Override
  public org.apache.spark.sql.connector.expressions.filter.Predicate[] pushedFilters() {
    return DistributedScanHelper.filtersToV2Predicates(this.pushedSparkFilters);
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    // Determine kernel Scan: use distributed data skipping when enabled
    Scan kernelScan;
    if (isDistributedScanEnabled()) {
      kernelScan = buildDistributedScan();
    } else {
      kernelScan = kernelScanBuilder.build();
    }

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

  /**
   * Uses {@link DistributedScanBuilder} to build a Kernel Scan with Spark-level data skipping. The
   * distributed builder reads checkpoint + delta log files via Spark DataFrames and delegates to V1
   * shared components for filter planning and scan execution.
   */
  private Scan buildDistributedScan() {
    SparkSession spark = SparkSession.active();
    int numPartitions = spark.sessionState().conf().numShufflePartitions();

    // Build read schema for Kernel delegation
    io.delta.kernel.types.StructType kernelReadSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(requiredDataSchema);

    DistributedScanBuilder distributedBuilder =
        new DistributedScanBuilder(spark, initialSnapshot, numPartitions);
    // Pass raw Catalyst expressions — no Filter→Expression conversion needed
    distributedBuilder
        .withPushdownExpressions(allCatalystExpressions)
        .withReadSchema(kernelReadSchema);

    return distributedBuilder.build();
  }

  /**
   * Returns true if distributed data skipping is enabled via configuration. Default: false (use
   * Kernel-native scan).
   */
  private boolean isDistributedScanEnabled() {
    return true;
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
}
