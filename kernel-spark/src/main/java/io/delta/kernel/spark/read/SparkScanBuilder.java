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
package io.delta.kernel.spark.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.utils.ExpressionUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class SparkScanBuilder
    implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {

  private io.delta.kernel.ScanBuilder kernelScanBuilder;
  private final String tablePath;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final CaseInsensitiveStringMap options;
  private final Set<String> partitionColumnSet;
  private StructType requiredDataSchema;
  private Predicate[] pushedPredicates;
  private Filter[]
      pushedFilters; // same as pushedPredicates, but in Spark Filter, returned by pushedFilters()
  private Filter[] dataFilters;

  public SparkScanBuilder(
      String tableName,
      String tablePath,
      StructType dataSchema,
      StructType partitionSchema,
      SnapshotImpl snapshot,
      CaseInsensitiveStringMap options) {
    this.kernelScanBuilder = requireNonNull(snapshot, "snapshot is null").getScanBuilder();
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.options = requireNonNull(options, "options is null");
    this.requiredDataSchema = this.dataSchema;
    this.partitionColumnSet =
        Arrays.stream(this.partitionSchema.fields())
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    this.pushedPredicates = new Predicate[0];
    this.dataFilters = new Filter[0];
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
    // 1. Split filters into kernel-supported and kernel-unsupported ones
    Map<Boolean, List<Filter>> partitioned =
        Arrays.stream(filters)
            .collect(
                Collectors.partitioningBy(
                    f -> ExpressionUtils.translateSparkFilterToKernelPredicate(f).isPresent()));
    List<Filter> kernelSupportedFilters = partitioned.get(true);
    List<Filter> kernelUnsupportedFilters = partitioned.get(false);

    // kernelSupportedFilters will be pushed to kernel, set pushedPredicates
    this.pushedFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedPredicates =
        kernelSupportedFilters.stream()
            .map(f -> ExpressionUtils.translateSparkFilterToKernelPredicate(f).get())
            .toArray(Predicate[]::new);
    if (this.pushedPredicates.length > 0) {
      Optional<Predicate> kernelAnd = Arrays.stream(this.pushedPredicates).reduce(And::new);
      this.kernelScanBuilder = this.kernelScanBuilder.withFilter(kernelAnd.get());
    }

    // 2. Split filters into partition filters and data filters
    Map<Boolean, List<Filter>> byPartitionUsage =
        Arrays.stream(filters)
            .collect(
                Collectors.partitioningBy(
                    f -> {
                      String[] refs = f.references();
                      return refs != null
                          && refs.length > 0
                          && Arrays.stream(refs)
                              .anyMatch(
                                  col -> partitionColumnSet.contains(col.toLowerCase(Locale.ROOT)));
                    }));
    List<Filter> dataFilters = byPartitionUsage.get(false);
    this.dataFilters = dataFilters.toArray(new Filter[0]);

    // 3. Return all data filters + kernel-unsupported partition filters
    // they will be evaluated after scanning
    Set<Filter> filterSet = new HashSet<>(dataFilters); // deduplicate
    filterSet.addAll(kernelUnsupportedFilters);
    return filterSet.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return this.pushedFilters;
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    return new SparkScan(
        tablePath,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        pushedPredicates,
        dataFilters,
        kernelScanBuilder.build(),
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
}
