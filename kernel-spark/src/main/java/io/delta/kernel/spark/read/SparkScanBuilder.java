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
import io.delta.kernel.spark.utils.ExpressionUtils;
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
  // pushedKernelPredicates: Predicates that have been pushed down to the Delta Kernel for
  // evaluation.
  // pushedSparkFilters: The same pushed predicates, but represented using Spark’s {@link Filter}
  // API (needed because Spark operates on Filter objects while the Kernel uses Predicate)
  private Predicate[] pushedKernelPredicates;
  private Filter[] pushedSparkFilters;
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
    this.pushedKernelPredicates = new Predicate[0];
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
    List<Filter> kernelSupportedFilters = new ArrayList<>();
    List<Predicate> convertedKernelPredicates = new ArrayList<>();
    List<Filter> dataFilterList = new ArrayList<>();
    List<Filter> postScanFilters = new ArrayList<>();

    for (Filter filter : filters) {
      boolean addedToPostScan = false;

      // 1. check if the filter can be converted to a kernel predicate
      ExpressionUtils.ConvertedPredicate convertedPredicate =
          ExpressionUtils.convertSparkFilterToConvertedKernelPredicate(filter);
      if (convertedPredicate.isPresent()) {
        convertedKernelPredicates.add(convertedPredicate.get());
        if (convertedPredicate.isPartial()) {
          // if the conversion is partial, it should be added to post-scan filters
          postScanFilters.add(filter);
          addedToPostScan = true;
        } else {
          kernelSupportedFilters.add(filter);
        }
      } else {
        // if the filter cannot be converted, it should be added to post-scan filters
        postScanFilters.add(filter);
        addedToPostScan = true;
      }

      // 2. check if the filter is a partition filter or a data filter
      String[] refs = filter.references();
      if (refs != null
          && refs.length > 0
          && Arrays.stream(refs)
              .allMatch(col -> partitionColumnSet.contains(col.toLowerCase(Locale.ROOT)))) {
        // if refs is not null and all refs are in partitionColumnSet, it is a partition filter
        // nothing to do here, as kernel-supported partition filter does not need post-scan
        // evaluation, and kernel-unsupported partition filter has already been added to post-scan
        // filters
      } else {
        // If the filter is a data filter, it should be evaluated after scanning, if it haven't been
        // added already
        dataFilterList.add(filter);
        if (!addedToPostScan) {
          postScanFilters.add(filter);
        }
      }
    }

    this.pushedSparkFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedKernelPredicates = convertedKernelPredicates.toArray(new Predicate[0]);
    if (this.pushedKernelPredicates.length > 0) {
      Optional<Predicate> kernelAnd = Arrays.stream(this.pushedKernelPredicates).reduce(And::new);
      this.kernelScanBuilder = this.kernelScanBuilder.withFilter(kernelAnd.get());
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
    return new SparkScan(
        tablePath,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        pushedKernelPredicates,
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
