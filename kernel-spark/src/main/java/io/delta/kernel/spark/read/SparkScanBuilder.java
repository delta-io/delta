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
  // pushedFilters: The same pushed predicates, but represented using Spark’s {@link Filter} API
  //                (needed because Spark operates on Filter objects while the Kernel uses
  // Predicate)
  private Predicate[] pushedKernelPredicates;
  private Filter[] pushedFilters;
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

  /*
   * In order to handle partial predicate pushdown of AND filters,
   * recursively breakdown AND filters into individual filters and add to filterList
   */
  private void breakdownAndFilters(Filter filter, List<Filter> filterList) {
    if (filter instanceof org.apache.spark.sql.sources.And) {
      org.apache.spark.sql.sources.And andFilter = (org.apache.spark.sql.sources.And) filter;
      breakdownAndFilters(andFilter.left(), filterList);
      breakdownAndFilters(andFilter.right(), filterList);
    } else {
      filterList.add(filter);
    }
  }

  /*
   * Get all references (column names) from a filter
   * If the filter is a composite filter (AND/OR/NOT), recursively get references from its children
   * Else, return filter.references()
   */
  private String[] getFilterReferences(Filter filter) {
    List<String> refs = new ArrayList<>();
    if (filter instanceof org.apache.spark.sql.sources.And) {
      org.apache.spark.sql.sources.And andFilter = (org.apache.spark.sql.sources.And) filter;
      refs.addAll(Arrays.asList(getFilterReferences(andFilter.left())));
      refs.addAll(Arrays.asList(getFilterReferences(andFilter.right())));
    } else if (filter instanceof org.apache.spark.sql.sources.Or) {
      org.apache.spark.sql.sources.Or orFilter = (org.apache.spark.sql.sources.Or) filter;
      refs.addAll(Arrays.asList(getFilterReferences(orFilter.left())));
      refs.addAll(Arrays.asList(getFilterReferences(orFilter.right())));
    } else if (filter instanceof org.apache.spark.sql.sources.Not) {
      org.apache.spark.sql.sources.Not notFilter = (org.apache.spark.sql.sources.Not) filter;
      refs.addAll(Arrays.asList(getFilterReferences(notFilter.child())));
    } else {
      return filter.references();
    }
    return refs.toArray(new String[0]);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // 0. Breakdown AND filters
    List<Filter> filterList = new ArrayList<>();
    for (Filter filter : filters) {
      breakdownAndFilters(filter, filterList);
    }

    // 1. Split filters into kernel-supported and kernel-unsupported ones
    List<Filter> kernelSupportedFilters = new ArrayList<>();
    List<Filter> kernelUnsupportedFilters = new ArrayList<>();
    List<Predicate> convertedKernelPredicates = new ArrayList<>();

    for (Filter filter : filterList) {
      Optional<Predicate> convertedOpt =
          ExpressionUtils.convertSparkFilterToKernelPredicate(filter);
      if (convertedOpt.isPresent()) {
        kernelSupportedFilters.add(filter);
        convertedKernelPredicates.add(convertedOpt.get());
      } else {
        kernelUnsupportedFilters.add(filter);
      }
    }

    // kernelSupportedFilters will be pushed to kernel, set pushedPredicates
    this.pushedFilters = kernelSupportedFilters.toArray(new Filter[0]);
    this.pushedKernelPredicates = convertedKernelPredicates.toArray(new Predicate[0]);
    if (this.pushedKernelPredicates.length > 0) {
      Optional<Predicate> kernelAnd = Arrays.stream(this.pushedKernelPredicates).reduce(And::new);
      this.kernelScanBuilder = this.kernelScanBuilder.withFilter(kernelAnd.get());
    }

    // 2. Split filters into partition filters and data filters
    List<Filter> dataFilters = new ArrayList<>();
    for (Filter filter : filterList) {
      String[] refs = getFilterReferences(filter);
      // if refs is not null and all refs are in partitionColumnSet, it's a partition filter
      if (refs != null
          && refs.length > 0
          && Arrays.stream(refs)
              .allMatch(col -> partitionColumnSet.contains(col.toLowerCase(Locale.ROOT)))) {
        // partition filter, do nothing
      } else {
        dataFilters.add(filter);
      }
    }
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
