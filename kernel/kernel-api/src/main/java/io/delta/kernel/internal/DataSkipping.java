/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;
import static io.delta.kernel.internal.skipping.StatsSchemaHelper.getStatsSchema;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnScanFileSchema;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.skipping.DataSkippingUtils;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

// data skipping class
public class DataSkipping {
  Optional<Tuple2<Predicate, Predicate>> partitionAndDataFilters;
  CloseableIterator<FilteredColumnarBatch> scanFileIter;
  Engine engine;
  Supplier<Map<String, StructField>> partitionColToStructFieldMap;
  Metadata metadata;

  // todo: serialize metadata and predicate
  // metadata: 1. getSchema 2. getDataSchema 3. getPartitionColNames
  public DataSkipping(
      Optional<Predicate> predicate,
      CloseableIterator<FilteredColumnarBatch> scanFileIter,
      Engine engine,
      Metadata metadata) {
    this.partitionAndDataFilters = splitFilters(predicate);
    this.scanFileIter = scanFileIter;
    this.engine = engine;
    this.metadata = metadata;

    this.partitionColToStructFieldMap =
        () -> {
          Set<String> partitionColNames = metadata.getPartitionColNames();
          return metadata.getSchema().fields().stream()
              .filter(field -> partitionColNames.contains(field.getName().toLowerCase(Locale.ROOT)))
              .collect(toMap(field -> field.getName().toLowerCase(Locale.ROOT), identity()));
        };
  }

  public CloseableIterator<FilteredColumnarBatch> applyFilter() {

    // Apply partition pruning
    scanFileIter = applyPartitionPruning(engine, scanFileIter);

    Optional<DataSkippingPredicate> dataSkippingFilter = getDataSkippingFilter();
    boolean hasDataSkippingFilter = dataSkippingFilter.isPresent();

    // Apply data skipping
    if (hasDataSkippingFilter) {
      // there was a usable data skipping filter --> apply data skipping
      scanFileIter = applyDataSkipping(engine, scanFileIter, dataSkippingFilter.get());
    }
    return scanFileIter;
  }

  private Optional<Tuple2<Predicate, Predicate>> splitFilters(Optional<Predicate> filter) {
    return filter.map(
        predicate ->
            PartitionUtils.splitMetadataAndDataPredicates(
                predicate, metadata.getPartitionColNames()));
  }

  private Optional<Predicate> getDataFilters() {
    return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._2));
  }

  private Optional<Predicate> getPartitionsFilters() {
    return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._1));
  }

  private Optional<DataSkippingPredicate> getDataSkippingFilter() {
    return getDataFilters()
        .flatMap(
            dataFilters ->
                DataSkippingUtils.constructDataSkippingFilter(
                    dataFilters, metadata.getDataSchema()));
  }

  /** Consider `ALWAYS_TRUE` as no predicate. */
  private Optional<Predicate> removeAlwaysTrue(Optional<Predicate> predicate) {
    return predicate.filter(filter -> !filter.getName().equalsIgnoreCase("ALWAYS_TRUE"));
  }

  CloseableIterator<FilteredColumnarBatch> applyPartitionPruning(
      Engine engine, CloseableIterator<FilteredColumnarBatch> scanFileIter) {
    Optional<Predicate> partitionPredicate = getPartitionsFilters();
    if (!partitionPredicate.isPresent()) {
      // There is no partition filter, return the scan file iterator as is.
      return scanFileIter;
    }

    Predicate predicateOnScanFileBatch =
        rewritePartitionPredicateOnScanFileSchema(
            partitionPredicate.get(), partitionColToStructFieldMap.get());

    return new CloseableIterator<FilteredColumnarBatch>() {
      PredicateEvaluator predicateEvaluator = null;

      @Override
      public boolean hasNext() {
        return scanFileIter.hasNext();
      }

      @Override
      public FilteredColumnarBatch next() {
        FilteredColumnarBatch next = scanFileIter.next();
        if (predicateEvaluator == null) {
          predicateEvaluator =
              wrapEngineException(
                  () ->
                      engine
                          .getExpressionHandler()
                          .getPredicateEvaluator(
                              next.getData().getSchema(), predicateOnScanFileBatch),
                  "Get the predicate evaluator for partition pruning with schema=%s and"
                      + " filter=%s",
                  next.getData().getSchema(),
                  predicateOnScanFileBatch);
        }
        ColumnVector newSelectionVector =
            wrapEngineException(
                () -> predicateEvaluator.eval(next.getData(), next.getSelectionVector()),
                "Evaluating the partition expression %s",
                predicateOnScanFileBatch);
        return new FilteredColumnarBatch(next.getData(), Optional.of(newSelectionVector));
      }

      @Override
      public void close() throws IOException {
        scanFileIter.close();
      }
    };
  }

  private CloseableIterator<FilteredColumnarBatch> applyDataSkipping(
      Engine engine,
      CloseableIterator<FilteredColumnarBatch> scanFileIter,
      DataSkippingPredicate dataSkippingFilter) {
    // Get the stats schema
    // It's possible to instead provide the referenced columns when building the schema but
    // pruning it after is much simpler
    StructType prunedStatsSchema =
        DataSkippingUtils.pruneStatsSchema(
            getStatsSchema(metadata.getDataSchema()), dataSkippingFilter.getReferencedCols());

    // Skipping happens in two steps:
    // 1. The predicate produces false for any file whose stats prove we can safely skip it. A
    //    value of true means the stats say we must keep the file, and null means we could not
    //    determine whether the file is safe to skip, because its stats were missing/null.
    // 2. The coalesce(skip, true) converts null (= keep) to true
    Predicate filterToEval =
        new Predicate(
            "=",
            new ScalarExpression(
                "COALESCE", Arrays.asList(dataSkippingFilter, Literal.ofBoolean(true))),
            AlwaysTrue.ALWAYS_TRUE);

    PredicateEvaluator predicateEvaluator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getPredicateEvaluator(prunedStatsSchema, filterToEval),
            "Get the predicate evaluator for data skipping with schema=%s and filter=%s",
            prunedStatsSchema,
            filterToEval);

    return scanFileIter.map(
        filteredScanFileBatch -> {
          ColumnVector newSelectionVector =
              wrapEngineException(
                  () ->
                      predicateEvaluator.eval(
                          DataSkippingUtils.parseJsonStats(
                              engine, filteredScanFileBatch, prunedStatsSchema),
                          filteredScanFileBatch.getSelectionVector()),
                  "Evaluating the data skipping filter %s",
                  filterToEval);

          return new FilteredColumnarBatch(
              filteredScanFileBatch.getData(), Optional.of(newSelectionVector));
        });
  }
}
