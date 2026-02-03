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

import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.*;
import java.util.*;
import org.apache.spark.sql.sources.*;

/**
 * An opaque predicate that evaluates Spark SQL filters on AddFile rows.
 *
 * <p><b>Architecture:</b> This class implements Kernel's {@link OpaquePredicateOp} callback
 * interface. When Kernel needs to evaluate this predicate during scan file enumeration, it calls
 * {@link #evaluateOnAddFile} with each AddFile row.
 *
 * <p><b>Evaluation Strategy:</b> Follows V1's DataSkippingReader logic:
 *
 * <ul>
 *   <li><b>Partition Filters</b>: Evaluated against {@code add.partitionValues}
 *   <li><b>Data Filters</b>: Evaluated against {@code add.stats} (minValues, maxValues, nullCount)
 * </ul>
 *
 * <p>This design allows distributed log replay (DataFrame) and data skipping (Kernel) to be cleanly
 * separated while maintaining Kernel API compatibility.
 */
public class SparkFilterOpaquePredicate implements OpaquePredicateOp {
  private final List<Filter> filters;
  private final Set<String> partitionColumns;

  /**
   * Creates a new Spark filter opaque predicate.
   *
   * @param filters Spark filters to evaluate
   * @param partitionColumns set of partition column names
   */
  public SparkFilterOpaquePredicate(List<Filter> filters, Set<String> partitionColumns) {
    this.filters = new ArrayList<>(filters);
    this.partitionColumns = new HashSet<>(partitionColumns);
  }

  @Override
  public String name() {
    return "SparkFilters";
  }

  @Override
  public Optional<Boolean> evalPredScalar(
      ScalarExpressionEvaluator evalExpr,
      DirectPredicateEvaluator evalPred,
      List<Expression> exprs,
      boolean inverted)
      throws KernelException {
    // Not implemented for Spark filters - use evaluateOnAddFile instead
    // In Rust Kernel, this would be used for partition pruning with Kernel evaluators
    return Optional.empty();
  }

  @Override
  public Optional<Boolean> evalAsDataSkippingPredicate(
      DirectDataSkippingPredicateEvaluator evaluator, List<Expression> exprs, boolean inverted) {
    // Not implemented for Spark filters - use evaluateOnAddFile instead
    // In Rust Kernel, this would be used for Parquet row group skipping
    return Optional.empty();
  }

  @Override
  public Optional<Predicate> asDataSkippingPredicate(
      IndirectDataSkippingPredicateEvaluator evaluator, List<Expression> exprs, boolean inverted) {
    // Not implemented for Spark filters - use evaluateOnAddFile instead
    // In Rust Kernel, this would transform predicates to reference stats columns
    // e.g., "price > 100" -> "stats.minValues.price > 100"
    return Optional.empty();
  }

  @Override
  public Optional<Boolean> evaluateOnAddFile(Row addFileRow) {
    try {
      // Evaluate all filters - all must pass for file to be included
      for (Filter filter : filters) {
        Optional<Boolean> result = evaluateFilter(filter, addFileRow);

        // If evaluation fails or returns false, exclude the file
        if (!result.isPresent() || !result.get()) {
          return Optional.of(false);
        }
      }

      // All filters passed
      return Optional.of(true);
    } catch (Exception e) {
      // On error, conservatively include the file
      return Optional.of(true);
    }
  }

  /**
   * Evaluates a single filter against an AddFile row. Returns Optional.empty() if evaluation is
   * uncertain.
   */
  private Optional<Boolean> evaluateFilter(Filter filter, Row addFileRow) {
    // For now, implement basic partition filtering
    // Data skipping (using stats) will be added next

    if (filter instanceof EqualTo) {
      return evaluateEqualTo((EqualTo) filter, addFileRow);
    } else if (filter instanceof GreaterThan) {
      return evaluateGreaterThan((GreaterThan) filter, addFileRow);
    } else if (filter instanceof GreaterThanOrEqual) {
      return evaluateGreaterThanOrEqual((GreaterThanOrEqual) filter, addFileRow);
    } else if (filter instanceof LessThan) {
      return evaluateLessThan((LessThan) filter, addFileRow);
    } else if (filter instanceof LessThanOrEqual) {
      return evaluateLessThanOrEqual((LessThanOrEqual) filter, addFileRow);
    } else if (filter instanceof org.apache.spark.sql.sources.And) {
      return evaluateAnd((org.apache.spark.sql.sources.And) filter, addFileRow);
    } else if (filter instanceof org.apache.spark.sql.sources.Or) {
      return evaluateOr((org.apache.spark.sql.sources.Or) filter, addFileRow);
    } else if (filter instanceof Not) {
      return evaluateNot((Not) filter, addFileRow);
    } else if (filter instanceof IsNull) {
      return evaluateIsNull((IsNull) filter, addFileRow);
    } else if (filter instanceof IsNotNull) {
      return evaluateIsNotNull((IsNotNull) filter, addFileRow);
    }

    // Unknown filter type - conservatively include file
    return Optional.of(true);
  }

  private Optional<Boolean> evaluateEqualTo(EqualTo filter, Row addFileRow) {
    String attribute = filter.attribute();
    Object value = filter.value();

    if (partitionColumns.contains(attribute)) {
      // Partition filter: check partitionValues map
      return evaluatePartitionFilter(
          attribute,
          value,
          addFileRow,
          (partValue) -> partValue != null && partValue.equals(value));
    } else {
      // Data filter: check stats (minValues/maxValues)
      return evaluateDataFilter(
          attribute,
          value,
          addFileRow,
          (min, max) -> {
            // File could contain the value if min <= value <= max
            if (min == null || max == null) return true;
            @SuppressWarnings("unchecked")
            Comparable<Object> minComp = (Comparable<Object>) min;
            @SuppressWarnings("unchecked")
            Comparable<Object> maxComp = (Comparable<Object>) max;
            return minComp.compareTo(value) <= 0 && maxComp.compareTo(value) >= 0;
          });
    }
  }

  private Optional<Boolean> evaluateGreaterThan(GreaterThan filter, Row addFileRow) {
    String attribute = filter.attribute();
    Object value = filter.value();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(
          attribute,
          value,
          addFileRow,
          (partValue) -> partValue != null && partValue.compareTo(String.valueOf(value)) > 0);
    } else {
      // Data filter: file could contain values > value if max > value
      return evaluateDataFilter(
          attribute,
          value,
          addFileRow,
          (min, max) -> {
            if (max == null) return true;
            @SuppressWarnings("unchecked")
            Comparable<Object> maxComp = (Comparable<Object>) max;
            return maxComp.compareTo(value) > 0;
          });
    }
  }

  private Optional<Boolean> evaluateGreaterThanOrEqual(GreaterThanOrEqual filter, Row addFileRow) {
    String attribute = filter.attribute();
    Object value = filter.value();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(
          attribute,
          value,
          addFileRow,
          (partValue) -> partValue != null && partValue.compareTo(String.valueOf(value)) >= 0);
    } else {
      return evaluateDataFilter(
          attribute,
          value,
          addFileRow,
          (min, max) -> {
            if (max == null) return true;
            @SuppressWarnings("unchecked")
            Comparable<Object> maxComp = (Comparable<Object>) max;
            return maxComp.compareTo(value) >= 0;
          });
    }
  }

  private Optional<Boolean> evaluateLessThan(LessThan filter, Row addFileRow) {
    String attribute = filter.attribute();
    Object value = filter.value();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(
          attribute,
          value,
          addFileRow,
          (partValue) -> partValue != null && partValue.compareTo(String.valueOf(value)) < 0);
    } else {
      return evaluateDataFilter(
          attribute,
          value,
          addFileRow,
          (min, max) -> {
            if (min == null) return true;
            @SuppressWarnings("unchecked")
            Comparable<Object> minComp = (Comparable<Object>) min;
            return minComp.compareTo(value) < 0;
          });
    }
  }

  private Optional<Boolean> evaluateLessThanOrEqual(LessThanOrEqual filter, Row addFileRow) {
    String attribute = filter.attribute();
    Object value = filter.value();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(
          attribute,
          value,
          addFileRow,
          (partValue) -> partValue != null && partValue.compareTo(String.valueOf(value)) <= 0);
    } else {
      return evaluateDataFilter(
          attribute,
          value,
          addFileRow,
          (min, max) -> {
            if (min == null) return true;
            @SuppressWarnings("unchecked")
            Comparable<Object> minComp = (Comparable<Object>) min;
            return minComp.compareTo(value) <= 0;
          });
    }
  }

  private Optional<Boolean> evaluateAnd(org.apache.spark.sql.sources.And filter, Row addFileRow) {
    Optional<Boolean> left = evaluateFilter(filter.left(), addFileRow);
    if (!left.isPresent() || !left.get()) {
      return left; // Short-circuit if left is false or uncertain
    }
    return evaluateFilter(filter.right(), addFileRow);
  }

  private Optional<Boolean> evaluateOr(org.apache.spark.sql.sources.Or filter, Row addFileRow) {
    Optional<Boolean> left = evaluateFilter(filter.left(), addFileRow);
    if (left.isPresent() && left.get()) {
      return left; // Short-circuit if left is true
    }
    Optional<Boolean> right = evaluateFilter(filter.right(), addFileRow);
    if (right.isPresent() && right.get()) {
      return right;
    }
    // Both must be evaluated - if either is uncertain, be conservative
    if (!left.isPresent() || !right.isPresent()) {
      return Optional.of(true);
    }
    return Optional.of(false);
  }

  private Optional<Boolean> evaluateNot(Not filter, Row addFileRow) {
    Optional<Boolean> child = evaluateFilter(filter.child(), addFileRow);
    if (!child.isPresent()) {
      return Optional.of(true); // Uncertain - include file
    }
    return Optional.of(!child.get());
  }

  private Optional<Boolean> evaluateIsNull(IsNull filter, Row addFileRow) {
    String attribute = filter.attribute();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(attribute, null, addFileRow, (partValue) -> partValue == null);
    } else {
      // For data filters, check if nullCount > 0
      // TODO: Implement stats-based null checking
      return Optional.of(true); // Conservative: include file
    }
  }

  private Optional<Boolean> evaluateIsNotNull(IsNotNull filter, Row addFileRow) {
    String attribute = filter.attribute();

    if (partitionColumns.contains(attribute)) {
      return evaluatePartitionFilter(attribute, null, addFileRow, (partValue) -> partValue != null);
    } else {
      // For data filters, check if nullCount < numRecords
      // TODO: Implement stats-based null checking
      return Optional.of(true); // Conservative: include file
    }
  }

  /** Evaluates a partition filter by checking the partitionValues map. */
  private Optional<Boolean> evaluatePartitionFilter(
      String attribute,
      Object value,
      Row addFileRow,
      java.util.function.Predicate<String> predicate) {
    try {
      // Access partitionValues map from AddFile row
      // AddFile schema: struct<path, partitionValues, size, modificationTime, ...>
      io.delta.kernel.data.MapValue partitionValues =
          addFileRow.getMap(addFileRow.getSchema().indexOf("partitionValues"));

      if (partitionValues == null) {
        return Optional.of(true); // No partition values - include file
      }

      // Look up the attribute in the map
      io.delta.kernel.data.ColumnVector keys = partitionValues.getKeys();
      io.delta.kernel.data.ColumnVector values = partitionValues.getValues();

      for (int i = 0; i < keys.getSize(); i++) {
        String key = keys.getString(i);
        if (key.equals(attribute)) {
          String partValue = values.isNullAt(i) ? null : values.getString(i);
          return Optional.of(predicate.test(partValue));
        }
      }

      // Attribute not found in partition values
      return Optional.of(false);
    } catch (Exception e) {
      // On error, conservatively include file
      return Optional.of(true);
    }
  }

  /** Evaluates a data filter by checking stats (minValues/maxValues). */
  private Optional<Boolean> evaluateDataFilter(
      String attribute,
      Object value,
      Row addFileRow,
      java.util.function.BiFunction<Object, Object, Boolean> predicate) {
    try {
      // Access stats from AddFile row
      // AddFile schema: struct<..., stats:struct<numRecords,minValues,maxValues,nullCount>>
      int statsIdx = addFileRow.getSchema().indexOf("stats");
      if (statsIdx < 0 || addFileRow.isNullAt(statsIdx)) {
        return Optional.of(true); // No stats - include file
      }

      Row stats = addFileRow.getStruct(statsIdx);
      if (stats == null) {
        return Optional.of(true);
      }

      // Access minValues and maxValues maps
      int minIdx = stats.getSchema().indexOf("minValues");
      int maxIdx = stats.getSchema().indexOf("maxValues");

      if (minIdx < 0 || maxIdx < 0) {
        return Optional.of(true); // Stats schema doesn't have min/max
      }

      Object minValue =
          stats.isNullAt(minIdx) ? null : getMapValue(stats.getMap(minIdx), attribute);
      Object maxValue =
          stats.isNullAt(maxIdx) ? null : getMapValue(stats.getMap(maxIdx), attribute);

      return Optional.of(predicate.apply(minValue, maxValue));
    } catch (Exception e) {
      // On error, conservatively include file
      return Optional.of(true);
    }
  }

  /** Helper to get a value from a Kernel MapValue by key. */
  private Object getMapValue(io.delta.kernel.data.MapValue map, String key) {
    if (map == null) return null;

    io.delta.kernel.data.ColumnVector keys = map.getKeys();
    io.delta.kernel.data.ColumnVector values = map.getValues();

    for (int i = 0; i < keys.getSize(); i++) {
      String mapKey = keys.getString(i);
      if (mapKey.equals(key)) {
        if (values.isNullAt(i)) return null;
        // Try to get the value as the appropriate type
        // For now, just get as string - we'll need to handle types properly
        return values.getString(i);
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SparkFilterOpaquePredicate)) return false;
    SparkFilterOpaquePredicate that = (SparkFilterOpaquePredicate) obj;
    return Objects.equals(filters, that.filters)
        && Objects.equals(partitionColumns, that.partitionColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filters, partitionColumns);
  }

  @Override
  public String toString() {
    return "SparkFilterOpaquePredicate(filters=" + filters.size() + ")";
  }
}
