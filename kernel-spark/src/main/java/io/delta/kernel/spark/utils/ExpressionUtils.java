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
package io.delta.kernel.spark.utils;

import static org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.InternalUtils;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

/**
 * Utility class for converting Spark SQL filter expressions to Delta Kernel predicates.
 *
 * <p>This class provides methods to convert Spark's {@link Filter} objects into Delta Kernel's
 * {@link Predicate} objects for push-down query optimization.
 *
 * <p>Note: Only expressions that can be safely converted are processed
 */
public final class ExpressionUtils {

  /**
   * Converts a Spark SQL filter to a Delta Kernel predicate.
   *
   * <p>Supported filter types:
   *
   * <ul>
   *   <li>Comparison: EqualTo, GreaterThan, LessThan, etc.
   *   <li>Null tests: IsNull, IsNotNull
   *   <li>Null-safe comparison: EqualNullSafe
   *   <li>Logical operators: And, Or, Not
   * </ul>
   *
   * @param filter the Spark SQL filter to convert
   * @return Optional containing the converted Kernel predicate, or empty if conversion is not
   *     supported
   */
  public static Optional<Predicate> convertSparkFilterToKernelPredicate(Filter filter) {
    return convertSparkFilterToKernelPredicate(filter, true /*canPartialPushDown*/);
  }

  /**
   * Converts a Spark SQL filter to a Delta Kernel predicate.
   *
   * <p>Supported filter types:
   *
   * <ul>
   *   <li>Comparison: EqualTo, GreaterThan, LessThan, etc.
   *   <li>Null tests: IsNull, IsNotNull
   *   <li>Null-safe comparison: EqualNullSafe
   *   <li>Logical operators: And, Or, Not
   * </ul>
   *
   * @param filter the Spark SQL filter to convert
   * @return ConvertedPredicate containing the converted Kernel predicate, or empty if conversion is
   *     not supported, along with a boolean indicating whether the conversion was partial
   */
  public static ConvertedPredicate convertSparkFilterToConvertedKernelPredicate(Filter filter) {
    return convertSparkFilterToConvertedKernelPredicate(filter, true /*canPartialPushDown*/);
  }

  /**
   * Converts a Spark SQL filter to a Delta Kernel predicate with partial pushdown control. When
   * canPartialPushDown is true, AND filters can be partially converted if at least one operand can
   * be converted. OR filters always require both operands to be convertible. NOT filters disable
   * partial pushdown for their child to preserve semantic correctness.
   */
  @VisibleForTesting
  static Optional<Predicate> convertSparkFilterToKernelPredicate(
      Filter filter, boolean canPartialPushDown) {
    return convertSparkFilterToConvertedKernelPredicate(filter, canPartialPushDown)
        .getConvertedPredicate();
  }

  /**
   * Converts a Spark SQL filter to a Delta Kernel predicate with partial pushdown control. When
   * canPartialPushDown is true, AND filters can be partially converted if at least one operand can
   * be converted. OR filters always require both operands to be convertible. NOT filters disable
   * partial pushdown for their child to preserve semantic correctness.
   *
   * <p>Return a ConvertedPredicate object, which contains: - Optional<Predicate>: the converted
   * Kernel predicate, or empty if conversion is not supported - boolean isPartial: indicates
   * whether the conversion was partial
   */
  static ConvertedPredicate convertSparkFilterToConvertedKernelPredicate(
      Filter filter, boolean canPartialPushDown) {
    if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      return new ConvertedPredicate(
          convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate("=", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      // EqualNullSafe with null value should be translated to IS_NULL
      // For non-null values, we use "=" operator.
      return new ConvertedPredicate(
          f.value() == null
              ? Optional.of(new Predicate("IS_NULL", kernelColumn(f.attribute())))
              : convertValueToKernelLiteral(f.value())
                  .map(l -> new Predicate("=", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      return new ConvertedPredicate(
          convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate(">", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      return new ConvertedPredicate(
          convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate(">=", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      return new ConvertedPredicate(
          convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate("<", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      return new ConvertedPredicate(
          convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate("<=", kernelColumn(f.attribute()), l)));
    }
    if (filter instanceof IsNull) {
      IsNull f = (IsNull) filter;
      return new ConvertedPredicate(
          Optional.of(new Predicate("IS_NULL", kernelColumn(f.attribute()))));
    }
    if (filter instanceof IsNotNull) {
      IsNotNull f = (IsNotNull) filter;
      return new ConvertedPredicate(
          Optional.of(new Predicate("IS_NOT_NULL", kernelColumn(f.attribute()))));
    }
    if (filter instanceof org.apache.spark.sql.sources.And) {
      org.apache.spark.sql.sources.And f = (org.apache.spark.sql.sources.And) filter;
      ConvertedPredicate left =
          convertSparkFilterToConvertedKernelPredicate(f.left(), canPartialPushDown);
      ConvertedPredicate right =
          convertSparkFilterToConvertedKernelPredicate(f.right(), canPartialPushDown);
      boolean isPartial = left.isPartial() || right.isPartial();
      if (left.isPresent() && right.isPresent()) {
        return new ConvertedPredicate(Optional.of(new And(left.get(), right.get())), isPartial);
      }
      if (canPartialPushDown && left.isPresent()) {
        return new ConvertedPredicate(left.getConvertedPredicate(), true);
      }
      if (canPartialPushDown && right.isPresent()) {
        return new ConvertedPredicate(right.getConvertedPredicate(), true);
      }
      return new ConvertedPredicate(Optional.empty(), isPartial);
    }
    if (filter instanceof org.apache.spark.sql.sources.Or) {
      org.apache.spark.sql.sources.Or f = (org.apache.spark.sql.sources.Or) filter;
      ConvertedPredicate left =
          convertSparkFilterToConvertedKernelPredicate(f.left(), canPartialPushDown);
      ConvertedPredicate right =
          convertSparkFilterToConvertedKernelPredicate(f.right(), canPartialPushDown);
      // OR requires both operands to be convertible for correctness
      boolean isPartial = left.isPartial() || right.isPartial();
      if (!left.isPresent() || !right.isPresent()) {
        return new ConvertedPredicate(Optional.empty(), isPartial);
      }
      return new ConvertedPredicate(Optional.of(new Or(left.get(), right.get())), isPartial);
    }
    if (filter instanceof Not) {
      Not f = (Not) filter;
      // NOT disables partial pushdown for semantic correctness.
      // Example: Pushing down NOT(A AND B) requires both A and B to be convertible.
      // We cannot convert it to just return NOT A if only A is convertible when B is not,
      // because:
      //
      // Original: NOT(age < 30 AND name = "John")
      //
      // Row 1: age=25, name="John"
      // Row 2: age=25, name="Mike"
      // (age < 30 AND name = "John") = (true AND true) = true
      // (age < 30 AND name = "Mike") = (true AND false) = false
      // NOT(true) = false → row 1 should be EXCLUDED
      // NOT(false) = true → row 2 should be INCLUDED

      // But if we naively push down just NOT(age < 30):
      //
      // NOT(age < 30) = NOT(true) = false → system excludes both row
      // We will return incorrect result, then.
      ConvertedPredicate child =
          convertSparkFilterToConvertedKernelPredicate(f.child(), false /*canPartialPushDown*/);
      return new ConvertedPredicate(
          child.getConvertedPredicate().map(c -> new Predicate("NOT", c)), child.isPartial());
    }

    return new ConvertedPredicate(Optional.empty());
  }

  /**
   * Creates a Delta Kernel Column from a Spark SQL column attribute name.
   *
   * <p>This method handles nested column references (e.g., "user.profile.name") by parsing the
   * dot-separated path into an array of field names using Spark's column path parser.
   *
   * <p>If a column name contains literal dots that should not be treated as field separators, it
   * must be properly quoted/escaped in the original Spark SQL. For example:
   *
   * <ul>
   *   <li>{@code `my.column.with.dots`} - treats the entire string as a single column name
   *   <li>{@code my.nested.field} - treats this as nested field access: my -> nested -> field
   * </ul>
   *
   * @param attribute the column attribute name, potentially dot-separated for nested fields
   * @return Delta Kernel Column object representing the parsed column path
   */
  private static Column kernelColumn(String attribute) {
    scala.collection.Seq<String> seq = parseColumnPath(attribute);
    String[] parts = JavaConverters.seqAsJavaList(seq).toArray(new String[0]);
    return new Column(parts);
  }

  /**
   * Converts a Java object to a Delta Kernel Literal with appropriate type inference.
   *
   * <p>This method handles the most common Java types and converts them to their corresponding
   * Delta Kernel Literal representations. The type mapping follows standard SQL data type
   * conventions.
   *
   * <p>Supported types:
   *
   * <ul>
   *   <li>Primitives: Boolean, Byte, Short, Integer, Long, Float, Double
   *   <li>BigDecimal (with precision and scale preservation)
   *   <li>String (for string literals from Spark V1 filters)
   *   <li>byte[] (binary data)
   *   <li>java.sql.Date (converted to days since epoch)
   *   <li>java.sql.Timestamp (converted to microseconds since epoch)
   * </ul>
   *
   * <p>Note: null values return empty Optional, which is correct SQL behavior for most operations.
   * Only EqualNullSafe should handle null values explicitly.
   *
   * @param value the Java object to convert
   * @return Optional containing the Delta Kernel Literal, or empty if the value is null or of an
   *     unsupported type
   */
  @VisibleForTesting
  static Optional<Literal> convertValueToKernelLiteral(Object value) {
    // TODO: convert null to NULL literal.
    if (value == null) return Optional.empty();

    if (value instanceof Boolean) {
      Boolean b = (Boolean) value;
      return Optional.of(Literal.ofBoolean(b));
    }
    if (value instanceof Byte) {
      Byte b = (Byte) value;
      return Optional.of(Literal.ofByte(b));
    }
    if (value instanceof Short) {
      Short s = (Short) value;
      return Optional.of(Literal.ofShort(s));
    }
    if (value instanceof Integer) {
      Integer i = (Integer) value;
      return Optional.of(Literal.ofInt(i));
    }
    if (value instanceof Long) {
      Long l = (Long) value;
      return Optional.of(Literal.ofLong(l));
    }
    if (value instanceof Float) {
      Float f = (Float) value;
      return Optional.of(Literal.ofFloat(f));
    }
    if (value instanceof Double) {
      Double d = (Double) value;
      return Optional.of(Literal.ofDouble(d));
    }
    if (value instanceof java.math.BigDecimal) {
      // Preserve precision and scale from the original BigDecimal
      java.math.BigDecimal bd = (java.math.BigDecimal) value;
      return Optional.of(Literal.ofDecimal(bd, bd.precision(), bd.scale()));
    }
    if (value instanceof UTF8String) {
      UTF8String s = (UTF8String) value;
      return Optional.of(Literal.ofString(s.toString()));
    }
    if (value instanceof String) {
      String s = (String) value;
      return Optional.of(Literal.ofString(s));
    }
    if (value instanceof byte[]) {
      byte[] arr = (byte[]) value;
      return Optional.of(Literal.ofBinary(arr));
    }
    if (value instanceof Date) {
      // Convert java.sql.Date to days since epoch
      Date date = (Date) value;
      return Optional.of(Literal.ofDate(InternalUtils.daysSinceEpoch(date)));
    }
    if (value instanceof Timestamp) {
      // Convert java.sql.Timestamp to microseconds since epoch
      Timestamp timestamp = (Timestamp) value;
      return Optional.of(Literal.ofTimestamp(InternalUtils.microsSinceEpoch(timestamp)));
    }

    // Unsupported type - return empty Optional to skip the conversion.
    return Optional.empty();
  }

  /*
   * Wrapper class to hold the result of converting a Spark Filter to a Kernel Predicate,
   * including a boolean indicator for whether the conversion was partial.
   */
  public static final class ConvertedPredicate {
    private final Optional<Predicate> convertedPredicate;
    private final boolean isPartial;

    public ConvertedPredicate(Optional<Predicate> convertedPredicate) {
      this.convertedPredicate = convertedPredicate;
      this.isPartial = false;
    }

    public ConvertedPredicate(Optional<Predicate> convertedPredicate, boolean isPartial) {
      this.convertedPredicate = convertedPredicate;
      this.isPartial = isPartial;
    }

    public Optional<Predicate> getConvertedPredicate() {
      return convertedPredicate;
    }

    public boolean isPartial() {
      return isPartial;
    }

    public boolean isPresent() {
      return convertedPredicate.isPresent();
    }

    public Predicate get() {
      assert convertedPredicate.isPresent();
      return convertedPredicate.get();
    }
  }

  private ExpressionUtils() {}
}
