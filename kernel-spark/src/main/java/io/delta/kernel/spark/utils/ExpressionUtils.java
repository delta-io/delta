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
import scala.jdk.CollectionConverters;

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
    return convertSparkFilterToKernelPredicate(filter, true);
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
    if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      return convertValueToKernelLiteral(f.value())
          .map(l -> new Predicate("=", column(f.attribute()), l));
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      // EqualNullSafe with null value should be translated to IS_NULL
      // For non-null values, we use "=" operator.
      return f.value() == null
          ? Optional.of(new Predicate("IS_NULL", column(f.attribute())))
          : convertValueToKernelLiteral(f.value())
              .map(l -> new Predicate("=", column(f.attribute()), l));
    }
    if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      return convertValueToKernelLiteral(f.value())
          .map(l -> new Predicate(">", column(f.attribute()), l));
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      return convertValueToKernelLiteral(f.value())
          .map(l -> new Predicate(">=", column(f.attribute()), l));
    }
    if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      return convertValueToKernelLiteral(f.value())
          .map(l -> new Predicate("<", column(f.attribute()), l));
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      return convertValueToKernelLiteral(f.value())
          .map(l -> new Predicate("<=", column(f.attribute()), l));
    }
    if (filter instanceof IsNull) {
      IsNull f = (IsNull) filter;
      return Optional.of(new Predicate("IS_NULL", column(f.attribute())));
    }
    if (filter instanceof IsNotNull) {
      IsNotNull f = (IsNotNull) filter;
      return Optional.of(new Predicate("IS_NOT_NULL", column(f.attribute())));
    }
    if (filter instanceof org.apache.spark.sql.sources.And) {
      org.apache.spark.sql.sources.And f = (org.apache.spark.sql.sources.And) filter;
      Optional<Predicate> left = convertSparkFilterToKernelPredicate(f.left(), canPartialPushDown);
      Optional<Predicate> right =
          convertSparkFilterToKernelPredicate(f.right(), canPartialPushDown);
      if (left.isPresent() && right.isPresent()) {
        return Optional.of(new And(left.get(), right.get()));
      }
      if (canPartialPushDown && left.isPresent()) {
        return left;
      }
      if (canPartialPushDown && right.isPresent()) {
        return right;
      }
      return Optional.empty();
    }
    if (filter instanceof org.apache.spark.sql.sources.Or) {
      org.apache.spark.sql.sources.Or f = (org.apache.spark.sql.sources.Or) filter;
      Optional<Predicate> left = convertSparkFilterToKernelPredicate(f.left(), canPartialPushDown);
      Optional<Predicate> right =
          convertSparkFilterToKernelPredicate(f.right(), canPartialPushDown);
      // OR requires both operands to be convertible for correctness
      if (!left.isPresent() || !right.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(new Or(left.get(), right.get()));
    }
    if (filter instanceof Not) {
      Not f = (Not) filter;
      // NOT disables partial pushdown for semantic correctness
      // NOT (A AND B) cannot become NOT A - they have different meanings
      Optional<Predicate> child = convertSparkFilterToKernelPredicate(f.child(), false);
      return child.map(c -> new Predicate("NOT", c));
    }

    return Optional.empty();
  }

  /**
   * Creates a Delta Kernel Column from a Spark SQL column attribute name.
   *
   * <p>This method handles nested column references (e.g., "user.profile.name") by parsing the
   * dot-separated path into an array of field names.
   *
   * @param attribute the column attribute name, potentially dot-separated for nested fields
   * @return Delta Kernel Column object
   */
  private static Column column(String attribute) {
    scala.collection.Seq<String> seq = parseColumnPath(attribute);
    String[] parts = CollectionConverters.asJavaCollection(seq).toArray(new String[0]);
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

  private ExpressionUtils() {}
}
