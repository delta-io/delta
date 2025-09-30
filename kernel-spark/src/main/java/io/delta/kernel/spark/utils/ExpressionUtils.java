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
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
   * @return ConvertedPredicate containing the converted Kernel predicate, or empty if conversion is
   *     not supported, along with a boolean indicating whether the conversion was partial
   */
  public static ConvertedPredicate convertSparkFilterToKernelPredicate(Filter filter) {
    return convertSparkFilterToKernelPredicate(filter, true /*canPartialPushDown*/);
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
  @VisibleForTesting
  static ConvertedPredicate convertSparkFilterToKernelPredicate(
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
      ConvertedPredicate left = convertSparkFilterToKernelPredicate(f.left(), canPartialPushDown);
      ConvertedPredicate right = convertSparkFilterToKernelPredicate(f.right(), canPartialPushDown);
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
      ConvertedPredicate left = convertSparkFilterToKernelPredicate(f.left(), canPartialPushDown);
      ConvertedPredicate right = convertSparkFilterToKernelPredicate(f.right(), canPartialPushDown);
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
          convertSparkFilterToKernelPredicate(f.child(), false /*canPartialPushDown*/);
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

  /*
   * Helper class to hold the classification result of a Filter
   */
  public static class FilterClassificationResult {
    public final Boolean isKernelSupported;
    public final Boolean isPartialConversion;
    public final Boolean isDataFilter;
    public final Optional<Predicate> kernelPredicate;

    public FilterClassificationResult(
        Boolean isKernelSupported,
        Boolean isPartialConversion,
        Boolean isDataFilter,
        Optional<Predicate> kernelPredicate) {
      this.isKernelSupported = isKernelSupported;
      this.isPartialConversion = isPartialConversion;
      this.isDataFilter = isDataFilter;
      this.kernelPredicate = kernelPredicate;
    }
  }

  /**
   * Classifies a Spark Filter based on its convertibility to a Kernel Predicate and whether it is a
   * data filter (i.e., references non-partition columns).
   *
   * @param filter the Spark Filter to classify
   * @param partitionColumnSet a set of partition column names (in lower case) for identifying data
   *     filters
   * @return FilterClassificationResult containing:
   *     <ul>
   *       <li>isKernelSupported: true if the filter can be converted to a Kernel Predicate
   *       <li>isPartialConversion: true if the conversion was partial (for AND filters)
   *       <li>isDataFilter: true if the filter references at least one non-partition column
   *       <li>kernelPredicate: Optional containing the converted Kernel Predicate, if any
   *     </ul>
   */
  public static FilterClassificationResult classifyFilter(
      Filter filter, Set<String> partitionColumnSet) {
    // try to convert Spark filter to Kernel Predicate
    ExpressionUtils.ConvertedPredicate convertedPredicate =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    boolean isKernelSupported = convertedPredicate.isPresent();
    boolean isPartialConversion = convertedPredicate.isPartial();
    Optional<Predicate> kernelPredicate = convertedPredicate.getConvertedPredicate();

    // check if the filter is a data filter
    // A data filter is a filter that references at least one non-partition column.
    String[] refs = filter.references();
    boolean isDataFilter =
        refs != null
            && refs.length > 0
            && Arrays.stream(refs)
                .anyMatch((col -> !partitionColumnSet.contains(col.toLowerCase(Locale.ROOT))));

    return new FilterClassificationResult(
        isKernelSupported, isPartialConversion, isDataFilter, kernelPredicate);
  }

  /**
   * Converts a Spark DataSourceV2 Predicate to a Catalyst Expression for filter pushdown.
   *
   * <p>This method translates supported DSV2 predicates into their equivalent Catalyst expressions
   * using the provided schema for column resolution. Unsupported predicates default to a literal
   * true expression to avoid filtering out any data.
   *
   * <p>Supported predicates include:
   *
   * <ul>
   *   <li>Comparison: =, >, >=, <, <=
   *   <li>Null tests: IS_NULL, IS_NOT_NULL
   *   <li>Logical operators: AND, OR, NOT
   *   <li>IN operator
   * </ul>
   *
   * @param predicate the DSV2 Predicate to convert
   * @param schema the schema used for resolving column references
   * @return Catalyst Expression representing the converted predicate, or a literal true for
   *     unsupported predicates
   */
  public static Expression dsv2PredicateToCatalystExpression(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate, StructType schema) {

    String predicateName = predicate.name();
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();

    switch (predicateName) {
      case "IS_NULL":
        if (children.length == 1) {
          return new org.apache.spark.sql.catalyst.expressions.IsNull(
              resolveExpression(children[0], schema));
        }
        break;

      case "IS_NOT_NULL":
        if (children.length == 1) {
          return new org.apache.spark.sql.catalyst.expressions.IsNotNull(
              resolveExpression(children[0], schema));
        }
        break;

      case "STARTS_WITH":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.StartsWith(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "ENDS_WITH":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.EndsWith(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "CONTAINS":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.Contains(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "IN":
        if (children.length >= 2) {
          List<Expression> values = new ArrayList<>();
          for (int i = 1; i < children.length; i++) {
            values.add(resolveExpression(children[i], schema));
          }
          return new org.apache.spark.sql.catalyst.expressions.In(
              resolveExpression(children[0], schema), JavaConverters.asScalaBuffer(values).toSeq());
        }
        break;

      case "=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.EqualTo(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "<>":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.Not(
              new org.apache.spark.sql.catalyst.expressions.EqualTo(
                  resolveExpression(children[0], schema), resolveExpression(children[1], schema)));
        }
        break;

      case "<=>":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.EqualNullSafe(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "<":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.LessThan(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "<=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case ">":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.GreaterThan(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case ">=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "AND":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.And(
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema),
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[1],
                  schema));
        }
        break;

      case "OR":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.Or(
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema),
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[1],
                  schema));
        }
        break;

      case "NOT":
        if (children.length == 1) {
          return new org.apache.spark.sql.catalyst.expressions.Not(
              dsv2PredicateToCatalystExpression(
                  (org.apache.spark.sql.connector.expressions.filter.Predicate)
                      predicate.children()[0],
                  schema));
        }
        break;

      case "ALWAYS_TRUE":
        if (children.length == 0) {
          return org.apache.spark.sql.catalyst.expressions.Literal.create(
              true, org.apache.spark.sql.types.DataTypes.BooleanType);
        }
        break;

      case "ALWAYS_FALSE":
        if (children.length == 0) {
          return org.apache.spark.sql.catalyst.expressions.Literal.create(
              false, org.apache.spark.sql.types.DataTypes.BooleanType);
        }
        break;
    }

    // Default to always true for unsupported predicates
    return org.apache.spark.sql.catalyst.expressions.Literal.create(
        true, org.apache.spark.sql.types.DataTypes.BooleanType);
  }

  /**
   * Resolves a DSV2 Expression to a Catalyst Expression using the provided schema.
   *
   * <p>This method handles NamedReference and LiteralValue expressions. NamedReferences are
   * resolved to BoundReferences based on the schema, while LiteralValues are converted to Catalyst
   * Literals. Unsupported expression types default to a literal true expression.
   *
   * @param expr the DSV2 Expression to resolve
   * @param schema the schema used for resolving column references
   * @return Catalyst Expression representing the resolved expression, or a literal true for
   *     unsupported expressions
   */
  private static Expression resolveExpression(
      org.apache.spark.sql.connector.expressions.Expression expr, StructType schema) {
    if (expr instanceof NamedReference) {
      NamedReference ref = (NamedReference) expr;
      String columnName = ref.fieldNames()[0];
      int index = java.util.Arrays.asList(schema.fieldNames()).indexOf(columnName);
      if (index >= 0) {
        StructField field = schema.fields()[index];
        return new BoundReference(index, field.dataType(), field.nullable());
      }
      throw new IllegalArgumentException("Column not found: " + columnName);
    } else if (expr instanceof LiteralValue) {
      LiteralValue<?> literal = (LiteralValue<?>) expr;
      return org.apache.spark.sql.catalyst.expressions.Literal.create(
          literal.value(), literal.dataType());
    } else {
      // For unsupported expression types, return a literal true
      return org.apache.spark.sql.catalyst.expressions.Literal.create(
          true, org.apache.spark.sql.types.DataTypes.BooleanType);
    }
  }

  private ExpressionUtils() {}
}
