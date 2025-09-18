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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.spark.sql.sources.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link ExpressionUtils}. */
public class ExpressionUtilsTest {

  // Test data provider for comparison filters
  static Stream<Arguments> comparisonFiltersProvider() {
    return Stream.of(
        Arguments.of("EqualTo", (Supplier<Filter>) () -> new EqualTo("id", 42), "="),
        Arguments.of("GreaterThan", (Supplier<Filter>) () -> new GreaterThan("age", 18), ">"),
        Arguments.of(
            "GreaterThanOrEqual", (Supplier<Filter>) () -> new GreaterThanOrEqual("age", 18), ">="),
        Arguments.of("LessThan", (Supplier<Filter>) () -> new LessThan("age", 65), "<"),
        Arguments.of(
            "LessThanOrEqual", (Supplier<Filter>) () -> new LessThanOrEqual("age", 65), "<="));
  }

  @ParameterizedTest(name = "{0} filter should be converted to {2}")
  @MethodSource("comparisonFiltersProvider")
  public void testComparisonFilters(
      String filterName, Supplier<Filter> filterSupplier, String expectedOperator) {
    Filter filter = filterSupplier.get();
    Optional<Predicate> result = ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), filterName + " filter should be converted");
    assertEquals(expectedOperator, result.get().getName());
    assertEquals(2, result.get().getChildren().size());
  }

  // Test data provider for null filters
  static Stream<Arguments> nullFiltersProvider() {
    return Stream.of(
        Arguments.of("IsNull", (Supplier<Filter>) () -> new IsNull("name"), "IS_NULL"),
        Arguments.of("IsNotNull", (Supplier<Filter>) () -> new IsNotNull("name"), "IS_NOT_NULL"));
  }

  @ParameterizedTest(name = "{0} filter should be converted to {2}")
  @MethodSource("nullFiltersProvider")
  public void testNullFilters(
      String filterName, Supplier<Filter> filterSupplier, String expectedOperator) {
    Filter filter = filterSupplier.get();
    Optional<Predicate> result = ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), filterName + " filter should be converted");
    assertEquals(expectedOperator, result.get().getName());
    assertEquals(1, result.get().getChildren().size());
  }

  @Test
  public void testEqualNullSafeFilter() {
    // Test EqualNullSafe with null value - converted to IS_NULL
    // Cannot use IS NOT DISTINCT FROM because kernel requires typed null literals
    EqualNullSafe nullFilter = new EqualNullSafe("name", null);
    Optional<Predicate> nullResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nullFilter);

    assertTrue(nullResult.isPresent(), "EqualNullSafe with null should be converted");
    assertEquals("IS_NULL", nullResult.get().getName());
    assertEquals(1, nullResult.get().getChildren().size());

    // Test EqualNullSafe with non-null value - uses "=" operator
    EqualNullSafe nonNullFilter = new EqualNullSafe("id", 42);
    Optional<Predicate> nonNullResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nonNullFilter);

    assertTrue(nonNullResult.isPresent(), "EqualNullSafe with value should be converted");
    assertEquals("=", nonNullResult.get().getName());
    assertEquals(2, nonNullResult.get().getChildren().size());
  }

  // Test data provider for parameterized literal conversion tests
  static Stream<Arguments> valueTypesProvider() {
    return Stream.of(
        // Primitive types
        Arguments.of("Boolean", true, BooleanType.BOOLEAN),
        Arguments.of("Byte", (byte) 42, ByteType.BYTE),
        Arguments.of("Short", (short) 1000, ShortType.SHORT),
        Arguments.of("Integer", 12345, IntegerType.INTEGER),
        Arguments.of("Long", 123456789L, LongType.LONG),
        Arguments.of("Float", 3.14f, FloatType.FLOAT),
        Arguments.of("Double", 2.718281828, DoubleType.DOUBLE),

        // BigDecimal - precision=6, scale=3 for "123.456"
        Arguments.of("BigDecimal", new BigDecimal("123.456"), new DecimalType(6, 3)),

        // String type
        Arguments.of("String", "hello world", StringType.STRING),
        Arguments.of("UTF8String", UTF8String.fromString("hello world"), StringType.STRING),

        // Binary data
        Arguments.of("byte[]", new byte[] {1, 2, 3, 4, 5}, BinaryType.BINARY),

        // Date/time types (java.sql types for V1 Filters)
        Arguments.of("java.sql.Date", java.sql.Date.valueOf("2023-01-15"), DateType.DATE),
        Arguments.of(
            "java.sql.Timestamp",
            java.sql.Timestamp.valueOf("2023-01-15 10:30:00"),
            TimestampType.TIMESTAMP));
  }

  @Test
  public void testUnsupportedFilter() {
    // Create an unsupported filter (StringContains is not implemented in our conversion method)
    Filter unsupportedFilter = new StringContains("col1", "test");

    Optional<Predicate> result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(unsupportedFilter);
    assertFalse(result.isPresent(), "Unsupported filters should return empty Optional");
  }

  @Test
  public void testAndFilter() {
    EqualTo leftFilter = new EqualTo("id", 1);
    GreaterThan rightFilter = new GreaterThan("age", 18);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(leftFilter, rightFilter);

    Optional<Predicate> andResult = ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter);

    assertTrue(andResult.isPresent(), "And filter should be converted");
    assertTrue(
        andResult.get() instanceof io.delta.kernel.expressions.And,
        "Result should be And predicate");
    assertEquals(2, andResult.get().getChildren().size());
  }

  @Test
  public void testAndFilter_PartialPushDownWithLeftConvertible() {
    // Create an AND filter where left can be converted but right cannot
    EqualTo leftFilter = new EqualTo("id", 1);
    Filter unsupportedRightFilter = new StringContains("unsupported_col", "test");

    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(leftFilter, unsupportedRightFilter);

    // Without partial pushdown - should return empty
    Optional<Predicate> resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, false);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "AND filter with unconvertible operand should return empty without partial pushdown");

    // With partial pushdown - should return the convertible part
    ExpressionUtils.ConvertedPredicate resultWithPartial =
        ExpressionUtils.convertSparkFilterToConvertedKernelPredicate(andFilter, true);
    assertTrue(
        resultWithPartial.isPresent(),
        "AND filter with partial pushdown should return the convertible operand");
    assertTrue(
        resultWithPartial.isPartial(),
        "AND filter with partial pushdown should be marked as partial");
    assertEquals("=", resultWithPartial.get().getName());
    assertEquals(2, resultWithPartial.get().getChildren().size());
  }

  @Test
  public void testAndFilter_PartialPushDownWithRightConvertible() {
    // Create an AND filter where right can be converted but left cannot
    Filter unsupportedLeftFilter = new StringContains("unsupported_col", "test");
    GreaterThan rightFilter = new GreaterThan("age", 18);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(unsupportedLeftFilter, rightFilter);

    // With partial pushdown - should return the convertible part (right side)
    ExpressionUtils.ConvertedPredicate resultWithPartial =
        ExpressionUtils.convertSparkFilterToConvertedKernelPredicate(andFilter, true);
    assertTrue(resultWithPartial.isPresent(), "AND filter should return the convertible operand");
    assertTrue(resultWithPartial.isPartial(), "AND filter should be marked as partial");
    assertEquals(">", resultWithPartial.get().getName());
    assertEquals(2, resultWithPartial.get().getChildren().size());

    // Without partial pushdown - should return empty
    Optional<Predicate> resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, false);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "AND filter should return empty without partial pushdown");
  }

  @Test
  public void testAndFilter_PartialPushDown_BothUnconvertible() {
    // Create an AND filter where neither side can be converted
    Filter unsupportedLeftFilter = new StringContains("unsupported_col1", "test");
    Filter unsupportedRightFilter = new StringContains("unsupported_col2", "test");
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(unsupportedLeftFilter, unsupportedRightFilter);

    Optional<Predicate> resultWithPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter);
    assertFalse(
        resultWithPartial.isPresent(),
        "AND filter should return empty if both operands are unconvertible");
  }

  @Test
  public void testOrFilter_RequiresBothConvertible() {
    // Create an OR filter where left can be converted but right cannot
    EqualTo leftFilter = new EqualTo("id", 1);
    Filter unsupportedRightFilter = new StringContains("unsupported_col", "test");

    org.apache.spark.sql.sources.Or orFilter =
        new org.apache.spark.sql.sources.Or(leftFilter, unsupportedRightFilter);

    Optional<Predicate> resultWithPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(orFilter);
    assertFalse(
        resultWithPartial.isPresent(),
        "OR filter with unconvertible operand should return empty even with partial pushdown");
  }

  @Test
  public void testNotFilter() {
    EqualTo leftFilter = new EqualTo("id", 1);
    Not notFilter = new Not(leftFilter);

    Optional<Predicate> notResult = ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter);

    assertTrue(notResult.isPresent(), "Not filter should be converted");
    assertEquals("NOT", notResult.get().getName());
    assertEquals(1, notResult.get().getChildren().size());
  }

  @Test
  public void testNotFilter_RequiresChildConvertible() {
    // StringContains is not yet supported
    Filter unsupportedFilter = new StringContains("unsupported_col", "test");

    Not notFilter = new Not(unsupportedFilter);

    // NOT requires child to be convertible.
    Optional<Predicate> resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "NOT filter with unconvertible child should return empty");

    // Create NOT(A AND B) where A is convertible but B is not
    // This tests that NOT disables partial pushdown for semantic correctness
    EqualTo convertibleFilter = new EqualTo("id", 1);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(convertibleFilter, unsupportedFilter);

    // Now verify that NOT(AND) returns empty because NOT disables partial pushdown
    Not notAndFilter = new Not(andFilter);
    Optional<Predicate> notResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notAndFilter);
    assertFalse(
        notResult.isPresent(),
        "NOT(A AND B) should return empty when B is unconvertible, even with partial pushdown enabled"
            + " - this preserves semantic correctness as NOT(A AND B) != NOT(A)");
  }

  @ParameterizedTest(name = "convertValueToKernelLiteral should support {0}")
  @MethodSource("valueTypesProvider")
  public void testConvertValueToKernelLiteral(
      String typeName, Object value, DataType expectedDataType) {
    Optional<Literal> result = ExpressionUtils.convertValueToKernelLiteral(value);

    assertTrue(result.isPresent(), "Value of type " + typeName + " should be convertible");

    Literal literal = result.get();
    assertNotNull(literal, "Literal should not be null");
    assertNotNull(literal.getDataType(), "DataType should not be null");
    assertEquals(
        expectedDataType,
        literal.getDataType(),
        "DataType should match expected type for " + typeName);
  }

  @Test
  public void testConvertValueToKernelLiteral_NullValue() {
    Optional<Literal> result = ExpressionUtils.convertValueToKernelLiteral(null);
    assertFalse(result.isPresent(), "null values should return empty Optional");
  }

  @Test
  public void testConvertValueToKernelLiteral_UnsupportedType() {
    // Test with an unsupported type like a custom object
    Object unsupportedValue = new Object();
    Optional<Literal> result = ExpressionUtils.convertValueToKernelLiteral(unsupportedValue);
    assertFalse(result.isPresent(), "Unsupported types should return empty Optional");
  }

  @Test
  public void testNestedFieldParsing() {
    EqualTo nestedFieldFilter = new EqualTo("user.profile.name", "John");

    Optional<Predicate> result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nestedFieldFilter);

    assertTrue(result.isPresent(), "Nested field filter should be convertible");
    Predicate predicate = result.get();
    io.delta.kernel.expressions.Column column =
        (io.delta.kernel.expressions.Column) predicate.getChildren().get(0);
    assertArrayEquals(
        new String[] {"user", "profile", "name"},
        column.getNames(),
        "Nested field names should be parsed correctly");
  }

  @Test
  public void testSingleColumnNameWithDots() {
    EqualTo singleColumnFilter = new EqualTo("`user.profile.name`", "value");

    Optional<Predicate> result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(singleColumnFilter);

    assertTrue(result.isPresent(), "Single column filter should be convertible");
    Predicate predicate = result.get();
    io.delta.kernel.expressions.Column column =
        (io.delta.kernel.expressions.Column) predicate.getChildren().get(0);
    assertArrayEquals(
        new String[] {"user.profile.name"},
        column.getNames(),
        "Single column name with dots should be preserved as-is");
  }
}
