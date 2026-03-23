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

package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
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
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), filterName + " filter should be converted");
    assertFalse(result.isPartial(), filterName + " filter should be fully converted");
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
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), filterName + " filter should be converted");
    assertFalse(result.isPartial(), filterName + " filter should be fully converted");
    assertEquals(expectedOperator, result.get().getName());
    assertEquals(1, result.get().getChildren().size());
  }

  @Test
  public void testEqualNullSafeFilter() {
    // Test EqualNullSafe with null value - converted to IS_NULL
    // Cannot use IS NOT DISTINCT FROM because kernel requires typed null literals
    EqualNullSafe nullFilter = new EqualNullSafe("name", null);
    ExpressionUtils.ConvertedPredicate nullResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nullFilter);

    assertTrue(nullResult.isPresent(), "EqualNullSafe with null should be converted");
    assertFalse(nullResult.isPartial(), "EqualNullSafe with null should be fully converted");
    assertEquals("IS_NULL", nullResult.get().getName());
    assertEquals(1, nullResult.get().getChildren().size());

    // Test EqualNullSafe with non-null value - uses "=" operator
    EqualNullSafe nonNullFilter = new EqualNullSafe("id", 42);
    ExpressionUtils.ConvertedPredicate nonNullResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nonNullFilter);

    assertTrue(nonNullResult.isPresent(), "EqualNullSafe with value should be converted");
    assertFalse(nonNullResult.isPartial(), "EqualNullSafe with value should be fully converted");
    assertEquals("=", nonNullResult.get().getName());
    assertEquals(2, nonNullResult.get().getChildren().size());
  }

  static Stream<Arguments> stringStartsWithProvider() {
    return Stream.of(Arguments.of("non-empty prefix", "Al"), Arguments.of("empty prefix", ""));
  }

  @ParameterizedTest(name = "StringStartsWith with {0} should be converted")
  @MethodSource("stringStartsWithProvider")
  public void testStringStartsWithFilter(String desc, String prefix) {
    StringStartsWith filter = new StringStartsWith("name", prefix);
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "StringStartsWith filter should be converted");
    assertFalse(result.isPartial(), "StringStartsWith filter should be fully converted");
    assertEquals("STARTS_WITH", result.get().getName());
    // Children: column + string literal
    assertEquals(2, result.get().getChildren().size());
  }

  @Test
  public void testStringStartsWithFilter_NullValue() {
    // A null prefix cannot be converted — treated as unsupported, falls back to post-scan
    StringStartsWith filter = new StringStartsWith("name", null);
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertFalse(result.isPresent(), "StringStartsWith with null value should not be converted");
  }

  @Test
  public void testInFilter_BasicConversion() {
    In filter = new In("city", new Object[] {"hz", "sh", "bj"});
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "In filter should be converted");
    assertFalse(result.isPartial(), "In filter should be fully converted");
    assertEquals("IN", result.get().getName());
    // Children: column + 3 literals
    assertEquals(4, result.get().getChildren().size());
  }

  @Test
  public void testInFilter_SingleValue() {
    In filter = new In("id", new Object[] {42});
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "In filter with single value should be converted");
    assertFalse(result.isPartial(), "In filter should be fully converted");
    assertEquals("IN", result.get().getName());
    // Children: column + 1 literal
    assertEquals(2, result.get().getChildren().size());
  }

  @Test
  public void testInFilter_EmptyValues() {
    In filter = new In("city", new Object[] {});
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    // Empty IN list always evaluates to FALSE; push ALWAYS_FALSE so the kernel skips all files.
    assertTrue(result.isPresent(), "In filter with empty values should push ALWAYS_FALSE");
    assertEquals("ALWAYS_FALSE", result.get().getName());
  }

  @Test
  public void testInFilter_WithNullValue() {
    // null in the values array makes the IN expression unsafe to push down (SQL null semantics)
    In filter = new In("city", new Object[] {"hz", null, "bj"});
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertFalse(result.isPresent(), "In filter with null value should not be pushed down");
  }

  @Test
  public void testInFilter_WithUnsupportedType() {
    In filter = new In("col", new Object[] {42, new Object()});
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertFalse(result.isPresent(), "In filter with unconvertible value should not be pushed down");
  }

  @Test
  public void testInFilter_InAndFilter() {
    // AND(In(...), EqualTo(...)) — both convertible, should be fully pushed down
    In inFilter = new In("city", new Object[] {"hz", "sh"});
    EqualTo eqFilter = new EqualTo("part", 1);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(inFilter, eqFilter);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter);

    assertTrue(result.isPresent(), "AND(In, EqualTo) should be converted");
    assertFalse(result.isPartial(), "AND(In, EqualTo) should be fully converted");
    assertTrue(
        result.get() instanceof io.delta.kernel.expressions.And,
        "Result should be an AND predicate");
  }

  @Test
  public void testInFilter_NotInWithNullValue() {
    // NOT(IN(col, 1, null)): null in the IN list causes IN to bail → NOT also bails
    In inFilter = new In("city", new Object[] {"hz", null});
    Not notFilter = new Not(inFilter);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter);

    assertFalse(
        result.isPresent(), "NOT(IN(..., null)) should not be pushed down due to null in IN list");
  }

  @Test
  public void testInFilter_ORWithUnsupportedFilter() {
    // OR(In(...), StringEndsWith(...)) — one branch unsupported, whole OR cannot be pushed
    In inFilter = new In("city", new Object[] {"hz", "sh"});
    StringEndsWith endsWithFilter = new StringEndsWith("name", "foo");
    Or orFilter = new Or(inFilter, endsWithFilter);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(orFilter);

    assertFalse(
        result.isPresent(),
        "OR(In, unsupported) should not be pushed down when one branch is unsupported");
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
  public void testAlwaysTrueFilter() {
    Filter filter = new org.apache.spark.sql.sources.AlwaysTrue();
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "AlwaysTrue should be converted");
    assertFalse(result.isPartial());
    assertEquals("ALWAYS_TRUE", result.get().getName());
  }

  @Test
  public void testAlwaysFalseFilter() {
    Filter filter = new org.apache.spark.sql.sources.AlwaysFalse();
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "AlwaysFalse should be converted");
    assertFalse(result.isPartial());
    assertEquals("ALWAYS_FALSE", result.get().getName());
  }

  @Test
  public void testUnsupportedFilter() {
    // Create an unsupported filter (StringContains is not implemented in our conversion method)
    Filter unsupportedFilter = new StringContains("col1", "test");

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(unsupportedFilter);
    assertFalse(result.isPresent(), "Unsupported filters should return empty Optional");
    assertFalse(result.isPartial(), "Unsupported filters should not be marked as partial");
  }

  @Test
  public void testAndFilter() {
    EqualTo leftFilter = new EqualTo("id", 1);
    GreaterThan rightFilter = new GreaterThan("age", 18);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(leftFilter, rightFilter);

    ExpressionUtils.ConvertedPredicate andResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter);

    assertTrue(andResult.isPresent(), "And filter should be converted");
    assertFalse(andResult.isPartial(), "And filter should be fully converted");
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
    ExpressionUtils.ConvertedPredicate resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, false);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "AND filter with unconvertible operand should return empty without partial pushdown");
    assertFalse(resultWithoutPartial.isPartial(), "Empty result should not be marked as partial");

    // With partial pushdown - should return the convertible part
    ExpressionUtils.ConvertedPredicate resultWithPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, true);
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
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, true);
    assertTrue(resultWithPartial.isPresent(), "AND filter should return the convertible operand");
    assertTrue(resultWithPartial.isPartial(), "AND filter should be marked as partial");
    assertEquals(">", resultWithPartial.get().getName());
    assertEquals(2, resultWithPartial.get().getChildren().size());

    // Without partial pushdown - should return empty
    ExpressionUtils.ConvertedPredicate resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, false);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "AND filter should return empty without partial pushdown");
    assertFalse(resultWithoutPartial.isPartial(), "Empty result should not be marked as partial");
  }

  @Test
  public void testAndFilter_PartialPushDown_BothUnconvertible() {
    // Create an AND filter where neither side can be converted
    Filter unsupportedLeftFilter = new StringContains("unsupported_col1", "test");
    Filter unsupportedRightFilter = new StringContains("unsupported_col2", "test");
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(unsupportedLeftFilter, unsupportedRightFilter);

    ExpressionUtils.ConvertedPredicate resultWithPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter);
    assertFalse(
        resultWithPartial.isPresent(),
        "AND filter should return empty if both operands are unconvertible");
    assertFalse(resultWithPartial.isPartial(), "Empty result should not be marked as partial");
  }

  @Test
  public void testOrFilter_RequiresBothConvertible() {
    // Create an OR filter where left can be converted but right cannot
    EqualTo leftFilter = new EqualTo("id", 1);
    Filter unsupportedRightFilter = new StringContains("unsupported_col", "test");

    org.apache.spark.sql.sources.Or orFilter =
        new org.apache.spark.sql.sources.Or(leftFilter, unsupportedRightFilter);

    ExpressionUtils.ConvertedPredicate resultWithPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(orFilter);
    assertFalse(
        resultWithPartial.isPresent(),
        "OR filter with unconvertible operand should return empty even with partial pushdown");
    assertFalse(resultWithPartial.isPartial(), "Empty result should not be marked as partial");
  }

  @Test
  public void testNotFilter() {
    EqualTo leftFilter = new EqualTo("id", 1);
    Not notFilter = new Not(leftFilter);

    ExpressionUtils.ConvertedPredicate notResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter);

    assertTrue(notResult.isPresent(), "Not filter should be converted");
    assertFalse(notResult.isPartial(), "Not filter should be fully converted");
    assertEquals("NOT", notResult.get().getName());
    assertEquals(1, notResult.get().getChildren().size());
  }

  @Test
  public void testNotFilter_RequiresChildConvertible() {
    // StringContains is not yet supported
    Filter unsupportedFilter = new StringContains("unsupported_col", "test");

    Not notFilter = new Not(unsupportedFilter);

    // NOT requires child to be convertible.
    ExpressionUtils.ConvertedPredicate resultWithoutPartial =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter);
    assertFalse(
        resultWithoutPartial.isPresent(),
        "NOT filter with unconvertible child should return empty");
    assertFalse(resultWithoutPartial.isPartial(), "Empty result should not be marked as partial");

    // Create NOT(A AND B) where A is convertible but B is not
    // This tests that NOT disables partial pushdown for semantic correctness
    EqualTo convertibleFilter = new EqualTo("id", 1);
    org.apache.spark.sql.sources.And andFilter =
        new org.apache.spark.sql.sources.And(convertibleFilter, unsupportedFilter);

    // Now verify that NOT(AND) returns empty because NOT disables partial pushdown
    Not notAndFilter = new Not(andFilter);
    ExpressionUtils.ConvertedPredicate notResult =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notAndFilter);
    assertFalse(
        notResult.isPresent(),
        "NOT(A AND B) should return empty when B is unconvertible, even with partial pushdown enabled"
            + " - this preserves semantic correctness as NOT(A AND B) != NOT(A)");
    assertFalse(notResult.isPartial(), "Empty result should not be marked as partial");
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

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(nestedFieldFilter);

    assertTrue(result.isPresent(), "Nested field filter should be convertible");
    assertFalse(result.isPartial(), "Nested field filter should be fully convertible");
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

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(singleColumnFilter);

    assertTrue(result.isPresent(), "Single column filter should be convertible");
    assertFalse(result.isPartial(), "Single column filter should be fully convertible");
    Predicate predicate = result.get();
    io.delta.kernel.expressions.Column column =
        (io.delta.kernel.expressions.Column) predicate.getChildren().get(0);
    assertArrayEquals(
        new String[] {"user.profile.name"},
        column.getNames(),
        "Single column name with dots should be preserved as-is");
  }

  // Tests for dsv2PredicateToCatalystExpression

  private final org.apache.spark.sql.types.StructType testSchema =
      new org.apache.spark.sql.types.StructType()
          .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, false)
          .add("name", org.apache.spark.sql.types.DataTypes.StringType, true)
          .add("age", org.apache.spark.sql.types.DataTypes.IntegerType, true);

  @Test
  public void testDsv2PredicateToCatalystExpression_IsNull() {
    NamedReference nameRef = FieldReference.apply("name");
    org.apache.spark.sql.connector.expressions.filter.Predicate isNullPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "IS_NULL", new org.apache.spark.sql.connector.expressions.Expression[] {nameRef});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(isNullPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.IsNull,
        "Result should be IsNull expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_IsNotNull() {
    NamedReference nameRef = FieldReference.apply("name");
    org.apache.spark.sql.connector.expressions.filter.Predicate isNotNullPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "IS_NOT_NULL", new org.apache.spark.sql.connector.expressions.Expression[] {nameRef});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(isNotNullPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.IsNotNull,
        "Result should be IsNotNull expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_EqualTo() {
    NamedReference idRef = FieldReference.apply("id");
    LiteralValue<Integer> value =
        LiteralValue.apply(42, org.apache.spark.sql.types.DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.filter.Predicate equalToPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "=", new org.apache.spark.sql.connector.expressions.Expression[] {idRef, value});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(equalToPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.EqualTo,
        "Result should be EqualTo expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_LessThan() {
    NamedReference ageRef = FieldReference.apply("age");
    LiteralValue<Integer> value =
        LiteralValue.apply(30, org.apache.spark.sql.types.DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.filter.Predicate lessThanPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "<", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(lessThanPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.LessThan,
        "Result should be LessThan expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_GreaterThanOrEqual() {
    NamedReference ageRef = FieldReference.apply("age");
    LiteralValue<Integer> value =
        LiteralValue.apply(18, org.apache.spark.sql.types.DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.filter.Predicate greaterThanOrEqualPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            ">=", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(greaterThanOrEqualPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual,
        "Result should be GreaterThanOrEqual expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_In() {
    NamedReference idRef = FieldReference.apply("id");
    LiteralValue<Integer> val1 =
        LiteralValue.apply(1, org.apache.spark.sql.types.DataTypes.IntegerType);
    LiteralValue<Integer> val2 =
        LiteralValue.apply(2, org.apache.spark.sql.types.DataTypes.IntegerType);
    LiteralValue<Integer> val3 =
        LiteralValue.apply(3, org.apache.spark.sql.types.DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.filter.Predicate inPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "IN",
            new org.apache.spark.sql.connector.expressions.Expression[] {idRef, val1, val2, val3});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(inPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.In,
        "Result should be In expression");
    org.apache.spark.sql.catalyst.expressions.In inExpr =
        (org.apache.spark.sql.catalyst.expressions.In) result.get();
    assertEquals(3, inExpr.list().size(), "IN expression should have 3 values");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_And() {
    NamedReference ageRef = FieldReference.apply("age");
    LiteralValue<Integer> value1 =
        LiteralValue.apply(18, org.apache.spark.sql.types.DataTypes.IntegerType);
    LiteralValue<Integer> value2 =
        LiteralValue.apply(65, org.apache.spark.sql.types.DataTypes.IntegerType);

    org.apache.spark.sql.connector.expressions.filter.Predicate leftPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            ">", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value1});
    org.apache.spark.sql.connector.expressions.filter.Predicate rightPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "<", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value2});
    org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "AND",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              leftPredicate, rightPredicate
            });

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(andPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.And,
        "Result should be And expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_Or() {
    NamedReference ageRef = FieldReference.apply("age");
    LiteralValue<Integer> value1 =
        LiteralValue.apply(18, org.apache.spark.sql.types.DataTypes.IntegerType);
    LiteralValue<Integer> value2 =
        LiteralValue.apply(65, org.apache.spark.sql.types.DataTypes.IntegerType);

    org.apache.spark.sql.connector.expressions.filter.Predicate leftPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "<", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value1});
    org.apache.spark.sql.connector.expressions.filter.Predicate rightPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            ">", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value2});
    org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              leftPredicate, rightPredicate
            });

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(orPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.Or,
        "Result should be Or expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_Not() {
    NamedReference ageRef = FieldReference.apply("age");
    LiteralValue<Integer> value =
        LiteralValue.apply(18, org.apache.spark.sql.types.DataTypes.IntegerType);

    org.apache.spark.sql.connector.expressions.filter.Predicate childPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "<", new org.apache.spark.sql.connector.expressions.Expression[] {ageRef, value});
    org.apache.spark.sql.connector.expressions.filter.Predicate notPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "NOT", new org.apache.spark.sql.connector.expressions.Expression[] {childPredicate});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(notPredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.Not,
        "Result should be Not expression");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_AlwaysTrue() {
    org.apache.spark.sql.connector.expressions.filter.Predicate alwaysTruePredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "ALWAYS_TRUE", new org.apache.spark.sql.connector.expressions.Expression[] {});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(alwaysTruePredicate, testSchema);

    assertTrue(result.isPresent(), "Result should be present");
    assertTrue(
        result.get() instanceof org.apache.spark.sql.catalyst.expressions.Literal,
        "Result should be Literal expression");
    org.apache.spark.sql.catalyst.expressions.Literal literal =
        (org.apache.spark.sql.catalyst.expressions.Literal) result.get();
    assertEquals(true, literal.value(), "ALWAYS_TRUE should return literal true");
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_UnsupportedPredicate() {
    org.apache.spark.sql.connector.expressions.filter.Predicate unsupportedPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "UNSUPPORTED_OPERATOR", new org.apache.spark.sql.connector.expressions.Expression[] {});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(unsupportedPredicate, testSchema);

    assertFalse(result.isPresent(), "Unsupported predicates should return empty Optional");
  }

  // ===== Tests for decimal type alignment with table schema =====

  private final org.apache.spark.sql.types.StructType decimalSchema =
      new org.apache.spark.sql.types.StructType()
          .add("price", new org.apache.spark.sql.types.DecimalType(7, 2), true)
          .add("quantity", org.apache.spark.sql.types.DataTypes.IntegerType, true);

  @Test
  public void testDecimalLiteralWidenedToColumnType() {
    // Literal 100.00 has Decimal(5,2), column is Decimal(7,2) → should widen to Decimal(7,2)
    EqualTo filter = new EqualTo("price", new BigDecimal("100.00"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "Decimal filter should be converted with widened type");
    assertFalse(result.isPartial());
    Predicate pred = result.get();
    assertEquals("=", pred.getName());
    Literal literal = (Literal) pred.getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralScaleWidened() {
    // Literal 100 has Decimal(3,0), column is Decimal(7,2) → should widen to Decimal(7,2)
    EqualTo filter = new EqualTo("price", new BigDecimal("100"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "Decimal filter with lower scale should be widened");
    Literal literal = (Literal) result.get().getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
    assertEquals(new BigDecimal("100.00"), literal.getValue());
  }

  @Test
  public void testDecimalLiteralHigherScaleThanColumn() {
    // Literal 99.999 has scale=3, column is Decimal(7,2) → scale exceeds column, skip pushdown
    GreaterThan filter = new GreaterThan("price", new BigDecimal("99.999"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertFalse(
        result.isPresent(),
        "Decimal literal with higher scale than column should not be pushed down");
  }

  @Test
  public void testDecimalLiteralExceedsColumnPrecision() {
    // Literal 123456.00 has 6 integral digits + 2 scale = precision 8,
    // column is Decimal(7,2) which holds max 99999.99 → skip pushdown
    LessThan filter = new LessThan("price", new BigDecimal("123456.00"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertFalse(
        result.isPresent(), "Decimal literal exceeding column precision should not be pushed down");
  }

  @Test
  public void testDecimalLiteralMatchingType() {
    // Literal already matches column type Decimal(7,2) → no widening needed
    EqualTo filter = new EqualTo("price", new BigDecimal("12345.67"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "Decimal filter with matching type should be converted");
    Literal literal = (Literal) result.get().getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralWithoutSchema() {
    // Without schema, decimal literal retains its intrinsic type
    EqualTo filter = new EqualTo("price", new BigDecimal("100.00"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter);

    assertTrue(result.isPresent(), "Decimal filter without schema should still be converted");
    Literal literal = (Literal) result.get().getChildren().get(1);
    // 100.00 has precision=5, scale=2
    assertEquals(new DecimalType(5, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralNonDecimalColumn() {
    // Column "quantity" is IntegerType, not DecimalType → use default conversion
    EqualTo filter = new EqualTo("quantity", new BigDecimal("42"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "BigDecimal for non-decimal column should use default type");
    Literal literal = (Literal) result.get().getChildren().get(1);
    // Default BigDecimal conversion: precision=2, scale=0
    assertEquals(new DecimalType(2, 0), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralCaseInsensitiveColumnLookup() {
    // Filter uses "PRICE" but schema has "price" → should still widen
    EqualTo filter = new EqualTo("PRICE", new BigDecimal("100.00"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "Case-insensitive column lookup should match");
    Literal literal = (Literal) result.get().getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralColumnNotInSchema() {
    // Column "unknown" not in schema → use default conversion
    EqualTo filter = new EqualTo("unknown", new BigDecimal("100.00"));
    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "Unknown column should fall back to default conversion");
    Literal literal = (Literal) result.get().getChildren().get(1);
    assertEquals(new DecimalType(5, 2), literal.getDataType());
  }

  @Test
  public void testDecimalPartialPushDownInAndFilter() {
    // AND(price > 99.999, price < 200.00) where left has scale=3 exceeding column's scale=2.
    // Only the right side should be pushed down (partial pushdown).
    GreaterThan left = new GreaterThan("price", new BigDecimal("99.999"));
    LessThan right = new LessThan("price", new BigDecimal("200.00"));
    org.apache.spark.sql.sources.And andFilter = new org.apache.spark.sql.sources.And(left, right);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, decimalSchema);

    assertTrue(result.isPresent(), "AND filter should partially push down the valid side");
    assertTrue(result.isPartial(), "Result should be marked as partial conversion");
    // Only the right side (price < 200.00) should be pushed, not a compound AND
    Predicate pred = result.get();
    assertNotEquals(
        "AND",
        pred.getName(),
        "Left side (price > 99.999) should be dropped, not pushed as compound AND");
    assertEquals("<", pred.getName());
    Literal literal = (Literal) pred.getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
    assertEquals(new BigDecimal("200.00"), literal.getValue());
  }

  @Test
  public void testDecimalLiteralInCompoundFilter() {
    // AND(price >= 100.00, price <= 200.00) with Decimal(7,2) column
    GreaterThanOrEqual left = new GreaterThanOrEqual("price", new BigDecimal("100.00"));
    LessThanOrEqual right = new LessThanOrEqual("price", new BigDecimal("200.00"));
    org.apache.spark.sql.sources.And andFilter = new org.apache.spark.sql.sources.And(left, right);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(andFilter, decimalSchema);

    assertTrue(result.isPresent(), "AND filter with decimal operands should be converted");
    assertFalse(result.isPartial());
    // Verify both literals are widened to Decimal(7,2)
    io.delta.kernel.expressions.And andPred = (io.delta.kernel.expressions.And) result.get();
    Predicate leftPred = (Predicate) andPred.getChildren().get(0);
    Predicate rightPred = (Predicate) andPred.getChildren().get(1);
    assertEquals(new DecimalType(7, 2), ((Literal) leftPred.getChildren().get(1)).getDataType());
    assertEquals(new DecimalType(7, 2), ((Literal) rightPred.getChildren().get(1)).getDataType());
  }

  @Test
  public void testDecimalLiteralInOrFilter() {
    // OR(price >= 100.00, price >= 200.00) with Decimal(7,2) column
    GreaterThanOrEqual left = new GreaterThanOrEqual("price", new BigDecimal("100.00"));
    GreaterThanOrEqual right = new GreaterThanOrEqual("price", new BigDecimal("200.00"));
    org.apache.spark.sql.sources.Or orFilter = new org.apache.spark.sql.sources.Or(left, right);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(orFilter, decimalSchema);

    assertTrue(result.isPresent(), "OR filter with decimal operands should be converted");
  }

  @Test
  public void testDecimalLiteralInNotFilter() {
    // NOT(price = 100.00) with Decimal(7,2) column
    EqualTo eq = new EqualTo("price", new BigDecimal("100.00"));
    Not notFilter = new Not(eq);

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(notFilter, decimalSchema);

    assertTrue(result.isPresent(), "NOT filter with decimal operand should be converted");
    Predicate notPred = result.get();
    assertEquals("NOT", notPred.getName());
    Predicate innerPred = (Predicate) notPred.getChildren().get(0);
    Literal literal = (Literal) innerPred.getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralWithEqualNullSafe() {
    // EqualNullSafe(price, 100.00) with Decimal(7,2) column
    EqualNullSafe filter = new EqualNullSafe("price", new BigDecimal("100.00"));

    ExpressionUtils.ConvertedPredicate result =
        ExpressionUtils.convertSparkFilterToKernelPredicate(filter, decimalSchema);

    assertTrue(result.isPresent(), "EqualNullSafe with decimal should be converted");
    Literal literal = (Literal) result.get().getChildren().get(1);
    assertEquals(new DecimalType(7, 2), literal.getDataType());
  }

  @Test
  public void testDecimalLiteralAllComparisonOperators() {
    // Test all comparison operators widen decimals correctly
    BigDecimal value = new BigDecimal("50.00"); // Decimal(4,2) → should widen to Decimal(7,2)
    Filter[] filters =
        new Filter[] {
          new EqualTo("price", value),
          new GreaterThan("price", value),
          new GreaterThanOrEqual("price", value),
          new LessThan("price", value),
          new LessThanOrEqual("price", value),
        };
    String[] expectedOps = new String[] {"=", ">", ">=", "<", "<="};

    for (int i = 0; i < filters.length; i++) {
      ExpressionUtils.ConvertedPredicate result =
          ExpressionUtils.convertSparkFilterToKernelPredicate(filters[i], decimalSchema);
      assertTrue(result.isPresent(), expectedOps[i] + " filter should be converted");
      Literal literal = (Literal) result.get().getChildren().get(1);
      assertEquals(
          new DecimalType(7, 2),
          literal.getDataType(),
          expectedOps[i] + " should widen decimal to column type");
    }
  }

  @Test
  public void testClassifyFilterWithNullSchemaMatchesTwoArgOverload() {
    // Directly validates that classifyFilter(filter, partitionColumns, null) produces
    // the same result as the 2-arg classifyFilter(filter, partitionColumns).
    Set<String> partitionColumns = new HashSet<>();
    partitionColumns.add("dep_id");

    EqualTo filter = new EqualTo("price", new BigDecimal("100.00"));

    ExpressionUtils.FilterClassificationResult resultTwoArg =
        ExpressionUtils.classifyFilter(filter, partitionColumns);
    ExpressionUtils.FilterClassificationResult resultThreeArg =
        ExpressionUtils.classifyFilter(filter, partitionColumns, null);

    assertEquals(resultTwoArg.isKernelSupported, resultThreeArg.isKernelSupported);
    assertEquals(resultTwoArg.isPartialConversion, resultThreeArg.isPartialConversion);
    assertEquals(resultTwoArg.isDataFilter, resultThreeArg.isDataFilter);
    assertEquals(resultTwoArg.kernelPredicate, resultThreeArg.kernelPredicate);
  }

  @Test
  public void testDsv2PredicateToCatalystExpression_ColumnNotFound() {
    NamedReference invalidRef = FieldReference.apply("nonexistent_column");
    LiteralValue<Integer> value =
        LiteralValue.apply(42, org.apache.spark.sql.types.DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.filter.Predicate predicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "=", new org.apache.spark.sql.connector.expressions.Expression[] {invalidRef, value});

    Optional<org.apache.spark.sql.catalyst.expressions.Expression> result =
        ExpressionUtils.dsv2PredicateToCatalystExpression(predicate, testSchema);

    assertFalse(
        result.isPresent(), "Should return empty Optional when column is not found in schema");
  }
}
