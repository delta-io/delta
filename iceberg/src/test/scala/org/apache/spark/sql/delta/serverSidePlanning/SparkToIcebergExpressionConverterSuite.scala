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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.sources._
import org.scalatest.funsuite.AnyFunSuite
import shadedForDelta.org.apache.iceberg.expressions.{Expression, ExpressionUtil, Expressions}

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  // Types that support comparison (LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
  private val comparableTypes = Seq(
    // (column name, test value, label to identify test case)
    ("intCol", 42, "Int"),
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float"),
    ("decimalCol", BigDecimal("123.45"), "Decimal"),
    ("stringCol", "test", "String"),
    ("dateCol", java.sql.Date.valueOf("2023-12-31"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Timestamp"),
    ("address.intCol", 42, "Nested Int"),
    ("metadata.stringCol", "test", "Nested String")
  )

  // Types that only support equality operators (EqualTo, NotEqualTo, IsNull, IsNotNull)
  private val equalityOnlyTypes = Seq(
    ("boolCol", true, "Boolean")
  )

  private val allTypes = comparableTypes ++ equalityOnlyTypes
  private val testSchema = TestSchemas.testSchema.asStruct()

  private def assertConvert(testCases: Seq[(Filter, Option[Expression], String)]): Unit = {
    testCases.foreach { case (input, expectedOpt, description) =>
      val result = SparkToIcebergExpressionConverter.convert(input)

      expectedOpt match {
        case Some(expected) =>
          assert(result.isDefined, s"[$description] Should convert: $input")
    assert(
            ExpressionUtil.equivalent(expected, result.get, testSchema, true),
            s"[$description] Expected: $expected, got: ${result.get}"
          )
        case None =>
          assert(result.isEmpty, s"[$description] Should return None for: $input")
  }
    }
  }

  test("operations on comparable types") {
    val comparisonOperators = Seq(
      ("LessThan", // name
        (col: String, v: Any) => LessThan(col, v), // Spark filter
        (col: String, v: Any) => Expressions.lessThan(col, v)), // Expected Iceberg expression
      ("GreaterThan",
        (col: String, v: Any) => GreaterThan(col, v),
        (col: String, v: Any) => Expressions.greaterThan(col, v)),
      ("LessThanOrEqual",
        (col: String, v: Any) => LessThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.lessThanOrEqual(col, v)),
      ("GreaterThanOrEqual",
        (col: String, v: Any) => GreaterThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.greaterThanOrEqual(col, v))
    )

    // All combinations of comparable types x comparison operators
    val testCases = for {
      (col, value, typeDesc) <- comparableTypes
      (opName, sparkOp, icebergOp) <- comparisonOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("general operators on all types") {
    val generalOperators = Seq(
      ("EqualTo", // name
        (col: String, v: Any) => EqualTo(col, v), // Spark filter
        (col: String, v: Any) => Expressions.equal(col, v)), // Expected Iceberg expression
      ("NotEqualTo",
        (col: String, v: Any) => Not(EqualTo(col, v)),
        (col: String, v: Any) => Expressions.notEqual(col, v)),
      ("IsNull",
        (col: String, _: Any) => IsNull(col),
        (col: String, _: Any) => Expressions.isNull(col)),
      ("IsNotNull",
        (col: String, _: Any) => IsNotNull(col),
        (col: String, _: Any) => Expressions.notNull(col))
    )

    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (opName, sparkOp, icebergOp) <- generalOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("logical operators recursively call convert") {
    // Verify we recursively call convert() on left/right and combine with AND/OR
    val testCases = Seq(
      ( // AND of two different types
        And(
          EqualTo("intCol", 42), 
          GreaterThan("longCol", 100L)
        ), // Spark filter is AND(Equal, Greater)
       Some(
        Expressions.and(
          Expressions.equal("intCol", 42),
          Expressions.greaterThan("longCol", 100L))
        ), // Expected Iceberg expression is also AND(Equal, Greater)
       "AND with two different types"
      ),

      ( // OR of two different types
        Or(
          LessThan("doubleCol", 99.99), IsNull("stringCol")
        ),
        Some(
          Expressions.or(
            Expressions.lessThan("doubleCol", 99.99),
            Expressions.isNull("stringCol")
          )
        ), // Expected Iceberg expression is also OR(Less, IsNull)
        "OR with two different types"
      ),

      ( // AND of OR and AND (nested logical operators)
        And(
          Or(
              EqualTo("intCol", 1), EqualTo("intCol", 2)
            ),
          And(
            GreaterThan("longCol", 0L), LessThan("longCol", 100L)
          )
        ), 
        Some(
          Expressions.and(
            Expressions.or(
              Expressions.equal("intCol", 1), Expressions.equal("intCol", 2)
            ),
            Expressions.and(
              Expressions.greaterThan("longCol", 0L), Expressions.lessThan("longCol", 100L)
            )
          )
        ), 
        "Nested logical operators")
    )

    assertConvert(testCases)
  }

  test("special cases and boundary values") {
    val testCases = Seq(
      // Null comparison becomes IsNull
      (
        EqualTo("stringCol", null), // input
        Some(Expressions.isNull("stringCol")), // expected output
        "EqualTo(col, null) -> IsNull" // label to identify test case
      ),

      // Min and MaxValue boundaries
      (
        EqualTo("intCol", Int.MinValue), // input
        Some(Expressions.equal("intCol", Int.MinValue)), // expected output
        "Int.MinValue boundary" // label to identify test case
      ),
      
      (
        EqualTo("longCol", Long.MaxValue), // input
        Some(Expressions.equal("longCol", Long.MaxValue)), // expected output
        "Long.MaxValue boundary" // label to identify test case
      ),

      // NaN handling
      (
        EqualTo("doubleCol", Double.NaN), // input
        Some(Expressions.equal("doubleCol", Double.NaN: java.lang.Double)), // expected output
        "EqualTo with Double.NaN" // label to identify test case
      ),

      (
        EqualTo("floatCol", Float.NaN), // input
        Some(Expressions.equal("floatCol", Float.NaN: java.lang.Float)), // expected output
        "EqualTo with Float.NaN" // label to identify test case
      ),

      (
        Not(EqualTo("doubleCol", Double.NaN)), // input
        Some(Expressions.notEqual("doubleCol", Double.NaN: java.lang.Double)), // expected output
        "NotEqualTo with Double.NaN" // label to identify test case
      ),

      (
        Not(EqualTo("floatCol", Float.NaN)), // input
        Some(Expressions.notEqual("floatCol", Float.NaN: java.lang.Float)), // expected output
        "NotEqualTo with Float.NaN" // label to identify test case
      ),

      // Range filter (same column used twice)
      (
        And(GreaterThan("intCol", 0), LessThan("intCol", 100)), // input
        Some(Expressions.and(
          Expressions.greaterThan("intCol", 0),
          Expressions.lessThan("intCol", 100)
        )), // expected output
        "Range filter: 0 < intCol < 100" // label to identify test case
      )
    )

    assertConvert(testCases)

    // NotEqualTo with null throws exception (matches Iceberg OSS behavior)
    // In SQL 3-value logic: col <> NULL returns NULL (unknown), treated as false in WHERE
    // Iceberg's notEqual is not null-safe, so this is explicitly rejected
    val filter = Not(EqualTo("stringCol", null))
    val exception = intercept[IllegalArgumentException] {
      SparkToIcebergExpressionConverter.convert(filter)
    }
    assert(exception.getMessage.contains("NotEqualTo with null"))
  }

  // In operator requires special handling because:
  // 1. It accepts arrays of values, requiring per-element type coercion
  // 2. Null values must be filtered out (SQL semantics: col IN (1, NULL) = col IN (1))
  // 3. Empty arrays after null filtering result in always-false predicates
  // 4. Type conversion needed for each array element (Scala -> Java types)
  test("In operator with type coercion and null handling") {
    val testCases = Seq(
      // Basic In with different types
      (In("intCol", Array(1, 2, 3)),
       Some(Expressions.in("intCol", 1: Integer, 2: Integer, 3: Integer)),
       "In with integers"),

      (In("stringCol", Array("a", "b", "c")),
       Some(Expressions.in("stringCol", "a", "b", "c")),
       "In with strings"),

      (In("longCol", Array(100L, 200L)),
       Some(Expressions.in("longCol", 100L: java.lang.Long, 200L: java.lang.Long)),
       "In with longs"),

      (In("address.intCol", Array(42, 43)),
       Some(Expressions.in("address.intCol", 42: Integer, 43: Integer)),
       "In with nested column"),

      // Null handling: nulls are filtered out (SQL semantics)
      (In("stringCol", Array(null, "value1", "value2")),
       Some(Expressions.in("stringCol", "value1", "value2")),
       "In with null values (nulls filtered)"),

      (In("intCol", Array(null, 1, 2)),
       Some(Expressions.in("intCol", 1: Integer, 2: Integer)),
       "In with null and integers"),

      // Edge case: In with only null becomes empty In (always false)
      (In("stringCol", Array(null)),
       Some(Expressions.in("stringCol")),
       "In with only null")
    )

    assertConvert(testCases)
  }

  test("string operations and special filters") {
    val testCases = Seq(
      // Spark: StringStartsWith("stringCol", "prefix") → Iceberg: startsWith("stringCol", "prefix")
      (StringStartsWith("stringCol", "prefix"),
       Some(Expressions.startsWith("stringCol", "prefix")),
       "StringStartsWith"),

      // Spark: StringStartsWith("metadata.stringCol", "test") → Iceberg: startsWith("metadata.stringCol", "test")
      (StringStartsWith("metadata.stringCol", "test"),
       Some(Expressions.startsWith("metadata.stringCol", "test")),
       "StringStartsWith on nested column"),

      // Spark: AlwaysTrue() → Iceberg: alwaysTrue()
      (AlwaysTrue(),
       Some(Expressions.alwaysTrue()),
       "AlwaysTrue"),

      // Spark: AlwaysFalse() → Iceberg: alwaysFalse()
      (AlwaysFalse(),
       Some(Expressions.alwaysFalse()),
       "AlwaysFalse")
    )

    assertConvert(testCases)
  }

  test("invalid filter combinations return None") {
    // When AND/OR have one side that fails conversion, the whole expression returns None
    val validFilter = EqualTo("intCol", 42)
    val unsupportedFilter = StringEndsWith("stringCol", "suffix")

    val testCases = Seq(
      // Spark: And(validFilter, unsupportedFilter) → None
      (And(validFilter, unsupportedFilter), None,
        "AND with unsupported right side"),

      // Spark: And(unsupportedFilter, validFilter) → None
      (And(unsupportedFilter, validFilter), None,
        "AND with unsupported left side"),

      // Spark: Or(validFilter, unsupportedFilter) → None
      (Or(validFilter, unsupportedFilter), None,
        "OR with unsupported right side"),

      // Spark: Or(unsupportedFilter, validFilter) → None
      (Or(unsupportedFilter, validFilter), None,
        "OR with unsupported left side"),

      // Nested: And(valid, Or(valid, unsupported)) → None
      (And(validFilter, Or(validFilter, unsupportedFilter)), None,
        "Nested AND with unsupported in OR")
    )

    assertConvert(testCases)
  }

  test("unsupported filters return None") {
    // Filters with no Iceberg equivalent
    val unsupportedFilters = Seq(
      (StringEndsWith("stringCol", "suffix"), None, "StringEndsWith"),
      (StringContains("stringCol", "substr"), None, "StringContains"),
      (Not(LessThan("intCol", 5)), None, "Not(LessThan) - only NOT IN is supported"),
      (EqualNullSafe("intCol", 5), None, "EqualNullSafe")
    )

    assertConvert(unsupportedFilters)
  }
}
