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

  // Test data format: (columnName, testValue, description)
  // Used to generate test cases: Spark filter with columnName+testValue → Iceberg expression
  private val numericTypes = Seq(
    ("intCol", 42, "Int"),
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float"),
    ("address.intCol", 42, "Nested Int")  // Tests nested path pass-through
  )

  private val nonNumericTypes = Seq(
    ("stringCol", "test", "String"),
    ("boolCol", true, "Boolean"),
    ("decimalCol", BigDecimal("123.45"), "Decimal"),
    ("dateCol", java.sql.Date.valueOf("2023-12-31"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Timestamp"),
    ("metadata.stringCol", "test", "Nested String")  // Tests nested path pass-through
  )

  private val allTypes = numericTypes ++ nonNumericTypes
  private val testSchema = TestSchemas.comprehensiveSchemaWithNesting.asStruct()

  /**
   * Test case format: (sparkFilter, expectedIcebergExpression, description)
   * Verifies that sparkFilter converts to expectedIcebergExpression (or None if unsupported).
   */
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

  test("comparison operators on numeric types") {
    // Operator format: (
    // name,
    // sparkFilterConstructor,
    // icebergExpressionConstructor)
    val comparisonOperators = Seq(
      ("LessThan",
        (col: String, v: Any) => LessThan(col, v),           // Spark filter
        (col: String, v: Any) => Expressions.lessThan(col, v)),  // Expected Iceberg expression
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

    // Generate all combinations of numeric types x comparison operators
    val testCases = for {
      (col, value, typeDesc) <- numericTypes
      (opName, sparkOp, icebergOp) <- comparisonOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("general operators on all types") {
    // Operator format: (name, sparkFilterConstructor, icebergExpressionConstructor)
    val generalOperators = Seq(
      ("EqualTo",
        (col: String, v: Any) => EqualTo(col, v),           // Spark filter
        (col: String, v: Any) => Expressions.equal(col, v)),    // Expected Iceberg expression
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

    // Generate all combinations: 11 types x 4 operators = 44 test cases
    // Example: EqualTo("intCol", 42) -> Expressions.equal("intCol", 42)
    //          NotEqualTo("intCol", 42) -> Expressions.notEqual("intCol", 42)
    //          IsNull("stringCol") -> Expressions.isNull("stringCol")
    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (opName, sparkOp, icebergOp) <- generalOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("logical operators recursively call convert") {
    // Smoke tests: verify we recursively call convert() on left/right and combine with AND/OR
    val testCases = Seq(
      // Spark: And(EqualTo("intCol", 42), GreaterThan("longCol", 100L))
      // → Iceberg: and(equal("intCol", 42), greaterThan("longCol", 100L))
      (And(EqualTo("intCol", 42), GreaterThan("longCol", 100L)),
       Some(Expressions.and(
         Expressions.equal("intCol", 42),
         Expressions.greaterThan("longCol", 100L)
       )),
       "AND with two different types"),

      // Spark: Or(LessThan("doubleCol", 99.99), IsNull("stringCol"))
      // → Iceberg: or(lessThan("doubleCol", 99.99), isNull("stringCol"))
      (Or(LessThan("doubleCol", 99.99), IsNull("stringCol")),
       Some(Expressions.or(
         Expressions.lessThan("doubleCol", 99.99),
         Expressions.isNull("stringCol")
       )),
       "OR with two different types"),

      // Spark: And(Or(EqualTo("intCol", 1), EqualTo("intCol", 2)),
      //            And(GreaterThan("longCol", 0L), LessThan("longCol", 100L)))
      // → Iceberg: and(or(equal("intCol", 1), equal("intCol", 2)),
      //                and(greaterThan("longCol", 0L), lessThan("longCol", 100L)))
      (And(Or(EqualTo("intCol", 1), EqualTo("intCol", 2)),
           And(GreaterThan("longCol", 0L), LessThan("longCol", 100L))),
       Some(Expressions.and(
         Expressions.or(Expressions.equal("intCol", 1), Expressions.equal("intCol", 2)),
         Expressions.and(Expressions.greaterThan("longCol", 0L),
           Expressions.lessThan("longCol", 100L))
       )),
       "Nested logical operators")
    )

    assertConvert(testCases)
  }

  test("edge cases and boundary values") {
    val testCases = Seq(
      // Spark: EqualTo("stringCol", null) -> Iceberg: isNull("stringCol")
      // Special case: null comparison becomes IsNull
      (EqualTo("stringCol", null),
       Some(Expressions.isNull("stringCol")),
       "EqualTo(col, null) -> IsNull"),

      // Spark: EqualTo("intCol", Int.MinValue) -> Iceberg: equal("intCol", Int.MinValue)
      (EqualTo("intCol", Int.MinValue),
       Some(Expressions.equal("intCol", Int.MinValue)),
       "Int.MinValue boundary"),

      // Spark: EqualTo("longCol", Long.MaxValue) -> Iceberg: equal("longCol", Long.MaxValue)
      (EqualTo("longCol", Long.MaxValue),
       Some(Expressions.equal("longCol", Long.MaxValue)),
       "Long.MaxValue boundary"),

      // Spark: EqualTo("doubleCol", Double.NaN) -> Iceberg: equal("doubleCol", Double.NaN)
      (EqualTo("doubleCol", Double.NaN),
       Some(Expressions.equal("doubleCol", Double.NaN)),
       "Double.NaN handling"),

      // Spark: And(GreaterThan("intCol", 0), LessThan("intCol", 100))
      // -> Iceberg: and(greaterThan("intCol", 0), lessThan("intCol", 100))
      (And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
       Some(Expressions.and(
         Expressions.greaterThan("intCol", 0),
         Expressions.lessThan("intCol", 100)
       )),
       "Range filter: 0 < intCol < 100")
    )

    assertConvert(testCases)
  }

  test("In operator") {
    val testCases = Seq(
      // Spark: In("intCol", [1, 2, 3]) → Iceberg: in("intCol", 1, 2, 3)
      (In("intCol", Array(1, 2, 3)),
       Some(Expressions.in("intCol", 1: Integer, 2: Integer, 3: Integer)),
       "In with integers"),

      // Spark: In("stringCol", ["a", "b"]) → Iceberg: in("stringCol", "a", "b")
      (In("stringCol", Array("a", "b", "c")),
       Some(Expressions.in("stringCol", "a", "b", "c")),
       "In with strings"),

      // Spark: In("longCol", [100L]) → Iceberg: in("longCol", 100L)
      (In("longCol", Array(100L, 200L)),
       Some(Expressions.in("longCol", 100L: java.lang.Long, 200L: java.lang.Long)),
       "In with longs"),

      // Nested column
      (In("address.intCol", Array(42, 43)),
       Some(Expressions.in("address.intCol", 42: Integer, 43: Integer)),
       "In with nested column")
    )

    assertConvert(testCases)
  }

  test("In operator with null values") {
    // Following Iceberg OSS behavior: null values are filtered out
    val testCases = Seq(
      // Spark: In("stringCol", [null, "a", "b"]) → Iceberg: in("stringCol", "a", "b")
      // Nulls are removed
      (In("stringCol", Array(null, "value1", "value2")),
       Some(Expressions.in("stringCol", "value1", "value2")),
       "In with null values (nulls filtered out)"),

      // Spark: In("intCol", [null, 1, 2]) → Iceberg: in("intCol", 1, 2)
      (In("intCol", Array(null, 1, 2)),
       Some(Expressions.in("intCol", 1: Integer, 2: Integer)),
       "In with null and integers"),

      // Spark: In("stringCol", [null]) → Iceberg: in("stringCol") (empty)
      (In("stringCol", Array(null)),
       Some(Expressions.in("stringCol")),
       "In with only null")
    )

    assertConvert(testCases)
  }

  test("NaN handling") {
    val testCases = Seq(
      // Spark: EqualTo("doubleCol", Double.NaN) → Iceberg: equal("doubleCol", Double.NaN)
      (EqualTo("doubleCol", Double.NaN),
       Some(Expressions.equal("doubleCol", Double.NaN: java.lang.Double)),
       "EqualTo with Double.NaN"),

      // Spark: EqualTo("floatCol", Float.NaN) → Iceberg: equal("floatCol", Float.NaN)
      (EqualTo("floatCol", Float.NaN),
       Some(Expressions.equal("floatCol", Float.NaN: java.lang.Float)),
       "EqualTo with Float.NaN"),

      // Spark: NotEqualTo("doubleCol", Double.NaN) → Iceberg: notEqual("doubleCol", Double.NaN)
      (Not(EqualTo("doubleCol", Double.NaN)),
       Some(Expressions.notEqual("doubleCol", Double.NaN: java.lang.Double)),
       "NotEqualTo with Double.NaN"),

      // Spark: NotEqualTo("floatCol", Float.NaN) → Iceberg: notEqual("floatCol", Float.NaN)
      (Not(EqualTo("floatCol", Float.NaN)),
       Some(Expressions.notEqual("floatCol", Float.NaN: java.lang.Float)),
       "NotEqualTo with Float.NaN")
    )

    assertConvert(testCases)
  }

  test("NotEqualTo with null throws exception") {
    // Following Iceberg OSS behavior: NotEqualTo with null should throw
    // because it's always false in SQL (3-value logic)
    val filter = Not(EqualTo("stringCol", null))
    val exception = intercept[IllegalArgumentException] {
      SparkToIcebergExpressionConverter.convert(filter)
    }
    assert(exception.getMessage.contains("NotEqualTo with null"))
  }

  test("invalid filter combinations return None") {
    // When AND/OR have one side that fails conversion, the whole expression returns None
    val validFilter = EqualTo("intCol", 42)
    val unsupportedFilter = StringStartsWith("stringCol", "prefix")

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
    // These Spark filters have no Iceberg equivalent and should return None
    val unsupportedFilters = Seq(
      (StringStartsWith("stringCol", "prefix"), None, "StringStartsWith"),  // Spark only
      (StringEndsWith("stringCol", "suffix"), None, "StringEndsWith"),      // Spark only
      (StringContains("stringCol", "substr"), None, "StringContains"),      // Spark only
      (Not(LessThan("intCol", 5)), None, "Not(LessThan)"),                 // Spark only
      (EqualNullSafe("intCol", 5), None, "EqualNullSafe"),                 // Spark only
      (AlwaysTrue(), None, "AlwaysTrue"),                                   // Spark only
      (AlwaysFalse(), None, "AlwaysFalse")                                  // Spark only
    )

    assertConvert(unsupportedFilters)
  }
}
