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

  // Base data: define once, reuse everywhere
  // l1: Numeric types (flat + one nested example)
  private val numericTypes = Seq(
    ("intCol", 42, "Int"),
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float"),
    ("address.intCol", 42, "Nested Int")  // One nested example to prove string pass-through
  )

  // l2: Non-numeric types (flat + one nested example)
  private val nonNumericTypes = Seq(
    ("stringCol", "test", "String"),
    ("boolCol", true, "Boolean"),
    ("decimalCol", BigDecimal("123.45"), "Decimal"),
    ("dateCol", java.sql.Date.valueOf("2023-12-31"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Timestamp"),
    ("metadata.stringCol", "test", "Nested String")  // One nested example
  )

  // Combined (l1 + l2)
  private val allTypes = numericTypes ++ nonNumericTypes

  // Test schema used to bind expressions for semantic comparison
  private val testSchema = TestSchemas.comprehensiveSchemaWithNesting.asStruct()


  // Helper: assert that a sequence of test cases all convert correctly
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

  // ========================================
  // TEST 1: Comparison Operators on Numeric Types (l1 × o1)
  // ========================================
  test("comparison operators on numeric types") {
    // o1: Comparison operators
    val comparisonOperators = Seq(
      ("LessThan", (col: String, v: Any) => LessThan(col, v),
        (col: String, v: Any) => Expressions.lessThan(col, v)),
      ("GreaterThan", (col: String, v: Any) => GreaterThan(col, v),
        (col: String, v: Any) => Expressions.greaterThan(col, v)),
      ("LessThanOrEqual", (col: String, v: Any) => LessThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.lessThanOrEqual(col, v)),
      ("GreaterThanOrEqual", (col: String, v: Any) => GreaterThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.greaterThanOrEqual(col, v))
    )

    // Generate l1 × o1 test cases
    val testCases = for {
      (col, value, typeDesc) <- numericTypes
      (opName, sparkOp, icebergOp) <- comparisonOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  // ========================================
  // TEST 2: General Operators on All Types ((l1 + l2) × o2)
  // ========================================
  test("general operators on all types") {
    // o2: General operators (equality and null checks)
    val generalOperators = Seq(
      ("EqualTo", (col: String, v: Any) => EqualTo(col, v),
        (col: String, v: Any) => Expressions.equal(col, v)),
      ("IsNull", (col: String, _: Any) => IsNull(col),
        (col: String, _: Any) => Expressions.isNull(col)),
      ("IsNotNull", (col: String, _: Any) => IsNotNull(col),
        (col: String, _: Any) => Expressions.notNull(col))
    )

    // Generate (l1 + l2) × o2 test cases
    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (opName, sparkOp, icebergOp) <- generalOperators
    } yield (sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  // ========================================
  // TEST 3: Logical Operators (Smoke Tests)
  // ========================================
  test("logical operators recursively call convert") {
    // Simple smoke tests to verify we call convert() on left/right and combine with AND/OR
    val testCases = Seq(
      // Simple AND
      (And(EqualTo("intCol", 42), GreaterThan("longCol", 100L)),
       Some(Expressions.and(Expressions.equal("intCol", 42), Expressions.greaterThan("longCol", 100L))),
       "AND with two different types"),

      // Simple OR
      (Or(LessThan("doubleCol", 99.99), IsNull("stringCol")),
       Some(Expressions.or(Expressions.lessThan("doubleCol", 99.99), Expressions.isNull("stringCol"))),
       "OR with two different types"),

      // Nested AND(OR, AND)
      (And(Or(EqualTo("intCol", 1), EqualTo("intCol", 2)),
           And(GreaterThan("longCol", 0L), LessThan("longCol", 100L))),
       Some(Expressions.and(
         Expressions.or(Expressions.equal("intCol", 1), Expressions.equal("intCol", 2)),
         Expressions.and(Expressions.greaterThan("longCol", 0L), Expressions.lessThan("longCol", 100L))
       )),
       "Nested logical operators")
    )

    assertConvert(testCases)
  }

  // ========================================
  // TEST 4: Edge Cases
  // ========================================
  test("edge cases and boundary values") {
    val testCases = Seq(
      (EqualTo("stringCol", null), Some(Expressions.isNull("stringCol")),
        "EqualTo(col, null) → IsNull"),
      (EqualTo("intCol", Int.MinValue), Some(Expressions.equal("intCol", Int.MinValue)),
        "Int.MinValue boundary"),
      (EqualTo("longCol", Long.MaxValue), Some(Expressions.equal("longCol", Long.MaxValue)),
        "Long.MaxValue boundary"),
      (EqualTo("doubleCol", Double.NaN), Some(Expressions.equal("doubleCol", Double.NaN)),
        "Double.NaN handling"),
      (And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
       Some(Expressions.and(Expressions.greaterThan("intCol", 0),
         Expressions.lessThan("intCol", 100))),
       "Range filter: 0 < intCol < 100")
    )

    assertConvert(testCases)
  }

  // ========================================
  // TEST 5: Unsupported Filters
  // ========================================
  test("unsupported filters return None") {
    val unsupportedFilters = Seq(
      (StringStartsWith("stringCol", "prefix"), None, "StringStartsWith"),
      (StringEndsWith("stringCol", "suffix"), None, "StringEndsWith"),
      (StringContains("stringCol", "substr"), None, "StringContains"),
      (In("intCol", Array(1, 2, 3)), None, "In"),
      (Not(EqualTo("intCol", 5)), None, "Not"),
      (EqualNullSafe("intCol", 5), None, "EqualNullSafe"),
      (AlwaysTrue(), None, "AlwaysTrue"),
      (AlwaysFalse(), None, "AlwaysFalse")
    )

    assertConvert(unsupportedFilters)
  }
}
