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
  // Flat fields
  private val flatNumericTypes = Seq(
    ("intCol", 42, "Int"),
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float")
  )

  private val flatNonNumericTypes = Seq(
    ("stringCol", "test", "String"),
    ("boolCol", true, "Boolean"),
    ("decimalCol", BigDecimal("123.45"), "Decimal"),
    ("dateCol", java.sql.Date.valueOf("2023-12-31"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Timestamp")
  )

  // 2-level nested fields
  private val nested2LevelNumeric = Seq(
    ("address.intCol", 42, "Nested2 Int"),
    ("address.longCol", 100L, "Nested2 Long"),
    ("address.doubleCol", 99.99, "Nested2 Double"),
    ("address.floatCol", 10.5f, "Nested2 Float")
  )

  private val nested2LevelNonNumeric = Seq(
    ("metadata.stringCol", "test", "Nested2 String"),
    ("metadata.boolCol", true, "Nested2 Boolean"),
    ("metadata.decimalCol", BigDecimal("123.45"), "Nested2 Decimal"),
    ("metadata.dateCol", java.sql.Date.valueOf("2023-12-31"), "Nested2 Date"),
    ("metadata.timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Nested2 Timestamp")
  )

  // 3-level nested fields
  private val nested3LevelNumeric = Seq(
    ("outer.inner.intCol", 42, "Nested3 Int"),
    ("outer.inner.longCol", 100L, "Nested3 Long"),
    ("outer.inner.doubleCol", 99.99, "Nested3 Double"),
    ("outer.inner.floatCol", 10.5f, "Nested3 Float")
  )

  private val nested3LevelNonNumeric = Seq(
    ("outer.inner.stringCol", "test", "Nested3 String"),
    ("outer.inner.boolCol", true, "Nested3 Boolean"),
    ("outer.inner.decimalCol", BigDecimal("123.45"), "Nested3 Decimal"),
    ("outer.inner.dateCol", java.sql.Date.valueOf("2023-12-31"), "Nested3 Date"),
    ("outer.inner.timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Nested3 Timestamp")
  )

  // Combine all types
  private val numericTypes = flatNumericTypes ++ nested2LevelNumeric ++ nested3LevelNumeric
  private val nonNumericTypes = flatNonNumericTypes ++ nested2LevelNonNumeric ++ nested3LevelNonNumeric
  private val allTypes = numericTypes ++ nonNumericTypes

  // Test schema used to bind expressions for semantic comparison
  private val testSchema = TestSchemas.superSchema.asStruct()

  // Pre-generated test data for reuse across multiple tests
  private val equalToTests = allTypes.map { case (col, value, desc) =>
    (EqualTo(col, value), Some(Expressions.equal(col, value)), s"EqualTo $desc")
  }

  private val lessThanTests = numericTypes.map { case (col, value, desc) =>
    (LessThan(col, value), Some(Expressions.lessThan(col, value)), s"LessThan $desc")
  }

  private val greaterThanTests = numericTypes.map { case (col, value, desc) =>
    (GreaterThan(col, value), Some(Expressions.greaterThan(col, value)), s"GreaterThan $desc")
  }

  private val lessThanOrEqualTests = numericTypes.map { case (col, value, desc) =>
    (LessThanOrEqual(col, value), Some(Expressions.lessThanOrEqual(col, value)),
      s"LessThanOrEqual $desc")
  }

  private val greaterThanOrEqualTests = numericTypes.map { case (col, value, desc) =>
    (GreaterThanOrEqual(col, value), Some(Expressions.greaterThanOrEqual(col, value)),
      s"GreaterThanOrEqual $desc")
  }

  private val isNullTests = allTypes.map { case (col, _, desc) =>
    (IsNull(col), Some(Expressions.isNull(col)), s"IsNull $desc")
  }

  private val isNotNullTests = allTypes.map { case (col, _, desc) =>
    (IsNotNull(col), Some(Expressions.notNull(col)), s"IsNotNull $desc")
  }

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

  // Cartesian product helper
  private def cartesianProduct[T, U](seqA: Seq[T], seqB: Seq[U]): Seq[(T, U)] = {
    for { a <- seqA; b <- seqB } yield (a, b)
  }

  // Unique pairs helper (reuses cartesianProduct)
  private def uniquePairs[T](seq: Seq[T]): Seq[(T, T)] = {
    cartesianProduct(seq, seq).zipWithIndex.collect {
      case ((a, b), _) if seq.indexOf(a) < seq.indexOf(b) => (a, b)
    }
  }

  // ========================================
  // TEST 1: General Operators on All Types
  // ========================================
  test("convert general operators on all types") {
    assertConvert(equalToTests ++ isNullTests ++ isNotNullTests)
  }

  // ========================================
  // TEST 2: Numeric Comparison Operators
  // ========================================
  test("convert numeric comparison operators") {
    assertConvert(lessThanTests ++ greaterThanTests ++
      lessThanOrEqualTests ++ greaterThanOrEqualTests)
  }

  // ========================================
  // TEST 3: Logical Operators (All Pairs + Full Cartesian Nesting)
  // ========================================
  test("convert logical operators with diverse type pairs") {
    // Get first element from each operator type (5 total)
    val representatives = Seq(
      equalToTests.head,
      greaterThanTests.head,
      lessThanTests.head,
      greaterThanOrEqualTests.head,
      lessThanOrEqualTests.head
    )

    // Generate all unique pairs for And/Or
    val andTests = uniquePairs(representatives).map { case ((lf, le, ld), (rf, re, rd)) =>
      (And(lf, rf), Some(Expressions.and(le.get, re.get)), s"And($ld, $rd)")
    }

    val orTests = uniquePairs(representatives).map { case ((lf, le, ld), (rf, re, rd)) =>
      (Or(lf, rf), Some(Expressions.or(le.get, re.get)), s"Or($ld, $rd)")
  }

    // Full cartesian product of all And × all Or for nested tests
    val nestedTests = cartesianProduct(andTests, orTests).map {
      case ((lf, le, ld), (rf, re, rd)) =>
        (And(lf, rf), Some(Expressions.and(le.get, re.get)), s"Nested: And($ld, $rd)")
    }

    assertConvert(andTests ++ orTests ++ nestedTests)
  }

  // ========================================
  // TEST 4: Edge Cases (manual, high-value)
  // ========================================
  test("convert edge cases and special scenarios") {
    val edgeCases = Seq(
      (EqualTo("stringCol", null), Some(Expressions.isNull("stringCol")),
        "EqualTo(col, null) → IsNull"),
      (EqualTo("intCol", Int.MinValue), Some(Expressions.equal("intCol", Int.MinValue)),
        "Int.MinValue boundary"),
      (EqualTo("longCol", Long.MaxValue), Some(Expressions.equal("longCol", Long.MaxValue)),
        "Long.MaxValue boundary"),
      (And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
       Some(Expressions.and(Expressions.greaterThan("intCol", 0),
         Expressions.lessThan("intCol", 100))),
       "Range filter: 0 < intCol < 100"),
      (EqualTo("doubleCol", Double.NaN), Some(Expressions.equal("doubleCol", Double.NaN)),
        "Double.NaN handling")
    )

    assertConvert(edgeCases)
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
