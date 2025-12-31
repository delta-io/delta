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
  private val numericTypes = Seq(
    ("intCol", 42, "Int"),
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float")
  )

  private val nonNumericTypes = Seq(
    ("stringCol", "test", "String"),
    ("boolCol", true, "Boolean"),
    ("decimalCol", BigDecimal("123.45"), "Decimal"),
    ("dateCol", java.sql.Date.valueOf("2023-12-31"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "Timestamp")
  )

  private val allTypes = numericTypes ++ nonNumericTypes

  // Test schema used to bind expressions for semantic comparison
  private val testSchema = TestSchemas.comprehensiveSchema.asStruct()

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
  // TEST 1: Comparison + Null Operators (Combined)
  // ========================================
  test("convert comparison and null check operators") {
    // EqualTo on all types
    val equalToTests = allTypes.map { case (col, value, desc) =>
      (EqualTo(col, value), Some(Expressions.equal(col, value)), s"EqualTo $desc")
    }

    // Comparison operators (only on numeric types)
    val comparisonOperators = Seq(
      ("LessThan", (c: String, v: Any) => LessThan(c, v), Expressions.lessThan _),
      ("GreaterThan", (c: String, v: Any) => GreaterThan(c, v), Expressions.greaterThan _),
      ("LessThanOrEqual", (c: String, v: Any) => LessThanOrEqual(c, v),
        Expressions.lessThanOrEqual _),
      ("GreaterThanOrEqual", (c: String, v: Any) => GreaterThanOrEqual(c, v),
        Expressions.greaterThanOrEqual _)
    )

    val comparisonTests = comparisonOperators.flatMap { case (name, sparkOp, icebergOp) =>
      numericTypes.map { case (col, value, desc) =>
        (sparkOp(col, value), Some(icebergOp(col, value)), s"$name $desc")
      }
    }

    // Null checks on all types
    val nullTests = allTypes.flatMap { case (col, _, desc) =>
      Seq(
        (IsNull(col), Some(Expressions.isNull(col)), s"IsNull $desc"),
        (IsNotNull(col), Some(Expressions.notNull(col)), s"IsNotNull $desc")
      )
    }

    assertConvert(equalToTests ++ comparisonTests ++ nullTests)
  }

  // ========================================
  // TEST 2: Logical Operators (All Pairs + Full Cartesian Nesting)
  // ========================================
  test("convert logical operators with diverse type pairs") {
    // Reuse test data from Test 1 patterns
    val equalToTests = allTypes.map { case (col, value, desc) =>
      (EqualTo(col, value), Some(Expressions.equal(col, value)), s"EqualTo $desc")
    }

    val greaterThanTests = numericTypes.map { case (col, value, desc) =>
      (GreaterThan(col, value), Some(Expressions.greaterThan(col, value)), s"GreaterThan $desc")
    }

    val lessThanTests = numericTypes.map { case (col, value, desc) =>
      (LessThan(col, value), Some(Expressions.lessThan(col, value)), s"LessThan $desc")
    }

    val greaterThanOrEqualTests = numericTypes.map { case (col, value, desc) =>
      (GreaterThanOrEqual(col, value), Some(Expressions.greaterThanOrEqual(col, value)),
        s"GreaterThanOrEqual $desc")
    }

    val lessThanOrEqualTests = numericTypes.map { case (col, value, desc) =>
      (LessThanOrEqual(col, value), Some(Expressions.lessThanOrEqual(col, value)),
        s"LessThanOrEqual $desc")
    }

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
  // TEST 3: Nested Column Filters (Dynamically Generated for All Types)
  // ========================================
  test("convert filters on nested columns with all data types") {
    // Dynamically generate nested columns from base types
    val nestedNumericCols = numericTypes.map { case (col, value, desc) =>
      (s"address.$col", value, s"nested $desc in address")
    }

    val nestedNonNumericCols = nonNumericTypes.map { case (col, value, desc) =>
      (s"metadata.$col", value, s"nested $desc in metadata")
    }

    val allNestedCols = nestedNumericCols ++ nestedNonNumericCols

    // Basic operators on all nested columns
    val basicOps = allNestedCols.flatMap { case (col, value, desc) =>
      Seq(
        (EqualTo(col, value), Some(Expressions.equal(col, value)), s"EqualTo $desc"),
        (LessThan(col, value), Some(Expressions.lessThan(col, value)), s"LessThan $desc"),
        (IsNull(col), Some(Expressions.isNull(col)), s"IsNull $desc")
      )
    }

    // All unique pairs for And/Or
    val logicalOps = uniquePairs(allNestedCols).flatMap {
      case ((c1, v1, d1), (c2, v2, d2)) =>
        Seq(
          (And(EqualTo(c1, v1), EqualTo(c2, v2)),
           Some(Expressions.and(Expressions.equal(c1, v1), Expressions.equal(c2, v2))),
           s"And($d1, $d2)"),
          (Or(EqualTo(c1, v1), EqualTo(c2, v2)),
           Some(Expressions.or(Expressions.equal(c1, v1), Expressions.equal(c2, v2))),
           s"Or($d1, $d2)")
        )
    }

    // Use nestedSchema for validation
    val nestedTestSchema = TestSchemas.nestedSchema.asStruct()
    (basicOps ++ logicalOps).foreach { case (input, expectedOpt, description) =>
      val result = SparkToIcebergExpressionConverter.convert(input)
      assert(result.isDefined, s"[$description] Should convert: $input")
      assert(
        ExpressionUtil.equivalent(expectedOpt.get, result.get, nestedTestSchema, true),
        s"[$description] Expected: ${expectedOpt.get}, got: ${result.get}"
      )
    }
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
