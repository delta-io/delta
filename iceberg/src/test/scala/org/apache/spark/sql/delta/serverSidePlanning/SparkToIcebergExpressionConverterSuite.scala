/*
 * Copyright (2026) The Delta Lake Project Authors.
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

  private case class FilterTest(
    spark: Filter,
    iceberg: Option[Expression],
    label: String
  )

  // Types that support ordering operations (LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
  // Note: Spark converts Date to Int and Timestamp to Long before filters reach the converter,
  // so Date/Timestamp columns are implicitly tested via Int/Long types.
  private val orderableTypes = Seq(
    ("intCol", 42, "Int"), // (column name, test value, label to identify test case)
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float"),
    ("decimalCol", BigDecimal("123.45").bigDecimal, "Decimal"),
    ("stringCol", "test", "String"),
    ("address.intCol", 42, "Nested Int"),
    ("metadata.stringCol", "test", "Nested String")
  )

  // Types that only support equality operators (EqualTo, NotEqualTo, IsNull, IsNotNull)
  private val equalityOnlyTypes = Seq(
    ("boolCol", true, "Boolean")
  )

  private val allTypes = orderableTypes ++ equalityOnlyTypes
  private val testSchema = TestSchemas.testSchema.asStruct()

  private def assertConvert(testCases: Seq[FilterTest]): Unit = {
    testCases.foreach { tc =>
      val result = SparkToIcebergExpressionConverter.convert(tc.spark)

      tc.iceberg match {
        case Some(expected) =>
          assert(result.isDefined, s"[${tc.label}] Should convert: ${tc.spark}")
          assert(
            ExpressionUtil.equivalent(expected, result.get, testSchema, true),
            s"[${tc.label}] Expected: $expected, got: ${result.get}"
          )
        case None =>
          assert(result.isEmpty, s"[${tc.label}] Should return None for: ${tc.spark}")
      }
    }
  }

  test("operations on orderable types") {
    val comparisonOperators = Seq(
      ((col: String, v: Any) => LessThan(col, v), // Spark filter
        (col: String, v: Any) => Expressions.lessThan(col, v), // Expected Iceberg expression
        "LessThan"), // label
      ((col: String, v: Any) => GreaterThan(col, v),
        (col: String, v: Any) => Expressions.greaterThan(col, v),
        "GreaterThan"),
      ((col: String, v: Any) => LessThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.lessThanOrEqual(col, v),
        "LessThanOrEqual"),
      ((col: String, v: Any) => GreaterThanOrEqual(col, v),
        (col: String, v: Any) => Expressions.greaterThanOrEqual(col, v),
        "GreaterThanOrEqual")
    )

    // All combinations of orderable types x comparison operators
    val testCases = for {
      (col, value, typeDesc) <- orderableTypes
      (sparkOp, icebergOp, opName) <- comparisonOperators
    } yield FilterTest(sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("general operators on all types") {
    val generalOperators = Seq(
      ((col: String, v: Any) => EqualTo(col, v), // Spark filter
        (col: String, v: Any) => Expressions.equal(col, v), // Expected Iceberg expression
        "EqualTo"), // label
      ((col: String, v: Any) => Not(EqualTo(col, v)),
        (col: String, v: Any) => Expressions.notEqual(col, v),
        "NotEqualTo"),
      ((col: String, _: Any) => IsNull(col),
        (col: String, _: Any) => Expressions.isNull(col),
        "IsNull"),
      ((col: String, _: Any) => IsNotNull(col),
        (col: String, _: Any) => Expressions.notNull(col),
        "IsNotNull")
    )

    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (sparkOp, icebergOp, opName) <- generalOperators
    } yield FilterTest(sparkOp(col, value), Some(icebergOp(col, value)), s"$opName $typeDesc")

    assertConvert(testCases)
  }

  test("logical operators recursively call convert") {
    // Verify we recursively call convert() on left/right and combine with AND/OR
    val testCases = Seq(
      FilterTest(
        And(
          EqualTo("intCol", 42), 
          GreaterThan("longCol", 100L)
        ),
        Some(
          Expressions.and(
            Expressions.equal("intCol", 42),
            Expressions.greaterThan("longCol", 100L))
        ),
        "AND with two different types"
      ),

      FilterTest(
        Or(
          LessThan("doubleCol", 99.99), IsNull("stringCol")
        ),
        Some(
          Expressions.or(
            Expressions.lessThan("doubleCol", 99.99),
            Expressions.isNull("stringCol")
          )
        ),
        "OR with two different types"
      ),

      FilterTest(
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
        "Nested logical operators"
      )
    )

    assertConvert(testCases)
  }

  // IN operator requires special handling because:
  // - It accepts arrays of values, requiring per-element type coercion
  // - Null values must be filtered out (SQL semantics: col IN (1, NULL) = col IN (1))
  // - Empty arrays after null filtering result in always-false predicates
  // - Type conversion needed for each array element (Scala -> Java types)
  test("IN Operator with Type Coercion and Null Handling") {
    val testCases = Seq(
      // Basic In with different types
      FilterTest(
        In("intCol", Array(1, 2, 3)),
        Some(Expressions.in("intCol", 1: Integer, 2: Integer, 3: Integer)),
        "In with integers"
      ),

      FilterTest(
        In("stringCol", Array("a", "b", "c")),
        Some(Expressions.in("stringCol", "a", "b", "c")),
        "In with strings"
      ),

      FilterTest(
        In("longCol", Array(100L, 200L)),
        Some(Expressions.in("longCol", 100L: java.lang.Long, 200L: java.lang.Long)),
        "In with longs"
      ),

      FilterTest(
        In("address.intCol", Array(42, 43)),
        Some(Expressions.in("address.intCol", 42: Integer, 43: Integer)),
        "In with nested column"
      ),

      FilterTest(
        In("decimalCol", Array(BigDecimal("10.00").bigDecimal, BigDecimal("20.00").bigDecimal)),
        Some(Expressions.in("decimalCol", BigDecimal("10.00").bigDecimal, BigDecimal("20.00").bigDecimal)),
        "In with BigDecimal"
      ),

      // Null handling: nulls are filtered out 
      FilterTest(
        In("stringCol", Array(null, "value1", "value2")),
        Some(Expressions.in("stringCol", "value1", "value2")),
        "In with null values (nulls filtered)"
      ),

      FilterTest(
        In("intCol", Array(null, 1, 2)),
        Some(Expressions.in("intCol", 1: Integer, 2: Integer)),
        "In with null and integers"
      ),

      // Edge case: In with only null becomes empty In (always false)
      FilterTest(
        In("stringCol", Array(null)),
        Some(Expressions.in("stringCol")),
        "In with only null"
      )
    )

    assertConvert(testCases)
  }

  test("string operations and boolean literals") {
    val testCases = Seq(
      // String operations
      FilterTest(
        StringStartsWith("stringCol", "prefix"),
        Some(Expressions.startsWith("stringCol", "prefix")),
        "StringStartsWith"
      ),

      FilterTest(
        StringStartsWith("metadata.stringCol", "test"),
        Some(Expressions.startsWith("metadata.stringCol", "test")),
        "StringStartsWith on nested column"
      ),

      // Boolean literals
      FilterTest(
        AlwaysTrue(),
        Some(Expressions.alwaysTrue()),
        "AlwaysTrue"
      ),

      FilterTest(
        AlwaysFalse(),
        Some(Expressions.alwaysFalse()),
        "AlwaysFalse"
      ),

      // Logical combinations
      FilterTest(
        And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
        Some(Expressions.and(
          Expressions.greaterThan("intCol", 0), Expressions.lessThan("intCol", 100)
        )),
        "Range filter: 0 < intCol < 100"
      )
    )

    assertConvert(testCases)
  }

  test("invalid filter combinations return None") {
    // When AND/OR have one side that fails conversion, the whole expression returns None
    val validFilter = EqualTo("intCol", 42)
    val unsupportedFilter = StringEndsWith("stringCol", "suffix")

    val testCases = Seq(
      FilterTest(
        And(validFilter, unsupportedFilter),
        None,
        "AND with unsupported right side"
      ),

      FilterTest(
        And(unsupportedFilter, validFilter),
        None,
        "AND with unsupported left side"
      ),

      FilterTest(
        Or(validFilter, unsupportedFilter),
        None,
        "OR with unsupported right side"
      ),

      FilterTest(
        Or(unsupportedFilter, validFilter),
        None,
        "OR with unsupported left side"
      ),

      FilterTest(
        And(
          validFilter, Or(
            validFilter, 
            unsupportedFilter
          )
        ),
        None,
        "Nested AND with unsupported in OR"
      )
    )

    assertConvert(testCases)
  }


  test("edge cases: null, NaN, and boundaries") {
    val testCases = Seq(
      // Null handling
      FilterTest(
        EqualTo("stringCol", null),
        Some(Expressions.isNull("stringCol")),
        "EqualTo(col, null) converts to IsNull"
      ),

      FilterTest(
        Not(EqualTo("stringCol", null)),
        Some(Expressions.notNull("stringCol")),
        "NotEqualTo(col, null) converts to notNull (IS NOT NULL)"
      ),

      // NaN handling: EqualTo/NotEqualTo convert to isNaN/notNaN predicates
      FilterTest(
        EqualTo("doubleCol", Double.NaN),
        Some(Expressions.isNaN("doubleCol")),
        "EqualTo with Double.NaN converts to isNaN"
      ),

      FilterTest(
        EqualTo("floatCol", Float.NaN),
        Some(Expressions.isNaN("floatCol")),
        "EqualTo with Float.NaN converts to isNaN"
      ),

      FilterTest(
        Not(EqualTo("doubleCol", Double.NaN)),
        Some(Expressions.notNaN("doubleCol")),
        "NotEqualTo with Double.NaN converts to notNaN"
      ),

      FilterTest(
        Not(EqualTo("floatCol", Float.NaN)),
        Some(Expressions.notNaN("floatCol")),
        "NotEqualTo with Float.NaN converts to notNaN"
      ),

      // NaN with comparison operators returns None (mathematically undefined)
      FilterTest(
        LessThan("doubleCol", Double.NaN),
        None,
        "LessThan with NaN returns None"
      ),

      FilterTest(
        GreaterThan("floatCol", Float.NaN),
        None,
        "GreaterThan with NaN returns None"
      ),

      // Boundary values
      FilterTest(
        EqualTo("intCol", Int.MinValue),
        Some(Expressions.equal("intCol", Int.MinValue)),
        "Int.MinValue boundary"
      ),

      FilterTest(
        EqualTo("longCol", Long.MaxValue),
        Some(Expressions.equal("longCol", Long.MaxValue)),
        "Long.MaxValue boundary"
      )
    )

    assertConvert(testCases)
  }

  test("unsupported filters return None") {
    // Filters with no Iceberg equivalent
    val unsupportedFilters = Seq(
      FilterTest(StringEndsWith("stringCol", "suffix"), None, "StringEndsWith"),
      FilterTest(StringContains("stringCol", "substr"), None, "StringContains"),
      FilterTest(Not(LessThan("intCol", 5)), None, "Not(LessThan) - only NOT IN is supported"),
      FilterTest(EqualNullSafe("intCol", 5), None, "EqualNullSafe")
    )

    assertConvert(unsupportedFilters)
  }
}
