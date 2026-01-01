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

  private case class FilterConversionCase(
    spark: Filter,
    iceberg: Option[Expression],
    label: String
  )

  // Types that support ordering operations (LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
  // Note: Spark Filter API sends Date/Timestamp as java.sql.Date/Timestamp, but our converter
  // transforms them to Int (days since epoch) and Long (microseconds since epoch) for Iceberg.
  private val orderableTypes = Seq(
    ("intCol", 42, "Int"), // (column name, test value, label to identify test case)
    ("longCol", 100L, "Long"),
    ("doubleCol", 99.99, "Double"),
    ("floatCol", 10.5f, "Float"),
    ("decimalCol", BigDecimal("123.45").bigDecimal, "Decimal"),
    ("stringCol", "test", "String"),
    ("dateCol", java.sql.Date.valueOf("2024-01-01"), "Date"),
    ("timestampCol", java.sql.Timestamp.valueOf("2024-01-01 12:00:00"), "Timestamp"),
    ("address.intCol", 42, "Nested Int"),
    ("metadata.stringCol", "test", "Nested String")
  )

  // Types that only support equality operators (EqualTo, NotEqualTo, IsNull, IsNotNull)
  private val equalityOnlyTypes = Seq(
    ("boolCol", true, "Boolean")
  )

  private val allTypes = orderableTypes ++ equalityOnlyTypes
  private val testSchema = TestSchemas.testSchema.asStruct()

  /**
   * Convert test values to Iceberg-compatible types.
   * Date/Timestamp need to be converted to Int/Long for Iceberg expressions.
   */
  private def toIcebergValue(value: Any): Any = value match {
    case v: java.sql.Date =>
      (v.getTime / (1000L * 60 * 60 * 24)).toInt
    case v: java.sql.Timestamp =>
      v.getTime * 1000 + (v.getNanos % 1000000) / 1000
    case v => v
  }

  private def assertConvert(testCases: Seq[FilterConversionCase]): Unit = {
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
    } yield FilterConversionCase(
      sparkOp(col, value),
      Some(icebergOp(col, toIcebergValue(value))),
      s"$opName $typeDesc"
    )

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
    } yield FilterConversionCase(
      sparkOp(col, value),
      Some(icebergOp(col, toIcebergValue(value))),
      s"$opName $typeDesc"
    )

    assertConvert(testCases)
  }

  test("logical operators recursively call convert") {
    // Verify we recursively call convert() on left/right and combine with AND/OR
    val testCases = Seq(
      FilterConversionCase(
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

      FilterConversionCase(
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

      FilterConversionCase(
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
    // Helper to generate multiple test values for IN operator
    def generateInValues(value: Any): Array[Any] = value match {
      case v: Int => Array(v, v + 1, v + 2)
      case v: Long => Array(v, v + 1L, v + 2L)
      case v: Float => Array(v, v + 1.0f, v + 2.0f)
      case v: Double => Array(v, v + 1.0, v + 2.0)
      case v: String => Array(v, s"${v}_2", s"${v}_3")
      case v: java.math.BigDecimal => 
        Array(v, v.add(java.math.BigDecimal.ONE), v.add(java.math.BigDecimal.TEN))
      case v: Boolean => Array(v, !v)
      case v: java.sql.Date => 
        Array(v, new java.sql.Date(v.getTime + 86400000L)) // +1 day in millis
      case v: java.sql.Timestamp => 
        Array(v, new java.sql.Timestamp(v.getTime + 3600000L)) // +1 hour in millis
      case _ => Array(value)
    }

    // Test IN operator for all types
    val inTestCases = allTypes.map { case (col, value, typeDesc) =>
      val values = generateInValues(value)
      val icebergValues = values.map(toIcebergValue)
      FilterConversionCase(
        In(col, values),
        Some(Expressions.in(col, icebergValues: _*)),
        s"In with $typeDesc"
      )
    }

    val nullHandlingTests = Seq(
      // Null handling: nulls are filtered out 
      FilterConversionCase(
        In("stringCol", Array(null, "value1", "value2")),
        Some(Expressions.in("stringCol", "value1", "value2")),
        "In with null values (nulls filtered)"
      ),

      FilterConversionCase(
        In("intCol", Array(null, 1, 2)),
        Some(Expressions.in("intCol", 1: Integer, 2: Integer)),
        "In with null and integers"
      ),

      // Edge case: In with only null becomes empty In (always false)
      FilterConversionCase(
        In("stringCol", Array(null)),
        Some(Expressions.in("stringCol")),
        "In with only null"
      )
    )

    assertConvert(inTestCases ++ nullHandlingTests)
  }

  test("string operations and boolean literals") {
    val testCases = Seq(
      // String operations
      FilterConversionCase(
        StringStartsWith("stringCol", "prefix"),
        Some(Expressions.startsWith("stringCol", "prefix")),
        "StringStartsWith"
      ),

      FilterConversionCase(
        StringStartsWith("metadata.stringCol", "test"),
        Some(Expressions.startsWith("metadata.stringCol", "test")),
        "StringStartsWith on nested column"
      ),

      // Boolean literals
      FilterConversionCase(
        AlwaysTrue(),
        Some(Expressions.alwaysTrue()),
        "AlwaysTrue"
      ),

      FilterConversionCase(
        AlwaysFalse(),
        Some(Expressions.alwaysFalse()),
        "AlwaysFalse"
      ),

      // Logical combinations
      FilterConversionCase(
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
      FilterConversionCase(
        And(validFilter, unsupportedFilter),
        None,
        "AND with unsupported right side"
      ),

      FilterConversionCase(
        And(unsupportedFilter, validFilter),
        None,
        "AND with unsupported left side"
      ),

      FilterConversionCase(
        Or(validFilter, unsupportedFilter),
        None,
        "OR with unsupported right side"
      ),

      FilterConversionCase(
        Or(unsupportedFilter, validFilter),
        None,
        "OR with unsupported left side"
      ),

      FilterConversionCase(
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
      FilterConversionCase(
        EqualTo("stringCol", null),
        Some(Expressions.isNull("stringCol")),
        "EqualTo(col, null) converts to IsNull"
      ),

      FilterConversionCase(
        Not(EqualTo("stringCol", null)),
        Some(Expressions.notNull("stringCol")),
        "NotEqualTo(col, null) converts to notNull (IS NOT NULL)"
      ),

      // NaN handling: EqualTo/NotEqualTo convert to isNaN/notNaN predicates
      FilterConversionCase(
        EqualTo("doubleCol", Double.NaN),
        Some(Expressions.isNaN("doubleCol")),
        "EqualTo with Double.NaN converts to isNaN"
      ),

      FilterConversionCase(
        EqualTo("floatCol", Float.NaN),
        Some(Expressions.isNaN("floatCol")),
        "EqualTo with Float.NaN converts to isNaN"
      ),

      FilterConversionCase(
        Not(EqualTo("doubleCol", Double.NaN)),
        Some(Expressions.notNaN("doubleCol")),
        "NotEqualTo with Double.NaN converts to notNaN"
      ),

      FilterConversionCase(
        Not(EqualTo("floatCol", Float.NaN)),
        Some(Expressions.notNaN("floatCol")),
        "NotEqualTo with Float.NaN converts to notNaN"
      ),

      // NaN with comparison operators returns None (mathematically undefined)
      FilterConversionCase(
        LessThan("doubleCol", Double.NaN),
        None,
        "LessThan with NaN returns None"
      ),

      FilterConversionCase(
        GreaterThan("floatCol", Float.NaN),
        None,
        "GreaterThan with NaN returns None"
      ),

      // Boundary values
      FilterConversionCase(
        EqualTo("intCol", Int.MinValue),
        Some(Expressions.equal("intCol", Int.MinValue)),
        "Int.MinValue boundary"
      ),

      FilterConversionCase(
        EqualTo("longCol", Long.MaxValue),
        Some(Expressions.equal("longCol", Long.MaxValue)),
        "Long.MaxValue boundary"
      ),

      // Date/Timestamp: Spark sends java.sql types, but we convert to Int/Long for Iceberg
      FilterConversionCase(
        EqualTo("dateCol", java.sql.Date.valueOf("2024-01-01")),
        Some(Expressions.equal("dateCol", 19723: Integer)), // 2024-01-01 = 19723 days since epoch
        "Date converted to days since epoch"
      ),

      FilterConversionCase(
        EqualTo("timestampCol", java.sql.Timestamp.valueOf("2024-01-01 00:00:00")),
        Some(Expressions.equal("timestampCol", 1704067200000000L: java.lang.Long)), // microseconds since epoch
        "Timestamp converted to microseconds since epoch"
      )
    )

    assertConvert(testCases)
  }

  test("unsupported filters return None") {
    // Filters with no Iceberg equivalent
    val unsupportedFilters = Seq(
      FilterConversionCase(StringEndsWith("stringCol", "suffix"), None, "StringEndsWith"),
      FilterConversionCase(StringContains("stringCol", "substr"), None, "StringContains"),
      FilterConversionCase(Not(LessThan("intCol", 5)), None, "Not(LessThan) - only NOT IN is supported"),
      FilterConversionCase(EqualNullSafe("intCol", 5), None, "EqualNullSafe")
    )

    assertConvert(unsupportedFilters)
  }
}
