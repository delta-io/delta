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

  private case class ExprConvTestCase(
    spark: Filter,
    iceberg: Option[Expression],
    label: String
  )

  // Types that support equality and ordering operations
  // (EqualTo, NotEqualTo, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
  // Note: Spark Filter API sends Date/Timestamp as java.sql.Date/Timestamp, but our converter
  // transforms them to Int (days since epoch) and Long (microseconds since epoch) for Iceberg.
  private val orderableTypeTestCases = Seq(
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

  private val allTypes = orderableTypeTestCases ++ equalityOnlyTypes
  private val testSchema = TestSchemas.testSchema.asStruct()

  private def assertConvert(testCases: Seq[ExprConvTestCase]): Unit = {
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

  // ========================================================================
  // EQUALITY OPERATORS (=, !=)
  // ========================================================================

  test("equality operators (=, !=) on all types including null and NaN handling") {
    // Parameterize test to avoid duplication: test equality ops × all types.
    // Each tuple: (Spark Filter builder, Iceberg Expression builder, operation label)
    val equalityOperators = Seq(
      ((col: String, v: Any) => EqualTo(col, v),         // Spark filter builder
        (col: String, v: Any) => Expressions.equal(col, v),  // Iceberg expression builder
        "EqualTo"),  // Operator name
      ((col: String, v: Any) => Not(EqualTo(col, v)),    // Spark filter builder
        (col: String, v: Any) => Expressions.notEqual(col, v),  // Iceberg expression builder
        "NotEqualTo")  // Operator name
    )

    // Generate all combinations: all types × equality operators
    val standardTests = for {
      (col, value, typeDesc) <- allTypes
      (sparkOp, icebergOp, opName) <- equalityOperators
    } yield ExprConvTestCase(
      sparkOp(col, value),
      // supportBoolean=true because equality operators work on all types including Boolean
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, true))),
      s"$opName $typeDesc"
    )

    // Null handling: EqualTo(col, null) -> isNull, Not(EqualTo(col, null)) -> notNull
    val nullHandlingTests = Seq(
      ExprConvTestCase(
        EqualTo("stringCol", null),
        Some(Expressions.isNull("stringCol")),
        "EqualTo(col, null) converts to isNull"
      ),
      ExprConvTestCase(
        Not(EqualTo("stringCol", null)),
        Some(Expressions.notNull("stringCol")),
        "Not(EqualTo(col, null)) converts to notNull (IS NOT NULL)"
      )
    )

    // NaN handling: EqualTo/NotEqualTo with NaN convert to isNaN/notNaN predicates
    val nanHandlingTests = Seq(
      ExprConvTestCase(
        EqualTo("doubleCol", Double.NaN),
        Some(Expressions.isNaN("doubleCol")),
        "EqualTo with Double.NaN converts to isNaN"
      ),
      ExprConvTestCase(
        EqualTo("floatCol", Float.NaN),
        Some(Expressions.isNaN("floatCol")),
        "EqualTo with Float.NaN converts to isNaN"
      ),
      ExprConvTestCase(
        Not(EqualTo("doubleCol", Double.NaN)),
        Some(Expressions.notNaN("doubleCol")),
        "Not(EqualTo) with Double.NaN converts to notNaN"
      ),
      ExprConvTestCase(
        Not(EqualTo("floatCol", Float.NaN)),
        Some(Expressions.notNaN("floatCol")),
        "Not(EqualTo) with Float.NaN converts to notNaN"
      )
    )

    assertConvert(standardTests ++ nullHandlingTests ++ nanHandlingTests)
  }

  // ========================================================================
  // ORDERING COMPARISON OPERATORS (<, >, <=, >=)
  // ========================================================================

  test("ordering comparison operators (<, >, <=, >=) on orderable types") {
    // Parameterize test to avoid duplication: test all ordering comparison ops × all orderable
    // types. Each tuple: (Spark Filter builder, Iceberg Expression builder, operation label)
    // Note: This tests ordering comparisons (<, >, <=, >=), not equality or other operations
    val comparisonOpMappings = Seq(
      ((col: String, v: Any) => LessThan(col, v),
        (col: String, v: Any) => Expressions.lessThan(col, v),
        "LessThan"),
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

    // Generate all combinations: orderable types × comparison operators
    val supportedTests = for {
      (col, value, typeDesc) <- orderableTypeTestCases
      (sparkOp, icebergOp, opName) <- comparisonOpMappings
    } yield ExprConvTestCase(
      sparkOp(col, value),
      // supportBoolean=false because ordering operators don't work on Boolean type
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, false))),
      s"$opName $typeDesc"
    )

    // NaN with comparison operators returns None (mathematically undefined)
    val nanRejectionTests = Seq(
      ExprConvTestCase(
        LessThan("doubleCol", Double.NaN),
        None,
        "LessThan with NaN returns None (undefined)"
      ),
      ExprConvTestCase(
        GreaterThan("floatCol", Float.NaN),
        None,
        "GreaterThan with NaN returns None (undefined)"
      ),
      ExprConvTestCase(
        LessThanOrEqual("doubleCol", Double.NaN),
        None,
        "LessThanOrEqual with NaN returns None (undefined)"
      ),
      ExprConvTestCase(
        GreaterThanOrEqual("floatCol", Float.NaN),
        None,
        "GreaterThanOrEqual with NaN returns None (undefined)"
      )
    )

    assertConvert(supportedTests ++ nanRejectionTests)
  }

  // ========================================================================
  // NULL CHECK OPERATORS (IsNull, IsNotNull)
  // ========================================================================

  test("null check operators (IsNull, IsNotNull) on all types") {
    // Each tuple: (Spark Filter builder, Iceberg Expression builder, operation label)
    val nullCheckOperators = Seq(
      ((col: String, _: Any) => IsNull(col),              // Spark filter builder
        (col: String, _: Any) => Expressions.isNull(col),  // Iceberg expression builder
        "IsNull"),  // Operator name
      ((col: String, _: Any) => IsNotNull(col),           // Spark filter builder
        (col: String, _: Any) => Expressions.notNull(col),  // Iceberg expression builder
        "IsNotNull")  // Operator name
    )

    // Generate all combinations: all types × null check operators
    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (sparkOp, icebergOp, opName) <- nullCheckOperators
    } yield ExprConvTestCase(
      sparkOp(col, value),
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, true))),
      s"$opName $typeDesc"
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // IN OPERATOR
  // ========================================================================

  // IN operator requires special handling because:
  // - It accepts arrays of values, requiring per-element type coercion
  // - Null values must be filtered out (SQL semantics: col IN (1, NULL) = col IN (1))
  // - Empty arrays after null filtering result in always-false predicates
  // - Type conversion needed for each array element (Scala -> Java types)
  test("IN operator with type coercion and null handling") {
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
      val icebergValues = values.map(v =>
        SparkToIcebergExpressionConverter.toIcebergValue(v, supportBoolean = true))
      ExprConvTestCase(
        In(col, values),
        Some(Expressions.in(col, icebergValues: _*)),
        s"In with $typeDesc"
      )
    }

    val nullHandlingTests = Seq(
      // Null handling: nulls are filtered out
      ExprConvTestCase(
        In("stringCol", Array(null, "value1", "value2")),
        Some(Expressions.in("stringCol", "value1", "value2")),
        "In with null values (nulls filtered)"
      ),
      ExprConvTestCase(
        In("intCol", Array(null, 1, 2)),
        Some(Expressions.in("intCol", 1: Integer, 2: Integer)),
        "In with null and integers"
      ),
      // Edge case: In with only null becomes empty In (always false)
      ExprConvTestCase(
        In("stringCol", Array(null)),
        Some(Expressions.in("stringCol")),
        "In with only null"
      )
    )

    assertConvert(inTestCases ++ nullHandlingTests)
  }

  // ========================================================================
  // STRING OPERATIONS
  // ========================================================================

  test("string operations (startsWith supported, endsWith/contains unsupported)") {
    val testCases = Seq(
      // Supported: StringStartsWith
      ExprConvTestCase(
        StringStartsWith("stringCol", "prefix"),
        Some(Expressions.startsWith("stringCol", "prefix")),
        "StringStartsWith on top-level column"
      ),
      ExprConvTestCase(
        StringStartsWith("metadata.stringCol", "test"),
        Some(Expressions.startsWith("metadata.stringCol", "test")),
        "StringStartsWith on nested column"
      ),

      // Unsupported: StringEndsWith, StringContains
      ExprConvTestCase(
        StringEndsWith("stringCol", "suffix"),
        None,
        "StringEndsWith (unsupported)"
      ),
      ExprConvTestCase(
        StringContains("stringCol", "substr"),
        None,
        "StringContains (unsupported)"
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // LOGICAL OPERATORS (AND, OR)
  // ========================================================================

  test("logical operators (AND, OR) with valid and invalid combinations") {
    // Valid combinations: both sides convert successfully
    val validCombinations = Seq(
      ExprConvTestCase(
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
      ExprConvTestCase(
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
      ExprConvTestCase(
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
      ),
      ExprConvTestCase(
        And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
        Some(Expressions.and(
          Expressions.greaterThan("intCol", 0), Expressions.lessThan("intCol", 100)
        )),
        "Range filter: 0 < intCol < 100"
      )
    )

    // Invalid combinations: when one side fails conversion, the whole expression returns None
    val validFilter = EqualTo("intCol", 42)
    val unsupportedFilter = StringEndsWith("stringCol", "suffix")

    val invalidCombinations = Seq(
      ExprConvTestCase(
        And(validFilter, unsupportedFilter),
        None,
        "AND with unsupported right side"
      ),
      ExprConvTestCase(
        And(unsupportedFilter, validFilter),
        None,
        "AND with unsupported left side"
      ),
      ExprConvTestCase(
        Or(validFilter, unsupportedFilter),
        None,
        "OR with unsupported right side"
      ),
      ExprConvTestCase(
        Or(unsupportedFilter, validFilter),
        None,
        "OR with unsupported left side"
      ),
      ExprConvTestCase(
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

    assertConvert(validCombinations ++ invalidCombinations)
  }

  // ========================================================================
  // NOT OPERATOR
  // ========================================================================

  test("NOT operator (only NOT EqualTo is supported)") {
    val testCases = Seq(
      // Supported: Not(EqualTo) - covered in equality operators test, but included here for
      // completeness
      ExprConvTestCase(
        Not(EqualTo("intCol", 42)),
        Some(Expressions.notEqual("intCol", 42)),
        "Not(EqualTo) converts to NotEqualTo (supported)"
      ),

      // Unsupported: Not with other operators
      ExprConvTestCase(
        Not(LessThan("intCol", 5)),
        None,
        "Not(LessThan) is unsupported"
      ),
      ExprConvTestCase(
        Not(GreaterThan("longCol", 100L)),
        None,
        "Not(GreaterThan) is unsupported"
      ),
      ExprConvTestCase(
        Not(IsNull("stringCol")),
        None,
        "Not(IsNull) is unsupported (use IsNotNull instead)"
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // BOOLEAN LITERALS
  // ========================================================================

  test("boolean literals (AlwaysTrue, AlwaysFalse)") {
    val testCases = Seq(
      ExprConvTestCase(
        AlwaysTrue(),
        Some(Expressions.alwaysTrue()),
        "AlwaysTrue"
      ),
      ExprConvTestCase(
        AlwaysFalse(),
        Some(Expressions.alwaysFalse()),
        "AlwaysFalse"
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // TYPE CONVERSIONS AND BOUNDARY VALUES
  // ========================================================================

  test("type conversions (Date/Timestamp) and boundary values") {
    // For Date/Timestamp tests, compute expected values from the actual objects
    // to avoid timezone-dependent hardcoded values
    val testDate = java.sql.Date.valueOf("2024-01-01")
    val expectedDateDays = (testDate.getTime / (1000L * 60 * 60 * 24)).toInt

    val testTimestamp = java.sql.Timestamp.valueOf("2024-01-01 00:00:00")
    val expectedTimestampMicros =
      testTimestamp.getTime * 1000 + (testTimestamp.getNanos % 1000000) / 1000

    val testCases = Seq(
      // Date/Timestamp: Spark sends java.sql types, but we convert to Int/Long for Iceberg
      ExprConvTestCase(
        EqualTo("dateCol", testDate),
        Some(Expressions.equal("dateCol", expectedDateDays: Integer)),
        "Date converted to days since epoch"
      ),
      ExprConvTestCase(
        EqualTo("timestampCol", testTimestamp),
        Some(Expressions.equal("timestampCol", expectedTimestampMicros: java.lang.Long)),
        "Timestamp converted to microseconds since epoch"
      ),

      // Boundary values
      ExprConvTestCase(
        EqualTo("intCol", Int.MinValue),
        Some(Expressions.equal("intCol", Int.MinValue)),
        "Int.MinValue boundary"
      ),
      ExprConvTestCase(
        EqualTo("intCol", Int.MaxValue),
        Some(Expressions.equal("intCol", Int.MaxValue)),
        "Int.MaxValue boundary"
      ),
      ExprConvTestCase(
        EqualTo("longCol", Long.MinValue),
        Some(Expressions.equal("longCol", Long.MinValue)),
        "Long.MinValue boundary"
      ),
      ExprConvTestCase(
        EqualTo("longCol", Long.MaxValue),
        Some(Expressions.equal("longCol", Long.MaxValue)),
        "Long.MaxValue boundary"
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // UNSUPPORTED FILTERS
  // ========================================================================

  test("unsupported filters (EqualNullSafe, etc.)") {
    val testCases = Seq(
      ExprConvTestCase(
        EqualNullSafe("intCol", 5),
        None,
        "EqualNullSafe (unsupported)"
      )
    )

    assertConvert(testCases)
  }
}
