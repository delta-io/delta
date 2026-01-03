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
    label: String,
    spark: Filter,
    iceberg: Option[Expression]
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
    // Parameterize test to avoid duplication: test equality ops x all types.
    // Each tuple: (operation label, Spark Filter builder, Iceberg Expression builder)
    val equalityOperators = Seq(
      ("EqualTo",  // Operator name
        (col: String, v: Any) => EqualTo(col, v),         // Spark filter builder
        (col: String, v: Any) => Expressions.equal(col, v)),  // Iceberg expression builder
      ("NotEqualTo",  // Operator name
        (col: String, v: Any) => Not(EqualTo(col, v)),    // Spark filter builder
        (col: String, v: Any) => Expressions.notEqual(col, v))  // Iceberg expression builder
    )

    // Generate all combinations: all types x equality operators
    val standardTests = for {
      (col, value, typeDesc) <- allTypes
      (opName, sparkOp, icebergOp) <- equalityOperators
    } yield ExprConvTestCase(
      s"$opName $typeDesc",
      sparkOp(col, value),
      // supportBoolean=true because equality operators work on all types including Boolean
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, true)))
    )

    // Null handling: EqualTo(col, null) -> isNull, Not(EqualTo(col, null)) -> notNull
    val nullHandlingTests = Seq(
      ExprConvTestCase(
        "EqualTo(col, null) converts to isNull",
        EqualTo("stringCol", null),
        Some(Expressions.isNull("stringCol"))
      ),
      ExprConvTestCase(
        "Not(EqualTo(col, null)) converts to notNull (IS NOT NULL)",
        Not(EqualTo("stringCol", null)),
        Some(Expressions.notNull("stringCol"))
      )
    )

    // NaN handling: EqualTo/NotEqualTo with NaN convert to isNaN/notNaN predicates
    val nanHandlingTests = Seq(
      ExprConvTestCase(
        "EqualTo with Double.NaN converts to isNaN",
        EqualTo("doubleCol", Double.NaN),
        Some(Expressions.isNaN("doubleCol"))
      ),
      ExprConvTestCase(
        "EqualTo with Float.NaN converts to isNaN",
        EqualTo("floatCol", Float.NaN),
        Some(Expressions.isNaN("floatCol"))
      ),
      ExprConvTestCase(
        "Not(EqualTo) with Double.NaN converts to notNaN",
        Not(EqualTo("doubleCol", Double.NaN)),
        Some(Expressions.notNaN("doubleCol"))
      ),
      ExprConvTestCase(
        "Not(EqualTo) with Float.NaN converts to notNaN",
        Not(EqualTo("floatCol", Float.NaN)),
        Some(Expressions.notNaN("floatCol"))
      )
    )

    assertConvert(standardTests ++ nullHandlingTests ++ nanHandlingTests)
  }

  // ========================================================================
  // ORDERING COMPARISON OPERATORS (<, >, <=, >=)
  // ========================================================================

  test("ordering comparison operators (<, >, <=, >=) on orderable types") {
    // Parameterize test to avoid duplication: test all ordering comparison ops x all orderable
    // types. Each tuple: (operation label, Spark Filter builder, Iceberg Expression builder)
    // Note: This tests ordering comparisons (<, >, <=, >=), not equality or other operations
    val comparisonOpMappings = Seq(
      ("LessThan",
        (col: String, v: Any) => LessThan(col, v),
        (col: String, v: Any) => Expressions.lessThan(col, v)),
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

    // Generate all combinations: orderable types x comparison operators
    val supportedTests = for {
      (col, value, typeDesc) <- orderableTypeTestCases
      (opName, sparkOp, icebergOp) <- comparisonOpMappings
    } yield ExprConvTestCase(
      s"$opName $typeDesc",
      sparkOp(col, value),
      // supportBoolean=false because ordering operators don't work on Boolean type
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, false)))
    )

    // NaN with comparison operators returns None (mathematically undefined)
    val nanRejectionTests = Seq(
      ExprConvTestCase(
        "LessThan with NaN returns None (undefined)",
        LessThan("doubleCol", Double.NaN),
        None
      ),
      ExprConvTestCase(
        "GreaterThan with NaN returns None (undefined)",
        GreaterThan("floatCol", Float.NaN),
        None
      ),
      ExprConvTestCase(
        "LessThanOrEqual with NaN returns None (undefined)",
        LessThanOrEqual("doubleCol", Double.NaN),
        None
      ),
      ExprConvTestCase(
        "GreaterThanOrEqual with NaN returns None (undefined)",
        GreaterThanOrEqual("floatCol", Float.NaN),
        None
      )
    )

    assertConvert(supportedTests ++ nanRejectionTests)
  }

  // ========================================================================
  // NULL CHECK OPERATORS (IsNull, IsNotNull)
  // ========================================================================

  test("null check operators (IsNull, IsNotNull) on all types") {
    // Each tuple: (operation label, Spark Filter builder, Iceberg Expression builder)
    val nullCheckOperators = Seq(
      ("IsNull",  // Operator name
        (col: String, _: Any) => IsNull(col),              // Spark filter builder
        (col: String, _: Any) => Expressions.isNull(col)),  // Iceberg expression builder
      ("IsNotNull",  // Operator name
        (col: String, _: Any) => IsNotNull(col),           // Spark filter builder
        (col: String, _: Any) => Expressions.notNull(col))  // Iceberg expression builder
    )

    // Generate all combinations: all types x null check operators
    val testCases = for {
      (col, value, typeDesc) <- allTypes
      (opName, sparkOp, icebergOp) <- nullCheckOperators
    } yield ExprConvTestCase(
      s"$opName $typeDesc",
      sparkOp(col, value),
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(value, true)))
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
        s"In with $typeDesc",
        In(col, values),
        Some(Expressions.in(col, icebergValues: _*))
      )
    }

    val nullHandlingTests = Seq(
      // Null handling: nulls are filtered out
      ExprConvTestCase(
        "In with null values (nulls filtered)",
        In("stringCol", Array(null, "value1", "value2")),
        Some(Expressions.in("stringCol", "value1", "value2"))
      ),
      ExprConvTestCase(
        "In with null and integers",
        In("intCol", Array(null, 1, 2)),
        Some(Expressions.in("intCol", 1: Integer, 2: Integer))
      ),
      // Edge case: In with only null becomes empty In (always false)
      ExprConvTestCase(
        "In with only null",
        In("stringCol", Array(null)),
        Some(Expressions.in("stringCol"))
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
        "StringStartsWith on top-level column",
        StringStartsWith("stringCol", "prefix"),
        Some(Expressions.startsWith("stringCol", "prefix"))
      ),
      ExprConvTestCase(
        "StringStartsWith on nested column",
        StringStartsWith("metadata.stringCol", "test"),
        Some(Expressions.startsWith("metadata.stringCol", "test"))
      ),

      // Unsupported: StringEndsWith, StringContains
      ExprConvTestCase(
        "StringEndsWith (unsupported)",
        StringEndsWith("stringCol", "suffix"),
        None
      ),
      ExprConvTestCase(
        "StringContains (unsupported)",
        StringContains("stringCol", "substr"),
        None
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
        "AND with two different types",
        And(
          EqualTo("intCol", 42),
          GreaterThan("longCol", 100L)
        ),
        Some(
          Expressions.and(
            Expressions.equal("intCol", 42),
            Expressions.greaterThan("longCol", 100L))
        )
      ),
      ExprConvTestCase(
        "OR with two different types",
        Or(
          LessThan("doubleCol", 99.99), IsNull("stringCol")
        ),
        Some(
          Expressions.or(
            Expressions.lessThan("doubleCol", 99.99),
            Expressions.isNull("stringCol")
          )
        )
      ),
      ExprConvTestCase(
        "Nested logical operators",
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
        )
      ),
      ExprConvTestCase(
        "Range filter: 0 < intCol < 100",
        And(GreaterThan("intCol", 0), LessThan("intCol", 100)),
        Some(Expressions.and(
          Expressions.greaterThan("intCol", 0), Expressions.lessThan("intCol", 100)
        ))
      )
    )

    // Invalid combinations: when one side fails conversion, the whole expression returns None
    val validFilter = EqualTo("intCol", 42)
    val unsupportedFilter = StringEndsWith("stringCol", "suffix")

    val invalidCombinations = Seq(
      ExprConvTestCase(
        "AND with unsupported right side",
        And(validFilter, unsupportedFilter),
        None
      ),
      ExprConvTestCase(
        "AND with unsupported left side",
        And(unsupportedFilter, validFilter),
        None
      ),
      ExprConvTestCase(
        "OR with unsupported right side",
        Or(validFilter, unsupportedFilter),
        None
      ),
      ExprConvTestCase(
        "OR with unsupported left side",
        Or(unsupportedFilter, validFilter),
        None
      ),
      ExprConvTestCase(
        "Nested AND with unsupported in OR",
        And(
          validFilter, Or(
            validFilter,
            unsupportedFilter
          )
        ),
        None
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
        "Not(EqualTo) converts to NotEqualTo (supported)",
        Not(EqualTo("intCol", 42)),
        Some(Expressions.notEqual("intCol", 42))
      ),

      // Unsupported: Not with other operators
      ExprConvTestCase(
        "Not(LessThan) is unsupported",
        Not(LessThan("intCol", 5)),
        None
      ),
      ExprConvTestCase(
        "Not(GreaterThan) is unsupported",
        Not(GreaterThan("longCol", 100L)),
        None
      ),
      ExprConvTestCase(
        "Not(IsNull) is unsupported (use IsNotNull instead)",
        Not(IsNull("stringCol")),
        None
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
        "AlwaysTrue",
        AlwaysTrue(),
        Some(Expressions.alwaysTrue())
      ),
      ExprConvTestCase(
        "AlwaysFalse",
        AlwaysFalse(),
        Some(Expressions.alwaysFalse())
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
        "Date converted to days since epoch",
        EqualTo("dateCol", testDate),
        Some(Expressions.equal("dateCol", expectedDateDays: Integer))
      ),
      ExprConvTestCase(
        "Timestamp converted to microseconds since epoch",
        EqualTo("timestampCol", testTimestamp),
        Some(Expressions.equal("timestampCol", expectedTimestampMicros: java.lang.Long))
      ),

      // Boundary values
      ExprConvTestCase(
        "Int.MinValue boundary",
        EqualTo("intCol", Int.MinValue),
        Some(Expressions.equal("intCol", Int.MinValue))
      ),
      ExprConvTestCase(
        "Int.MaxValue boundary",
        EqualTo("intCol", Int.MaxValue),
        Some(Expressions.equal("intCol", Int.MaxValue))
      ),
      ExprConvTestCase(
        "Long.MinValue boundary",
        EqualTo("longCol", Long.MinValue),
        Some(Expressions.equal("longCol", Long.MinValue))
      ),
      ExprConvTestCase(
        "Long.MaxValue boundary",
        EqualTo("longCol", Long.MaxValue),
        Some(Expressions.equal("longCol", Long.MaxValue))
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // UNSUPPORTED FILTERS
  // ========================================================================

  test("unsupported filters return None gracefully") {
    // This test ensures that all known unsupported Spark Filter types return None
    // without throwing exceptions. This is important for forward compatibility -
    // if Spark adds new filter types, our converter will gracefully skip them.
    val testCases = Seq(
      // EqualNullSafe - Iceberg doesn't have null-safe equality
      ExprConvTestCase(
        "EqualNullSafe",
        EqualNullSafe("intCol", 5),
        None
      ),
      // StringEndsWith - Iceberg API doesn't provide this predicate
      ExprConvTestCase(
        "StringEndsWith",
        StringEndsWith("stringCol", "suffix"),
        None
      ),
      // StringContains - Iceberg API doesn't provide this predicate
      ExprConvTestCase(
        "StringContains",
        StringContains("stringCol", "substring"),
        None
      ),
      // Not with non-EqualTo inner filter - Iceberg doesn't support arbitrary NOT
      // Only Not(EqualTo) is converted as a special case
      ExprConvTestCase(
        "Not(LessThan) - arbitrary NOT unsupported",
        Not(LessThan("intCol", 10)),
        None
      ),
      ExprConvTestCase(
        "Not(GreaterThan) - arbitrary NOT unsupported",
        Not(GreaterThan("intCol", 10)),
        None
      ),
      ExprConvTestCase(
        "Not(IsNull) - arbitrary NOT unsupported",
        Not(IsNull("intCol")),
        None
      ),
      ExprConvTestCase(
        "Not(In) - arbitrary NOT unsupported",
        Not(In("intCol", Array(1, 2, 3))),
        None
      ),
      ExprConvTestCase(
        "Not(And) - arbitrary NOT unsupported",
        Not(And(EqualTo("intCol", 1), EqualTo("longCol", 2L))),
        None
      ),
      ExprConvTestCase(
        "Not(StringStartsWith) - arbitrary NOT unsupported",
        Not(StringStartsWith("stringCol", "prefix")),
        None
      )
    )

    assertConvert(testCases)
  }
}
