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

import org.apache.spark.sql.Row
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
    ("localDateCol", java.time.LocalDate.of(2024, 1, 1), "LocalDate"),
    ("localDateTimeCol", java.time.LocalDateTime.of(2024, 1, 1, 12, 0, 0), "LocalDateTime"),
    ("instantCol", java.time.Instant.parse("2024-01-01T12:00:00Z"), "Instant"),
    ("address.intCol", 42, "Nested Int"),
    ("metadata.stringCol", "test", "Nested String")
  )

  // Types that only support equality operators (EqualTo, NotEqualTo, IsNull, IsNotNull)
  private val equalityOnlyTypesTestCases = Seq(
    ("boolCol", true, "Boolean")
  )

  private val allTypesTestCases = orderableTypeTestCases ++ equalityOnlyTypesTestCases
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
    val equalityOpMappings = Seq(
      ("EqualTo",  // Test case label
        (col: String, v: Any) => EqualTo(col, v),         // Spark filter builder
        (col: String, v: Any) => Expressions.equal(col, v)),  // Iceberg expression builder
      ("NotEqualTo",
        (col: String, v: Any) => Not(EqualTo(col, v)),
        (col: String, v: Any) => Expressions.notEqual(col, v))
    )

    // Generate all combinations: all types x equality operators
    val standardTests = for {
      (col, value, typeDesc) <- allTypesTestCases
      (opName, sparkOp, icebergOp) <- equalityOpMappings
    } yield ExprConvTestCase(
      s"$opName $typeDesc",
      sparkOp(col, value),
      // supportBoolean=true because equality operators work on all types including Boolean
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(
        value, supportBoolean = true)))
    )

    // Null handling: EqualTo(col, null) -> isNull, Not(EqualTo(col, null)) -> notNull
    val nullHandlingTests = Seq(
      ExprConvTestCase(
        "EqualTo(col, null) converts to isNull", // Test case label
        EqualTo("stringCol", null), // Spark filter builder
        Some(Expressions.isNull("stringCol")) // Iceberg expression builder
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
        "EqualTo with Double.NaN converts to isNaN", // Test case label
        EqualTo("doubleCol", Double.NaN), // Spark filter builder
        Some(Expressions.isNaN("doubleCol")) // Iceberg expression builder
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
    // Note: This only tests ordering comparisons (<, >, <=, >=), not equality or other operations
    val comparisonOpMappings = Seq(
      ("LessThan", // Test case label
        (col: String, v: Any) => LessThan(col, v), // Spark filter builder
        (col: String, v: Any) => Expressions.lessThan(col, v)), // Iceberg expression builder
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
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(
        value, supportBoolean = false)))
    )

    // NaN with comparison operators returns None
    val nanRejectionTests = Seq(
      ExprConvTestCase(
        "LessThan with NaN returns None (undefined)", // Test case label
        LessThan("doubleCol", Double.NaN), // Spark filter builder
        None // Iceberg expression builder
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
  // NULL CHECK OPERATORS (IsNull, IsNotNull, Not(IsNull))
  // ========================================================================

  test("null check operators (IsNull, IsNotNull, Not(IsNull)) on all types") {
    val nullCheckOpMappings = Seq(
      ("IsNull",  // Test case label
        (col: String, _: Any) => IsNull(col),              // Spark filter builder
        (col: String, _: Any) => Expressions.isNull(col)),  // Iceberg expression builder
      ("IsNotNull",
        (col: String, _: Any) => IsNotNull(col),
        (col: String, _: Any) => Expressions.notNull(col)),
      ("Not(IsNull)",
        (col: String, _: Any) => Not(IsNull(col)),
        (col: String, _: Any) => Expressions.notNull(col))
    )

    // Generate all combinations: all types x null check operators
    val testCases = for {
      (col, value, typeDesc) <- allTypesTestCases
      (opName, sparkOp, icebergOp) <- nullCheckOpMappings
    } yield ExprConvTestCase(
      s"$opName $typeDesc",
      sparkOp(col, value),
      Some(icebergOp(col, SparkToIcebergExpressionConverter.toIcebergValue(
        value, supportBoolean = true)))
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // IN AND NOT IN OPERATORS
  // ========================================================================

  // IN and NOT IN operators require special handling because:
  // - They accept arrays of values, requiring per-element type coercion
  // - Null values must be filtered out (SQL semantics: col IN (1, NULL) = col IN (1))
  // - Empty arrays after null filtering result in always-false/true predicates
  // - Type conversion needed for each array element (Scala -> Java types)
  test("IN and NOT IN operators with type coercion and null handling") {
    // Helper to generate multiple test values for IN/NOT IN operators
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
      case v: java.time.LocalDate =>
        Array(v, v.plusDays(1), v.plusDays(2))
      case v: java.time.LocalDateTime =>
        Array(v, v.plusHours(1), v.plusHours(2))
      case v: java.time.Instant =>
        Array(v, v.plusSeconds(3600), v.plusSeconds(7200)) // +1 hour, +2 hours
      case _ => Array(value)
    }

    val inOpMappings = Seq(
      ("In",
        (col: String, values: Array[Any]) => In(col, values),
        (col: String, values: Array[Any]) => Expressions.in(col, values: _*)),
      ("Not(In)",
        (col: String, values: Array[Any]) => Not(In(col, values)),
        (col: String, values: Array[Any]) => Expressions.notIn(col, values: _*))
    )

    // Test IN and NOT IN operators for all types
    val typeTests = for {
      (col, value, typeDesc) <- allTypesTestCases
      (opName, sparkOp, icebergOp) <- inOpMappings
    } yield {
      val values = generateInValues(value)
      val icebergValues = values.map(v =>
        SparkToIcebergExpressionConverter.toIcebergValue(v, supportBoolean = true))
      ExprConvTestCase(
        s"$opName with $typeDesc",
        sparkOp(col, values),
        Some(icebergOp(col, icebergValues))
      )
    }

    // Null handling tests for both In and Not(In)
    val nullHandlingTests = for {
      (opName, sparkOp, icebergOp) <- inOpMappings
    } yield Seq(
      ExprConvTestCase(
        s"$opName with null values (nulls filtered)",
        sparkOp("stringCol", Array(null, "value1", "value2")),
        Some(icebergOp("stringCol", Array("value1", "value2")))
      ),
      ExprConvTestCase(
        s"$opName with null and integers",
        sparkOp("intCol", Array(null, 1, 2)),
        Some(icebergOp("intCol", Array(1: Integer, 2: Integer)))
      ),
      ExprConvTestCase(
        s"$opName with only null",
        sparkOp("stringCol", Array(null)),
        Some(icebergOp("stringCol", Array()))
      ),
      ExprConvTestCase(
        s"$opName with empty array",
        sparkOp("intCol", Array()),
        Some(icebergOp("intCol", Array()))
      )
    )

    // Specific examples for both In and Not(In)
    val specificExamples = for {
      (opName, sparkOp, icebergOp) <- inOpMappings
    } yield Seq(
      ExprConvTestCase(
        s"$opName with string values",
        sparkOp("stringCol", Array("value1", "value2")),
        Some(icebergOp("stringCol", Array("value1", "value2")))
      ),
      ExprConvTestCase(
        s"$opName with single value",
        sparkOp("intCol", Array(42)),
        Some(icebergOp("intCol", Array(42: Integer)))
      ),
      ExprConvTestCase(
        s"$opName with nested column",
        sparkOp("address.intCol", Array(1, 2, 3)),
        Some(icebergOp("address.intCol", Array(1: Integer, 2: Integer, 3: Integer)))
      )
    )

    assertConvert(typeTests ++ nullHandlingTests.flatten ++ specificExamples.flatten)
  }

  // ========================================================================
  // STRING OPERATIONS
  // ========================================================================

  test("string operations (startsWith/notStartsWith supported, endsWith/contains unsupported)") {
    val stringOpMappings = Seq(
      ("StringStartsWith",
        (col: String, prefix: String) => StringStartsWith(col, prefix),
        (col: String, prefix: String) => Expressions.startsWith(col, prefix)),
      ("Not(StringStartsWith)",
        (col: String, prefix: String) => Not(StringStartsWith(col, prefix)),
        (col: String, prefix: String) => Expressions.notStartsWith(col, prefix))
    )

    val stringColumns = Seq(
      ("stringCol", "string column"),
      ("metadata.stringCol", "nested string column")
    )

    val prefixTestCases = Seq(
      ("prefix", "basic prefix"),
      ("", "empty prefix")
    )

    // Generate all combinations: string columns x prefixes x [startsWith, notStartsWith]
    val supportedTests = for {
      (col, colDesc) <- stringColumns
      (prefix, prefixDesc) <- prefixTestCases
      (opName, sparkOp, icebergOp) <- stringOpMappings
    } yield ExprConvTestCase(
      s"$opName with $prefixDesc on $colDesc",
      sparkOp(col, prefix),
      Some(icebergOp(col, prefix))
    )

    // Unsupported: StringEndsWith, StringContains
    val unsupportedTests = Seq(
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

    assertConvert(supportedTests ++ unsupportedTests)
  }

  // ========================================================================
  // LOGICAL OPERATORS (AND, OR)
  // ========================================================================

  test("logical operators (AND, OR) with valid and invalid combinations") {
    // Valid combinations: both sides convert successfully
    val validCombinations = Seq(
      ExprConvTestCase(
        "AND with two different types", // Test case label
        And( // Spark filter builder
          EqualTo("intCol", 42),
          GreaterThan("longCol", 100L)
        ),
        Some( // Iceberg expression builder
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
        "AND with unsupported right side", // Test case label
        And(validFilter, unsupportedFilter), // Spark filter builder
        None // Iceberg expression builder
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
  // NOT OPERATOR (unsupported cases)
  // ========================================================================

  test("NOT operator with unsupported inner filters") {
    // Note: Supported NOT patterns are tested in their respective operator pair tests:
    // - Not(EqualTo) tested in equality operators
    // - Not(In) tested in IN and NOT IN operators
    // - Not(IsNull) tested in null check operators
    // - Not(StringStartsWith) tested in string operations
    //
    // This test only covers unsupported NOT patterns
    val testCases = Seq(
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
        "Not(And) is unsupported",
        Not(And(EqualTo("intCol", 1), EqualTo("longCol", 2L))),
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
        "AlwaysTrue", // Test case label
        AlwaysTrue(), // Spark filter builder
        Some(Expressions.alwaysTrue()) // Iceberg expression builder
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
    val testDate = java.sql.Date.valueOf("2024-01-01")
    val expectedDateDays = (testDate.getTime / (1000L * 60 * 60 * 24)).toInt

    val testTimestamp = java.sql.Timestamp.valueOf("2024-01-01 00:00:00")
    val expectedTimestampMicros =
      testTimestamp.getTime * 1000 + (testTimestamp.getNanos % 1000000) / 1000

    // java.time types
    val testLocalDate = java.time.LocalDate.of(2024, 1, 1)
    val expectedLocalDateDays = testLocalDate.toEpochDay.toInt

    val testLocalDateTime = java.time.LocalDateTime.of(2024, 1, 1, 12, 30, 45)
    val expectedLocalDateTimeMicros = testLocalDateTime.toEpochSecond(
      java.time.ZoneOffset.UTC) * 1000000 + testLocalDateTime.getNano / 1000

    val testInstant = java.time.Instant.parse("2024-01-01T12:30:45.123456Z")
    val expectedInstantMicros = testInstant.getEpochSecond * 1000000 + testInstant.getNano / 1000

    val testCases = Seq(
      // Date/Timestamp: Spark sends java.sql types, but we convert to Int/Long for Iceberg
      ExprConvTestCase(
        "Date converted to days since epoch", // Test case label
        EqualTo("dateCol", testDate), // Spark filter builder
        Some(Expressions.equal("dateCol", expectedDateDays: Integer)) // Iceberg expression builder
      ),
      ExprConvTestCase(
        "Timestamp converted to microseconds since epoch",
        EqualTo("timestampCol", testTimestamp),
        Some(Expressions.equal("timestampCol", expectedTimestampMicros: java.lang.Long))
      ),

      // java.time types: converted to Int (days) or Long (microseconds)
      ExprConvTestCase(
        "LocalDate converted to days since epoch",
        EqualTo("localDateCol", testLocalDate),
        Some(Expressions.equal("localDateCol", expectedLocalDateDays: Integer))
      ),
      ExprConvTestCase(
        "LocalDateTime converted to microseconds since epoch",
        EqualTo("localDateTimeCol", testLocalDateTime),
        Some(Expressions.equal("localDateTimeCol", expectedLocalDateTimeMicros: java.lang.Long))
      ),
      ExprConvTestCase(
        "Instant converted to microseconds since epoch",
        EqualTo("instantCol", testInstant),
        Some(Expressions.equal("instantCol", expectedInstantMicros: java.lang.Long))
      ),

      // Boundary values
      ExprConvTestCase(
        "Int.MinValue boundary", // Test case label
        EqualTo("intCol", Int.MinValue), // Spark filter builder
        Some(Expressions.equal("intCol", Int.MinValue)) // Iceberg expression builder
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

  test("unsupported filters return None") {
    // This test ensures that all known unsupported Spark Filter types return None
    // If Spark adds new filter types, our converter will skip them via case _ => None
    val testCases = Seq(
      // EqualNullSafe - Iceberg doesn't have null-safe equality
      ExprConvTestCase(
        "EqualNullSafe", // Test case label
        EqualNullSafe("intCol", 5), // Spark filter builder
        None // Iceberg expression builder
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
        "Not(And) - arbitrary NOT unsupported",
        Not(And(EqualTo("intCol", 1), EqualTo("longCol", 2L))),
        None
      )
    )

    assertConvert(testCases)
  }

  // ========================================================================
  // UNSUPPORTED VALUE TYPES
  // ========================================================================

  test("filters with unsupported value types return None") {
    // Define unsupported types for which conversion must fail
    val unsupportedTypes = Seq(
      (Array(1, 2, 3), "Array"),
      (Map("key" -> 1), "Map"),
      (Row(1, "test"), "Row/Struct"),
      (Array[Byte](1, 2, 3), "byte array"),
      (5.toByte, "Byte"),
      (5.toShort, "Short")
    )

    // Define operators that must reject unsupported types
    val operators = Seq(
      ("EqualTo", (col: String, v: Any) => EqualTo(col, v)),
      ("LessThan", (col: String, v: Any) => LessThan(col, v)),
      ("GreaterThan", (col: String, v: Any) => GreaterThan(col, v)),
      ("LessThanOrEqual", (col: String, v: Any) => LessThanOrEqual(col, v)),
      ("GreaterThanOrEqual", (col: String, v: Any) => GreaterThanOrEqual(col, v))
    )

    // Generate all combinations: unsupported types x operators
    val operatorTests = for {
      (value, typeDesc) <- unsupportedTypes
      (opName, sparkOp) <- operators
    } yield ExprConvTestCase(
      s"$opName with $typeDesc should be unsupported", // Test case label
      sparkOp("intCol", value), // Spark filter builder
      None // Iceberg expression builder
    )

    // Boolean with comparison operators (supportBoolean=false for these)
    val booleanComparisonTests = Seq(
      ("LessThan", (col: String, v: Any) => LessThan(col, v)),
      ("GreaterThan", (col: String, v: Any) => GreaterThan(col, v)),
      ("LessThanOrEqual", (col: String, v: Any) => LessThanOrEqual(col, v)),
      ("GreaterThanOrEqual", (col: String, v: Any) => GreaterThanOrEqual(col, v))
    ).map { case (opName, sparkOp) =>
      ExprConvTestCase(
        s"$opName with Boolean (unsupported for comparison)",
        sparkOp("boolCol", true),
        None
      )
    }

    // Special case: In with nested Array values
    val inWithNestedArrays = ExprConvTestCase(
      "In with nested Array values",
      In("intCol", Array(Array(1), Array(2))),
      None
    )

    assertConvert(operatorTests ++ booleanComparisonTests ++ Seq(inWithNestedArrays))
  }
}
