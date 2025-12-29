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
import shadedForDelta.org.apache.iceberg.expressions.Expression.Operation

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  // Helper: Assert filter converts successfully with type-safe operator validation
  private def assertConvert(
      filter: Filter,
      expectedOp: Operation,
      expectedTerms: String*): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")

    val expr = result.get

    // Type-safe operator validation (prevents LT matching LT_EQ, etc.)
    assert(expr.op() == expectedOp,
      s"Expected operation $expectedOp but got ${expr.op()}")

    // Still validate column names/values appear in output
    val exprStr = expr.toString
    expectedTerms.foreach(term =>
      assert(exprStr.contains(term), s"Missing '$term' in: $exprStr")
    )
  }

  // Helper: Assert filter returns None (unsupported)
  private def assertReturnsNone(filter: Filter): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isEmpty, s"Should return None for unsupported filter: $filter")
  }

  // EqualTo tests
  test("convert EqualTo with various types") {
    assertConvert(EqualTo("id", 5), Operation.EQ, "id", "5")  // integer
    assertConvert(EqualTo("name", "Alice"), Operation.EQ, "name")  // string
    assertConvert(EqualTo("id", 1234567890L), Operation.EQ, "id")  // long
    assertConvert(EqualTo("active", true), Operation.EQ, "active")  // boolean

    // EqualTo with null becomes IsNull
    val nullResult = SparkToIcebergExpressionConverter.convert(EqualTo("name", null))
    assert(nullResult.isDefined, "Should convert EqualTo with null")
    val nullExpr = nullResult.get
    assert(nullExpr.op() == Operation.IS_NULL, s"Should be IS_NULL operator")
    assert(nullExpr.toString.contains("name"), s"Should contain column name")
  }

  // Comparison operator tests
  test("convert comparison operators") {
    assertConvert(LessThan("age", 30), Operation.LT, "age", "30")
    assertConvert(GreaterThan("age", 18), Operation.GT, "age", "18")
    assertConvert(LessThanOrEqual("price", 99.99), Operation.LT_EQ, "price")
    assertConvert(GreaterThanOrEqual("timestamp", 1234567890L), Operation.GT_EQ, "timestamp")
  }

  // Null check tests
  test("convert null check operators") {
    assertConvert(IsNull("name"), Operation.IS_NULL, "name")
    assertConvert(IsNotNull("name"), Operation.NOT_NULL, "name")
  }

  // Logical operator tests
  test("convert logical operators") {
    assertConvert(
      And(EqualTo("id", 5), GreaterThan("age", 18)),
      Operation.AND,
      "id", "age")

    assertConvert(
      Or(EqualTo("status", "active"), EqualTo("status", "pending")),
      Operation.OR,
      "status")

    // Nested And/Or
    assertConvert(
      And(
        Or(EqualTo("status", "active"), EqualTo("status", "pending")),
        GreaterThan("age", 18)
      ),
      Operation.AND,
      "status", "age")
  }

  // Unsupported filter tests
  test("unsupported filters return None") {
    assertReturnsNone(StringStartsWith("name", "A"))  // StringStartsWith
    assertReturnsNone(StringEndsWith("name", "Z"))  // StringEndsWith
    assertReturnsNone(StringContains("name", "foo"))  // StringContains
    assertReturnsNone(In("id", Array(1, 2, 3)))  // In
    assertReturnsNone(Not(EqualTo("id", 5)))  // Not
  }

  // convertMultiple (array) tests
  test("convertMultiple with single filter") {
    val filters = Array[Filter](EqualTo("id", 5))
    val result = SparkToIcebergExpressionConverter.convertMultiple(filters)

    assert(result.isDefined, "Should convert single filter")
    // Operator conversion already tested in convert() tests
    assert(result.get.toString.contains("id"), "Expression should reference 'id'")
  }

  test("convertMultiple combines multiple filters with AND") {
    val filters = Array[Filter](
      EqualTo("id", 5),
      GreaterThan("age", 18),
      IsNotNull("name")
    )
    val result = SparkToIcebergExpressionConverter.convertMultiple(filters)

    assert(result.isDefined, "Should convert multiple filters")
    val expr = result.get

    // Type-safe validation that result is AND
    assert(expr.op() == Operation.AND, "Multiple filters should be combined with AND")

    // Validate column references appear (operator conversion already tested in convert() tests)
    val exprStr = expr.toString
    assert(exprStr.contains("id"), "Should contain 'id' column")
    assert(exprStr.contains("age"), "Should contain 'age' column")
    assert(exprStr.contains("name"), "Should contain 'name' column")
  }

  test("convertMultiple skips unsupported filters") {
    val filters = Array[Filter](
      EqualTo("id", 5),              // Supported
      StringStartsWith("name", "A"),  // Unsupported
      GreaterThan("age", 18)          // Supported
    )
    val result = SparkToIcebergExpressionConverter.convertMultiple(filters)

    assert(result.isDefined, "Should convert supported filters and skip unsupported")
    val expr = result.get

    // Type-safe validation that result is AND
    assert(expr.op() == Operation.AND, "Should combine remaining filters with AND")

    // Validate column references appear (operator conversion already tested in convert() tests)
    val exprStr = expr.toString
    assert(exprStr.contains("id"), "Should contain 'id' column")
    assert(exprStr.contains("age"), "Should contain 'age' column")
  }

  test("convertMultiple returns None when all filters are unsupported") {
    val filters = Array[Filter](
      StringStartsWith("name", "A"),
      StringEndsWith("name", "Z"),
      StringContains("name", "foo")
    )
    val result = SparkToIcebergExpressionConverter.convertMultiple(filters)

    assert(result.isEmpty, "Should return None when all filters are unsupported")
  }

  test("convertMultiple returns None for empty array") {
    val filters = Array.empty[Filter]
    val result = SparkToIcebergExpressionConverter.convertMultiple(filters)

    assert(result.isEmpty, "Empty filter array should return None")
  }

  // Type conversion tests
  test("convert with different numeric types") {
    assertConvert(EqualTo("count", 42), Operation.EQ, "count")  // int
    assertConvert(EqualTo("bigCount", 1234567890123L), Operation.EQ, "bigCount")  // long
    assertConvert(GreaterThan("price", 19.99), Operation.GT, "price")  // double
    assertConvert(LessThan("rating", 4.5f), Operation.LT, "rating")  // float
  }
}
