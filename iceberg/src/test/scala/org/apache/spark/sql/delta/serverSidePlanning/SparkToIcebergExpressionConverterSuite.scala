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

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  // Helper: Assert filter converts successfully and output contains expected terms
  private def assertConverts(filter: Filter, expectedTerms: String*): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")
    val expr = result.get.toString
    expectedTerms.foreach(term =>
      assert(expr.contains(term), s"Missing '$term' in expression: $expr")
    )
  }

  // Helper: Assert filter returns None (unsupported)
  private def assertReturnsNone(filter: Filter): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isEmpty, s"Should return None for unsupported filter: $filter")
  }

  // Helper: Assert filter converts and output contains operator (case-insensitive)
  private def assertContainsOperator(filter: Filter, operators: String*): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")
    val expr = result.get.toString.toLowerCase(java.util.Locale.ROOT)
    operators.foreach(op =>
      assert(expr.contains(op), s"Missing operator '$op' in: $expr")
    )
  }

  // EqualTo tests
  test("convert EqualTo with various types") {
    assertConverts(EqualTo("id", 5), "id", "5")  // integer
    assertConverts(EqualTo("name", "Alice"), "name")  // string
    assertConverts(EqualTo("id", 1234567890L), "id")  // long
    assertConverts(EqualTo("active", true), "active")  // boolean

    // EqualTo with null becomes IsNull
    val nullResult = SparkToIcebergExpressionConverter.convert(EqualTo("name", null))
    assert(nullResult.isDefined, "Should convert EqualTo with null")
    assert(
      nullResult.get.toString.toLowerCase(java.util.Locale.ROOT).contains("null"),
      "Should handle null value")
  }

  // Comparison operator tests
  test("convert comparison operators") {
    assertConverts(LessThan("age", 30), "age", "30")
    assertConverts(GreaterThan("age", 18), "age", "18")
    assertConverts(LessThanOrEqual("price", 99.99), "price")
    assertConverts(GreaterThanOrEqual("timestamp", 1234567890L), "timestamp")
  }

  // Null check tests
  test("convert null check operators") {
    assertConverts(IsNull("name"), "name", "null")
    assertConverts(IsNotNull("name"), "name", "null")
  }

  // Logical operator tests
  test("convert logical operators") {
    assertContainsOperator(
      And(EqualTo("id", 5), GreaterThan("age", 18)), "and", "id", "age")

    assertContainsOperator(
      Or(EqualTo("status", "active"), EqualTo("status", "pending")), "or")

    // Nested And/Or
    assertContainsOperator(
      And(
        Or(EqualTo("status", "active"), EqualTo("status", "pending")),
        GreaterThan("age", 18)
      ),
      "and", "or")
  }

  // Unsupported filter tests
  test("unsupported filters return None") {
    assertReturnsNone(StringStartsWith("name", "A"))  // StringStartsWith
    assertReturnsNone(StringEndsWith("name", "Z"))  // StringEndsWith
    assertReturnsNone(StringContains("name", "foo"))  // StringContains
    assertReturnsNone(In("id", Array(1, 2, 3)))  // In
    assertReturnsNone(Not(EqualTo("id", 5)))  // Not
  }

  // convertFilters (array) tests
  test("convertFilters with single filter") {
    val filters = Array[Filter](EqualTo("id", 5))
    val result = SparkToIcebergExpressionConverter.convertFilters(filters)

    assert(result.isDefined, "Should convert single filter")
    assert(result.get.toString.contains("id"), "Expression should reference 'id'")
  }

  test("convertFilters combines multiple filters with AND") {
    val filters = Array[Filter](
      EqualTo("id", 5),
      GreaterThan("age", 18),
      IsNotNull("name")
    )
    val result = SparkToIcebergExpressionConverter.convertFilters(filters)

    assert(result.isDefined, "Should convert multiple filters")
    val exprStr = result.get.toString.toLowerCase(java.util.Locale.ROOT)
    assert(exprStr.contains("and"), "Multiple filters should be combined with AND")
    assert(exprStr.contains("id"), "Should contain 'id' filter")
    assert(exprStr.contains("age"), "Should contain 'age' filter")
    assert(exprStr.contains("name"), "Should contain 'name' filter")
  }

  test("convertFilters skips unsupported filters") {
    val filters = Array[Filter](
      EqualTo("id", 5),              // Supported
      StringStartsWith("name", "A"),  // Unsupported
      GreaterThan("age", 18)          // Supported
    )
    val result = SparkToIcebergExpressionConverter.convertFilters(filters)

    assert(result.isDefined, "Should convert supported filters and skip unsupported")
    val exprStr = result.get.toString
    assert(exprStr.contains("id"), "Should contain 'id' filter")
    assert(exprStr.contains("age"), "Should contain 'age' filter")
    assert(
      exprStr.toLowerCase(java.util.Locale.ROOT).contains("and"),
      "Should combine remaining filters with AND")
  }

  test("convertFilters returns None when all filters are unsupported") {
    val filters = Array[Filter](
      StringStartsWith("name", "A"),
      StringEndsWith("name", "Z"),
      StringContains("name", "foo")
    )
    val result = SparkToIcebergExpressionConverter.convertFilters(filters)

    assert(result.isEmpty, "Should return None when all filters are unsupported")
  }

  test("convertFilters returns None for empty array") {
    val filters = Array.empty[Filter]
    val result = SparkToIcebergExpressionConverter.convertFilters(filters)

    assert(result.isEmpty, "Empty filter array should return None")
  }

  // Type conversion tests
  test("convert with different numeric types") {
    assertConverts(EqualTo("count", 42), "count")  // int
    assertConverts(EqualTo("bigCount", 1234567890123L), "bigCount")  // long
    assertConverts(GreaterThan("price", 19.99), "price")  // double
    assertConverts(LessThan("rating", 4.5f), "rating")  // float
  }
}
