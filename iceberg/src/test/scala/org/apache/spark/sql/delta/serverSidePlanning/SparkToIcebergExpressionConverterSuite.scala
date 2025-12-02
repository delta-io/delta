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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  private val testSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("age", IntegerType)
  ))

  // EqualTo tests
  test("convert EqualTo with integer value") {
    val filter = EqualTo("id", 5)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert EqualTo with integer")
    assert(result.get.toString.contains("id"), "Expression should reference column 'id'")
    assert(result.get.toString.contains("5"), "Expression should contain value 5")
  }

  test("convert EqualTo with string value") {
    val filter = EqualTo("name", "Alice")
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert EqualTo with string")
    assert(result.get.toString.contains("name"), "Expression should reference column 'name'")
  }

  test("convert EqualTo with long value") {
    val filter = EqualTo("id", 1234567890L)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert EqualTo with long")
  }

  test("convert EqualTo with boolean value") {
    val filter = EqualTo("active", true)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert EqualTo with boolean")
  }

  test("convert EqualTo with null value becomes IsNull") {
    val filter = EqualTo("name", null)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert EqualTo with null")
    // When value is null, should create isNull expression
    assert(result.get.toString.toLowerCase.contains("null"),
      "Expression should handle null value")
  }

  // Comparison operator tests
  test("convert LessThan with integer") {
    val filter = LessThan("age", 30)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert LessThan")
    assert(result.get.toString.contains("age"), "Expression should reference column 'age'")
  }

  test("convert GreaterThan with integer") {
    val filter = GreaterThan("age", 18)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert GreaterThan")
    assert(result.get.toString.contains("age"), "Expression should reference column 'age'")
  }

  test("convert LessThanOrEqual with double") {
    val filter = LessThanOrEqual("price", 99.99)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert LessThanOrEqual")
  }

  test("convert GreaterThanOrEqual with long") {
    val filter = GreaterThanOrEqual("timestamp", 1234567890L)
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert GreaterThanOrEqual")
  }

  // Null check tests
  test("convert IsNull") {
    val filter = IsNull("name")
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert IsNull")
    assert(result.get.toString.contains("name"), "Expression should reference column 'name'")
    assert(result.get.toString.toLowerCase.contains("null"),
      "Expression should be null check")
  }

  test("convert IsNotNull") {
    val filter = IsNotNull("name")
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert IsNotNull")
    assert(result.get.toString.contains("name"), "Expression should reference column 'name'")
    assert(result.get.toString.toLowerCase.contains("null"),
      "Expression should be not-null check")
  }

  // Logical operator tests
  test("convert And expression") {
    val filter = And(
      EqualTo("id", 5),
      GreaterThan("age", 18)
    )
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert And")
    val exprStr = result.get.toString.toLowerCase
    assert(exprStr.contains("and"), "Expression should contain AND operator")
    assert(exprStr.contains("id"), "Expression should reference 'id'")
    assert(exprStr.contains("age"), "Expression should reference 'age'")
  }

  test("convert Or expression") {
    val filter = Or(
      EqualTo("status", "active"),
      EqualTo("status", "pending")
    )
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert Or")
    val exprStr = result.get.toString.toLowerCase
    assert(exprStr.contains("or"), "Expression should contain OR operator")
  }

  test("convert nested And/Or expressions") {
    val filter = And(
      Or(EqualTo("status", "active"), EqualTo("status", "pending")),
      GreaterThan("age", 18)
    )
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isDefined, "Should convert nested And/Or")
    val exprStr = result.get.toString.toLowerCase
    assert(exprStr.contains("and"), "Expression should contain AND")
    assert(exprStr.contains("or"), "Expression should contain OR")
  }

  // Unsupported filter tests
  test("convert unsupported filter returns None") {
    val filter = StringStartsWith("name", "A")
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isEmpty, "Unsupported filter should return None")
  }

  test("convert In filter returns None (unsupported)") {
    val filter = In("id", Array(1, 2, 3))
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isEmpty, "In filter should return None (not supported yet)")
  }

  test("convert Not filter returns None (unsupported)") {
    val filter = Not(EqualTo("id", 5))
    val result = SparkToIcebergExpressionConverter.convert(filter, testSchema)

    assert(result.isEmpty, "Not filter should return None (not supported yet)")
  }

  // convertFilters (array) tests
  test("convertFilters with single filter") {
    val filters = Array[Filter](EqualTo("id", 5))
    val result = SparkToIcebergExpressionConverter.convertFilters(filters, testSchema)

    assert(result.isDefined, "Should convert single filter")
    assert(result.get.toString.contains("id"), "Expression should reference 'id'")
  }

  test("convertFilters combines multiple filters with AND") {
    val filters = Array[Filter](
      EqualTo("id", 5),
      GreaterThan("age", 18),
      IsNotNull("name")
    )
    val result = SparkToIcebergExpressionConverter.convertFilters(filters, testSchema)

    assert(result.isDefined, "Should convert multiple filters")
    val exprStr = result.get.toString.toLowerCase
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
    val result = SparkToIcebergExpressionConverter.convertFilters(filters, testSchema)

    assert(result.isDefined, "Should convert supported filters and skip unsupported")
    val exprStr = result.get.toString
    assert(exprStr.contains("id"), "Should contain 'id' filter")
    assert(exprStr.contains("age"), "Should contain 'age' filter")
    // Should have AND because two supported filters remain
    assert(exprStr.toLowerCase.contains("and"), "Should combine remaining filters with AND")
  }

  test("convertFilters returns None when all filters are unsupported") {
    val filters = Array[Filter](
      StringStartsWith("name", "A"),
      StringEndsWith("name", "Z"),
      StringContains("name", "foo")
    )
    val result = SparkToIcebergExpressionConverter.convertFilters(filters, testSchema)

    assert(result.isEmpty, "Should return None when all filters are unsupported")
  }

  test("convertFilters returns None for empty array") {
    val filters = Array.empty[Filter]
    val result = SparkToIcebergExpressionConverter.convertFilters(filters, testSchema)

    assert(result.isEmpty, "Empty filter array should return None")
  }

  // Type conversion tests
  test("convert with different numeric types") {
    val intFilter = EqualTo("count", 42)
    val longFilter = EqualTo("bigCount", 1234567890123L)
    val doubleFilter = GreaterThan("price", 19.99)
    val floatFilter = LessThan("rating", 4.5f)

    assert(SparkToIcebergExpressionConverter.convert(intFilter, testSchema).isDefined)
    assert(SparkToIcebergExpressionConverter.convert(longFilter, testSchema).isDefined)
    assert(SparkToIcebergExpressionConverter.convert(doubleFilter, testSchema).isDefined)
    assert(SparkToIcebergExpressionConverter.convert(floatFilter, testSchema).isDefined)
  }
}
