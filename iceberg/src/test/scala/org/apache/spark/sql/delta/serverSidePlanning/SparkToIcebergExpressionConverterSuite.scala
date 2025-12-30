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
import shadedForDelta.org.apache.iceberg.types.Types

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  // Test schema used to bind expressions for semantic comparison.
  // ExpressionUtil.equivalent() needs a schema to:
  // 1. Bind unbound expressions (resolve column names to field IDs and types)
  // 2. Enable type-safe comparison that validates literal types match (e.g., int vs string)
  // Without binding, we can't verify that "age < 30" (int) isn't incorrectly "age < "30"" (string)
  private val TEST_STRUCT = Types.StructType.of(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "age", Types.IntegerType.get()),
    Types.NestedField.optional(4, "price", Types.DoubleType.get()),
    Types.NestedField.optional(5, "timestamp", Types.LongType.get()),
    Types.NestedField.optional(6, "active", Types.BooleanType.get())
  )

  // Helper: Assert conversion using semantic equivalence (catches type mismatches)
  private def assertConvert(sparkFilter: Filter, expectedIcebergExpr: Expression): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(sparkFilter)
    assert(result.isDefined, s"Should convert: $sparkFilter")
    
    val actual = result.get
    // ExpressionUtil.equivalent(left, right, struct, caseSensitive)
    // caseSensitive=true: column names must match case exactly
    assert(
      ExpressionUtil.equivalent(expectedIcebergExpr, actual, TEST_STRUCT, true),
      s"Expressions not equivalent:\n  Expected: $expectedIcebergExpr\n  Actual: $actual"
    )
  }

  // Helper: Assert filter returns None (unsupported)
  private def assertReturnsNone(filter: Filter): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isEmpty, s"Should return None for unsupported filter: $filter")
  }

  // EqualTo tests
  test("convert EqualTo with various types") {
    assertConvert(EqualTo("id", 5), Expressions.equal("id", 5))  // integer
    assertConvert(EqualTo("name", "Alice"), Expressions.equal("name", "Alice"))  // string
    assertConvert(EqualTo("id", 1234567890L), Expressions.equal("id", 1234567890L))  // long
    assertConvert(EqualTo("active", true), Expressions.equal("active", true))  // boolean

    // EqualTo with null becomes IsNull
    assertConvert(EqualTo("name", null), Expressions.isNull("name"))
  }

  // Comparison operator tests
  test("convert comparison operators") {
    assertConvert(LessThan("age", 30), Expressions.lessThan("age", 30))
    assertConvert(GreaterThan("age", 18), Expressions.greaterThan("age", 18))
    assertConvert(LessThanOrEqual("price", 99.99), Expressions.lessThanOrEqual("price", 99.99))
    assertConvert(
      GreaterThanOrEqual("timestamp", 1234567890L),
      Expressions.greaterThanOrEqual("timestamp", 1234567890L))
  }

  // Null check tests
  test("convert null check operators") {
    assertConvert(IsNull("name"), Expressions.isNull("name"))
    assertConvert(IsNotNull("name"), Expressions.notNull("name"))
  }

  // Logical operator tests
  test("convert logical operators") {
    assertConvert(
      And(EqualTo("id", 5), GreaterThan("age", 18)),
      Expressions.and(Expressions.equal("id", 5), Expressions.greaterThan("age", 18)))

    assertConvert(
      Or(EqualTo("name", "active"), EqualTo("name", "pending")),
      Expressions.or(Expressions.equal("name", "active"), Expressions.equal("name", "pending")))

    // Nested And/Or
    assertConvert(
      And(
        Or(EqualTo("name", "active"), EqualTo("name", "pending")),
        GreaterThan("age", 18)
      ),
      Expressions.and(
        Expressions.or(Expressions.equal("name", "active"), Expressions.equal("name", "pending")),
        Expressions.greaterThan("age", 18)))
  }

  // Unsupported filter tests
  test("unsupported filters return None") {
    assertReturnsNone(StringStartsWith("name", "A"))  // StringStartsWith
    assertReturnsNone(StringEndsWith("name", "Z"))  // StringEndsWith
    assertReturnsNone(StringContains("name", "foo"))  // StringContains
    assertReturnsNone(In("id", Array(1, 2, 3)))  // In
    assertReturnsNone(Not(EqualTo("id", 5)))  // Not
  }

  // Type conversion tests - validates literal types are preserved correctly
  test("convert with different numeric types") {
    assertConvert(EqualTo("age", 42), Expressions.equal("age", 42))  // int
    assertConvert(EqualTo("id", 1234567890123L), Expressions.equal("id", 1234567890123L))  // long
    assertConvert(GreaterThan("price", 19.99), Expressions.greaterThan("price", 19.99))  // double
    // Note: Spark represents floats as doubles in Filter expressions, so 4.5f becomes 4.5 (double)
    assertConvert(LessThan("price", 4.5), Expressions.lessThan("price", 4.5))
  }
}
