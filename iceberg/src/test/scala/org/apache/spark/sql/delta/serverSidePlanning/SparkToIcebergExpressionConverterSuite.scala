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
import shadedForDelta.org.apache.iceberg.expressions.{UnboundPredicate, Expressions}
import scala.jdk.CollectionConverters._

class SparkToIcebergExpressionConverterSuite extends AnyFunSuite {

  // Helper: Assert comparison/equality filter (validates term and literal in correct positions)
  private def assertConvert(
      filter: Filter,
      expectedOp: Operation,
      expectedTerm: String,
      expectedLiteralStr: String): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")

    val expr = result.get
    assert(expr.op() == expectedOp,
      s"Expected operation $expectedOp but got ${expr.op()}")

    // Validate term (column name) is on left and literal value is on right.
    // Note: Spark Filter contract guarantees column is always the first parameter:
    // LessThan(attribute: String, value: Any) produces "attribute < value", never "value < attribute"
    expr match {
      // UnboundPredicate: Iceberg's expression representation before binding to a schema.
      // Once bound, column names are resolved to field IDs and types.
      case unbound: UnboundPredicate[_] =>
        // Extract column name from term string: ref(name="id") -> id
        val termStr = unbound.term().toString
        val termName = if (termStr.startsWith("ref(name=\"") && termStr.endsWith("\")")) {
          termStr.substring(10, termStr.length - 2)
        } else {
          termStr
        }
        assert(termName == expectedTerm,
          s"Expected term '$expectedTerm' but got '$termName' (from: $termStr)")
        assert(unbound.literal().toString.contains(expectedLiteralStr),
          s"Expected literal containing '$expectedLiteralStr' but got '${unbound.literal()}'")
      case _ =>
        fail(s"Expected UnboundPredicate but got ${expr.getClass.getSimpleName}")
    }
  }

  // Helper: Assert null check filter (only validates term, no literal)
  private def assertConvertNullCheck(
      filter: Filter,
      expectedOp: Operation,
      expectedTerm: String): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")

    val expr = result.get
    assert(expr.op() == expectedOp,
      s"Expected operation $expectedOp but got ${expr.op()}")

    // UnboundPredicate: Iceberg's expression representation before binding to a schema
    expr match {
      case unbound: UnboundPredicate[_] =>
        // Extract column name from term string: ref(name="id") -> id
        val termStr = unbound.term().toString
        val termName = if (termStr.startsWith("ref(name=\"") && termStr.endsWith("\")")) {
          termStr.substring(10, termStr.length - 2)
        } else {
          termStr
        }
        assert(termName == expectedTerm,
          s"Expected term '$expectedTerm' but got '$termName' (from: $termStr)")
      case _ =>
        fail(s"Expected UnboundPredicate but got ${expr.getClass.getSimpleName}")
    }
  }

  // Helper: Assert logical operator (And/Or) - just validates operation and terms present
  private def assertConvertLogical(
      filter: Filter,
      expectedOp: Operation,
      expectedTerms: String*): Unit = {
    val result = SparkToIcebergExpressionConverter.convert(filter)
    assert(result.isDefined, s"Should convert: $filter")

    val expr = result.get
    assert(expr.op() == expectedOp,
      s"Expected operation $expectedOp but got ${expr.op()}")

    val exprStr = expr.toString
    expectedTerms.foreach(term =>
      assert(exprStr.contains(term), s"Missing term '$term' in: $exprStr")
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
    assertConvert(EqualTo("name", "Alice"), Operation.EQ, "name", "Alice")  // string
    assertConvert(EqualTo("id", 1234567890L), Operation.EQ, "id", "1234567890")  // long
    assertConvert(EqualTo("active", true), Operation.EQ, "active", "true")  // boolean

    // EqualTo with null becomes IsNull
    assertConvertNullCheck(EqualTo("name", null), Operation.IS_NULL, "name")
  }

  // Comparison operator tests
  test("convert comparison operators") {
    assertConvert(LessThan("age", 30), Operation.LT, "age", "30")
    assertConvert(GreaterThan("age", 18), Operation.GT, "age", "18")
    assertConvert(LessThanOrEqual("price", 99.99), Operation.LT_EQ, "price", "99.99")
    assertConvert(GreaterThanOrEqual("timestamp", 1234567890L), Operation.GT_EQ, "timestamp", "1234567890")
  }

  // Null check tests
  test("convert null check operators") {
    assertConvertNullCheck(IsNull("name"), Operation.IS_NULL, "name")
    assertConvertNullCheck(IsNotNull("name"), Operation.NOT_NULL, "name")
  }

  // Logical operator tests
  test("convert logical operators") {
    assertConvertLogical(
      And(EqualTo("id", 5), GreaterThan("age", 18)),
      Operation.AND,
      "id", "age")

    assertConvertLogical(
      Or(EqualTo("status", "active"), EqualTo("status", "pending")),
      Operation.OR,
      "status")

    // Nested And/Or
    assertConvertLogical(
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

  // Type conversion tests
  test("convert with different numeric types") {
    assertConvert(EqualTo("count", 42), Operation.EQ, "count", "42")  // int
    assertConvert(EqualTo("bigCount", 1234567890123L), Operation.EQ, "bigCount", "1234567890123")  // long
    assertConvert(GreaterThan("price", 19.99), Operation.GT, "price", "19.99")  // double
    assertConvert(LessThan("rating", 4.5f), Operation.LT, "rating", "4.5")  // float
  }
}
