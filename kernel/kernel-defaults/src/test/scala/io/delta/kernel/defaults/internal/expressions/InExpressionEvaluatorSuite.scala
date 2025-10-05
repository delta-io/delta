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
package io.delta.kernel.defaults.internal.expressions

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.{Expression, In, Literal}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class InExpressionEvaluatorSuite extends AnyFunSuite {

  test("validateAndTransform - same types") {
    val valueExpr = Literal.ofInt(5)
    val listExprs: List[Expression] = List(Literal.ofInt(1), Literal.ofInt(2), Literal.ofInt(3))
    val in = new In(valueExpr, listExprs.asJava)

    val result = InExpressionEvaluator.validateAndTransform(
      in,
      valueExpr,
      IntegerType.INTEGER,
      listExprs.asJava,
      List[DataType](IntegerType.INTEGER, IntegerType.INTEGER, IntegerType.INTEGER).asJava)

    // Should return the same expressions since no casting is needed
    assert(result.getValueExpression == valueExpr)
    assert(result.getInListElements.size() == 3)
  }

  test("validateAndTransform - implicit casting byte to int") {
    val valueExpr = Literal.ofByte(5.toByte)
    val listExprs: List[Expression] = List(Literal.ofInt(1), Literal.ofInt(2), Literal.ofInt(3))
    val in = new In(valueExpr, listExprs.asJava)

    val result = InExpressionEvaluator.validateAndTransform(
      in,
      valueExpr,
      ByteType.BYTE,
      listExprs.asJava,
      List[DataType](IntegerType.INTEGER, IntegerType.INTEGER, IntegerType.INTEGER).asJava)

    // Value should be cast to int, list should remain the same
    assert(result.getValueExpression.isInstanceOf[ImplicitCastExpression])
    assert(result.getInListElements.size() == 3)
  }

  test("validateAndTransform - incompatible types should fail") {
    val valueExpr = Literal.ofString("test")
    val listExprs: List[Expression] = List(Literal.ofInt(1), Literal.ofInt(2))
    val in = new In(valueExpr, listExprs.asJava)

    assertThrows[RuntimeException] {
      InExpressionEvaluator.validateAndTransform(
        in,
        valueExpr,
        StringType.STRING,
        listExprs.asJava,
        List[DataType](IntegerType.INTEGER, IntegerType.INTEGER).asJava)
    }
  }
}
