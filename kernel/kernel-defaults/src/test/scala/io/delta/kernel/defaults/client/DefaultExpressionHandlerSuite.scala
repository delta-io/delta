/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.client

import io.delta.kernel.defaults.utils.ExpressionTestUtils
import io.delta.kernel.types.BooleanType.BOOLEAN
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class DefaultExpressionHandlerSuite extends AnyFunSuite with ExpressionTestUtils {

  test("create selection vector: single value") {
    Seq(true, false).foreach { testValue =>
      val outputVector = selectionVector(Seq(testValue).toArray, 0, 1)
      assert(outputVector.getDataType === BOOLEAN)
      assert(outputVector.getSize == 1)
      assert(outputVector.isNullAt(0) == false)
      assert(outputVector.getBoolean(0) == testValue)
    }
  }

  test("create selection vector: multiple values array, partial array") {
    Seq((0, testValues.length), (0, 3), (2, 2), (2, 4), (3, testValues.length)).foreach { pair =>
      val (from, to) = (pair._1, pair._2)
      val outputVector = selectionVector(testValues, from, to)
      assert(outputVector.getDataType === BOOLEAN)
      assert(outputVector.getSize == (to - from))
      Seq.range(from, to).foreach { rowId =>
        assert(outputVector.isNullAt(rowId - from) == false)
        assert(outputVector.getBoolean(rowId - from) == testValues(rowId))
      }
    }
  }

  test("create selection vector: update values array and expect no changes in output") {
    val outputVector = selectionVector(testValues, 0, testValues.length)
    // update the input values array and assert the value is not changed in the returned vector
    val oldValue = testValues(2)
    assert(oldValue == false)
    testValues(2) = true
    assert(outputVector.isNullAt(2) == false)
    assert(outputVector.getBoolean(2) == oldValue)
  }

  test("create selection vector: invalid to and/or from offset") {
    Seq((3, 2), (2, testValues.length + 1), (testValues.length + 1, 100)).foreach { pair =>
      val (from, to) = (pair._1, pair._2)
      val ex = intercept[IllegalArgumentException] {
        selectionVector(testValues, from, to)
      }
      assert(ex.getMessage.contains(
        s"invalid range from=$from, to=$to, values length=${testValues.length}"))
    }
  }

  test("create selection vector: null values array") {
    val ex = intercept[NullPointerException] {
      selectionVector(null, 0, 25)
    }
    assert(ex.getMessage.contains("values is null"))
  }

  val tableSchema = new StructType()
    .add("d1", INTEGER)
    .add("d2", STRING)
    .add("d3", new StructType()
      .add("d31", BOOLEAN)
      .add("d32", LONG))
    .add("p1", INTEGER)
    .add("p2", STRING)
  val unsupportedExpr = Map(
    (unsupported("d1"), BOOLEAN) -> false, // unsupported function
    (lt(col("d1"), int(12)), BOOLEAN) -> true,
    (lt(col("d1"), int(12)), INTEGER) -> false, // output type is not supported
    (lt(nestedCol("d3.d32"), int(12)), BOOLEAN) -> true, // implicit conversion from int to long
    (gt(col("d1"), str("sss")), STRING) -> false, // unexpected input type to > operator
    // unsupported expression in one of the AND inputs
    (and(gt(col("d2"), str("sss")), unsupported("d2")), BOOLEAN) -> false,
    // both unsupported expressions in AND inputs
    (and(gt(nestedCol("d3.d31"), str("sss")), unsupported("d2")), BOOLEAN) -> false,
    // unsupported expression in one of the OR inputs
    (or(gt(col("p2"), str("sss")), unsupported("d2")), BOOLEAN) -> false,
    // both unsupported expressions in OR inputs
    (or(gt(nestedCol("d3.d31"), str("sss")), unsupported("d2")), BOOLEAN) -> false
  ).foreach {
    case ((expr, outputType), expected) =>
      test(s"is expression supported: $expr -> $outputType") {
        assert(
          new DefaultExpressionHandler().isSupported(tableSchema, expr, outputType) == expected)
      }
  }

  private def selectionVector(values: Array[Boolean], from: Int, to: Int) = {
    new DefaultExpressionHandler().createSelectionVector(values, from, to)
  }

  private val testValues = Seq(false, true, false, false, true, true).toArray
}
