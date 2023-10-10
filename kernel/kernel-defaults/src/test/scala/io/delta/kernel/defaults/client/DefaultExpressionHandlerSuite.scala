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

import io.delta.kernel.types.BooleanType
import org.scalatest.funsuite.AnyFunSuite

class DefaultExpressionHandlerSuite extends AnyFunSuite {

  test("create selection vector: single value") {
    Seq(true, false).foreach { testValue =>
      val outputVector = selectionVector(Seq(testValue).toArray, 0, 1)
      assert(outputVector.getDataType === BooleanType.BOOLEAN)
      assert(outputVector.getSize == 1)
      assert(outputVector.isNullAt(0) == false)
      assert(outputVector.getBoolean(0) == testValue)
    }
  }

  test("create selection vector: multiple values array, partial array") {
    Seq((0, testValues.length), (0, 3), (2, 2), (2, 4), (3, testValues.length)).foreach { pair =>
      val (from, to) = (pair._1, pair._2)
      val outputVector = selectionVector(testValues, from, to)
      assert(outputVector.getDataType === BooleanType.BOOLEAN)
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

  private def selectionVector(values: Array[Boolean], from: Int, to: Int) = {
    new DefaultExpressionHandler().createSelectionVector(values, from, to)
  }

  private val testValues = Seq(false, true, false, false, true, true).toArray
}
