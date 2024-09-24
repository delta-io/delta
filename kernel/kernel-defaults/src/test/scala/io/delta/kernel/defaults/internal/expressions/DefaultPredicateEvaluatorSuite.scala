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
package io.delta.kernel.defaults.internal.expressions

import java.lang.{Boolean => BooleanJ}
import java.util.Optional
import java.util.Optional.empty
import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.expressions.{CollatedPredicate, CollationIdentifier, Column, Literal}
import io.delta.kernel.types.{BooleanType, StructType}
import org.scalatest.funsuite.AnyFunSuite

/**
 * [[DefaultPredicateEvaluator]] internally uses [[DefaultExpressionEvaluator]]. In this suite
 * test the code specific to the [[DefaultPredicateEvaluator]] such as taking into consideration of
 * existing selection vector.
 */
class DefaultPredicateEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  private val testLeftCol = booleanVector(
    Seq[BooleanJ](true, true, false, true, true, false, null, true, null, false, null))
  private val testRightCol = booleanVector(
    Seq[BooleanJ](true, false, false, true, false, false, true, null, false, null, null))

  private val testSchema = new StructType()
    .add("left", BooleanType.BOOLEAN)
    .add("right", BooleanType.BOOLEAN)

  private val batch = new DefaultColumnarBatch(
    testLeftCol.getSize, testSchema, Array(testLeftCol, testRightCol))

  private val left = comparator("=", new Column("left"), Literal.ofBoolean(true))
  private val right = comparator("=", new Column("right"), Literal.ofBoolean(true))

  private val orPredicate = or(left, right)

  private val expOrOutput = booleanVector(
    Seq[BooleanJ](true, true, false, true, true, false, true, true, null, null, null))

  test("evaluate predicate: with no starting selection vector") {
    val batch = new DefaultColumnarBatch(
      testLeftCol.getSize, testSchema, Array(testLeftCol, testRightCol))

    val actOutputVector = evalOr(batch)
    checkBooleanVectors(actOutputVector, expOrOutput)
  }

  test("evaluate predicate: with existing selection vector") {
    val existingSelVector = booleanVector(
      Seq[BooleanJ](false, true, true, true, false, false, null, null, null, null, null))
    val outputWithSelVector = booleanVector(
      Seq[BooleanJ](false, true, false, true, false, false, null, null, null, null, null))

    val actOutputVector = evalOr(batch, Optional.of(existingSelVector))
    checkBooleanVectors(actOutputVector, outputWithSelVector)
  }

  test("evaluate predicate: multiple rounds with selection vectors") {
    val output0 = evalOr(batch)
    checkBooleanVectors(output0, expOrOutput)

    val selVec1 = booleanVector(
      Seq[BooleanJ](false, true, false, true, false, false, null, null, null, null, null))
    val expOutputWithSelVec1 = booleanVector(
      Seq[BooleanJ](false, true, false, true, false, false, null, null, null, null, null))
    checkBooleanVectors(evalOr(batch, Optional.of(selVec1)), expOutputWithSelVec1)

    val selVec2 = booleanVector(
      Seq[BooleanJ](false, false, false, true, false, false, null, null, null, null, null))
    val expOutputWithSelVec2 = booleanVector(
      Seq[BooleanJ](false, false, false, true, false, false, null, null, null, null, null))
    checkBooleanVectors(evalOr(batch, Optional.of(selVec2)), expOutputWithSelVec2)
  }

  def evalOr(
    batch: ColumnarBatch, existingSelVector: Optional[ColumnVector] = empty()): ColumnVector = {
    val evaluator = new DefaultPredicateEvaluator(batch.getSchema, orPredicate)
    evaluator.eval(batch, existingSelVector)
  }
}
