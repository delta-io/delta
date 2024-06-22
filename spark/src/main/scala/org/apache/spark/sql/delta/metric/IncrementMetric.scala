/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.metric

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

/**
 * IncrementMetric is used to count the number of rows passing through it. It can be used to
 * wrap a child expression to count the number of rows. Its currently only accessible via the Scala
 * DSL.
 *
 * For example, consider the following expression returning a string literal:
 * If(SomeCondition,
 *  IncrementMetric(Literal("ValueIfTrue"), countTrueMetric),
 *  IncrementMetric(Literal("ValueIfFalse"), countFalseMetric))
 *
 * The SQLMetric `countTrueMetric` would be incremented whenever the condition `SomeCondition` is
 * true, and conversely `countFalseMetric` would be incremented whenever the condition is false.
 *
 * The expression does not really compute anything, and merely forwards the value computed by the
 * child expression.
 *
 * It is marked as non deterministic to ensure that it retains strong affinity with the `child`
 * expression, so as to accurately update the `metric`.
 *
 * It takes the following parameters:
 * @param child is the actual expression to call.
 * @param metric is the SQLMetric to increment.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, metric) - Returns `expr` as is, while incrementing metric.")
case class IncrementMetric(child: Expression, metric: SQLMetric)
  extends UnaryExpression with Nondeterministic {
  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def toString: String = child.toString

  override def prettyName: String = "increment_metric"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // codegen for children expressions
    val eval = child.genCode(ctx)
    val metricRef = ctx.addReferenceObj(metric.name.getOrElse("metric"), metric)
    eval.copy(code = code"""$metricRef.add(1L);""" + eval.code)
  }

  override def evalInternal(input: InternalRow): Any = {
    metric.add(1L)
    child.eval(input)
  }

  override protected def withNewChildInternal(newChild: Expression): IncrementMetric =
    copy(child = newChild)
}
