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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Literal, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType}

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

/**
 * ConditionalIncrementMetric is used to count the number of rows passing through it based on
 * a condition. It can be used to wrap a child expression to count the number of rows only when
 * the condition is true. Its currently only accessible via the Scala DSL.
 *
 * For example, consider the following expression:
 * ConditionalIncrementMetric(Literal("SomeValue"), GreaterThan(col("count"), Literal(10)),
 *   countMetric)
 *
 * The SQLMetric `countMetric` would be incremented whenever the condition is true.
 * Note: Be careful about nullability! The metric will not be incremented if the condition
 *       evaluates to NULL.
 *       When trying to invert a condition, use `condition = false` instead of `not condition`,
 *       if the metrics is supposed to be incremented if condition was NULL.
 *
 * The Expression returns the value computed by the child expression.
 *
 * It is marked as non deterministic to ensure that it retains strong affinity with the `child`
 * expression, and is not optimized out, so as to accurately update the `metric`.
 *
 * It takes the following parameters:
 * @param child is the actual expression to call.
 * @param condition is the Boolean expression that determines whether to increment the metric.
 * @param metric is the SQLMetric to increment.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, condition, metric) " +
    "- Returns `expr` as is, while incrementing metric when condition is true.")
case class ConditionalIncrementMetric(child: Expression, condition: Expression, metric: SQLMetric)
  extends Expression with Nondeterministic {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (condition.dataType != BooleanType) {
      TypeCheckResult.DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "second",
          "requiredType" -> TypeUtils.toSQLType(BooleanType),
          "inputSql" -> TypeUtils.toSQLExpr(condition),
          "inputType" -> TypeUtils.toSQLType(condition.dataType)
        )
      )
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def toString: String = s"conditional_increment_metric($child, $condition)"

  override def prettyName: String = "conditional_increment_metric"

  override def children: Seq[Expression] = Seq(child, condition)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childEval = child.genCode(ctx)
    val conditionEval = condition.genCode(ctx)
    val metricRef = ctx.addReferenceObj(metric.name.getOrElse("metric"), metric)

    val incrementCode = code"""
      |${conditionEval.code}
      |if (!${conditionEval.isNull} && ${conditionEval.value}) {
      |  $metricRef.add(1L);
      |}
      |""".stripMargin

    childEval.copy(code = incrementCode + childEval.code)
  }

  override def evalInternal(input: InternalRow): Any = {
    val conditionResult = condition.eval(input)
    if (conditionResult != null && conditionResult.asInstanceOf[Boolean]) {
      metric.add(1L)
    }
    child.eval(input)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ConditionalIncrementMetric = {
    require(newChildren.length == 2, "ConditionalIncrementMetric requires exactly 2 children")
    copy(child = newChildren(0), condition = newChildren(1))
  }
}

/**
 * Optimization rule that simplifies ConditionalIncrementMetric expressions with constant
 * conditions.
 */
object OptimizeConditionalIncrementMetric extends Rule[LogicalPlan] {
  private def isEnabled: Boolean =
    SQLConf.get.getConf(DeltaSQLConf.DELTA_OPTIMIZE_CONDITIONAL_INCREMENT_METRIC_ENABLED)

  override def apply(plan: LogicalPlan): LogicalPlan = if (isEnabled) {
    plan.transformAllExpressionsWithSubqueries {
      case ConditionalIncrementMetric(child, Literal(true, BooleanType), metric) =>
        // Always true condition: convert to regular IncrementMetric
        IncrementMetric(child, metric)

      case ConditionalIncrementMetric(child, Literal(false, BooleanType), metric) =>
        // Always false condition: remove metric logic, keep only child
        child

      case ConditionalIncrementMetric(child, Literal(null, BooleanType), metric) =>
        // Null condition: remove metric logic, keep only child
        child
    }
  } else {
    plan
  }
}
