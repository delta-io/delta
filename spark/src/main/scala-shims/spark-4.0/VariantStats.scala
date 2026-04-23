/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{DataType, VariantType}

// Stubs for Spark 4.0. Variant stats collection requires 4.1+, so these expressions evaluate to
// null in Spark 4.0.
case class MinVariantStats(child: Expression) extends DeclarativeAggregate
  with UnaryLike[Expression] {
  override def nullable: Boolean = true
  override def dataType: DataType = VariantType
  private lazy val min = AttributeReference("min", VariantType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = min :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, VariantType))
  override lazy val updateExpressions: Seq[Expression] =
    Seq(Literal.create(null, VariantType))
  override lazy val mergeExpressions: Seq[Expression] =
    Seq(Literal.create(null, VariantType))
  override lazy val evaluateExpression: Expression = min
  override protected def withNewChildInternal(newChild: Expression): MinVariantStats =
    copy(child = newChild)
}

case class MaxVariantStats(child: Expression) extends DeclarativeAggregate
  with UnaryLike[Expression] {
  override def nullable: Boolean = true
  override def dataType: DataType = VariantType
  private lazy val max = AttributeReference("max", VariantType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = max :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, VariantType))
  override lazy val updateExpressions: Seq[Expression] =
    Seq(Literal.create(null, VariantType))
  override lazy val mergeExpressions: Seq[Expression] =
    Seq(Literal.create(null, VariantType))
  override lazy val evaluateExpression: Expression = max
  override protected def withNewChildInternal(newChild: Expression): MaxVariantStats =
    copy(child = newChild)
}
