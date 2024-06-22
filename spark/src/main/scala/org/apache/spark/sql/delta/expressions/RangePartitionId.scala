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

package org.apache.spark.sql.delta.expressions

import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, RowOrdering, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._


/**
 * Unevaluable placeholder expression to be rewritten by the optimizer into [[PartitionerExpr]]
 *
 * This is just a convenient way to introduce the former, without the need to manually construct the
 * [[RangePartitioner]] beforehand, which requires an RDD to be sampled in order to determine range
 * partition boundaries. The optimizer rule will take care of all that.
 *
 * @see [[org.apache.spark.sql.delta.optimizer.RangeRepartitionIdRewrite]]
 */
case class RangePartitionId(child: Expression, numPartitions: Int)
  extends UnaryExpression with Unevaluable {

  require(numPartitions > 0, "expected the number partitions to be greater than zero")

  override def checkInputDataTypes(): TypeCheckResult = {
    if (RowOrdering.isOrderable(child.dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"cannot sort data type ${child.dataType.simpleString}")
    }
  }

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override protected def withNewChildInternal(newChild: Expression): RangePartitionId =
    copy(child = newChild)
}

/**
 * Thin wrapper around [[Partitioner]] instances that are used in Shuffle operations.
 * TODO: If needed elsewhere, consider moving it into its own file.
 */
case class PartitionerExpr(child: Expression, partitioner: Partitioner)
  extends UnaryExpression {

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  private lazy val row = new GenericInternalRow(Array[Any](null))

  override def eval(input: InternalRow): Any = {
    val value: Any = child.eval(input)
    row.update(0, value)
    partitioner.getPartition(row)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val partitionerReference = ctx.addReferenceObj("partitioner", partitioner)
    val rowReference = ctx.addReferenceObj("row", row)

    nullSafeCodeGen(ctx, ev, input =>
      s"""$rowReference.update(0, $input);
         |${ev.value} = $partitionerReference.getPartition($rowReference);
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): PartitionerExpr =
    copy(child = newChild)
}


