/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.commands.UpdateCommand

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Preprocesses the [[DeltaUpdateTable]] logical plan before converting it to [[UpdateCommand]].
 * - Adjusts the column order, which could be out of order, based on the destination table
 * - Generates expressions to compute the value of all target columns in Delta table, while taking
 * into account that the specified SET clause may only update some columns or nested fields of
 * columns.
 */
case class PreprocessTableUpdate(override val conf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: DeltaUpdateTable if u.resolved =>
      u.condition.foreach { cond =>
        if (SubqueryExpression.hasSubquery(cond)) {
          throw DeltaErrors.subqueryNotSupportedException("UPDATE", cond)
        }
      }
      toCommand(u)
  }

  def toCommand(update: DeltaUpdateTable): UpdateCommand = {
    val index = EliminateSubqueryAliases(update.child) match {
      case DeltaFullTable(tahoeFileIndex) =>
        tahoeFileIndex
      case o =>
        throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
    }

    val targetColNameParts = update.updateColumns.map(DeltaUpdateTable.getTargetColNameParts(_))
    val alignedUpdateExprs = generateUpdateExpressions(
      update.child.output, targetColNameParts, update.updateExpressions, conf.resolver)
    UpdateCommand(index, update.child, alignedUpdateExprs, update.condition)
  }
}
