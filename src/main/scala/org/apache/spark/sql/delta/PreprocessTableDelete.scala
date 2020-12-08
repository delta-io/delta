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

import org.apache.spark.sql.delta.commands.DeleteCommand

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{DeltaDelete, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Preprocess the [[DeltaDelete]] plan to convert to [[DeleteCommand]].
 */
case class PreprocessTableDelete(override val conf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case d: DeltaDelete if d.resolved =>
        d.condition.foreach { cond =>
          if (SubqueryExpression.hasSubquery(cond)) {
            throw DeltaErrors.subqueryNotSupportedException("DELETE", cond)
          }
        }
        toCommand(d)
    }
  }

  def toCommand(d: DeltaDelete): DeleteCommand = EliminateSubqueryAliases(d.child) match {
    case DeltaFullTable(tahoeFileIndex) =>
      DeleteCommand(tahoeFileIndex, d.child, d.condition)

    case o =>
      throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
  }
}
