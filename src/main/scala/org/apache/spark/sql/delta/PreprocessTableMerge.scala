/*
 * Copyright 2019 Databricks, Inc.
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

import java.util.Locale

import org.apache.spark.sql.delta.commands.MergeIntoCommand

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

case class PreprocessTableMerge(conf: SQLConf) extends UpdateExpressionsSupport {

  def apply(mergeInto: MergeInto): MergeIntoCommand = {
    val MergeInto(target, source, condition, matched, notMatched) = mergeInto

    def checkCondition(cond: Expression, conditionName: String): Unit = {
      if (!cond.deterministic) {
        throw DeltaErrors.nonDeterministicNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
      if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
        throw DeltaErrors.aggsNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
      if (SubqueryExpression.hasSubquery(cond)) {
        throw DeltaErrors.subqueryNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
    }

    checkCondition(condition, "search")
    (matched ++ notMatched).filter(_.condition.nonEmpty).foreach { clause =>
      checkCondition(clause.condition.get, clause.clauseType.toUpperCase(Locale.ROOT))
    }

    val processedMatched = matched.map {
      case m: MergeIntoUpdateClause =>
        val alignedUpdateExprs = {
          // Use the helper methods for in UpdateExpressionsSupport to generate expressions such
          // that nested fields can be updated.
          val updateOps =
            m.resolvedActions.map { a => UpdateOperation(a.targetColNameParts, a.expr) }
          generateUpdateExpressions(target.output, updateOps, conf.resolver)
        }
        val alignedActions: Seq[MergeAction] = alignedUpdateExprs.zip(target.output).map {
          case (expr, attrib) => MergeAction(Seq(attrib.name), expr)
        }
        m.copy(m.condition, alignedActions)

      case m: MergeIntoDeleteClause => m    // Delete does not need reordering
    }

    val processedNotMatched = notMatched.map { m =>
      // Check if columns are distinct. All actions should have targetColNameParts.size = 1.
      m.resolvedActions.foreach { a =>
        if (a.targetColNameParts.size > 1) {
          throw DeltaErrors.nestedFieldNotSupported(
            "INSERT clause of MERGE operation",
            a.targetColNameParts.mkString("`", "`.`", "`")
          )
        }
      }

      val targetColNames = m.resolvedActions.map(_.targetColNameParts.head)
      if (targetColNames.distinct.size < targetColNames.size) {
        throw new AnalysisException(s"Duplicate column names in INSERT clause")
      }

      // Reorder actions by the target column order.
      val alignedActions: Seq[MergeAction] = target.output.map { targetAttrib =>
        m.resolvedActions.find { a =>
          conf.resolver(targetAttrib.name, a.targetColNameParts.head)
        }.map { a =>
          MergeAction(Seq(targetAttrib.name), castIfNeeded(a.expr, targetAttrib.dataType))
        }.getOrElse {
          // If a target table column was not found in the INSERT columns and expressions,
          // then throw exception as there must be an expression to set every target column.
          throw new AnalysisException(
            s"Unable to find the column '${targetAttrib.name}' of the target table from " +
              s"the INSERT columns: ${targetColNames.mkString(", ")}. " +
              s"INSERT clause must specify value for all the columns of the target table.")
        }
      }
      m.copy(m.condition, alignedActions)
    }

    val tahoeFileIndex = EliminateSubqueryAliases(target) match {
      case DeltaFullTable(index) => index
      case o => throw DeltaErrors.notADeltaSourceException("MERGE", Some(o))
    }

    MergeIntoCommand(
      source, target, tahoeFileIndex, condition, processedMatched, processedNotMatched)
  }
}
