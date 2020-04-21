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

import java.util.Locale

import org.apache.spark.sql.delta.commands.MergeIntoCommand

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

case class PreprocessTableMerge(conf: SQLConf) extends UpdateExpressionsSupport {

  def apply(mergeInto: DeltaMergeInto): MergeIntoCommand = {
    val DeltaMergeInto(target, source, condition, matched, notMatched, migratedSchema) = mergeInto

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
      case m: DeltaMergeIntoUpdateClause =>
        // Get any new columns which are in the insert clause, but not the target output or this
        // update clause.
        val existingColumns = m.resolvedActions.map(_.targetColNameParts.head) ++
          target.output.map(_.name)
        val newColsFromInsert = notMatched.toSeq.flatMap {
          _.resolvedActions.filterNot { insertAct =>
            existingColumns.exists { colName =>
              conf.resolver(insertAct.targetColNameParts.head, colName)
            }
          }
        }.map { updateAction =>
          AttributeReference(updateAction.targetColNameParts.head, updateAction.dataType)()
        }

        // Get the operations for columns that already exist...
        val existingUpdateOps = m.resolvedActions.map { a =>
          UpdateOperation(a.targetColNameParts, a.expr)
        }

        // The operations for new columns...
        val newOpsFromTargetSchema = target.output.filterNot { col =>
          m.resolvedActions.exists { updateAct =>
            conf.resolver(updateAct.targetColNameParts.head, col.name)
          }
        }.map { col =>
          UpdateOperation(Seq(col.name), col)
        }

        // And construct operations for columns that the insert clause will add.
        val newOpsFromInsert = newColsFromInsert.map { col =>
          UpdateOperation(Seq(col.name), Literal(null, col.dataType))
        }

        // Get expressions for the final schema for alignment. We must use the attribute in the
        // target plan where it exists - for new columns we just construct an attribute reference
        // to be filled in later once we evolve.
        val finalSchemaExprs =
          migratedSchema.getOrElse(target.schema).map { field =>
            target.resolve(Seq(field.name), conf.resolver).getOrElse {
              AttributeReference(field.name, field.dataType)()
            }
          }

        // Use the helper methods for in UpdateExpressionsSupport to generate expressions such
        // that nested fields can be updated (only for existing columns).
        val alignedExprs = generateUpdateExpressions(
          finalSchemaExprs,
          existingUpdateOps ++ newOpsFromTargetSchema ++ newOpsFromInsert,
          conf.resolver)
        val alignedActions: Seq[DeltaMergeAction] = alignedExprs
          .zip(finalSchemaExprs)
          .map { case (expr, attrib) => DeltaMergeAction(Seq(attrib.name), expr) }

        m.copy(m.condition, alignedActions)

      case m: DeltaMergeIntoDeleteClause => m    // Delete does not need reordering
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

      val newActionsFromTargetSchema = target.output.filterNot { col =>
        m.resolvedActions.exists { insertAct =>
          conf.resolver(insertAct.targetColNameParts.head, col.name)
        }
      }.map { col =>
        DeltaMergeAction(Seq(col.name), Literal(null, col.dataType))
      }

      val newActionsFromUpdate = matched.flatMap {
        _.resolvedActions.filterNot { updateAct =>
          m.resolvedActions.exists { insertAct =>
            conf.resolver(insertAct.targetColNameParts.head, updateAct.targetColNameParts.head)
          }
        }.toSeq
      }.map { updateAction =>
        DeltaMergeAction(updateAction.targetColNameParts, Literal(null, updateAction.dataType))
      }

      // Reorder actions by the target column order, with columns to be schema evolved that
      // aren't currently in the target at the end.
      val finalSchema = migratedSchema.getOrElse(target.schema)
      val alignedActions: Seq[DeltaMergeAction] = finalSchema.map { targetAttrib =>
        (m.resolvedActions ++ newActionsFromTargetSchema ++ newActionsFromUpdate).find { a =>
          conf.resolver(targetAttrib.name, a.targetColNameParts.head)
        }.map { a =>
          DeltaMergeAction(Seq(targetAttrib.name), castIfNeeded(a.expr, targetAttrib.dataType))
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
      source, target, tahoeFileIndex, condition,
      processedMatched, processedNotMatched, migratedSchema)
  }
}
