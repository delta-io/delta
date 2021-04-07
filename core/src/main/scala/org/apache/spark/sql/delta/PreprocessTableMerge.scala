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

import scala.collection.mutable

import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, TypeCoercion, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

case class PreprocessTableMerge(override val conf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m: DeltaMergeInto if m.resolved => apply(m)
  }

  def apply(mergeInto: DeltaMergeInto): MergeIntoCommand = {
    val DeltaMergeInto(target, source, condition, matched, notMatched, migrateSchema) = mergeInto
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

    val shouldAutoMigrate = conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE) && migrateSchema
    val finalSchema = if (shouldAutoMigrate) {
      // The implicit conversions flag allows any type to be merged from source to target if Spark
      // SQL considers the source type implicitly castable to the target. Normally, mergeSchemas
      // enforces Parquet-level write compatibility, which would mean an INT source can't be merged
      // into a LONG target.
      SchemaUtils.mergeSchemas(target.schema, source.schema, allowImplicitConversions = true)
    } else {
      target.schema
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

        // Get expressions for the final schema for alignment. Note that attributes which already
        // exist in the target need to use the same expression ID, even if the schema will evolve.
        val finalSchemaExprs =
          finalSchema.map { field =>
            target.resolve(Seq(field.name), conf.resolver).map { r =>
              AttributeReference(field.name, field.dataType)(r.exprId)
            }.getOrElse {
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
      val alignedActions: Seq[DeltaMergeAction] = finalSchema.map { targetAttrib =>
        (m.resolvedActions ++ newActionsFromTargetSchema ++ newActionsFromUpdate).find { a =>
          conf.resolver(targetAttrib.name, a.targetColNameParts.head)
        }.map { a =>
          DeltaMergeAction(
            Seq(targetAttrib.name),
            castIfNeeded(
              a.expr,
              targetAttrib.dataType,
              allowStructEvolution = shouldAutoMigrate))
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
      processedMatched, processedNotMatched, Some(finalSchema))
  }
}
