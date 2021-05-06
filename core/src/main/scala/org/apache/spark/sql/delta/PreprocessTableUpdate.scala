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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField

/**
 * Preprocesses the [[DeltaUpdateTable]] logical plan before converting it to [[UpdateCommand]].
 * - Adjusts the column order, which could be out of order, based on the destination table
 * - Generates expressions to compute the value of all target columns in Delta table, while taking
 * into account that the specified SET clause may only update some columns or nested fields of
 * columns.
 */
case class PreprocessTableUpdate(sqlConf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def conf: SQLConf = sqlConf

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: DeltaUpdateTable if u.resolved =>
      u.condition.foreach { cond =>
        if (SubqueryExpression.hasSubquery(cond)) {
          throw DeltaErrors.subqueryNotSupportedException("UPDATE", cond)
        }
      }
      toCommand(u)
  }

  /**
   * Generate update expressions for generated columns that the user doesn't provide a update
   * expression. For each item in `updateExprs` that's None, we will find its generation expression
   * from `generatedColumns`. In order to resolve this generation expression, we will create a
   * fake Project which contains all update expressions and resolve the generation expression with
   * this project. Source columns of a generation expression will also be replaced with their
   * corresponding update expressions.
   *
   * For example, given a table that has a generated column `g` defined as `c1 + 10`. For the
   * following update command:
   *
   * UPDATE target SET c1 = c2 + 100, c2 = 1000
   *
   * We will generate the update expression `(c2 + 100) + 10`` for column `g`. Note: in this update
   * expression, we should use the old `c2` attribute rather than its new value 1000.
   *
   * @return a sequence of update expressions for all of columns in the table.
   */
  private def generateUpdateExprsForGeneratedColumns(
      update: DeltaUpdateTable,
      generatedColumns: Seq[StructField],
      updateExprs: Seq[Option[Expression]]): Seq[Expression] = {
    assert(
      update.child.output.size == updateExprs.length,
      s"'generateUpdateExpressions' should return expressions that are aligned with the column " +
        s"list. Expected size: ${update.child.output.size}, actual size: ${updateExprs.length}")
    val attrsWithExprs = update.child.output.zip(updateExprs)
    val exprsForProject = attrsWithExprs.flatMap {
      case (attr, Some(expr)) =>
        // Create a named expression so that we can use it in Project
        val exprForProject = Alias(expr, attr.name)()
        Some(exprForProject.exprId -> exprForProject)
      case (_, None) => None
    }.toMap
    // Create a fake Project to resolve the generation expressions
    val fakePlan = Project(exprsForProject.values.toArray[NamedExpression], update.child)
    attrsWithExprs.map {
      case (_, Some(expr)) => expr
      case (targetCol, None) =>
        // `targetCol` is a generated column and the user doesn't provide a update expression.
        val resolvedExpr =
          generatedColumns.find(f => conf.resolver(f.name, targetCol.name)) match {
            case Some(field) =>
              val expr = GeneratedColumn.getGenerationExpression(field).get
              resolveReferencesForExpressions(SparkSession.active, expr :: Nil, fakePlan).head
            case None =>
              // Should not happen
              throw new IllegalStateException(s"$targetCol is not a generated column " +
                s"but is missing its update expression")
          }
        // As `resolvedExpr` will refer to attributes in `fakePlan`, we need to manually replace
        // these attributes with their update expressions.
        resolvedExpr.transform {
          case a: AttributeReference if exprsForProject.contains(a.exprId) =>
            exprsForProject(a.exprId).child
        }
    }
  }

  def toCommand(update: DeltaUpdateTable): UpdateCommand = {
    val deltaLogicalNode = EliminateSubqueryAliases(update.child)
    val index = deltaLogicalNode match {
      case DeltaFullTable(tahoeFileIndex) =>
        tahoeFileIndex
      case o =>
        throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
    }

    val generatedColumns = GeneratedColumn.getGeneratedColumns(index.snapshotAtAnalysis)
    if (generatedColumns.nonEmpty && !deltaLogicalNode.isInstanceOf[LogicalRelation]) {
      // Disallow temp views referring to a Delta table that contains generated columns. When the
      // user doesn't provide expressions for generated columns, we need to create update
      // expressions for them automatically. Currently, we assume `update.child.output` is the same
      // as the table schema when checking whether a column in `update.child.output` is a generated
      // column in the table.
      throw DeltaErrors.updateOnTempViewWithGenerateColsNotSupported
    }

    val targetColNameParts = update.updateColumns.map(DeltaUpdateTable.getTargetColNameParts(_))
    val alignedUpdateExprs = generateUpdateExpressions(
      update.child.output,
      targetColNameParts,
      update.updateExpressions,
      conf.resolver,
      generatedColumns)
    val alignedUpdateExprsAfterAddingGenerationExprs =
      if (alignedUpdateExprs.forall(_.nonEmpty)) {
        alignedUpdateExprs.map(_.get)
      } else {
        // Some expressions for generated columns are not specified by the user, so we need to
        // create them based on the generation expressions.
        generateUpdateExprsForGeneratedColumns(update, generatedColumns, alignedUpdateExprs)
      }
    UpdateCommand(
      index,
      update.child,
      alignedUpdateExprsAfterAddingGenerationExprs,
      update.condition)
  }
}
