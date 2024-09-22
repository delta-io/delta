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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.commands.UpdateCommand

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

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

  override protected val supportMergeAndUpdateLegacyCastBehavior: Boolean = true

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
    val deltaLogicalNode = EliminateSubqueryAliases(update.child)
    val (relation, index) = deltaLogicalNode match {
      case DeltaFullTable(rel, tahoeFileIndex) => rel -> tahoeFileIndex
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
      throw DeltaErrors.operationOnTempViewWithGenerateColsNotSupported("UPDATE")
    }

    val targetColNameParts = update.updateColumns.map(DeltaUpdateTable.getTargetColNameParts(_))

    IdentityColumn.blockIdentityColumnUpdate(index.snapshotAtAnalysis.schema, targetColNameParts)

    val alignedUpdateExprs = generateUpdateExpressions(
      targetSchema = update.child.schema,
      defaultExprs = update.child.output,
      nameParts = targetColNameParts,
      updateExprs = update.updateExpressions,
      resolver = conf.resolver,
      generatedColumns = generatedColumns
    )
    val alignedUpdateExprsAfterAddingGenerationExprs =
      if (alignedUpdateExprs.forall(_.nonEmpty)) {
        alignedUpdateExprs.map(_.get)
      } else {
        // Some expressions for generated columns are not specified by the user, so we need to
        // create them based on the generation expressions.
        generateUpdateExprsForGeneratedColumns(update.child, generatedColumns, alignedUpdateExprs)
      }
    UpdateCommand(
      index,
      relation.catalogTable,
      update.child,
      alignedUpdateExprsAfterAddingGenerationExprs,
      update.condition)
  }
}
