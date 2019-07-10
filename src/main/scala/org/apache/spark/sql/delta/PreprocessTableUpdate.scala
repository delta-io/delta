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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaFullTable}
import org.apache.spark.sql.delta.commands.UpdateCommand

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UpdateTable}
import org.apache.spark.sql.internal.SQLConf

case class PreprocessTableUpdate(conf: SQLConf) extends UpdateExpressionsSupport {
  def apply(update: UpdateTable): UpdateCommand = {
    val index = EliminateSubqueryAliases(update.child) match {
      case DeltaFullTable(tahoeFileIndex) =>
        tahoeFileIndex
      case o =>
        throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
    }

    val targetColNameParts =
      update.updateColumns.map{col => new UnresolvedAttribute(col.name.split("\\.")).nameParts}

    val alignedUpdateExprs = generateUpdateExpressions(
      update.child.output, targetColNameParts, update.updateExpressions, conf.resolver)
    UpdateCommand(index, update.child, alignedUpdateExprs, update.condition)
  }
}
