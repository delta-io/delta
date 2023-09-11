/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, ResolvedTable, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class ResolveDeltaIdentifier(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedDeltaIdentifier =>
      resolveDeltaIdentifier(u)

    // Since the Delta's TimeTravel is a leaf node, we have to resolve the UnresolvedIdentifier.
    case tt @ TimeTravel(unresolvedId: UnresolvedDeltaIdentifier, _, _, _) =>
      val sourceRelation = resolveDeltaIdentifier(unresolvedId)
      tt.copy(relation = sourceRelation)
  }

  private def resolveDeltaIdentifier(
      unresolvedId: UnresolvedDeltaIdentifier): DataSourceV2Relation = {
    // Resolve the identifier as a Delta table.
    val optionalDeltaTableV2 = resolveAsTable(unresolvedId)

    // If the identifier is not a Delta table, try to resolve it as a Delta file table.
    val deltaTableV2 = optionalDeltaTableV2.getOrElse {
      val tableId = unresolvedId.asTableIdentifier
      if (DeltaTableUtils.isValidPath(tableId)) {
        DeltaTableV2(sparkSession, new Path(tableId.table))
      } else {
        unresolvedId.tableNotFound(unresolvedId.nameParts)
      }
    }
    DataSourceV2Relation.create(deltaTableV2, None, Some(unresolvedId.asIdentifier))
  }

  // Resolve the identifier as a table in the Spark catalogs.
  // Return None if the table doesn't exist.
  // If the table exists but is not a Delta table, throw an exception.
  // If the identifier is a view, throw an exception.
  private def resolveAsTable(unresolvedId: UnresolvedDeltaIdentifier): Option[DeltaTableV2] = {
    val unresolvedTable =
      UnresolvedTable(unresolvedId.nameParts, unresolvedId.commandName, None)
    val analyzer = sparkSession.sessionState.analyzer
    try {
      analyzer.ResolveRelations(unresolvedTable) match {
        case r: ResolvedTable =>
          r.table match {
            case deltaTableV2: DeltaTableV2 => Some(deltaTableV2)
            case _ => throw DeltaErrors.notADeltaTableException(unresolvedTable.commandName)
          }
        case _ => None
      }
    } catch {
      // If the resolved result is a view or temp view, Spark will throw exception with error
      // class _LEGACY_ERROR_TEMP_1013. We need to catch it and throw a Delta specific exception.
      case e: AnalysisException if e.errorClass.contains("_LEGACY_ERROR_TEMP_1013") =>
        throw DeltaErrors.notADeltaTableException(unresolvedTable.commandName)
    }
  }
}
