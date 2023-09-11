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
import org.apache.spark.sql.SparkSession
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
    val unresolvedTable =
      UnresolvedTable(unresolvedId.nameParts, unresolvedId.commandName, None)
    val analyzer = sparkSession.sessionState.analyzer
    val optionalDeltaTableV2 = analyzer.ResolveRelations(unresolvedTable) match {
      case r: ResolvedTable =>
        r.table match {
          case deltaTableV2: DeltaTableV2 => Some(deltaTableV2)
          case _ => throw DeltaErrors.notADeltaTableException(unresolvedTable.commandName)
        }
      case _ => None
    }

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
}
