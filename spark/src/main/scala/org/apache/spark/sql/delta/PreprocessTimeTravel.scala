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

import org.apache.spark.sql.catalyst.TimeTravel
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedTable, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves the [[UnresolvedRelation]] in command 's child [[TimeTravel]].
 *   Currently Delta depends on Spark 3.2 which does not resolve the [[UnresolvedRelation]]
 *   in [[TimeTravel]]. Once Delta upgrades to Spark 3.3, this code can be removed.
 *
 * TODO: refactoring this analysis using Spark's native [[TimeTravelRelation]] logical plan
 */
case class PreprocessTimeTravel(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case _ @ RestoreTableStatement(tt @ TimeTravel(ur @ UnresolvedRelation(_, _, _), _, _, _)) =>
      val sourceRelation = resolveTimeTravelTable(sparkSession, ur)
      return RestoreTableStatement(
        TimeTravel(
          sourceRelation,
          tt.timestamp,
          tt.version,
          tt.creationSource))

    case ct @ CloneTableStatement(
        tt @ TimeTravel(ur: UnresolvedRelation, _, _, _), _,
          _, _, _, _, _) =>
      val sourceRelation = resolveTimeTravelTable(sparkSession, ur)
      ct.copy(source = TimeTravel(
        sourceRelation,
        tt.timestamp,
        tt.version,
        tt.creationSource))
  }

  /**
   * Helper to resolve a [[TimeTravel]] logical plan to Delta DSv2 relation.
   */
  private def resolveTimeTravelTable(
      sparkSession: SparkSession,
      ur: UnresolvedRelation): LogicalPlan = {
    EliminateSubqueryAliases(sparkSession.sessionState.analyzer.ResolveRelations(ur)) match {
      case _: View =>
        // If table exists and not found to be a view, throw not supported error
        throw DeltaErrors.notADeltaTableException("RESTORE")
      case tableRelation =>
        if (tableRelation.resolved) {
          tableRelation
        } else {
          ResolveDeltaPathTable.resolveAsPathTable(sparkSession, ur.multipartIdentifier) match {
            case Some(r: ResolvedTable) =>
              DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
            case None =>
              ur.tableNotFound(ur.multipartIdentifier)
          }
        }
    }
  }
}
