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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Replaces [[UnresolvedTable]]s if the plan is for direct query on files.
 */
case class ResolveDeltaPathTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedTable =>
      ResolveDeltaPathTable
        .resolveAsPathTable(sparkSession, u.multipartIdentifier)
        .getOrElse(u)
  }
}

object ResolveDeltaPathTable {

  /**
   * Try resolving the input table as a Path table.
   * If the path table exists, return a [[DataSourceV2Relation]] instance. Otherwise, return None.
   */
  def resolveAsPathTableRelation(
      sparkSession: SparkSession,
      u: UnresolvedRelation) : Option[DataSourceV2Relation] = {
    resolveAsPathTable(sparkSession, u.multipartIdentifier)
      .map { resolvedTable =>
        DataSourceV2Relation.create(
          resolvedTable.table, Some(resolvedTable.catalog), Some(resolvedTable.identifier))
      }
  }

  /**
   * Try resolving the input table as a Path table.
   * If the path table exists, return a [[ResolvedTable]] instance. Otherwise, return None.
   */
  private def resolveAsPathTable(
      sparkSession: SparkSession,
      multipartIdentifier: Seq[String],
      options: Map[String, String] = Map.empty): Option[ResolvedTable] = {
    val sessionState = sparkSession.sessionState
    if (!sessionState.conf.runSQLonFile || multipartIdentifier.size != 2) {
      return None
    }
    val tableId = multipartIdentifier.asTableIdentifier
    if (!DeltaTableUtils.isValidPath(tableId)) {
      return None
    }
    val deltaTableV2 = DeltaTableV2(sparkSession, new Path(tableId.table), options = options)
    val sessionCatalog = sessionState.catalogManager.v2SessionCatalog.asTableCatalog
    Some(ResolvedTable.create(sessionCatalog, multipartIdentifier.asIdentifier, deltaTableV2))
  }
}
