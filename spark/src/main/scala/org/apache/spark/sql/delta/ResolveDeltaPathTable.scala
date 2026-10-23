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
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.collection.JavaConverters._

/**
 * Replaces [[UnresolvedTable]]s if the plan is for direct query on files.
 */
case class ResolveDeltaPathTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedTable =>
      ResolveDeltaPathTable
        .resolveAsPathTable(sparkSession, u.multipartIdentifier)
        .getOrElse(u)

    // Resolve a bare `delta.`path`` read relation here (before it falls through to the generic
    // V2 session-catalog `loadTable(ident)` path, which cannot carry per-relation options). This
    // preserves `UnresolvedRelation.options` so file-system credentials injected on the relation
    // reach DeltaLog's Hadoop configuration.
    case u: UnresolvedRelation
        if u.multipartIdentifier.length == 2 &&
          u.multipartIdentifier.head.equalsIgnoreCase("delta") =>
      ResolveDeltaPathTable
        .resolveAsPathTableRelation(sparkSession, u)
        .getOrElse(u)

    // `InsertIntoStatement.table` is a non-child slot, so the `UnresolvedRelation` case above does
    // not visit an INSERT target. Resolve a bare `delta.`path`` insert target here so its options
    // (e.g. injected file-system credentials) reach `DeltaLog`, matching the read path.
    case i: InsertIntoStatement =>
      i.table match {
        case u: UnresolvedRelation
            if u.multipartIdentifier.length == 2 &&
              u.multipartIdentifier.head.equalsIgnoreCase("delta") =>
          ResolveDeltaPathTable
            .resolveAsPathTableRelation(sparkSession, u)
            .map(resolved => i.copy(table = resolved))
            .getOrElse(i)
        case _ => i
      }
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
    // Forward the relation options (e.g. file-system credentials injected by a catalog) so they
    // reach DeltaLog's Hadoop configuration; otherwise bare `delta.`path`` reads cannot pick up
    // per-relation storage credentials the way file-format path relations do.
    resolveAsPathTable(sparkSession, u.multipartIdentifier, u.options.asScala.toMap)
      .map { resolvedTable =>
        DataSourceV2Relation.create(
          resolvedTable.table, Some(resolvedTable.catalog), Some(resolvedTable.identifier),
          u.options)
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
