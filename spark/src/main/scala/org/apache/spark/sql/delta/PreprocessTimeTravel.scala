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
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{IdentifierHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.delta.DeltaTableUtils.{resolveCatalogAndIdentifier, resolveDeltaIdentifier}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2SessionCatalog}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

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
    // Since the Delta's TimeTravel is a leaf node, we have to resolve the UnresolvedIdentifier.
    case _ @ RestoreTableStatement(tt @ TimeTravel(udi: UnresolvedDeltaIdentifier, _, _, _)) =>
      val sourceRelation = resolveIdentifier(sparkSession, udi)
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

  private def resolveIdentifier(
      sparkSession: SparkSession,
      unresolvedIdentifier: UnresolvedDeltaIdentifier): DataSourceV2Relation = {
    val relation = resolveDeltaIdentifier(sparkSession, unresolvedIdentifier)
    if  (relation.isDefined) {
      relation.get
    } else {
      val tableId = if (unresolvedIdentifier.nameParts.length == 1) {
        TableIdentifier(unresolvedIdentifier.nameParts.head)
      } else {
        unresolvedIdentifier.nameParts.asTableIdentifier
      }
      handleTableNotFound(sparkSession.sessionState.catalog, unresolvedIdentifier, tableId)
    }
  }

  /**
   * Helper to resolve a [[TimeTravel]] logical plan to Delta DSv2 relation.
   */
  private def resolveTimeTravelTable(
      sparkSession: SparkSession,
      ur: UnresolvedRelation): DataSourceV2Relation = {
      val tableId = ur.multipartIdentifier match {
        case Seq(tbl) => TableIdentifier(tbl)
        case Seq(db, tbl) => TableIdentifier(tbl, Some(db))
        case _ => throw new AnalysisException(s"Illegal table name ${ur.multipartIdentifier}")
      }

      val catalog = sparkSession.sessionState.catalog
      val deltaTableV2 = if (DeltaTableUtils.isDeltaTable(sparkSession, tableId)) {
        val tbl = catalog.getTableMetadata(tableId)
        DeltaTableV2(sparkSession, new Path(tbl.location), Some(tbl))
      } else if (DeltaTableUtils.isValidPath(tableId)) {
        DeltaTableV2(sparkSession, new Path(tableId.table))
      } else {
        handleTableNotFound(catalog, ur, tableId)
      }

      val identifier = deltaTableV2.getTableIdentifierIfExists.map(
        id => Identifier.of(id.database.toArray, id.table))
      DataSourceV2Relation.create(deltaTableV2, None, identifier)
  }

  private def handleTableNotFound(
      catalog: SessionCatalog,
      inputPlan: LeafNode,
      tableIdentifier: TableIdentifier): Nothing = {
    if (
      (catalog.tableExists(tableIdentifier) &&
        catalog.getTableMetadata(tableIdentifier).tableType == CatalogTableType.VIEW) ||
        catalog.isTempView(tableIdentifier)) {
      // If table exists and not found to be a view, throw not supported error
      throw DeltaErrors.notADeltaTableException("RESTORE")
    } else {
      inputPlan.tableNotFound(tableIdentifier.nameParts)
    }
  }
}
