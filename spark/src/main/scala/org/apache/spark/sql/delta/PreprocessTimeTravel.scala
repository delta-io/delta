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
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{IdentifierHelper, MultipartIdentifierHelper}
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

  private val globalTempDB = conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)


  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    // Since the Delta's TimeTravel is a leaf node, we have to resolve the UnresolvedIdentifier.
    case _ @ RestoreTableStatement(tt @ TimeTravel(ui: UnresolvedIdentifier, _, _, _)) =>
      val sourceRelation = resolveTimetravelIdentifier(sparkSession, ui)
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

  private def resolveTimetravelIdentifier(
      sparkSession: SparkSession,
      unresolvedIdentifier: UnresolvedIdentifier): DataSourceV2Relation = {
    val (catalog, identifier) =
      resolveCatalogAndIdentifier(sparkSession.sessionState.catalogManager,
        unresolvedIdentifier.nameParts)
    // If the identifier is not a multipart identifier, we assume it is a table or view name.
    val tableId = if (unresolvedIdentifier.nameParts.length == 1) {
      TableIdentifier(unresolvedIdentifier.nameParts.head)
    } else {
      identifier.asTableIdentifier
    }
    assert(catalog.isInstanceOf[TableCatalog], s"Catalog ${catalog.name()} must implement " +
      s"TableCatalog to support loading Delta table.")
    val tableCatalog = catalog.asInstanceOf[TableCatalog]
    val deltaTableV2 = if (DeltaTableUtils.isDeltaTable(tableCatalog, identifier)) {
      tableCatalog.loadTable(identifier).asInstanceOf[DeltaTableV2]
    } else if (DeltaTableUtils.isValidPath(tableId)) {
      DeltaTableV2(sparkSession, new Path(tableId.table))
    } else {
      handleTableNotFound(sparkSession.sessionState.catalog, unresolvedIdentifier, tableId)
    }
    DataSourceV2Relation.create(deltaTableV2, None, Some(identifier))
  }

  private def resolveCatalogAndIdentifier(
      catalogManager: CatalogManager,
      nameParts: Seq[String]): (CatalogPlugin, Identifier) = {
    assert(nameParts.nonEmpty)
    if (nameParts.length == 1) {
      (catalogManager.currentCatalog,
        Identifier.of(catalogManager.currentNamespace, nameParts.head))
    } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {
      // Conceptually global temp views are in a special reserved catalog. However, the v2 catalog
      // API does not support view yet, and we have to use v1 commands to deal with global temp
      // views. To simplify the implementation, we put global temp views in a special namespace
      // in the session catalog. The special namespace has higher priority during name resolution.
      // For example, if the name of a custom catalog is the same with `GLOBAL_TEMP_DATABASE`,
      // this custom catalog can't be accessed.
      (catalogManager.v2SessionCatalog, nameParts.asIdentifier)
    } else {
      try {
        (catalogManager.catalog(nameParts.head), nameParts.tail.asIdentifier)
      } catch {
        case _: CatalogNotFoundException =>
          (catalogManager.currentCatalog, nameParts.asIdentifier)
      }
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
