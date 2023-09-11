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
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, RestoreTableStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{TableIdentifier, TimeTravel}
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{IdentifierHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

case class ResolveDeltaIdentifier(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private val globalTempDB =
    sparkSession.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedDeltaIdentifier =>
      resolveDeltaIdentifier(sparkSession, u).getOrElse(u.tableNotFound(u.nameParts))

    // Since the Delta's TimeTravel is a leaf node, we have to resolve the UnresolvedIdentifier.
    case tt @ TimeTravel(udi: UnresolvedDeltaIdentifier, _, _, _) =>
      val sourceRelation = resolveIdentifierInTimeTravel(sparkSession, udi)
      tt.copy(relation = sourceRelation)
  }

  private def resolveDeltaIdentifier(
    sparkSession: SparkSession,
    unresolvedIdentifier: UnresolvedDeltaIdentifier): Option[DataSourceV2Relation] = {
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
    val optionalDeltaTableV2 = loadDeltaTable(tableCatalog, identifier)
    val deltaTableV2 = optionalDeltaTableV2.orElse {
      if (DeltaTableUtils.isValidPath(tableId)) {
        Some(DeltaTableV2(sparkSession, new Path(tableId.table)))
      } else {
        None
      }
    }
    deltaTableV2.map(d => DataSourceV2Relation.create(d, None, Some(identifier)))
  }

  // Resolve the input name parts to a CatalogPlugin and Identifier.
  // The code is from Apache Spark on org.apache.spark.sql.connector.catalog.CatalogAndIdentifier.
  // We need to use this because the original one is private.
  private def resolveCatalogAndIdentifier(
    catalogManager: CatalogManager,
    nameParts: Seq[String]): (CatalogPlugin, Identifier) = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
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

  private def resolveIdentifierInTimeTravel(
    sparkSession: SparkSession,
    unresolvedIdentifier: UnresolvedDeltaIdentifier): DataSourceV2Relation = {
    val relation = resolveDeltaIdentifier(sparkSession, unresolvedIdentifier)
    if (relation.isDefined) {
      relation.get
    } else {
      val tableId = if (unresolvedIdentifier.nameParts.length == 1) {
        TableIdentifier(unresolvedIdentifier.nameParts.head)
      } else {
        unresolvedIdentifier.nameParts.asTableIdentifier
      }
      ResolveDeltaIdentifier.handleTableNotFoundInTimeTravel(
        sparkSession.sessionState.catalog, unresolvedIdentifier, tableId)
    }
  }

  /**
   * Return a DeltaTableV2 instance if the table exists and it is a Delta table,
   * otherwise return None.
   */
  private def loadDeltaTable(
      tableCatalog: TableCatalog,
      identifier: Identifier): Option[DeltaTableV2] = {
    val tableExists = tableCatalog.tableExists(identifier)
    if (tableExists) {
      tableCatalog.loadTable(identifier) match {
        case v: DeltaTableV2 =>
          Some(v)
        case _ =>
          None
      }
    } else {
      None
    }
  }
}

object ResolveDeltaIdentifier {
  def handleTableNotFoundInTimeTravel(
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