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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, CatalogPlugin, Identifier, Table, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

/**
 * Helper object for resolving tables using a *non-session* catalog.
 */
object CatalogResolver {
  private def asDeltaTable(spark: SparkSession, table: Table): DeltaTableV2 = table match {
    case v2: DeltaTableV2 => v2
    case v1: V1Table if DeltaSourceUtils.isDeltaTable(v1.v1Table.provider) =>
      DeltaTableV2(spark, new Path(v1.v1Table.location), Some(v1.v1Table))
    case _ => throw DeltaErrors.notADeltaTableException(
      DeltaTableIdentifier(table = Some(TableIdentifier(table.name()))))
  }

  /**
   Returns a [[(CatalogPlugin, Identifier)]] if a catalog exists with the
   input name, otherwise throws a [[CatalogNotFoundException]]
  */
  def getCatalogPluginAndIdentifier(
      spark: SparkSession,
      catalog: String,
      ident: Seq[String]): (CatalogPlugin, Identifier) = {
    (spark.sessionState.catalogManager.catalog(catalog),
      MultipartIdentifierHelper(ident).asIdentifier)
  }

  def getDeltaTableFromCatalog(
      spark: SparkSession,
      catalog: CatalogPlugin,
      ident: Identifier): DeltaTableV2 = {
    val tblCatalog = catalog.asTableCatalog
    if (tblCatalog.tableExists(ident)) {
      asDeltaTable(spark, tblCatalog.loadTable(ident))
    } else {
      throw DeltaErrors.nonExistentDeltaTable(
        DeltaTableIdentifier(table = Some(TableIdentifier(ident.name()))))
    }
  }
}
