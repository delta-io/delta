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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier, DeltaTableUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, CatalogPlugin, Identifier, Table, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

/**
 * Helper object for resolving Delta tables using a *non-session* catalog.
 */
object CatalogResolver {
  def getDeltaTableFromCatalog(
      spark: SparkSession,
      catalog: CatalogPlugin,
      ident: Identifier): DeltaTableV2 = {
    catalog.asTableCatalog.loadTable(ident) match {
      case v2: DeltaTableV2 => v2
      case v1: V1Table if DeltaTableUtils.isDeltaTable(v1.v1Table) =>
        DeltaTableV2(
          spark,
          path = new Path(v1.v1Table.location),
          catalogTable = Some(v1.v1Table),
          tableIdentifier = Some(ident.toString)
        )
      case table => throw DeltaErrors.notADeltaTableException(
        DeltaTableIdentifier(table = Some(TableIdentifier(table.name()))))
    }
  }

  /**
   Returns a [[(CatalogPlugin, Identifier)]] if a catalog exists with the
   input name, otherwise throws a [[CatalogNotFoundException]]
  */
  def getCatalogPluginAndIdentifier(
      spark: SparkSession,
      catalog: String,
      ident: Seq[String]): (CatalogPlugin, Identifier) = {
    (spark.sessionState.catalogManager.catalog(catalog), ident.asIdentifier)
  }
}
