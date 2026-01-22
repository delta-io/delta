/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Metadata required for creating a server-side planning client.
 *
 * This interface captures all information from the catalog's loadTable response
 * that is needed to create and configure a ServerSidePlanningClient.
 */
private[serverSidePlanning] trait ServerSidePlanningMetadata {
  /**
   * The base URI for the planning endpoint.
   */
  def planningEndpointUri: String

  /**
   * Authentication token for the planning endpoint.
   */
  def authToken: Option[String]

  /**
   * Catalog name for configuration lookups.
   */
  def catalogName: String

  /**
   * Additional table properties that may be needed.
   * For example, table UUID, credential hints, etc.
   */
  def tableProperties: Map[String, String]
}

/**
 * Default metadata for non-UC catalogs.
 * Used when server-side planning is force-enabled for testing/development.
 */
private[serverSidePlanning] case class DefaultMetadata(
    catalogName: String,
    tableProps: Map[String, String] = Map.empty) extends ServerSidePlanningMetadata {
  override def planningEndpointUri: String = ""
  override def authToken: Option[String] = None
  override def tableProperties: Map[String, String] = tableProps
}

object ServerSidePlanningMetadata {
  /**
   * Create metadata from a loaded table.
   *
   * Returns UnityCatalogMetadata for Unity Catalog tables, or DefaultMetadata otherwise.
   */
  def fromTable(
      table: Table,
      spark: SparkSession,
      ident: Identifier,
      isUnityCatalog: Boolean): ServerSidePlanningMetadata = {

    if (isUnityCatalog) {
      UnityCatalogMetadata.fromTable(table, spark, ident)
    } else {
      val catalogName = extractCatalogName(ident)
      DefaultMetadata(catalogName, Map.empty)
    }
  }

  private def extractCatalogName(ident: Identifier): String = {
    if (ident.namespace().length > 1) {
      ident.namespace().head
    } else {
      "spark_catalog"
    }
  }
}
