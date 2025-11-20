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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Metadata required for creating a server-side planning client.
 *
 * This interface captures all information from the catalog's loadTable response
 * that is needed to create and configure a ServerSidePlanningClient.
 *
 * Implementations:
 * - UnityCatalogMetadata: Extracts UC URI, constructs IRC endpoint
 * - DefaultMetadata: For non-UC catalogs (returns None/empty)
 * - TestMetadata: For unit tests (injectable values)
 */
trait ServerSidePlanningMetadata {
  /**
   * The base URI for the planning endpoint.
   * For UC: This is the IRC REST catalog URI (constructed from UC URI).
   * Example: "http://localhost:8080/api/2.1/unity-catalog/iceberg"
   */
  def planningEndpointUri: Option[String]

  /**
   * Authentication token for the planning endpoint.
   * For UC: This is the UC token that works with IRC endpoints.
   */
  def authToken: Option[String]

  /**
   * Catalog name for configuration lookups.
   */
  def catalogName: String

  /**
   * Unity Catalog URI for credential refresh on executors.
   * This is passed to executors via Hadoop config for potential credential refresh.
   */
  def unityCatalogUri: Option[String]

  /**
   * Unity Catalog token for credential refresh on executors.
   */
  def unityCatalogToken: Option[String]

  /**
   * Additional table properties that may be needed.
   * For example, table UUID, credential hints, etc.
   */
  def tableProperties: Map[String, String]
}

/**
 * Default metadata for non-Unity Catalog tables.
 * Returns None/empty for all fields.
 */
case class DefaultMetadata(catalogName: String) extends ServerSidePlanningMetadata {
  override def planningEndpointUri: Option[String] = None
  override def authToken: Option[String] = None
  override def unityCatalogUri: Option[String] = None
  override def unityCatalogToken: Option[String] = None
  override def tableProperties: Map[String, String] = Map.empty
}

object ServerSidePlanningMetadata {
  /**
   * Create metadata from a loaded table.
   *
   * This factory method inspects the table to determine what type of metadata
   * to create (UC vs non-UC).
   */
  def fromTable(
      table: Table,
      spark: SparkSession,
      ident: Identifier,
      isUnityCatalog: Boolean): ServerSidePlanningMetadata = {

    if (isUnityCatalog) {
      UnityCatalogMetadata.fromTable(table, spark, ident)
    } else {
      DefaultMetadata(extractCatalogName(ident))
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
