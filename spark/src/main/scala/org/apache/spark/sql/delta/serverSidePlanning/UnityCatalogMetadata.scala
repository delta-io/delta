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
 * Metadata for Unity Catalog tables.
 * Provides base Iceberg REST endpoint for server-side planning.
 */
case class UnityCatalogMetadata(
    catalogName: String,
    ucUri: String,
    ucToken: String,
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: String = {
    // Return base Iceberg REST path up to /v1/
    // The IcebergRESTCatalogPlanningClient will call /v1/config to get the prefix
    // and construct the full URL according to the Iceberg REST catalog spec
    val base = if (ucUri.endsWith("/")) ucUri.dropRight(1) else ucUri
    s"$base/api/2.1/unity-catalog/iceberg-rest/v1"
  }

  override def authToken: Option[String] = Some(ucToken)

  override def tableProperties: Map[String, String] = tableProps
}

object UnityCatalogMetadata {
  def fromTable(
      table: Table,
      spark: SparkSession,
      ident: Identifier): UnityCatalogMetadata = {

    val catalogName = if (ident.namespace().length > 1) {
      ident.namespace().head
    } else {
      // Use current catalog from session
      // This allows queries with 2-part names (schema.table) to work with Unity Catalog
      spark.sessionState.catalogManager.currentCatalog.name()
    }

    // Read UC configuration from Spark conf
    val ucUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val ucToken = spark.conf.get(s"spark.sql.catalog.$catalogName.token", "")

    // Table properties currently unused, may be needed in future
    val tableProps = Map.empty[String, String]

    UnityCatalogMetadata(catalogName, ucUri, ucToken, tableProps)
  }
}
