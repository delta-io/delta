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
 * Metadata for Unity Catalog tables.
 * Constructs IRC REST endpoint from UC base URI.
 */
case class UnityCatalogMetadata(
    catalogName: String,
    ucUri: String,
    ucToken: String,
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: Option[String] = {
    // Construct IRC REST endpoint from UC URI
    Some(constructPlanEndpoint(ucUri))
  }

  override def authToken: Option[String] = Some(ucToken)

  override def unityCatalogUri: Option[String] = Some(ucUri)

  override def unityCatalogToken: Option[String] = Some(ucToken)

  override def tableProperties: Map[String, String] = tableProps

  /**
   * Construct Iceberg REST catalog endpoint for server-side planning.
   *
   * Current implementation (simple):
   * - UC URI: https://my-workspace.cloud.databricks.com
   * - IRC endpoint: https://my-workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg
   *
   * TODO: Implement full Iceberg REST catalog spec compliance:
   * 1. Call GET {ucUri}/v1/config to retrieve catalog configuration
   * 2. Extract optional "prefix" from config.overrides (e.g., "catalogs/my-catalog")
   * 3. Use prefix in subsequent API calls: {ucUri}/v1/{prefix}/namespaces/.../tables/.../plan
   * 4. Handle multi-tenant hierarchies (AWS Glue, S3 Tables)
   * See: https://iceberg.apache.org/rest-catalog-spec/
   *
   * For now, we use the simple UC-specific path which works for current UC deployments.
   */
  private def constructPlanEndpoint(ucUri: String): String = {
    val baseUri = if (ucUri.endsWith("/")) ucUri.dropRight(1) else ucUri
    s"$baseUri/api/2.1/unity-catalog/iceberg"
  }
}

object UnityCatalogMetadata {
  def fromTable(
      table: Table,
      spark: SparkSession,
      ident: Identifier): UnityCatalogMetadata = {

    val catalogName = if (ident.namespace().length > 1) {
      ident.namespace().head
    } else {
      "spark_catalog"
    }

    // Read UC configuration from Spark conf
    val ucUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val ucToken = spark.conf.get(s"spark.sql.catalog.$catalogName.token", "")

    // Table properties currently unused, may be needed in future
    val tableProps = Map.empty[String, String]

    UnityCatalogMetadata(catalogName, ucUri, ucToken, tableProps)
  }
}
