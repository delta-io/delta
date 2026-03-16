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

import scala.jdk.CollectionConverters._

import io.unitycatalog.client.auth.TokenProvider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Metadata for Unity Catalog tables.
 * Provides base Iceberg REST endpoint and authentication for server-side planning.
 *
 * @param catalogName Name of the catalog
 * @param ucUri Base URI of the Unity Catalog server
 * @param tokenSupplier Supplier of authentication tokens, called per-request to support
 *                      OAuth token refresh. None if no authentication is configured.
 * @param tableProps Additional table properties
 */
case class UnityCatalogMetadata(
    catalogName: String,
    ucUri: String,
    override val tokenSupplier: Option[() => String],
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: String = {
    // Return base Iceberg REST path up to /v1/
    // The IcebergRESTCatalogPlanningClient will call /v1/config to get the prefix
    // and construct the full URL according to the Iceberg REST catalog spec
    val base = if (ucUri.endsWith("/")) ucUri.dropRight(1) else ucUri
    s"$base/api/2.1/unity-catalog/iceberg-rest/v1"
  }

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

    // Read UC URI from Spark conf
    val ucUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")

    // Extract auth configuration from Spark conf
    // Supports both new auth.* format and legacy token format
    val authConfig = extractAuthConfig(spark, catalogName)

    // Create TokenProvider and wrap as per-request supplier
    val supplier: Option[() => String] = if (authConfig.nonEmpty) {
      val tokenProvider = TokenProvider.create(authConfig.asJava)
      Some(() => tokenProvider.accessToken())
    } else {
      None
    }

    // Table properties currently unused, may be needed in future
    val tableProps = Map.empty[String, String]

    UnityCatalogMetadata(catalogName, ucUri, supplier, tableProps)
  }

  /**
   * Extract authentication configuration for a catalog from Spark session configs.
   *
   * Supports two formats:
   * - New format: spark.sql.catalog.<name>.auth.type, auth.token, auth.oauth.uri, etc.
   * - Legacy format: spark.sql.catalog.<name>.token (auto-converted to static type)
   *
   * The returned map's keys have the auth prefix stripped (e.g., "type", "oauth.uri").
   * This map is passed directly to TokenProvider.create() which dispatches to
   * StaticTokenProvider or OAuthTokenProvider based on the "type" key.
   *
   * TODO: Consolidate with UCCommitCoordinatorBuilder.getCatalogConfigs() which has
   * the same auth config extraction logic. Kept separate for now to minimize scope.
   */
  private[serverSidePlanning] def extractAuthConfig(
      spark: SparkSession,
      catalogName: String): Map[String, String] = {
    val catalogPrefix = s"spark.sql.catalog.$catalogName."
    val authPrefix = s"${catalogPrefix}auth."

    // Extract all auth.* configs, stripping the prefix
    val newFormatConfig = spark.conf.getAll
      .filterKeys(_.startsWith(authPrefix))
      .map { case (fullKey, value) =>
        (fullKey.stripPrefix(authPrefix), value)
      }
      .toMap

    if (newFormatConfig.nonEmpty) {
      newFormatConfig
    } else {
      // Legacy fallback: spark.sql.catalog.<name>.token -> static type
      val legacyToken = spark.conf.get(s"${catalogPrefix}token", "")
      if (legacyToken.nonEmpty) {
        Map("type" -> "static", "token" -> legacyToken)
      } else {
        Map.empty
      }
    }
  }
}
