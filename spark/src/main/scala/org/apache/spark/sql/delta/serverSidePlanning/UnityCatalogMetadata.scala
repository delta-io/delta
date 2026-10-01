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
 */
case class UnityCatalogMetadata(
    catalogName: String,
    ucUri: String,
    override val tokenSupplier: Option[() => String],
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: String = {
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

    // Read UC configuration from Spark conf
    val ucUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val authConfig = extractAuthConfig(spark, catalogName)

    val supplier: Option[() => String] = if (authConfig.nonEmpty) {
      val tp = TokenProvider.create(authConfig.asJava)
      Some(() => tp.accessToken())
    } else {
      None
    }

    val tableProps = Map.empty[String, String]
    UnityCatalogMetadata(catalogName, ucUri, supplier, tableProps)
  }

  /**
   * Extract authentication config for the planning endpoint.
   *
   * Follows the same pattern as UC's AuthConfigUtils:
   * 1. Scan spark.sql.catalog.<name>.auth.* keys, strip prefix
   * 2. If legacy .token exists:
   *    - Error if auth.token also set (configured twice)
   *    - Otherwise convert to Map("type" -> "static", ...)
   * 3. Result passed to TokenProvider.create()
   */
  private[serverSidePlanning] def extractAuthConfig(
      spark: SparkSession,
      catalogName: String): Map[String, String] = {
    val catalogPrefix = s"spark.sql.catalog.$catalogName."
    val authPrefix = s"${catalogPrefix}auth."

    // Extract all auth.* configs, stripping the prefix
    val newFormatConfig = spark.conf.getAll
      .filterKeys(_.startsWith(authPrefix))
      .map { case (fullKey, value) => (fullKey.stripPrefix(authPrefix), value) }
      .toMap

    // Check legacy token
    val legacyToken = spark.conf.get(s"${catalogPrefix}token", "")

    if (legacyToken.nonEmpty) {
      // Conflict: both legacy .token and auth.token set
      require(!newFormatConfig.contains("token"),
        s"Static token configured twice. Use either " +
        s"'spark.sql.catalog.$catalogName.token' (legacy) or " +
        s"'spark.sql.catalog.$catalogName.auth.token' (new), not both.")

      // Legacy token: convert to static format. Matches UC AuthConfigUtils behavior.
      Map("type" -> "static", "token" -> legacyToken)
    } else {
      newFormatConfig
    }
  }
}
