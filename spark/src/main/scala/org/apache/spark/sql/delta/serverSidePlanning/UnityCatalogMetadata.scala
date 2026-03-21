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
 * Provides base Iceberg REST endpoint, authentication, and credential refresh
 * metadata for server-side planning.
 */
case class UnityCatalogMetadata(
    catalogName: String,
    override val ucUri: String,
    ucToken: String,
    override val tableId: Option[String],
    override val authConfig: Map[String, String],
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: String = {
    val base = if (ucUri.endsWith("/")) ucUri.dropRight(1) else ucUri
    s"$base/api/2.1/unity-catalog/iceberg-rest/v1"
  }

  override def authToken: Option[String] = Some(ucToken)

  override def tableProperties: Map[String, String] = tableProps
}

object UnityCatalogMetadata {

  // UC table property key for table UUID
  private val UC_TABLE_ID_KEY = "io.unitycatalog.tableId"

  def fromTable(
      table: Table,
      spark: SparkSession,
      ident: Identifier): UnityCatalogMetadata = {

    val catalogName = if (ident.namespace().length > 1) {
      ident.namespace().head
    } else {
      spark.sessionState.catalogManager.currentCatalog.name()
    }

    val ucUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val ucToken = spark.conf.get(
      s"spark.sql.catalog.$catalogName.token", "")

    // Extract table UUID from table properties (set by UC's loadTable)
    val tableId = if (table != null && table.properties() != null) {
      Option(table.properties().get(UC_TABLE_ID_KEY))
    } else {
      None
    }

    // Extract auth config for UC credential providers.
    // These are set as fs.unitycatalog.auth.* in Hadoop config so that
    // GenericCredentialProvider can reconstruct a TokenProvider for refresh.
    val authCfg = extractAuthConfig(spark, catalogName)

    val tableProps = Map.empty[String, String]

    UnityCatalogMetadata(
      catalogName, ucUri, ucToken, tableId, authCfg, tableProps)
  }

  /**
   * Extract authentication config for UC credential providers.
   * Reads spark.sql.catalog.<name>.auth.* configs. Falls back to
   * legacy .token config converted to static type.
   *
   * The returned map keys are stripped of prefix (e.g., "type",
   * "token", "oauth.uri"). These are set as fs.unitycatalog.auth.*
   * in Hadoop config for GenericCredentialProvider to read.
   */
  private[serverSidePlanning] def extractAuthConfig(
      spark: SparkSession,
      catalogName: String): Map[String, String] = {
    val catalogPrefix = s"spark.sql.catalog.$catalogName."
    val authPrefix = s"${catalogPrefix}auth."

    val newFormatConfig = spark.conf.getAll
      .filterKeys(_.startsWith(authPrefix))
      .map { case (fullKey, value) =>
        (fullKey.stripPrefix(authPrefix), value)
      }
      .toMap

    if (newFormatConfig.nonEmpty) {
      newFormatConfig
    } else {
      // Legacy fallback: spark.sql.catalog.<name>.token -> static
      val legacyToken = spark.conf.get(
        s"${catalogPrefix}token", "")
      if (legacyToken.nonEmpty) {
        Map("type" -> "static", "token" -> legacyToken)
      } else {
        Map.empty
      }
    }
  }
}
