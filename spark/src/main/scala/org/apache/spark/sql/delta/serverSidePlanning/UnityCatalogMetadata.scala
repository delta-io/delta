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

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.util.JsonUtils

/**
 * Case class for parsing Iceberg REST catalog /v1/config response.
 * Per the Iceberg REST spec, the config endpoint returns defaults and overrides.
 * The optional "prefix" in overrides is used for multi-tenant catalog paths.
 */
private case class CatalogConfigResponse(
    defaults: Map[String, String],
    overrides: Map[String, String])

/**
 * Metadata for Unity Catalog tables.
 * Constructs IRC REST endpoint from UC base URI.
 */
case class UnityCatalogMetadata(
    catalogName: String,
    ucUri: String,
    ucToken: String,
    tableProps: Map[String, String]) extends ServerSidePlanningMetadata {

  override def planningEndpointUri: String = {
    // Construct IRC REST endpoint from UC URI
    constructPlanEndpoint(ucUri)
  }

  override def authToken: Option[String] = Some(ucToken)

  override def tableProperties: Map[String, String] = tableProps

  /**
   * Base URI with trailing slash removed for consistent URL construction.
   */
  private def baseUri: String = if (ucUri.endsWith("/")) ucUri.dropRight(1) else ucUri

  /**
   * Lazily fetched catalog configuration from /v1/config endpoint.
   * Cached to avoid repeated HTTP calls for the same metadata instance.
   */
  private lazy val catalogConfig: Option[CatalogConfigResponse] = fetchCatalogConfig()

  /**
   * Fetch catalog configuration from Iceberg REST /v1/config endpoint.
   * Returns None on any error (network, parsing, HTTP error), which triggers
   * fallback to simple endpoint construction without prefix.
   */
  private def fetchCatalogConfig(): Option[CatalogConfigResponse] = {
    try {
      val icebergRestBase = s"$baseUri/api/2.1/unity-catalog/iceberg-rest"
      val configUri = s"$icebergRestBase/v1/config"
      val httpClient = HttpClientBuilder.create().build()
      try {
        val httpGet = new HttpGet(configUri)
        // Add auth header if token present
        if (ucToken.nonEmpty) {
          httpGet.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $ucToken")
        }
        val response = httpClient.execute(httpGet)
        try {
          if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
            val body = EntityUtils.toString(response.getEntity)
            Some(JsonUtils.fromJson[CatalogConfigResponse](body))
          } else {
            None // Fallback to simple path
          }
        } finally {
          response.close()
        }
      } finally {
        httpClient.close()
      }
    } catch {
      case _: Exception => None // Fallback to simple path on any error
    }
  }

  /**
   * Construct Iceberg REST catalog endpoint for server-side planning.
   *
   * This implementation follows the Iceberg REST catalog spec:
   * 1. Calls GET {icebergRestBase}/v1/config to retrieve catalog configuration
   * 2. Extracts optional "prefix" from config.overrides (e.g., "catalogs/my-catalog")
   * 3. If prefix exists, includes it in the endpoint: {icebergRestBase}/v1/{prefix}
   * 4. If prefix is missing or config call fails, uses simple path: {icebergRestBase}
   *
   * Examples:
   * - Without prefix:
   *   https://unity-catalog-server.example.com/api/2.1/unity-catalog/iceberg-rest
   * - With prefix:
   *   https://unity-catalog-server.example.com/api/2.1/unity-catalog/iceberg-rest/
   *   v1/catalogs/my-catalog
   *
   * See: https://iceberg.apache.org/rest-catalog-spec/
   *
   * Note: Multi-tenant hierarchies (AWS Glue, S3 Tables) are not yet handled.
   */
  private def constructPlanEndpoint(ucUri: String): String = {
    val base = baseUri
    val icebergRestBase = s"$base/api/2.1/unity-catalog/iceberg-rest"

    // Try to get prefix from config
    val prefix = catalogConfig.flatMap(_.overrides.get("prefix"))

    prefix match {
      case Some(p) => s"$icebergRestBase/v1/$p"
      case None => icebergRestBase
    }
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
