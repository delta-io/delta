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

package org.apache.spark.sql.delta.catalog

import java.util.Optional

import io.delta.kernel.spark.catalog.{CatalogWithManagedCommits, ManagedCommitClient, UCManagedCommitClientAdapter}
import io.delta.kernel.unitycatalog.UCCatalogManagedClient
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.CatalogPlugin

/**
 * Unity Catalog implementation that supports Delta catalog-managed commits.
 *
 * This class wraps Unity Catalog functionality and implements the CatalogWithManagedCommits
 * trait to enable Delta Kernel to work with Unity Catalog tables using managed commits.
 */
class UnityCatalog extends CatalogPlugin with CatalogWithManagedCommits with Logging {

  private var catalogName: String = _

  override def name(): String = catalogName

  override def initialize(
      name: String,
      options: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
  }

  override def getManagedCommitClient(
      tableId: String,
      tablePath: String): Optional[ManagedCommitClient] = {

    try {
      // Get UC catalog configuration from Spark session
      val spark = org.apache.spark.sql.SparkSession.active
      val catalogConfig = resolveCatalogConfig(spark, catalogName)

      if (catalogConfig.isEmpty) {
        logWarning(s"No UC configuration found for catalog $catalogName")
        return Optional.empty()
      }

      val (uri, token) = catalogConfig.get

      // Create UC REST client
      val ucClient = new UCTokenBasedRestClient(uri, token)
      val ucManagedClient = new UCCatalogManagedClient(ucClient)

      // Wrap in generic adapter
      val adapter = new UCManagedCommitClientAdapter(ucManagedClient, ucClient, tablePath)
      Optional.of(adapter)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to create UC managed commit client for table $tableId at $tablePath", e)
        Optional.empty()
    }
  }

  override def extractTableId(table: CatalogTable): Optional[String] = {
    val properties = table.storage.properties

    // Try the kernel UC table ID key first
    val kernelKey = "catalogManaged.unityCatalog.tableId"
    val tableIdOpt = properties.get(kernelKey)
      .orElse(properties.get("ucTableId")) // Fallback to legacy key
      .filter(_.nonEmpty)

    tableIdOpt match {
      case Some(id: String) => Optional.of(id)
      case _ => Optional.empty[String]()
    }
  }

  /**
   * Resolves Unity Catalog configuration from Spark session.
   *
   * Reads the following configuration keys:
   * - spark.sql.catalog.{catalogName}.uri
   * - spark.sql.catalog.{catalogName}.token
   *
   * @param spark SparkSession to read configuration from
   * @param catalogName name of the catalog
   * @return Option containing (uri, token) if both are configured, None otherwise
   */
  private def resolveCatalogConfig(
      spark: org.apache.spark.sql.SparkSession,
      catalogName: String): Option[(String, String)] = {

    val uriKey = s"spark.sql.catalog.$catalogName.uri"
    val tokenKey = s"spark.sql.catalog.$catalogName.token"

    val uri = spark.conf.getOption(uriKey)
    val token = spark.conf.getOption(tokenKey)

    (uri, token) match {
      case (Some(u), Some(t)) if u.nonEmpty && t.nonEmpty => Some((u, t))
      case _ =>
        logWarning(s"Missing or empty UC configuration for catalog $catalogName. " +
          s"Required: $uriKey and $tokenKey")
        None
    }
  }
}
