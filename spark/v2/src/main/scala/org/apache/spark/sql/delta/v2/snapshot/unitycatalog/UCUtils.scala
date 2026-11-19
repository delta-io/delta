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
package org.apache.spark.sql.delta.v2.snapshot.unitycatalog

import java.util.Objects.requireNonNull

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.util.CatalogTableUtils
import io.delta.storage.commit.uccommitcoordinator.UCConfigUtils
import io.delta.kernel.unitycatalog.{UCTableIdentifier => KernelUCTableIdentifier}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Utility class for extracting Unity Catalog table information from Spark catalog metadata.
 *
 * This class isolates Spark dependencies, allowing [[UCManagedTableSnapshotManager]] to be
 * created without Spark if table info is provided directly via [[UCTableInfo]].
 */
private[v2] object UCUtils {

  // Utility class - no instances

  /**
   * Extracts Unity Catalog table information from Spark catalog table metadata.
   *
   * @param catalogTable Spark catalog table metadata
   * @param spark SparkSession for resolving Unity Catalog configurations
   * @return table info if table is UC-managed, empty otherwise
   * @throws IllegalArgumentException if table is UC-managed but configuration is invalid
   */
  def extractTableInfo(catalogTable: CatalogTable, spark: SparkSession): Option[UCTableInfo] = {
    requireNonNull(catalogTable, "catalogTable is null")
    requireNonNull(spark, "spark is null")

    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      return None
    }

    val tableId = extractUCTableId(catalogTable)
    val tablePath = extractTablePath(catalogTable)

    // Get catalog name - require explicit catalog in identifier
    val catalogOption = catalogTable.identifier.catalog
    if (catalogOption.isEmpty) {
      throw new IllegalArgumentException(
        "Unable to determine Unity Catalog for table " + catalogTable.identifier +
        ": catalog name is missing. Use a fully-qualified table name with an explicit " +
        "catalog (e.g., catalog.schema.table).")
    }

    // Get UC endpoint and token from Spark configs
    val ucConfigs = UCCommitCoordinatorBuilder.getCatalogConfigMap(spark)
    val configOpt = ucConfigs.get(catalogOption.get)

    if (configOpt.isEmpty) {
      throw new IllegalArgumentException(
        "Cannot create UC client for table " + catalogTable.identifier +
        ": Unity Catalog configuration not found for catalog '" + catalogOption.get + "'.")
    }

    Some(new UCTableInfo(
      tableId = tableId,
      tablePath = tablePath,
      ucUri = configOpt.get.uri,
      kernelTableIdentifier = extractTableIdentifier(catalogTable),
      authConfig = configOpt.get.authConfig,
      optInFlags = selectOptInFlags(configOpt.get.ucConfig).asJava))
  }

  /**
   * Selects the per-catalog opt-in flag keys recognized by
   * `UCTokenBasedRestClientFactory.createUCClient` (e.g. `deltaRestApi.enabled`) so they
   * propagate through [[UCTableInfo.toUcConfig]] into the V2 connector's UC client
   * construction. Without this, the factory falls back to the legacy client even when the catalog
   * has opted into the new Delta API path.
   */
  private def selectOptInFlags(ucConfig: Map[String, String]): mutable.Map[String, String] = {
    val flags = mutable.HashMap.empty[String, String]
    val keys = Array(
      UCConfigUtils.DELTA_REST_API_ENABLED_KEY,
      UCConfigUtils.RENEW_CREDENTIAL_ENABLED_KEY,
      UCConfigUtils.CRED_SCOPED_FS_ENABLED_KEY)
    var index = 0
    while (index < keys.length) {
      val key = keys(index)
      val value = ucConfig.getOrElse(key, null)
      if (value != null) {
        flags(key) = value
      }
      index += 1
    }
    flags
  }

  private def extractUCTableId(catalogTable: CatalogTable): String = {
    val storageProperties = catalogTable.storage.properties

    // TODO: UC constants should be consolidated in a shared location (future PR)
    val ucTableId = storageProperties.getOrElse(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, null)
    if (ucTableId == null || ucTableId.isEmpty) {
      throw new IllegalArgumentException(
        "Cannot extract ucTableId from table " + catalogTable.identifier)
    }
    ucTableId
  }

  private def extractTablePath(catalogTable: CatalogTable): String = {
    if (catalogTable.location == null) {
      throw new IllegalArgumentException(
        "Cannot extract table path: location is null for table " + catalogTable.identifier)
    }
    catalogTable.location.toString
  }

  private def extractTableIdentifier(catalogTable: CatalogTable): KernelUCTableIdentifier = {
    val catalogOption = catalogTable.identifier.catalog
    if (catalogOption.isEmpty) {
      throw new IllegalArgumentException(
        "Unable to determine Unity Catalog for table " + catalogTable.identifier +
        ": catalog name is missing.")
    }
    val schemaOption = catalogTable.identifier.database
    if (schemaOption.isEmpty) {
      throw new IllegalArgumentException(
        "Unable to determine Unity Catalog schema for table " + catalogTable.identifier +
        ": schema name is missing.")
    }
    new KernelUCTableIdentifier(
      catalogOption.get,
      schemaOption.get,
      catalogTable.identifier.table)
  }
}
