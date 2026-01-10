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

package org.apache.spark.sql.delta.util

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Utility helpers for inspecting Delta-related metadata persisted on Spark CatalogTable instances.
 * This logic is shared between Delta Spark and Delta Kernel Spark integration.
 */
object CatalogTableUtils {

  // Feature keys for catalog-managed tables
  // Matches "delta.feature." + "catalogManaged"
  private val FEATURE_CATALOG_MANAGED = "delta.feature.catalogManaged"

  // Matches "delta.feature." + "catalogOwned-preview"
  private val FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview"

  private val SUPPORTED = "supported"

  /**
   * Checks whether any catalog manages this table via CCv2 semantics.
   *
   * @param table Spark CatalogTable descriptor
   * @return true when either catalog feature flag is set to supported
   */
  def isCatalogManaged(table: CatalogTable): Boolean = {
    if (table == null || table.storage == null || table.storage.properties == null) {
      return false
    }

    val properties = table.storage.properties
    isCatalogManagedFeatureEnabled(properties, FEATURE_CATALOG_MANAGED) ||
      isCatalogManagedFeatureEnabled(properties, FEATURE_CATALOG_OWNED_PREVIEW)
  }

  /**
   * Checks whether the table is Unity Catalog managed.
   *
   * @param table Spark CatalogTable descriptor
   * @return true when the table is catalog managed and contains the UC identifier
   */
  def isUnityCatalogManagedTable(table: CatalogTable): Boolean = {
    if (table == null || table.storage == null || table.storage.properties == null) {
      return false
    }

    val properties = table.storage.properties
    val isUCBacked = properties.contains(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)

    isUCBacked && isCatalogManaged(table)
  }

  /**
   * Checks whether the given feature key is enabled in the table properties.
   *
   * @param tableProperties The table properties
   * @param featureKey The feature key
   * @return true when the feature key is set to supported
   */
  private def isCatalogManagedFeatureEnabled(
      tableProperties: Map[String, String],
      featureKey: String): Boolean = {
    if (tableProperties == null || featureKey == null) {
      return false
    }

    tableProperties.get(featureKey).exists(_.equalsIgnoreCase(SUPPORTED))
  }
}

