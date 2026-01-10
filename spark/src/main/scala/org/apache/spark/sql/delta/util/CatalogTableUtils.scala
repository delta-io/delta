/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

/**
 * Utility helpers for inspecting Delta-related metadata persisted on Spark CatalogTable instances.
 * This logic is shared between Delta Spark and Delta Kernel Spark integration.
 */
object CatalogTableUtils {

  // We are not using TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName() because this class
  // is in the 'spark' module which does not depend on 'kernel-api' where TableFeatures resides.
  // The string literal matches:
  // TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX +
  //   TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()
  // = "delta.feature." + "catalogManaged"
  private val FEATURE_CATALOG_MANAGED = "delta.feature.catalogManaged"

  // Matches "delta.feature." + "catalogOwned-preview"
  private val FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview"

  /**
   * Checks whether any catalog manages this table via CCv2 semantics by inspecting
   * the storage properties map.
   *
   * @param properties Storage properties map
   * @return true when either catalog feature flag is set to supported
   */
  private def isCatalogManagedFromProperties(properties: Map[String, String]): Boolean = {
    if (properties == null || properties.isEmpty) {
      return false
    }
    properties.get(FEATURE_CATALOG_MANAGED).exists(_.equalsIgnoreCase("supported")) ||
      properties.get(FEATURE_CATALOG_OWNED_PREVIEW).exists(_.equalsIgnoreCase("supported"))
  }

  /**
   * Checks whether the table has Unity Catalog backing by inspecting the storage properties map.
   *
   * @param properties Storage properties map
   * @return true when the UC table ID is present
   */
  private def isUCBackedFromProperties(properties: Map[String, String]): Boolean = {
    if (properties == null || properties.isEmpty) {
      return false
    }
    properties.contains(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
  }

  /**
   * Checks whether any catalog manages this table via CCv2 semantics.
   *
   * @param table Spark CatalogTable descriptor
   * @return true when either catalog feature flag is set to supported
   */
  def isCatalogManaged(table: CatalogTable): Boolean = {
    if (table == null || table.storage == null) {
      return false
    }
    isCatalogManagedFromProperties(table.storage.properties)
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
    isUCBackedFromProperties(table.storage.properties) && isCatalogManaged(table)
  }

  /**
   * Checks whether the table is Unity Catalog managed based on storage properties map.
   *
   * This method checks the properties map (typically from table.storage.properties) for
   * UC markers, allowing UC table detection without requiring a full CatalogTable object.
   * This is useful when only the properties map is available, such as in DataSource APIs.
   *
   * @param properties Storage properties map (e.g., from CatalogTable.storage.properties or
   *                   DataSource parameters map)
   * @return true when the table is catalog managed and contains the UC identifier
   */
  def isUnityCatalogManagedTableFromProperties(properties: Map[String, String]): Boolean = {
    if (properties == null || properties.isEmpty) {
      return false
    }
    isUCBackedFromProperties(properties) && isCatalogManagedFromProperties(properties)
  }
}
