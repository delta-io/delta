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
  private val TEST_SIMULATE_UC = "test.simulateUC"

  private def isTesting: Boolean = System.getenv("DELTA_TESTING") != null

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

    val isFeatureEnabled =
      properties.get(FEATURE_CATALOG_MANAGED).exists(_.equalsIgnoreCase("supported")) ||
      properties.get(FEATURE_CATALOG_OWNED_PREVIEW).exists(_.equalsIgnoreCase("supported"))

    // Test-only escape hatch used by delta-spark suites to simulate Unity Catalog semantics
    if (isTesting && properties.contains(TEST_SIMULATE_UC)) {
      true
    } else {
      isFeatureEnabled
    }
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
}
