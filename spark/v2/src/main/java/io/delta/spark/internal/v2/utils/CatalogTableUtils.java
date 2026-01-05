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
package io.delta.spark.internal.v2.utils;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Utility helpers for inspecting Delta-related metadata persisted on Spark {@link CatalogTable}
 * instances by Unity Catalog.
 *
 * <p>Unity Catalog marks catalog-managed tables via feature flags stored in table storage
 * properties. This helper centralises the logic for interpreting those properties so the Kernel
 * connector can decide when to use catalog-owned (CCv2) behaviour.
 *
 * <ul>
 *   <li>{@link #isCatalogManaged(CatalogTable)} checks whether either {@code
 *       delta.feature.catalogManaged} or {@code delta.feature.catalogOwned-preview} is set to
 *       {@code supported}, signalling that a catalog manages the table.
 *   <li>{@link #isUnityCatalogManagedTable(CatalogTable)} additionally verifies the presence of the
 *       Unity Catalog table identifier ({@link UCCommitCoordinatorClient#UC_TABLE_ID_KEY}) to
 *       confirm that the table is backed by Unity Catalog.
 * </ul>
 */
public final class CatalogTableUtils {
  /**
   * Property key for catalog-managed feature flag. Constructed from {@link
   * TableFeatures#CATALOG_MANAGED_RW_FEATURE} (delta.feature.catalogManaged) and preview variant
   * (delta.feature.catalogOwned-preview)
   */
  static final String FEATURE_CATALOG_MANAGED =
      TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
          + TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName();

  static final String FEATURE_CATALOG_OWNED_PREVIEW =
      TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + "catalogOwned-preview";
  private static final String SUPPORTED = TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE;

  private CatalogTableUtils() {}

  /**
   * Checks whether any catalog manages this table via CCv2 semantics.
   *
   * @param table Spark {@link CatalogTable} descriptor
   * @return {@code true} when either catalog feature flag is set to {@code supported}
   */
  public static boolean isCatalogManaged(CatalogTable table) {
    requireNonNull(table, "table is null");
    Map<String, String> storageProperties = getStorageProperties(table);
    return isCatalogManagedFeatureEnabled(storageProperties, FEATURE_CATALOG_MANAGED)
        || isCatalogManagedFeatureEnabled(storageProperties, FEATURE_CATALOG_OWNED_PREVIEW);
  }

  /**
   * Checks whether the table is Unity Catalog managed.
   *
   * @param table Spark {@link CatalogTable} descriptor
   * @return {@code true} when the table is catalog managed and contains the UC identifier
   */
  public static boolean isUnityCatalogManagedTable(CatalogTable table) {
    requireNonNull(table, "table is null");
    Map<String, String> storageProperties = getStorageProperties(table);
    boolean isUCBacked = storageProperties.containsKey(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    return isUCBacked && isCatalogManaged(table);
  }

  /**
   * Checks whether the given feature key is enabled in the table properties.
   *
   * @param tableProperties The table properties
   * @param featureKey The feature key
   * @return {@code true} when the feature key is set to {@code supported}
   */
  private static boolean isCatalogManagedFeatureEnabled(
      Map<String, String> tableProperties, String featureKey) {
    requireNonNull(tableProperties, "tableProperties is null");
    requireNonNull(featureKey, "featureKey is null");
    String featureValue = tableProperties.get(featureKey);
    if (featureValue == null) {
      return false;
    }
    return featureValue.equalsIgnoreCase(SUPPORTED);
  }

  /**
   * Returns the catalog storage properties published with a {@link CatalogTable}.
   *
   * @param table Spark {@link CatalogTable} descriptor
   * @return Java map view of the storage properties, never null
   */
  private static Map<String, String> getStorageProperties(CatalogTable table) {
    requireNonNull(table, "table is null");
    if (table.storage() == null) {
      return Collections.emptyMap();
    }
    Map<String, String> javaStorageProperties = ScalaUtils.toJavaMap(table.storage().properties());
    return javaStorageProperties == null ? Collections.emptyMap() : javaStorageProperties;
  }
}
