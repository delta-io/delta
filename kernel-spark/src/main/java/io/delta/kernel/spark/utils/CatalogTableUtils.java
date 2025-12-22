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
package io.delta.kernel.spark.utils;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.util.DeltaCatalogUtils;

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
  private CatalogTableUtils() {}

  /**
   * Checks whether any catalog manages this table via CCv2 semantics.
   *
   * @param table Spark {@link CatalogTable} descriptor
   * @return {@code true} when either catalog feature flag is set to {@code supported}
   */
  public static boolean isCatalogManaged(CatalogTable table) {
    return DeltaCatalogUtils.isCatalogManaged(table);
  }

  /**
   * Checks whether the table is Unity Catalog managed.
   *
   * @param table Spark {@link CatalogTable} descriptor
   * @return {@code true} when the table is catalog managed and contains the UC identifier
   */
  public static boolean isUnityCatalogManagedTable(CatalogTable table) {
    return DeltaCatalogUtils.isUnityCatalogManagedTable(table);
  }
}
