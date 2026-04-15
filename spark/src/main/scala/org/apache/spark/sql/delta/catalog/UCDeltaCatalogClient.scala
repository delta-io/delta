/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.Snapshot
import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.Identifier

/**
 * Spark-level shim for Delta REST Catalog operations.
 *
 * Handles Spark type translation (CatalogTable, Snapshot) and routes catalog
 * operations to either the DRC path (via UCDeltaClient) or the legacy path
 * (via super delegation to UCProxy).
 *
 * In the non-DRC foundation, all methods delegate to the legacy path.
 * Phase 2 adds DRC routing when DELTA_REST_CATALOG_ENABLED is true.
 *
 * @param ucDeltaClient the shared UCDeltaClient instance (may be null if not configured)
 * @param isDrcEnabled whether the Delta REST Catalog flag is enabled
 */
class UCDeltaCatalogClient(
    ucDeltaClient: UCDeltaClient,
    isDrcEnabled: Boolean) {

  /**
   * Load table metadata. Returns None to signal the caller should use
   * the legacy path (super.loadTable via UCProxy).
   *
   * In Phase 2 (DRC enabled), this will call ucDeltaClient.loadTable()
   * and reassemble the result into a CatalogTable.
   */
  def loadTable(catalog: String, schema: String, table: String): Option[CatalogTable] = {
    // Phase 2: if (isDrcEnabled && ucDeltaClient != null) { ... }
    None // Fall through to legacy path
  }

  /**
   * Create table via DRC. Returns false to signal the caller should use
   * the legacy path (super.createTable via UCProxy).
   *
   * In Phase 2 (DRC enabled), this will extract Spark types from the
   * snapshot and call ucDeltaClient.createTable().
   */
  def createTable(
      ident: Identifier,
      table: CatalogTable,
      snapshot: Snapshot): Boolean = {
    // Phase 2: if (isDrcEnabled && ucDeltaClient != null) { ... }
    false // Fall through to legacy path
  }
}
