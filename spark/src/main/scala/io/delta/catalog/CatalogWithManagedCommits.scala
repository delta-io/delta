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

package io.delta.catalog

import io.delta.kernel.spark.catalog.ManagedCommitClient
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import java.util.Optional

/**
 * Extension trait for catalogs that support Delta catalog-managed commits.
 *
 * Catalogs implementing this trait can provide centralized commit coordination
 * for Delta tables, enabling features like cross-table transactions, stronger
 * consistency guarantees, and centralized auditing.
 *
 * This is a Delta-specific extension to Spark's CatalogPlugin, not part of
 * Spark Core.
 */
trait CatalogWithManagedCommits extends CatalogPlugin {

  /**
   * Returns a client for managing commits for the specified table.
   *
   * @param tableId catalog-specific table identifier
   * @param tablePath filesystem path to table data
   * @return ManagedCommitClient if table uses managed commits, empty otherwise
   */
  def getManagedCommitClient(
      tableId: String,
      tablePath: String): Optional[ManagedCommitClient]

  /**
   * Extracts the catalog-specific table identifier from table properties.
   *
   * Different catalogs use different property keys:
   * - Unity Catalog: "delta.coordinatedCommits.tableId"
   * - AWS Glue: "glue.tableId" (future)
   * - Polaris: "polaris.tableId" (future)
   *
   * @param table the Spark catalog table
   * @return table identifier if managed by this catalog, empty otherwise
   */
  def extractTableId(table: CatalogTable): Optional[String]
}
