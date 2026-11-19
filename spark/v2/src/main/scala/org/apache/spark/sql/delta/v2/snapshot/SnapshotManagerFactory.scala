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
package org.apache.spark.sql.delta.v2.snapshot

import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import org.apache.spark.sql.delta.v2.kernel.KernelContext
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory
import org.apache.spark.sql.delta.v2.snapshot.unitycatalog.{UCManagedTableSnapshotManager, UCTableInfo, UCUtils}
import io.delta.kernel.{Meta => KernelMeta}
import io.delta.kernel.unitycatalog.{UCCatalogManagedClient => KernelUCCatalogManagedClient}

import org.apache.spark.sql.SparkSession

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Factory for creating [[DeltaV2SnapshotManager]] instances.
 *
 * This factory determines the appropriate snapshot manager based on the table configuration:
 *  - For Unity Catalog managed tables: creates `UCManagedTableSnapshotManager`
 *  - For path-based tables: creates [[PathBasedSnapshotManager]]
 */
@Experimental
private[v2]
object SnapshotManagerFactory {

  /**
   * Creates a snapshot manager for the given table.
   *
   * @param tablePath the filesystem path to the Delta table
   * @param kernelContext the configured context to use for table operations
   * @param catalogTable optional Spark catalog table metadata
   * @return a [[DeltaV2SnapshotManager]] appropriate for the table type
   */
  def create(
      tablePath: String,
      kernelContext: KernelContext,
      catalogTable: Option[CatalogTable]): DeltaV2SnapshotManager = {
    val catalog = catalogTable.orNull
    if (catalog != null) {
      val ucTableInfo = UCUtils.extractTableInfo(catalog, SparkSession.active)
      if (!ucTableInfo.isEmpty) {
        return createUCManagedSnapshotManager(ucTableInfo.get, kernelContext)
      }
      // Catalog table without UC metadata falls back to path-based handling.
    }

    // Default: path-based snapshot manager for non-UC tables
    new PathBasedSnapshotManager(tablePath, kernelContext)
  }

  private def createUCManagedSnapshotManager(
      tableInfo: UCTableInfo,
      kernelContext: KernelContext): UCManagedTableSnapshotManager = {
    val ucConfig = tableInfo.toUcConfig()
    ucConfig.put("appVersions.Kernel", KernelMeta.KERNEL_VERSION)
    ucConfig.put("appVersions.Delta V2 connector", "true")
    val ucClient = UCTokenBasedRestClientFactory.createUCClient(ucConfig)
    val kernelUcCatalogClient = new KernelUCCatalogManagedClient(ucClient)
    new UCManagedTableSnapshotManager(kernelUcCatalogClient, tableInfo, kernelContext)
  }
}
