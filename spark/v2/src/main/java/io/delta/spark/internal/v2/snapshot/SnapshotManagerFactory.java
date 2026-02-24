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
package io.delta.spark.internal.v2.snapshot;

import io.delta.kernel.Meta;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.delta.util.CatalogTableUtils;

/**
 * Factory for creating {@link DeltaSnapshotManager} instances.
 *
 * <p>This factory determines the appropriate snapshot manager based on the table configuration:
 *
 * <ul>
 *   <li>For Unity Catalog managed tables: creates {@link UCManagedTableSnapshotManager}
 *   <li>For path-based tables: creates {@link PathBasedSnapshotManager}
 * </ul>
 */
@Experimental
public final class SnapshotManagerFactory {

  // Utility class - no instances
  private SnapshotManagerFactory() {}

  /**
   * Creates a snapshot manager for an existing Delta table.
   *
   * @param tablePath the filesystem path to the existing Delta table
   * @param kernelEngine the pre-configured Kernel {@link Engine} to use for table operations
   * @param catalogTable optional Spark catalog table metadata
   * @return a {@link DeltaSnapshotManager} appropriate for the table type
   */
  public static DeltaSnapshotManager forExistingTable(
      String tablePath, Engine kernelEngine, Optional<CatalogTable> catalogTable) {

    if (catalogTable.isPresent()) {
      Optional<UCTableInfo> ucTableInfo =
          UCUtils.extractTableInfo(catalogTable.get(), SparkSession.active());
      if (ucTableInfo.isPresent()) {
        return createUCManagedSnapshotManager(ucTableInfo.get(), kernelEngine);
      }
      // Catalog table without UC metadata falls back to path-based handling.
    }

    // Default: path-based snapshot manager for non-UC tables
    return new PathBasedSnapshotManager(tablePath, kernelEngine);
  }

  /**
   * Creates a snapshot manager for CREATE TABLE flows where a catalog table does not exist yet.
   *
   * @param tablePath resolved table path for the new table
   * @param kernelEngine the pre-configured Kernel {@link Engine} to use for table operations
   * @param ucTableInfo optional UC metadata for catalog-managed tables
   * @return a {@link DeltaSnapshotManager} appropriate for the new table type
   */
  public static DeltaSnapshotManager forCreateTable(
      String tablePath, Engine kernelEngine, Optional<UCTableInfo> ucTableInfo) {
    if (ucTableInfo.isPresent()) {
      System.err.println(
          "[KernelCTASDebug] SnapshotManagerFactory.forCreateTable -> UC manager tableId="
              + ucTableInfo.get().getTableId()
              + ", tablePath="
              + ucTableInfo.get().getTablePath());
      return createUCManagedSnapshotManager(ucTableInfo.get(), kernelEngine);
    }
    System.err.println(
        "[KernelCTASDebug] SnapshotManagerFactory.forCreateTable -> PathBased manager. "
            + "CatalogTableUtils.isUnityCatalogManagedTableFromProperties="
            + CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties));
    return new PathBasedSnapshotManager(tablePath, kernelEngine);
  }

  private static UCManagedTableSnapshotManager createUCManagedSnapshotManager(
      UCTableInfo tableInfo, Engine kernelEngine) {
    // Start from defaults (Delta, Spark, Scala, Java) and add connector-specific entries
    Map<String, String> appVersions =
        UCTokenBasedRestClientFactory$.MODULE$.defaultAppVersionsAsJava();
    appVersions.put("Kernel", Meta.KERNEL_VERSION);
    appVersions.put("Delta V2 connector", "true");
    UCClient ucClient =
        UCTokenBasedRestClientFactory$.MODULE$.createUCClientWithVersions(
            tableInfo.getUcUri(), tableInfo.getAuthConfig(), appVersions);
    UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
    return new UCManagedTableSnapshotManager(ucCatalogClient, tableInfo, kernelEngine);
  }
}
