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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;

/**
 * Factory for creating {@link DeltaV2SnapshotManager} instances.
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
   * Creates a batch snapshot manager for the given table.
   *
   * @param tablePath the filesystem path to the Delta table
   * @param kernelEngine the pre-configured Kernel {@link Engine} to use for table operations
   * @param catalogTable optional Spark catalog table metadata
   * @return a {@link DeltaV2SnapshotManager} appropriate for the table type
   */
  public static DeltaV2SnapshotManager create(
      String tablePath, Engine kernelEngine, Optional<CatalogTable> catalogTable) {
    return create(tablePath, kernelEngine, catalogTable, WorkloadType.BATCH);
  }

  /**
   * Creates a snapshot manager for the given table and workload.
   *
   * @param tablePath the filesystem path to the Delta table
   * @param kernelEngine the pre-configured Kernel {@link Engine} to use for table operations
   * @param catalogTable optional Spark catalog table metadata
   * @param workloadType the workload the manager serves; for a UC-managed table it is advertised in
   *     the catalog client's User-Agent (see {@link #connectorAppVersions})
   * @return a {@link DeltaV2SnapshotManager} appropriate for the table type
   */
  public static DeltaV2SnapshotManager create(
      String tablePath,
      Engine kernelEngine,
      Optional<CatalogTable> catalogTable,
      WorkloadType workloadType) {

    if (catalogTable.isPresent()) {
      Optional<UCTableInfo> ucTableInfo =
          UCUtils.extractTableInfo(catalogTable.get(), SparkSession.active());
      if (ucTableInfo.isPresent()) {
        return createUCManagedSnapshotManager(ucTableInfo.get(), kernelEngine, workloadType);
      }
      // Catalog table without UC metadata falls back to path-based handling.
    }

    // Default: path-based snapshot manager for non-UC tables. Path-based tables issue no catalog
    // requests, so there is no User-Agent to tag and the workload type is not threaded here.
    return new PathBasedSnapshotManager(tablePath, kernelEngine);
  }

  private static UCManagedTableSnapshotManager createUCManagedSnapshotManager(
      UCTableInfo tableInfo, Engine kernelEngine, WorkloadType workloadType) {
    Map<String, String> ucConfig = new HashMap<>(tableInfo.toUcConfig());
    ucConfig.putAll(connectorAppVersions(workloadType));
    UCClient ucClient = UCTokenBasedRestClientFactory$.MODULE$.createUCClient(ucConfig);
    UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
    return new UCManagedTableSnapshotManager(ucCatalogClient, tableInfo, kernelEngine);
  }

  /**
   * The {@code appVersions.*} entries the V2 connector contributes to the UC client's User-Agent.
   * Always advertises the Kernel version and the V2 connector marker; for a streaming workload it
   * also adds a {@code Streaming} marker so the catalog can tell a Structured Streaming read/write
   * apart from a batch one. Package-private for testing.
   */
  static Map<String, String> connectorAppVersions(WorkloadType workloadType) {
    Map<String, String> appVersions = new HashMap<>();
    appVersions.put("appVersions.Kernel", Meta.KERNEL_VERSION);
    appVersions.put("appVersions.Delta V2 connector", "true");
    if (workloadType == WorkloadType.STREAMING) {
      appVersions.put("appVersions.Streaming", "true");
    }
    return appVersions;
  }
}
