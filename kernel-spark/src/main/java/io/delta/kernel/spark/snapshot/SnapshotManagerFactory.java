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
package io.delta.kernel.spark.snapshot;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import io.delta.kernel.spark.snapshot.unitycatalog.UCTableInfo;
import io.delta.kernel.spark.snapshot.unitycatalog.UCUtils;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

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
   * Creates a snapshot manager for the given table.
   *
   * @param tablePath the filesystem path to the Delta table
   * @param hadoopConf the Hadoop configuration for table operations
   * @param catalogTable optional Spark catalog table metadata
   * @return a {@link DeltaSnapshotManager} appropriate for the table type
   */
  public static DeltaSnapshotManager create(
      String tablePath, Configuration hadoopConf, Optional<CatalogTable> catalogTable) {

    if (catalogTable.isPresent()) {
      Optional<UCTableInfo> ucTableInfo =
          UCUtils.extractTableInfo(catalogTable.get(), SparkSession.active());
      if (ucTableInfo.isPresent()) {
        return createUCManagedSnapshotManager(ucTableInfo.get(), hadoopConf);
      }
    }

    // Default: path-based snapshot manager for non-UC tables
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  private static UCManagedTableSnapshotManager createUCManagedSnapshotManager(
      UCTableInfo tableInfo, Configuration hadoopConf) {
    UCClient ucClient = new UCTokenBasedRestClient(tableInfo.getUcUri(), tableInfo.getUcToken());
    UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
    Engine engine = DefaultEngine.create(hadoopConf);
    return new UCManagedTableSnapshotManager(ucCatalogClient, tableInfo, engine);
  }
}
