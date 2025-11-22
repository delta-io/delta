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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.spark.utils.CatalogTableUtils;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Factory for creating {@link DeltaSnapshotManager} instances.
 *
 * <p>This factory determines the appropriate snapshot manager implementation based on table
 * characteristics and automatically handles the selection between:
 *
 * <ul>
 *   <li>{@link CatalogManagedSnapshotManager} - for Unity Catalog managed tables (CCv2)
 *   <li>{@link PathBasedSnapshotManager} - for regular filesystem-based Delta tables
 * </ul>
 *
 * <p>The factory encapsulates the decision logic so that callers (e.g., {@code SparkTable}) don't
 * need to know about specific manager implementations.
 *
 * <p><strong>Example usage:</strong>
 *
 * <pre>{@code
 * DeltaSnapshotManager manager = DeltaSnapshotManagerFactory.create(
 *     tablePath,
 *     Optional.of(catalogTable),
 *     spark,
 *     hadoopConf
 * );
 * Snapshot snapshot = manager.loadLatestSnapshot();
 * }</pre>
 */
@Experimental
public final class DeltaSnapshotManagerFactory {

  // Utility class - no instances
  private DeltaSnapshotManagerFactory() {}

  /**
   * Creates the appropriate snapshot manager for a Delta table.
   *
   * <p><strong>Selection logic:</strong>
   *
   * <ul>
   *   <li>If {@code catalogTable} is present and Unity Catalog managed → {@link
   *       CatalogManagedSnapshotManager}
   *   <li>Otherwise → {@link PathBasedSnapshotManager}
   * </ul>
   *
   * <p>Unity Catalog managed tables are identified by checking for catalog-managed feature flags
   * and the presence of a Unity Catalog table ID. See {@link
   * CatalogTableUtils#isUnityCatalogManagedTable(CatalogTable)} for details.
   *
   * @param tablePath filesystem path to the Delta table root
   * @param catalogTable optional Spark catalog table metadata
   * @param spark SparkSession for resolving Unity Catalog configurations
   * @param hadoopConf Hadoop configuration for the Delta Kernel engine
   * @return appropriate snapshot manager implementation
   * @throws NullPointerException if tablePath, spark, or hadoopConf is null
   * @throws IllegalArgumentException if catalogTable is UC-managed but configuration is invalid
   */
  public static DeltaSnapshotManager create(
      String tablePath,
      Optional<CatalogTable> catalogTable,
      SparkSession spark,
      Configuration hadoopConf) {

    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    // Check if table is Unity Catalog managed
    if (catalogTable.isPresent()
        && CatalogTableUtils.isUnityCatalogManagedTable(catalogTable.get())) {
      return new CatalogManagedSnapshotManager(catalogTable.get(), spark, hadoopConf);
    }

    // Default to path-based snapshot manager
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }
}
