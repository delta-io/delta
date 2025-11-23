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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.catalog.CatalogWithManagedCommits;
import io.delta.kernel.spark.catalog.ManagedCommitClient;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

/**
 * Factory for creating appropriate DeltaSnapshotManager implementations.
 *
 * <p>This factory uses capability-based detection to determine if a table uses catalog-managed
 * commits, without knowing about specific catalog implementations (Unity Catalog, Glue, Polaris,
 * etc.)
 */
public final class DeltaSnapshotManagerFactory {

  private DeltaSnapshotManagerFactory() {}

  /**
   * Creates the appropriate snapshot manager for a Delta table.
   *
   * <p>Decision logic:
   *
   * <ol>
   *   <li>If catalogTable is present, resolve its catalog from Spark
   *   <li>If catalog implements CatalogWithManagedCommits, try to get client
   *   <li>If client is present, use CatalogManagedSnapshotManager
   *   <li>Otherwise, fall back to PathBasedSnapshotManager
   * </ol>
   *
   * <p>This approach is completely catalog-agnostic.
   *
   * <p><b>Important:</b> The caller should merge any catalog-specific storage properties (from
   * {@code catalogTable.storage().properties()}) into the {@code hadoopConf} before calling this
   * method. These properties may contain filesystem credentials and endpoints required for table
   * access.
   *
   * @param tablePath filesystem path to the Delta table
   * @param catalogTable optional Spark catalog table metadata
   * @param spark SparkSession for resolving catalogs
   * @param hadoopConf Hadoop configuration (should include catalog storage properties)
   * @return appropriate snapshot manager implementation
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

    if (catalogTable.isPresent()) {
      CatalogTable table = catalogTable.get();

      // Resolve catalog name from table identifier
      String catalogName = extractCatalogName(table, spark);

      try {
        // Get catalog from Spark's catalog manager
        CatalogPlugin catalog = spark.sessionState().catalogManager().catalog(catalogName);

        // Check if catalog supports managed commits (generic capability check)
        if (catalog instanceof CatalogWithManagedCommits) {
          CatalogWithManagedCommits managedCatalog = (CatalogWithManagedCommits) catalog;

          // Extract table ID (catalog-specific, but catalog does it)
          Optional<String> tableId = managedCatalog.extractTableId(table);

          if (tableId.isPresent()) {
            // Get managed commit client (generic interface!)
            Optional<ManagedCommitClient> client =
                managedCatalog.getManagedCommitClient(tableId.get(), tablePath);

            if (client.isPresent()) {
              Engine engine = DefaultEngine.create(hadoopConf);

              // Use generic catalog-managed snapshot manager
              return new CatalogManagedSnapshotManager(
                  client.get(), tableId.get(), tablePath, engine);
            }
          }
        }
      } catch (Exception e) {
        // Log warning but fall back to path-based
        System.err.println("Failed to create catalog-managed snapshot manager: " + e.getMessage());
      }
    }

    // Default: path-based snapshot manager
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  private static String extractCatalogName(CatalogTable table, SparkSession spark) {
    scala.Option<String> catalogOption = table.identifier().catalog();
    if (catalogOption.isDefined()) {
      return catalogOption.get();
    }
    // Use current catalog if not specified
    return spark.sessionState().catalogManager().currentCatalog().name();
  }
}
