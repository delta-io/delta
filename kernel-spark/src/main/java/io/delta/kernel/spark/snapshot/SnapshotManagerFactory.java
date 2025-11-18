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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Factory for creating appropriate {@link DeltaSnapshotManager} implementations based on table
 * configuration.
 *
 * <p>This factory encapsulates the logic for determining which snapshot manager implementation to
 * use for a given Delta table. It supports:
 *
 * <ul>
 *   <li>{@link PathBasedSnapshotManager} - for traditional filesystem-based Delta tables
 *   <li>{@link UnityCatalogSnapshotManager} - for Unity Catalog managed tables with CCv2
 * </ul>
 *
 * <p>The factory keeps SparkTable catalog-agnostic by centralizing catalog-specific decision logic
 * here rather than coupling it directly into the table implementation.
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * SparkSession spark = SparkSession.active();
 * Optional<CatalogTable> catalogTable = Optional.of(myCatalogTable);
 * String tablePath = "/path/to/table";
 * Configuration hadoopConf = spark.sessionState().newHadoopConf();
 *
 * DeltaSnapshotManager manager = SnapshotManagerFactory.create(
 *     spark, catalogTable, tablePath, hadoopConf);
 * }</pre>
 */
public final class SnapshotManagerFactory {

  private SnapshotManagerFactory() {}

  /**
   * Creates an appropriate DeltaSnapshotManager based on table configuration.
   *
   * <p>Decision logic:
   *
   * <ul>
   *   <li>If catalogTable is present and {@link CatalogTableUtils#isUnityCatalogManagedTable}
   *       returns true, creates {@link UnityCatalogSnapshotManager}
   *   <li>Otherwise, creates {@link PathBasedSnapshotManager} (default/legacy behavior)
   * </ul>
   *
   * @param spark the active SparkSession for accessing catalog configuration
   * @param catalogTable optional CatalogTable descriptor (empty for path-based tables)
   * @param tablePath filesystem path to the Delta table root
   * @param hadoopConf Hadoop configuration for filesystem access
   * @return appropriate DeltaSnapshotManager implementation
   * @throws NullPointerException if spark, catalogTable, tablePath, or hadoopConf is null
   */
  public static DeltaSnapshotManager create(
      SparkSession spark,
      Optional<CatalogTable> catalogTable,
      String tablePath,
      Configuration hadoopConf) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    // Check if this is a Unity Catalog managed table with CCv2
    if (catalogTable.isPresent()
        && CatalogTableUtils.isUnityCatalogManagedTable(catalogTable.get())) {
      return new UnityCatalogSnapshotManager(spark, catalogTable.get(), tablePath, hadoopConf);
    }

    // Default to path-based snapshot manager (legacy behavior)
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }
}
