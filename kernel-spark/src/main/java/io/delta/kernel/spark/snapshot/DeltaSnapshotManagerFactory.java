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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Factory for creating {@link DeltaSnapshotManager} instances.
 *
 * <p>This factory provides two creation methods:
 *
 * <ul>
 *   <li>{@link #fromPath} - Creates a {@link PathBasedSnapshotManager} for filesystem-based Delta
 *       tables
 *   <li>{@link #fromCatalogTable} - Creates snapshot manager from catalog metadata, automatically
 *       selecting {@link CatalogManagedSnapshotManager} for UC tables or falling back to {@link
 *       PathBasedSnapshotManager}
 * </ul>
 *
 * <p><strong>Example usage:</strong>
 *
 * <pre>{@code
 * // For path-based tables
 * DeltaSnapshotManager manager = DeltaSnapshotManagerFactory.fromPath(
 *     tablePath,
 *     hadoopConf
 * );
 *
 * // For catalog tables
 * DeltaSnapshotManager manager = DeltaSnapshotManagerFactory.fromCatalogTable(
 *     catalogTable,
 *     spark,
 *     hadoopConf
 * );
 * }</pre>
 */
@Experimental
public final class DeltaSnapshotManagerFactory {

  // Utility class - no instances
  private DeltaSnapshotManagerFactory() {}

  /**
   * Creates a path-based snapshot manager for filesystem Delta tables.
   *
   * <p>Use this when no catalog metadata is available or when you want to work directly with a
   * filesystem path.
   *
   * @param tablePath filesystem path to the Delta table root
   * @param hadoopConf Hadoop configuration for the Delta Kernel engine
   * @return PathBasedSnapshotManager instance
   * @throws NullPointerException if tablePath or hadoopConf is null
   */
  public static DeltaSnapshotManager fromPath(String tablePath, Configuration hadoopConf) {
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  /**
   * Creates a snapshot manager from catalog table metadata.
   *
   * <p>Wire-up is intentionally deferred; this skeleton method currently throws until implemented
   * in a follow-up PR.
   *
   * @throws UnsupportedOperationException always, until UC wiring is added
   */
  public static DeltaSnapshotManager fromCatalogTable(
      CatalogTable catalogTable, SparkSession spark, Configuration hadoopConf) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");
    requireNonNull(hadoopConf, "hadoopConf is null");
    throw new UnsupportedOperationException("UC catalog wiring not implemented in skeleton");
  }
}
