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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.spark.client.UcClientFactory;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Implementation of DeltaSnapshotManager for Unity Catalog managed Delta tables with CCv2 (Catalog
 * Commit Coordinator v2).
 *
 * <p>This snapshot manager supports Unity Catalog managed tables by creating a Unity Catalog client
 * for potential CCv2 detection and metadata queries. Currently, it delegates snapshot operations to
 * {@link PathBasedSnapshotManager} for filesystem-based snapshot loading.
 *
 * <p><b>Architecture:</b>
 *
 * <p>This manager uses composition - it creates a {@link UCClient} via {@link UcClientFactory} to
 * enable CCv2-specific operations (detection, metadata queries) while delegating snapshot loading
 * to the proven {@link PathBasedSnapshotManager} implementation.
 *
 * <p><b>Future Enhancement:</b>
 *
 * <p>In a future PR, this manager can leverage the UC client for advanced CCv2 features like
 * catalog-coordinated commits, server-side table feature queries, and metastore-level operations.
 *
 * <p><b>Resource Management:</b>
 *
 * <p>This class implements {@link AutoCloseable} to properly manage the UC client lifecycle.
 * However, note that Spark's DSv2 Table interface doesn't provide lifecycle hooks, so the UC client
 * will be closed when the manager is garbage collected or when explicitly closed by the caller.
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * UnityCatalogSnapshotManager manager = new UnityCatalogSnapshotManager(
 *     spark, catalogTable, tablePath, hadoopConf);
 * Snapshot snapshot = manager.loadLatestSnapshot();
 * // Use snapshot...
 * // UC client closed on GC or explicit close()
 * }</pre>
 *
 * @see UcClientFactory
 * @see UCClient
 * @see PathBasedSnapshotManager
 */
@Experimental
public class UnityCatalogSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final String UC_TABLE_ID_KEY = "ucTableId";

  private final PathBasedSnapshotManager delegate;
  private final UCClient ucClient;
  private final String ucTableId;

  /**
   * Creates a Unity Catalog aware snapshot manager.
   *
   * @param spark the active SparkSession for accessing catalog configuration
   * @param catalogTable the CatalogTable descriptor containing UC metadata
   * @param tablePath filesystem path to the Delta table root
   * @param hadoopConf Hadoop configuration for filesystem access
   * @throws NullPointerException if any parameter is null
   * @throws IllegalArgumentException if catalog name or UC table ID cannot be extracted
   */
  public UnityCatalogSnapshotManager(
      SparkSession spark, CatalogTable catalogTable, String tablePath, Configuration hadoopConf) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    // Extract UC table ID from storage properties
    this.ucTableId = extractUcTableId(catalogTable);

    // Extract catalog name and create UC client for CCv2 operations
    String catalogName = extractCatalogName(catalogTable);
    this.ucClient = UcClientFactory.buildForCatalog(spark, catalogName);

    // Delegate snapshot operations to path-based manager
    this.delegate = new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  /**
   * Extracts the Unity Catalog table ID from the CatalogTable storage properties.
   *
   * @param catalogTable the CatalogTable descriptor
   * @return the UC table ID
   * @throws IllegalArgumentException if UC table ID is not found
   */
  private static String extractUcTableId(CatalogTable catalogTable) {
    Map<String, String> storageProperties =
        ScalaUtils.toJavaMap(catalogTable.storage().properties());
    String ucTableId = storageProperties.get(UC_TABLE_ID_KEY);
    if (ucTableId == null || ucTableId.trim().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "UC table ID not found in storage properties for table %s. "
                  + "Expected property key '%s'.",
              catalogTable.identifier(), UC_TABLE_ID_KEY));
    }
    return ucTableId;
  }

  /**
   * Extracts the catalog name from the CatalogTable identifier.
   *
   * @param catalogTable the CatalogTable descriptor
   * @return the catalog name
   * @throws IllegalArgumentException if catalog name is not present
   */
  private static String extractCatalogName(CatalogTable catalogTable) {
    scala.Option<String> catalogOpt = catalogTable.identifier().catalog();
    if (catalogOpt.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Catalog name not found for table %s. UC managed tables must belong to a catalog.",
              catalogTable.identifier()));
    }
    return catalogOpt.get();
  }

  /**
   * Returns the Unity Catalog client for CCv2 operations.
   *
   * <p>This client can be used for CCv2 detection, querying commit metadata, table features, and
   * metastore information.
   *
   * @return the UC client instance
   */
  public UCClient getUCClient() {
    return ucClient;
  }

  /**
   * Returns the Unity Catalog table ID.
   *
   * @return the UC table ID
   */
  public String getUCTableId() {
    return ucTableId;
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    return delegate.loadLatestSnapshot();
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    return delegate.loadSnapshotAt(version);
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    return delegate.getActiveCommitAtTime(
        timestampMillis, canReturnLastCommit, mustBeRecreatable, canReturnEarliestCommit);
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    delegate.checkVersionExists(version, mustBeRecreatable, allowOutOfRange);
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    return delegate.getTableChanges(engine, startVersion, endVersion);
  }

  /**
   * Closes the Unity Catalog client and releases associated resources.
   *
   * <p>This method is idempotent - calling it multiple times is safe.
   */
  @Override
  public void close() throws IOException {
    if (ucClient != null) {
      try {
        ucClient.close();
      } catch (Exception e) {
        throw new IOException("Failed to close UC client", e);
      }
    }
  }
}
