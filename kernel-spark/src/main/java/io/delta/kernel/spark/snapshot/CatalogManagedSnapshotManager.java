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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.delta.unity.UCCatalogManagedClient;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DeltaSnapshotManager for Unity Catalog managed tables (CCv2).
 *
 * <p>This snapshot manager handles tables where Unity Catalog acts as the commit coordinator. It
 * manages the lifecycle of UC client connections and delegates snapshot operations to {@link
 * UCCatalogManagedClient}.
 *
 * <p>The manager automatically extracts table metadata (ucTableId, tablePath) from the {@link
 * CatalogTable} and creates the necessary UC client for communication with Unity Catalog.
 *
 * <p><strong>Resource Management:</strong> This class implements {@link AutoCloseable} and must be
 * properly closed to release UC client resources. Use try-with-resources:
 *
 * <pre>{@code
 * try (CatalogManagedSnapshotManager manager = new CatalogManagedSnapshotManager(...)) {
 *   Snapshot snapshot = manager.loadLatestSnapshot();
 *   // Use snapshot...
 * }
 * }</pre>
 *
 * <p><strong>TODO (Next PR):</strong> Integrate proper lifecycle management into {@link
 * io.delta.kernel.spark.table.SparkTable}. Currently, SparkTable doesn't implement close() or
 * properly manage snapshot manager resources. Options to consider:
 *
 * <ul>
 *   <li>Make SparkTable implement AutoCloseable and propagate close() to snapshot manager
 *   <li>Add {@link AutoCloseable} to {@link DeltaSnapshotManager} interface with default no-op
 *   <li>Use finalization or shutdown hooks as safety net (not recommended as primary solution)
 * </ul>
 *
 * <p>Without proper lifecycle management, UC client connections may leak in long-running Spark
 * sessions. This is acceptable for this initial PR but must be addressed before production use.
 */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final UCClient ucClient;
  private final UCCatalogManagedClient ucManagedClient;
  private final Engine kernelEngine;
  private final String ucTableId;
  private final String tablePath;

  /**
   * Creates a snapshot manager for a Unity Catalog managed table.
   *
   * @param catalogTable the Spark catalog table (must be Unity Catalog managed)
   * @param spark the SparkSession containing Unity Catalog configurations
   * @param hadoopConf Hadoop configuration for the Delta Kernel engine
   * @throws NullPointerException if any parameter is null
   * @throws IllegalArgumentException if table is not Unity Catalog managed
   * @throws IllegalArgumentException if Unity Catalog configuration is not found
   */
  public CatalogManagedSnapshotManager(
      CatalogTable catalogTable, SparkSession spark, Configuration hadoopConf) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    // Validate table is Unity Catalog managed
    if (!CatalogTableUtils.isUnityCatalogManagedTable(catalogTable)) {
      throw new IllegalArgumentException(
          "Cannot create CatalogManagedSnapshotManager: table is not Unity Catalog managed. "
              + "Table identifier: "
              + catalogTable.identifier());
    }

    // Extract table metadata
    this.ucTableId = extractUCTableId(catalogTable);
    this.tablePath = extractTablePath(catalogTable);

    // Create UC client and managed client
    this.ucClient = createUCClientInternal(catalogTable, spark);
    this.ucManagedClient = new UCCatalogManagedClient(ucClient);
    this.kernelEngine = DefaultEngine.create(hadoopConf);

    logger.info(
        "Created CatalogManagedSnapshotManager for table {} at path {}", ucTableId, tablePath);
  }

  /**
   * Loads the latest snapshot of the Unity Catalog managed Delta table.
   *
   * @return the latest snapshot
   */
  @Override
  public Snapshot loadLatestSnapshot() {
    return ucManagedClient.loadSnapshot(
        kernelEngine, ucTableId, tablePath, Optional.empty(), Optional.empty());
  }

  /**
   * Loads a specific version of the Unity Catalog managed Delta table.
   *
   * @param version the version to load (must be >= 0)
   * @return the snapshot at the specified version
   */
  @Override
  public Snapshot loadSnapshotAt(long version) {
    checkArgument(version >= 0, "version must be non-negative");
    return ucManagedClient.loadSnapshot(
        kernelEngine, ucTableId, tablePath, Optional.of(version), Optional.empty());
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p><strong>Note:</strong> This operation is not yet supported for Unity Catalog managed tables
   * because it requires filesystem-based commit history which is not accessible for catalog-managed
   * tables. Unity Catalog coordinates commits differently than traditional Delta tables.
   *
   * @throws UnsupportedOperationException always - not yet implemented for catalog-managed tables
   */
  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    throw new UnsupportedOperationException(
        "getActiveCommitAtTime not yet implemented for catalog-managed tables. "
            + "This operation requires filesystem-based commit history which is not "
            + "accessible for Unity Catalog managed tables.");
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * <p><strong>Performance Note:</strong> For Unity Catalog managed tables, version checking
   * requires loading the full snapshot including all file metadata. This is less efficient than
   * filesystem-based checks which can verify log file existence without reading contents.
   *
   * <p><strong>TODO (Next PR):</strong> Add lightweight version checking API to
   * UCCatalogManagedClient to avoid loading full snapshots for existence checks.
   *
   * @throws VersionNotFoundException if the version is not available
   */
  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    checkArgument(version >= 0, "version must be non-negative");

    try {
      // Attempt to load the snapshot at the specified version
      // Note: This loads the full snapshot - see performance note above
      loadSnapshotAt(version);
    } catch (KernelException e) {
      // Specific Kernel exceptions indicate version doesn't exist or isn't accessible
      // Let other exceptions (network failures, auth errors, etc.) propagate to caller
      long latestVersion = loadLatestSnapshot().getVersion();
      throw new VersionNotFoundException(version, 0, latestVersion);
    }
  }

  /**
   * Gets a range of table changes between versions.
   *
   * <p><strong>Note:</strong> This operation delegates to {@link UCCatalogManagedClient} for Unity
   * Catalog managed tables.
   *
   * @throws UnsupportedOperationException if not yet implemented for catalog-managed tables
   */
  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    // TODO: Implement getTableChanges for UC-managed tables
    // This requires UCCatalogManagedClient to expose commit range functionality
    throw new UnsupportedOperationException(
        "getTableChanges not yet implemented for catalog-managed tables");
  }

  /**
   * Closes the UC client and releases resources.
   *
   * <p>This method should be called when the snapshot manager is no longer needed. Prefer using
   * try-with-resources to ensure proper cleanup.
   */
  @Override
  public void close() {
    try {
      ucClient.close();
      logger.info("Closed CatalogManagedSnapshotManager for table {}", ucTableId);
    } catch (Exception e) {
      logger.warn("Error closing UC client for table {}", ucTableId, e);
    }
  }

  /**
   * Creates a UC client for the table's catalog (internal helper).
   *
   * <p>This method resolves the catalog configuration directly from Spark and creates a {@link
   * UCTokenBasedRestClient}. All UC-specific logic is encapsulated here to avoid polluting
   * kernel-spark utilities with UC coupling.
   *
   * @param catalogTable the catalog table
   * @param spark the SparkSession
   * @return configured UC client
   * @throws IllegalArgumentException if catalog configuration is not found
   */
  private static UCClient createUCClientInternal(CatalogTable catalogTable, SparkSession spark) {
    // 1. Extract catalog name from table identifier (or use default)
    scala.Option<String> catalogOption = catalogTable.identifier().catalog();
    String catalogName =
        catalogOption.isDefined()
            ? catalogOption.get()
            : spark.sessionState().catalogManager().currentCatalog().name();

    // 2. Get all UC catalog configs from Scala code (inlined, not abstracted)
    scala.collection.immutable.List<scala.Tuple3<String, String, String>> scalaConfigs =
        org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$.MODULE$
            .getCatalogConfigs(spark);

    // 3. Find matching config for this catalog
    Optional<scala.Tuple3<String, String, String>> configTuple =
        scala.jdk.javaapi.CollectionConverters.asJava(scalaConfigs).stream()
            .filter(tuple -> tuple._1().equals(catalogName))
            .findFirst();

    if (!configTuple.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot create UC client: Unity Catalog configuration not found "
              + "for catalog '"
              + catalogName
              + "'. "
              + "Ensure spark.sql.catalog."
              + catalogName
              + ".uri and "
              + "spark.sql.catalog."
              + catalogName
              + ".token are configured.");
    }

    // 4. Extract URI and token from tuple (catalogName, uri, token)
    scala.Tuple3<String, String, String> config = configTuple.get();
    String uri = config._2();
    String token = config._3();

    // 5. Create UC client
    return new UCTokenBasedRestClient(uri, token);
  }

  /**
   * Extracts the Unity Catalog table ID from catalog table properties.
   *
   * @param catalogTable the catalog table
   * @return the UC table ID
   * @throws IllegalArgumentException if ucTableId is not found
   */
  private static String extractUCTableId(CatalogTable catalogTable) {
    java.util.Map<String, String> storageProperties =
        scala.collection.JavaConverters.mapAsJavaMap(catalogTable.storage().properties());

    String ucTableId = storageProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId == null || ucTableId.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot extract ucTableId: "
              + UCCommitCoordinatorClient.UC_TABLE_ID_KEY
              + " not found in table storage properties for table "
              + catalogTable.identifier());
    }
    return ucTableId;
  }

  /**
   * Extracts the table path from the catalog table location.
   *
   * @param catalogTable the catalog table
   * @return the table path
   * @throws IllegalArgumentException if location is not available
   */
  private static String extractTablePath(CatalogTable catalogTable) {
    if (catalogTable.location() == null) {
      throw new IllegalArgumentException(
          "Cannot extract table path: location is null for table " + catalogTable.identifier());
    }
    return catalogTable.location().toString();
  }
}
