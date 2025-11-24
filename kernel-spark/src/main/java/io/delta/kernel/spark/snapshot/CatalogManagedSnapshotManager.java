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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DeltaSnapshotManager for catalog-managed tables (e.g., UC).
 *
 * <p>This snapshot manager is agnostic to the underlying catalog implementation. It delegates to a
 * {@link ManagedCommitClient}, keeping catalog-specific wiring out of the manager itself.
 */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final ManagedCommitClient commitClient;
  private final Engine kernelEngine;

  public CatalogManagedSnapshotManager(ManagedCommitClient commitClient, Configuration hadoopConf) {
    this.commitClient = requireNonNull(commitClient, "commitClient is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    this.kernelEngine = DefaultEngine.create(hadoopConf);
    logger.info(
        "Created CatalogManagedSnapshotManager for table {} at path {}",
        commitClient.getTableId(),
        commitClient.getTablePath());
  }

  /** Loads the latest snapshot of the catalog-managed Delta table. */
  @Override
  public Snapshot loadLatestSnapshot() {
    return commitClient.loadSnapshot(kernelEngine, Optional.empty(), Optional.empty());
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
    return commitClient.loadSnapshot(
        kernelEngine, Optional.of(version), Optional.empty());
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
            + "This operation requires filesystem-based commit history which may not be "
            + "available for catalog-managed tables.");
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
   * <p><strong>Note:</strong> This operation delegates to the managed commit client.
   *
   * @throws UnsupportedOperationException if not yet implemented for catalog-managed tables
   */
  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    requireNonNull(engine, "engine is null");
    checkArgument(startVersion >= 0, "startVersion must be non-negative");
    endVersion.ifPresent(v -> checkArgument(v >= 0, "endVersion must be non-negative"));

    return commitClient.loadCommitRange(
        engine, Optional.of(startVersion), Optional.empty(), endVersion, Optional.empty());
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
      commitClient.close();
      logger.info("Closed CatalogManagedSnapshotManager for table {}", commitClient.getTableId());
    } catch (Exception e) {
      logger.warn("Error closing catalog-managed client for table {}", commitClient.getTableId(), e);
    }
  }
}
