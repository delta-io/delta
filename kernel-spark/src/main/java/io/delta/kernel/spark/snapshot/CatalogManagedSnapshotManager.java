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
import io.delta.kernel.spark.catalog.ManagedCommitClient;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import java.util.Optional;

/**
 * Generic snapshot manager for catalog-managed Delta tables.
 *
 * <p>This implementation is completely catalog-agnostic and works with any catalog that implements
 * ManagedCommitClient (Unity Catalog, Glue, Polaris, etc.)
 *
 * <p>The manager delegates all catalog operations to the ManagedCommitClient, which encapsulates
 * catalog-specific logic.
 */
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager {

  private final ManagedCommitClient catalogClient;
  private final Engine engine;
  private final String tableId;
  private final String tablePath;

  /**
   * Creates a snapshot manager for a catalog-managed table.
   *
   * @param catalogClient generic catalog client (could be UC, Glue, etc.)
   * @param tableId catalog-specific table identifier
   * @param tablePath filesystem path to table data
   * @param engine Delta Kernel engine
   */
  public CatalogManagedSnapshotManager(
      ManagedCommitClient catalogClient, String tableId, String tablePath, Engine engine) {
    this.catalogClient = requireNonNull(catalogClient, "catalogClient is null");
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.engine = requireNonNull(engine, "engine is null");
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    return catalogClient.getSnapshot(
        engine,
        tableId,
        tablePath,
        Optional.empty(), // no specific version
        Optional.empty()); // no timestamp
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    if (version < 0) {
      throw new IllegalArgumentException("version must be non-negative");
    }
    return catalogClient.getSnapshot(
        engine, tableId, tablePath, Optional.of(version), Optional.empty());
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {

    if (!catalogClient.versionExists(tableId, version)) {
      long latestVersion = catalogClient.getLatestVersion(tableId);
      throw new VersionNotFoundException(version, 0, latestVersion);
    }
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    throw new UnsupportedOperationException(
        "getActiveCommitAtTime is not yet supported for catalog-managed tables. "
            + "Time travel by timestamp requires additional catalog API support. "
            + "This feature will be implemented in a future release.");
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    throw new UnsupportedOperationException(
        "getTableChanges is not yet supported for catalog-managed tables. "
            + "This feature will be implemented in a future release.");
  }
}
