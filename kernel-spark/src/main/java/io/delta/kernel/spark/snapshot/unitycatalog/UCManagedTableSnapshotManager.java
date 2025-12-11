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
package io.delta.kernel.spark.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Snapshot manager for Unity Catalog managed tables.
 *
 * <p>Used for tables with the catalog-managed commit feature enabled. Unity Catalog serves as the
 * source of truth for the table's commit history.
 */
public class UCManagedTableSnapshotManager implements DeltaSnapshotManager {

  private final UCCatalogManagedClient ucCatalogManagedClient;
  private final String tableId;
  private final String tablePath;
  private final Engine engine;

  /**
   * Creates a new UCManagedTableSnapshotManager.
   *
   * @param ucCatalogManagedClient the UC client for catalog-managed operations
   * @param tableInfo the UC table information (tableId, tablePath, etc.)
   * @param engine the Kernel engine for table operations
   */
  public UCManagedTableSnapshotManager(
      UCCatalogManagedClient ucCatalogManagedClient, UCTableInfo tableInfo, Engine engine) {
    this.ucCatalogManagedClient =
        requireNonNull(ucCatalogManagedClient, "ucCatalogManagedClient is null");
    requireNonNull(tableInfo, "tableInfo is null");
    this.tableId = tableInfo.getTableId();
    this.tablePath = tableInfo.getTablePath();
    this.engine = requireNonNull(engine, "engine is null");
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    return ucCatalogManagedClient.loadSnapshot(
        engine,
        tableId,
        tablePath,
        Optional.empty() /* versionOpt */,
        Optional.empty() /* timestampOpt */);
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    return ucCatalogManagedClient.loadSnapshot(
        engine, tableId, tablePath, Optional.of(version), Optional.empty() /* timestampOpt */);
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p>For UC-managed tables, this loads the latest snapshot and uses {@link
   * DeltaHistoryManager#getActiveCommitAtTimestamp} to resolve the timestamp to a commit.
   */
  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        snapshot,
        snapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>() /* catalogCommits */);
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * <p>For UC-managed tables, all ratified commits are available, so the earliest version is
   * typically 0. This method validates that the requested version is within the valid range.
   */
  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    // Load latest to get the current version bounds
    Snapshot latestSnapshot = loadLatestSnapshot();
    long latestVersion = latestSnapshot.getVersion();

    // For UC tables, earliest recreatable version is 0 (all ratified commits are available)
    long earliestVersion = 0;

    if (version < earliestVersion || ((version > latestVersion) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliestVersion, latestVersion);
    }
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    return ucCatalogManagedClient.loadCommitRange(
        engine,
        tableId,
        tablePath,
        Optional.of(startVersion) /* startVersionOpt */,
        Optional.empty() /* startTimestampOpt */,
        endVersion /* endVersionOpt */,
        Optional.empty() /* endTimestampOpt */);
  }
}
