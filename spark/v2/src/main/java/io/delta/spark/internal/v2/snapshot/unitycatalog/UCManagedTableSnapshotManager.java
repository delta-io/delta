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
package io.delta.spark.internal.v2.snapshot.unitycatalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.util.List;
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

  /**
   * Loads and returns the latest snapshot of the UC-managed Delta table.
   *
   * @return the latest snapshot of the table
   */
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
   *
   * @param timestampMillis the timestamp to find the version for in milliseconds since the unix
   *     epoch
   * @param canReturnLastCommit whether we can return the latest version of the table if the
   *     provided timestamp is after the latest commit
   * @param mustBeRecreatable whether the state at the returned commit should be recreatable
   * @param canReturnEarliestCommit whether we can return the earliest version of the table if the
   *     provided timestamp is before the earliest commit
   * @return the commit that was active at the specified timestamp
   */
  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    List<ParsedCatalogCommitData> catalogCommits = snapshot.getLogSegment().getAllCatalogCommits();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        engine,
        snapshot,
        snapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        catalogCommits);
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * <p>For UC-managed tables with catalogManaged, log files may be cleaned up, so we need to use
   * DeltaHistoryManager to find the earliest available version based on filesystem state.
   *
   * @param version the version to check
   * @param mustBeRecreatable whether the state at this version should be recreatable
   * @param allowOutOfRange whether versions greater than the latest version are allowed without
   *     throwing an exception
   * @throws VersionNotFoundException if the version is not available or does not meet the specified
   *     criteria
   */
  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    // Load latest to get the current version bounds
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    // Latest version visible in this UC-managed snapshot.
    long latestSnapshotVersion = snapshot.getVersion();

    // Fast path: check upper bound before expensive filesystem operations
    if ((version > latestSnapshotVersion) && !allowOutOfRange) {
      throw new VersionNotFoundException(version, 0 /* earliest */, latestSnapshotVersion);
    }

    // Get the earliest version among catalog commits. This bounds the Kernel's filesystem search
    // for the earliest available version (e.g., if catalog has v0, no filesystem search is needed).
    List<ParsedCatalogCommitData> catalogCommits = snapshot.getLogSegment().getAllCatalogCommits();
    Optional<Long> earliestCatalogCommitVersion =
        catalogCommits.stream().map(ParsedCatalogCommitData::getVersion).min(Long::compare);

    long earliestVersion =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                engine, snapshot.getLogPath(), earliestCatalogCommitVersion)
            : DeltaHistoryManager.getEarliestDeltaFile(
                engine, snapshot.getLogPath(), earliestCatalogCommitVersion);

    if (version < earliestVersion) {
      throw new VersionNotFoundException(version, earliestVersion, latestSnapshotVersion);
    }
  }

  /**
   * Gets a range of table changes (commits) between start and end versions.
   *
   * @param engine the engine implementation for executing operations
   * @param startVersion the starting version (inclusive)
   * @param endVersion optional ending version (inclusive); if not provided, extends to latest
   * @return a CommitRange representing the specified range of commits
   */
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
