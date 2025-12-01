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
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DeltaSnapshotManager for catalog-managed tables (e.g., UC).
 *
 * <p>This snapshot manager is agnostic to the underlying catalog implementation. It delegates to a
 * {@link ManagedCatalogAdapter}, keeping catalog-specific wiring out of the manager itself.
 */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final ManagedCatalogAdapter catalogAdapter;
  private final Engine kernelEngine;

  public CatalogManagedSnapshotManager(ManagedCatalogAdapter catalogAdapter, Configuration hadoopConf) {
    this.catalogAdapter = requireNonNull(catalogAdapter, "catalogAdapter is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    this.kernelEngine = DefaultEngine.create(hadoopConf);
    logger.info(
        "Created CatalogManagedSnapshotManager for table {} at path {}",
        catalogAdapter.getTableId(),
        catalogAdapter.getTablePath());
  }

  /** Loads the latest snapshot of the catalog-managed Delta table. */
  @Override
  public Snapshot loadLatestSnapshot() {
    return catalogAdapter.loadSnapshot(
        kernelEngine,
        /* versionOpt = */ Optional.empty(),
        /* timestampOpt = */ Optional.empty());
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
    return catalogAdapter.loadSnapshot(
        kernelEngine, Optional.of(version), /* timestampOpt = */ Optional.empty());
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p>For catalog-managed tables, this method retrieves ratified commits from the catalog and uses
   * {@link DeltaHistoryManager#getActiveCommitAtTimestamp} to find the commit that was active at
   * the specified timestamp.
   *
   * @param timestampMillis the timestamp in milliseconds since epoch (UTC)
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *     commits; if false, throws an exception
   * @param mustBeRecreatable if true, only considers commits that can be fully recreated from
   *     available log files; if false, considers all commits
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *     all commits; if false, throws an exception
   * @return the commit that was active at the specified timestamp
   */
  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    // Load the latest snapshot for timestamp resolution
    SnapshotImpl latestSnapshot = (SnapshotImpl) loadLatestSnapshot();

    // Get ratified commits from the catalog
    List<ParsedLogData> logData =
        catalogAdapter.getRatifiedCommits(/* endVersionOpt = */ Optional.empty());

    // Convert to ParsedCatalogCommitData for DeltaHistoryManager
    List<ParsedCatalogCommitData> catalogCommits =
        logData.stream()
            .filter(data -> data instanceof ParsedCatalogCommitData)
            .map(data -> (ParsedCatalogCommitData) data)
            .collect(Collectors.toList());

    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        latestSnapshot,
        latestSnapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        catalogCommits);
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * <p>For catalog-managed tables, versions are assumed to be contiguous (enforced by the catalog
   * coordinator). This method performs a lightweight check by verifying the version is within the
   * valid range [0, latestRatifiedVersion].
   *
   * <p>This approach is consistent with the existing Spark Delta behavior in {@code
   * DeltaHistoryManager.checkVersionExists} which also assumes contiguous commits.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *     available log files. For catalog-managed tables, all versions are recreatable since the
   *     catalog maintains the complete commit history.
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *     throwing an exception; if false, throws exception for out-of-range versions
   * @throws VersionNotFoundException if the version is not available
   */
  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    checkArgument(version >= 0, "version must be non-negative");

    // For catalog-managed tables, the earliest recreatable version is 0 since the catalog
    // maintains the complete commit history
    long earliestVersion = 0;
    long latestVersion = catalogAdapter.getLatestRatifiedVersion();

    if (version < earliestVersion || ((version > latestVersion) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliestVersion, latestVersion);
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

    return catalogAdapter.loadCommitRange(
        engine,
        Optional.of(startVersion),
        /* startTimestampOpt = */ Optional.empty(),
        endVersion,
        /* endTimestampOpt = */ Optional.empty());
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
      catalogAdapter.close();
      logger.info("Closed CatalogManagedSnapshotManager for table {}", catalogAdapter.getTableId());
    } catch (Exception e) {
      logger.warn(
          "Error closing catalog-managed client for table {}", catalogAdapter.getTableId(), e);
    }
  }
}
