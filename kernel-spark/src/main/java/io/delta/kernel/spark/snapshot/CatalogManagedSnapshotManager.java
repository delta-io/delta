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
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import java.util.Comparator;
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
 * <p>This snapshot manager owns all Delta/Kernel logic for building snapshots and commit ranges
 * from catalog-provided commit metadata. It uses a {@link ManagedCatalogAdapter} to fetch commits
 * from the catalog, then applies Kernel's TableManager APIs to construct the appropriate Delta
 * objects.
 *
 * <p>This manager is catalog-agnostic - it works with any adapter that implements {@link
 * ManagedCatalogAdapter} (UC, Glue, Polaris, etc.). The adapter handles catalog-specific
 * communication while this manager handles Delta semantics.
 */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final ManagedCatalogAdapter catalogAdapter;
  private final String tableId;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final Engine kernelEngine;

  public CatalogManagedSnapshotManager(
      ManagedCatalogAdapter catalogAdapter,
      String tableId,
      String tablePath,
      Configuration hadoopConf) {
    this.catalogAdapter = requireNonNull(catalogAdapter, "catalogAdapter is null");
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");

    this.kernelEngine = DefaultEngine.create(this.hadoopConf);
    logger.info(
        "Created CatalogManagedSnapshotManager for table {} at path {}", tableId, tablePath);
  }

  /** Loads the latest snapshot of the catalog-managed Delta table. */
  @Override
  public Snapshot loadLatestSnapshot() {
    return loadSnapshotInternal(Optional.empty(), Optional.empty());
  }

  /**
   * Loads a specific version of the catalog-managed Delta table.
   *
   * @param version the version to load (must be >= 0)
   * @return the snapshot at the specified version
   */
  @Override
  public Snapshot loadSnapshotAt(long version) {
    checkArgument(version >= 0, "version must be non-negative");
    return loadSnapshotInternal(Optional.of(version), Optional.empty());
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

    // Extract catalog commits from the snapshot's log segment (avoids redundant catalog call)
    List<ParsedCatalogCommitData> catalogCommits =
        latestSnapshot.getLogSegment().getAllCatalogCommits();

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
   * @param engine the Kernel engine for I/O operations
   * @param startVersion the start version (inclusive)
   * @param endVersion optional end version (inclusive); if empty, uses latest
   * @return the commit range between the boundaries
   */
  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    requireNonNull(engine, "engine is null");
    checkArgument(startVersion >= 0, "startVersion must be non-negative");
    endVersion.ifPresent(v -> checkArgument(v >= 0, "endVersion must be non-negative"));

    return loadCommitRangeInternal(
        engine,
        Optional.of(startVersion),
        Optional.empty(),
        endVersion,
        Optional.empty());
  }

  /**
   * Closes the catalog adapter and releases resources.
   *
   * <p>This method should be called when the snapshot manager is no longer needed. Prefer using
   * try-with-resources to ensure proper cleanup.
   */
  @Override
  public void close() {
    try {
      catalogAdapter.close();
      logger.info("Closed CatalogManagedSnapshotManager for table {}", tableId);
    } catch (Exception e) {
      logger.warn("Error closing catalog-managed client for table {}", tableId, e);
    }
  }

  // ========== Internal implementation methods ==========

  /**
   * Internal method to load a snapshot at a specific version or timestamp.
   *
   * <p>This method fetches commits from the catalog adapter, converts them to Kernel's ParsedLogData
   * format, and uses TableManager to build the snapshot.
   */
  private Snapshot loadSnapshotInternal(Optional<Long> versionOpt, Optional<Long> timestampOpt) {
    checkArgument(
        !versionOpt.isPresent() || !timestampOpt.isPresent(),
        "Cannot provide both version and timestamp");

    logger.info(
        "[{}] Loading Snapshot at {}",
        tableId,
        getVersionOrTimestampString(versionOpt, timestampOpt));

    // Fetch commits from catalog
    GetCommitsResponse response = catalogAdapter.getCommits(0, versionOpt);
    long catalogVersion = getCatalogVersion(response.getLatestTableVersion());

    // Validate version if specified
    versionOpt.ifPresent(v -> validateVersionExists(v, catalogVersion));

    // Convert to Kernel ParsedLogData
    List<ParsedLogData> logData = convertToKernelLogData(response.getCommits());

    // Build snapshot using TableManager
    SnapshotBuilder snapshotBuilder = TableManager.loadSnapshot(tablePath);

    if (versionOpt.isPresent()) {
      snapshotBuilder = snapshotBuilder.atVersion(versionOpt.get());
    }

    if (timestampOpt.isPresent()) {
      // For timestamp queries, first build the latest snapshot for resolution
      Snapshot latestSnapshot =
          snapshotBuilder.withLogData(logData).withMaxCatalogVersion(catalogVersion).build(kernelEngine);
      snapshotBuilder = TableManager.loadSnapshot(tablePath)
          .atTimestamp(timestampOpt.get(), latestSnapshot);
    }

    return snapshotBuilder.withLogData(logData).withMaxCatalogVersion(catalogVersion).build(kernelEngine);
  }

  /**
   * Internal method to load a commit range with version or timestamp boundaries.
   */
  private CommitRange loadCommitRangeInternal(
      Engine engine,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt) {
    checkArgument(
        !startVersionOpt.isPresent() || !startTimestampOpt.isPresent(),
        "Cannot provide both start version and start timestamp");
    checkArgument(
        !endVersionOpt.isPresent() || !endTimestampOpt.isPresent(),
        "Cannot provide both end version and end timestamp");

    // For timestamp-based boundaries, don't filter by endVersion when fetching commits
    Optional<Long> endVersionForQuery =
        endVersionOpt.filter(v -> !startTimestampOpt.isPresent() && !endTimestampOpt.isPresent());

    // Fetch commits from catalog
    GetCommitsResponse response = catalogAdapter.getCommits(0, endVersionForQuery);
    long catalogVersion = getCatalogVersion(response.getLatestTableVersion());

    // Validate version boundaries
    startVersionOpt.ifPresent(v -> validateVersionExists(v, catalogVersion));
    endVersionOpt.ifPresent(v -> validateVersionExists(v, catalogVersion));

    // Convert to Kernel ParsedLogData
    List<ParsedLogData> logData = convertToKernelLogData(response.getCommits());

    // Build commit range using TableManager
    CommitRangeBuilder builder = TableManager.loadCommitRange(tablePath);

    if (startVersionOpt.isPresent()) {
      builder = builder.withStartBoundary(
          CommitRangeBuilder.CommitBoundary.atVersion(startVersionOpt.get()));
    }
    if (startTimestampOpt.isPresent()) {
      Snapshot latestSnapshot = loadLatestSnapshot();
      builder = builder.withStartBoundary(
          CommitRangeBuilder.CommitBoundary.atTimestamp(startTimestampOpt.get(), latestSnapshot));
    }
    if (endVersionOpt.isPresent()) {
      builder = builder.withEndBoundary(
          CommitRangeBuilder.CommitBoundary.atVersion(endVersionOpt.get()));
    }
    if (endTimestampOpt.isPresent()) {
      Snapshot latestSnapshot = loadLatestSnapshot();
      builder = builder.withEndBoundary(
          CommitRangeBuilder.CommitBoundary.atTimestamp(endTimestampOpt.get(), latestSnapshot));
    }

    return builder.withLogData(logData).build(engine);
  }

  /**
   * Converts catalog commits to Kernel's ParsedLogData format.
   */
  private List<ParsedLogData> convertToKernelLogData(List<Commit> commits) {
    return commits.stream()
        .sorted(Comparator.comparingLong(Commit::getVersion))
        .map(commit -> ParsedCatalogCommitData.forFileStatus(
            hadoopFileStatusToKernelFileStatus(commit.getFileStatus())))
        .collect(Collectors.toList());
  }

  /**
   * Converts Hadoop FileStatus to Kernel FileStatus.
   */
  private static io.delta.kernel.utils.FileStatus hadoopFileStatusToKernelFileStatus(
      org.apache.hadoop.fs.FileStatus hadoopFS) {
    return io.delta.kernel.utils.FileStatus.of(
        hadoopFS.getPath().toString(), hadoopFS.getLen(), hadoopFS.getModificationTime());
  }

  /**
   * Gets the true catalog version, handling the -1 case for newly created tables.
   */
  private long getCatalogVersion(long rawVersion) {
    // UC returns -1 when only 0.json exists but hasn't been registered with UC
    return rawVersion == -1 ? 0 : rawVersion;
  }

  /**
   * Validates that the requested version exists.
   */
  private void validateVersionExists(long version, long maxVersion) {
    if (version > maxVersion) {
      throw new IllegalArgumentException(
          String.format(
              "[%s] Cannot load version %d as the latest version ratified by catalog is %d",
              tableId, version, maxVersion));
    }
  }

  private String getVersionOrTimestampString(Optional<Long> versionOpt, Optional<Long> timestampOpt) {
    if (versionOpt.isPresent()) {
      return "version=" + versionOpt.get();
    } else if (timestampOpt.isPresent()) {
      return "timestamp=" + timestampOpt.get();
    } else {
      return "latest";
    }
  }
}
