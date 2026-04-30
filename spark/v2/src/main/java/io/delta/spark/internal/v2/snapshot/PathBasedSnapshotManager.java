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
package io.delta.spark.internal.v2.snapshot;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;

/** Implementation of DeltaSnapshotManager for managing Delta snapshots for Path-based Table. */
@Experimental
public class PathBasedSnapshotManager implements DeltaSnapshotManager {

  private final String tablePath;
  private final Engine kernelEngine;

  public PathBasedSnapshotManager(String tablePath, Configuration hadoopConf) {
    this(tablePath, DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null")));
  }

  public PathBasedSnapshotManager(String tablePath, Engine kernelEngine) {
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.kernelEngine = requireNonNull(kernelEngine, "kernelEngine is null");
  }

  /**
   * Loads the latest snapshot of the Delta table.
   *
   * @return the newly loaded snapshot
   */
  @Override
  public Snapshot loadLatestSnapshot() {
    return TableManager.loadSnapshot(tablePath).build(kernelEngine);
  }

  /**
   * Loads a specific version of the Delta table.
   *
   * @param version the version to load
   * @return the snapshot at the specified version
   */
  @Override
  public Snapshot loadSnapshotAt(long version) {
    return TableManager.loadSnapshot(tablePath).atVersion(version).build(kernelEngine);
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p>This method searches the Delta table's commit history to find the commit that was active at
   * the specified timestamp.
   *
   * @param timestampMillis the timestamp in milliseconds since epoch (UTC)
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *     commits
   * @param mustBeRecreatable if true, only considers commits that can be recreated (i.e., all
   *     necessary log files are available)
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *     all commits
   * @return the commit that was active at the specified timestamp
   */
  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timestampMillis,
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>());
  }

  /**
   * Checks if a specific version of the Delta table exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *     available log files
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *     throwing an exception
   * @throws VersionNotFoundException if the version is not available
   */
  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/);

    long latest = snapshot.getVersion();
    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    CommitRangeBuilder builder =
        TableManager.loadCommitRange(
            tablePath, CommitRangeBuilder.CommitBoundary.atVersion(startVersion));

    if (endVersion.isPresent()) {
      builder =
          builder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(endVersion.get()));
    }

    return builder.build(engine);
  }
}
