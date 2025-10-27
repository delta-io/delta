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

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;

/** Implementation of SnapshotManager for managing Delta snapshots for Path-based Table. */
@Experimental
public class PathBasedSnapshotManager implements SnapshotManager {

  private final String tablePath;
  private final AtomicReference<Snapshot> snapshotAtomicReference;
  private final Engine kernelEngine;

  public PathBasedSnapshotManager(String tablePath, Configuration hadoopConf) {
    this.tablePath = tablePath;
    this.snapshotAtomicReference = new AtomicReference<>();
    this.kernelEngine = DefaultEngine.create(hadoopConf);
  }

  /**
   * Returns the cached snapshot without guaranteeing its freshness.
   *
   * @return the cached snapshot, or a newly loaded snapshot if none exists
   */
  @Override
  public Snapshot unsafeVolatileSnapshot() {
    Snapshot unsafeVolatileSnapshot = snapshotAtomicReference.get();
    if (unsafeVolatileSnapshot == null) {
      return loadLatestSnapshot();
    }
    return unsafeVolatileSnapshot;
  }

  /**
   * Loads and caches the latest snapshot of the Delta table.
   *
   * @return the newly loaded snapshot
   */
  @Override
  public Snapshot loadLatestSnapshot() {
    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
    snapshotAtomicReference.set(snapshot);
    return snapshot;
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
   * @param timeStamp the timestamp to query
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
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timeStamp.getTime(),
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
  public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
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
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion));

    if (endVersion.isPresent()) {
      builder =
          builder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(endVersion.get()));
    }

    return builder.build(engine);
  }
}
