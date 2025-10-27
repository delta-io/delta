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
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import java.sql.Timestamp;
import java.util.Optional;
import org.apache.spark.annotation.Experimental;

/**
 * Interface for managing Delta table snapshots.
 *
 * <p>This interface provides methods for loading, caching, and querying Delta table snapshots. It
 * supports both current snapshot access and historical snapshot queries based on version or
 * timestamp.
 *
 * <p>Implementations of this interface are responsible for managing snapshot lifecycle, including
 * loading snapshots from storage and maintaining any necessary caching.
 */
@Experimental
public interface SnapshotManager {

  /**
   * Returns a cached snapshot without guaranteeing its freshness.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Returns the currently cached snapshot if one exists
   *   <li>If no cached snapshot exists, loads and returns the latest snapshot (equivalent to
   *       calling {@link #loadLatestSnapshot()})
   *   <li>The returned snapshot may be stale if the table has been modified since it was loaded
   * </ul>
   *
   * @return the cached snapshot, or a newly loaded latest snapshot if no cached snapshot exists
   */
  Snapshot unsafeVolatileSnapshot();

  /**
   * Loads and returns the latest snapshot of the Delta table.
   *
   * @return the latest snapshot of the Delta table
   */
  Snapshot loadLatestSnapshot();

  /**
   * Loads and returns a snapshot at a specific version of the Delta table.
   *
   * @param version the version number to load (must be >= 0)
   * @return the snapshot at the specified version
   * @throws io.delta.kernel.exceptions.KernelException if the version cannot be loaded
   */
  Snapshot loadSnapshotAt(long version);

  /**
   * Finds and returns the commit that was active at a specific timestamp.
   *
   * @param timeStamp the timestamp to query
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *     commits; if false, throws an exception
   * @param mustBeRecreatable if true, only considers commits that can be fully recreated from
   *     available log files; if false, considers all commits
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *     all commits; if false, throws an exception
   * @return the commit that was active at the specified timestamp
   * @throws io.delta.kernel.exceptions.KernelException if no suitable commit is found based on the
   *     provided flags
   */
  DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit);

  /**
   * Checks if a specific version of the Delta table exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *     available log files; if false, only requires that the version's log file exists
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *     throwing an exception; if false, throws exception for out-of-range versions
   * @throws VersionNotFoundException if the version is not available or does not meet the specified
   *     criteria
   */
  void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws VersionNotFoundException;

  /**
   * Gets the metadata of the current cached snapshot.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Returns the metadata from the snapshot returned by {@link #unsafeVolatileSnapshot()}
   *   <li>This is a convenience method to avoid requiring callers to cast to SnapshotImpl
   * </ul>
   *
   * <p><b>Use Case:</b> Use this method when you need table metadata without caring about the
   * specific snapshot version.
   *
   * @return the metadata of the current snapshot
   */
  Metadata getMetadata();

  /**
   * Gets a range of table changes (commits) between start and end versions.
   *
   * <p><b>Expected Behavior:</b>
   *
   * <ul>
   *   <li>Returns a {@link io.delta.kernel.CommitRange} representing all commits from the start
   *       version (inclusive) to the end version (inclusive if provided)
   *   <li>If endVersion is not provided, the range extends to the latest available version
   *   <li>The returned CommitRange can be used to iterate through actions in the version range
   *   <li>This is typically used for streaming and incremental processing scenarios
   * </ul>
   *
   * <p><b>Use Case:</b> Use this method for streaming queries, incremental processing, or CDC
   * scenarios where you need to process changes between versions.
   *
   * @param engine the engine implementation for executing operations
   * @param startVersion the starting version (inclusive)
   * @param endVersion optional ending version (inclusive); if not provided, extends to latest
   * @return a CommitRange representing the specified range of commits
   */
  CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion);
}
