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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import java.util.Optional;

/**
 * Snapshot manager for Unity Catalog managed tables.
 *
 * <p>Used for tables with the catalog-managed commit feature enabled. Unity Catalog serves as the
 * source of truth for the table's commit history.
 */
public class UCManagedTableSnapshotManager implements DeltaSnapshotManager {

  /** Creates a new UCManagedTableSnapshotManager. */
  public UCManagedTableSnapshotManager() {}

  @Override
  public Snapshot loadLatestSnapshot() {
    throw new UnsupportedOperationException(
        "UCManagedTableSnapshotManager.loadLatestSnapshot is not yet implemented");
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    throw new UnsupportedOperationException(
        "UCManagedTableSnapshotManager.loadSnapshotAt is not yet implemented");
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    throw new UnsupportedOperationException(
        "UCManagedTableSnapshotManager.getActiveCommitAtTime is not yet implemented");
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange) {
    throw new UnsupportedOperationException(
        "UCManagedTableSnapshotManager.checkVersionExists is not yet implemented");
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    throw new UnsupportedOperationException(
        "UCManagedTableSnapshotManager.getTableChanges is not yet implemented");
  }
}
