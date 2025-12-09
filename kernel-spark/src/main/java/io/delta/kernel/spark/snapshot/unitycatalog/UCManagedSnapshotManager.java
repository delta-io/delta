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
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

/**
 * Snapshot manager for Unity Catalog managed tables.
 *
 * <p>This implementation delegates to {@link UCCatalogManagedClient} for all snapshot and commit
 * operations, leveraging UC's ratified commits as the source of truth.
 */
public class UCManagedSnapshotManager implements DeltaSnapshotManager {

  private final UCCatalogManagedClient ucCatalogManagedClient;
  private final String tableId;
  private final String tablePath;
  private final Engine engine;

  /**
   * Creates a new UCManagedSnapshotManager.
   *
   * @param ucCatalogManagedClient the UC client for catalog-managed operations
   * @param tableInfo the UC table information (tableId, tablePath, etc.)
   * @param hadoopConf Hadoop configuration for filesystem operations
   */
  public UCManagedSnapshotManager(
      UCCatalogManagedClient ucCatalogManagedClient,
      UCTableInfo tableInfo,
      Configuration hadoopConf) {
    this.ucCatalogManagedClient =
        requireNonNull(ucCatalogManagedClient, "ucCatalogManagedClient is null");
    requireNonNull(tableInfo, "tableInfo is null");
    this.tableId = tableInfo.getTableId();
    this.tablePath = tableInfo.getTablePath();
    this.engine = DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null"));
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    // TODO: Implement in snapshotmanager-impl stack
    throw new UnsupportedOperationException(
        "UCManagedSnapshotManager.loadLatestSnapshot not yet implemented");
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    // TODO: Implement in snapshotmanager-impl stack
    throw new UnsupportedOperationException(
        "UCManagedSnapshotManager.loadSnapshotAt not yet implemented");
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    // TODO: Implement in snapshotmanager-impl stack
    throw new UnsupportedOperationException(
        "UCManagedSnapshotManager.getActiveCommitAtTime not yet implemented");
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    // TODO: Implement in snapshotmanager-impl stack
    throw new UnsupportedOperationException(
        "UCManagedSnapshotManager.checkVersionExists not yet implemented");
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    // TODO: Implement in snapshotmanager-impl stack
    throw new UnsupportedOperationException(
        "UCManagedSnapshotManager.getTableChanges not yet implemented");
  }
}
