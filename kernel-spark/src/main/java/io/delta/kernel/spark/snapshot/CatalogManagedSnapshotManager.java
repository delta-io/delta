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
import io.delta.kernel.engine.Engine;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wireframe implementation of DeltaSnapshotManager for catalog-managed tables (e.g., UC).
 *
 * <p>All operations are intentionally stubbed in this PR. Functionality will be implemented in a
 * follow-up PR.
 */
@Experimental
public class CatalogManagedSnapshotManager implements DeltaSnapshotManager, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CatalogManagedSnapshotManager.class);

  private final ManagedCatalogAdapter catalogAdapter;
  private final String tableId;
  private final String tablePath;

  public CatalogManagedSnapshotManager(
      ManagedCatalogAdapter catalogAdapter,
      String tableId,
      String tablePath,
      Configuration hadoopConf) {
    this.catalogAdapter = requireNonNull(catalogAdapter, "catalogAdapter is null");
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    logger.info(
        "Created CatalogManagedSnapshotManager for table {} at path {}", tableId, tablePath);
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    throw new UnsupportedOperationException("loadLatestSnapshot not implemented yet");
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    checkArgument(version >= 0, "version must be non-negative");
    throw new UnsupportedOperationException("loadSnapshotAt not implemented yet");
  }

  @Override
  public io.delta.kernel.internal.DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    throw new UnsupportedOperationException("getActiveCommitAtTime not implemented yet");
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange) {
    checkArgument(version >= 0, "version must be non-negative");
    throw new UnsupportedOperationException("checkVersionExists not implemented yet");
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    requireNonNull(engine, "engine is null");
    throw new UnsupportedOperationException("getTableChanges not implemented yet");
  }

  @Override
  public void close() {
    // no-op in wireframe; adapter may implement close in the future
    try {
      catalogAdapter.close();
      logger.info("Closed CatalogManagedSnapshotManager for table {}", tableId);
    } catch (Exception e) {
      logger.warn("Error closing catalog-managed client for table {}", tableId, e);
    }
  }
}
