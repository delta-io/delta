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

import io.delta.storage.commit.GetCommitsResponse;
import java.util.Optional;

/**
 * Adapter interface for catalog-managed Delta tables.
 *
 * <p>This is a thin, protocol-aligned interface that adapters implement to fetch commit metadata
 * from a catalog's commit coordinator API. Adapters are responsible only for communication with the
 * catalog - they don't know about Delta snapshots or Kernel internals.
 *
 * <p>The {@link CatalogManagedSnapshotManager} uses this interface to retrieve commits and then
 * builds Delta snapshots and commit ranges using Kernel's TableManager APIs.
 *
 * <p>Implementations should be catalog-specific (e.g., UnityCatalogAdapter, GlueCatalogAdapter) but
 * share this common interface so the snapshot manager can work with any catalog.
 */
public interface ManagedCatalogAdapter extends AutoCloseable {

  /**
   * Returns the unique identifier for this table in the catalog.
   *
   * @return the catalog-assigned table identifier
   */
  String getTableId();

  /**
   * Returns the storage path for this table.
   *
   * @return the filesystem path to the Delta table root
   */
  String getTablePath();

  /**
   * Retrieves commits from the catalog's commit coordinator.
   *
   * <p>This is the primary method that adapters must implement. It calls the catalog's API to get
   * the list of ratified commits within the specified version range.
   *
   * @param startVersion the starting version (inclusive), typically 0 for initial load
   * @param endVersion optional ending version (inclusive); if empty, returns up to latest
   * @return response containing the list of commits and the latest ratified table version
   */
  GetCommitsResponse getCommits(long startVersion, Optional<Long> endVersion);

  /**
   * Returns the latest ratified table version from the catalog.
   *
   * <p>For catalog-managed tables, this is the highest version that has been successfully ratified
   * by the catalog coordinator. Returns -1 if the catalog hasn't registered any commits yet (which
   * can happen when version 0 exists but hasn't been ratified).
   *
   * <p>Default implementation calls {@link #getCommits} with no end version and extracts the latest
   * version from the response. Implementations may override for efficiency if the catalog provides
   * a dedicated API.
   *
   * @return the latest version ratified by the catalog, or -1 if none registered
   */
  default long getLatestRatifiedVersion() {
    return getCommits(0, Optional.empty()).getLatestTableVersion();
  }

  @Override
  void close();
}
