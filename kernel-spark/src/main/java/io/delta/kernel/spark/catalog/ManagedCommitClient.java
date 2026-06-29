/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.kernel.spark.catalog;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import java.util.Optional;

/**
 * Generic interface for catalog-managed commit coordination.
 *
 * <p>This interface abstracts catalog-specific implementations (Unity Catalog, AWS Glue, Apache
 * Polaris) and provides a uniform API for loading snapshots and checking versions.
 *
 * <p>Implementations are provided by catalog modules and discovered through Spark's catalog system
 * via the CatalogWithManagedCommits trait.
 */
public interface ManagedCommitClient extends AutoCloseable {

  /**
   * Loads a snapshot of the table.
   *
   * @param engine Delta Kernel engine for reading Parquet/JSON
   * @param tableId catalog-specific table identifier
   * @param tablePath filesystem path to table data
   * @param version specific version to load (empty = latest)
   * @param timestampMillis timestamp for time travel (empty = not used)
   * @return snapshot at specified version/timestamp
   */
  Snapshot getSnapshot(
      Engine engine,
      String tableId,
      String tablePath,
      Optional<Long> version,
      Optional<Long> timestampMillis);

  /**
   * Checks if a specific version exists and is accessible.
   *
   * @param tableId catalog-specific table identifier
   * @param version version to check
   * @return true if version exists, false otherwise
   */
  boolean versionExists(String tableId, long version);

  /**
   * Gets the latest version number available.
   *
   * @param tableId catalog-specific table identifier
   * @return latest version number
   */
  long getLatestVersion(String tableId);

  /** Releases catalog client resources. */
  @Override
  void close() throws Exception;
}
