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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.defaults.catalog.CatalogWithManagedCommits;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;

/**
 * Factory for creating the appropriate {@link DeltaSnapshotManager} based on catalog capabilities.
 */
@Experimental
public final class DeltaSnapshotManagerFactory {

  private DeltaSnapshotManagerFactory() {}

  /**
   * Create a snapshot manager for a table path. If the provided catalog implements
   * {@link CatalogWithManagedCommits} and yields a table id from the provided properties, a
   * {@link CatalogManagedSnapshotManager} is returned; otherwise falls back to path-based.
   */
  public static DeltaSnapshotManager create(
      String tablePath,
      Map<String, String> tableProperties,
      Optional<CatalogWithManagedCommits> catalogOpt,
      Configuration hadoopConf) {
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(tableProperties, "tableProperties is null");
    requireNonNull(catalogOpt, "catalogOpt is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    if (catalogOpt.isPresent()) {
      CatalogWithManagedCommits catalog = catalogOpt.get();
      Optional<String> tableId = catalog.extractTableId(tableProperties);
      if (tableId.isPresent()) {
        return new CatalogManagedSnapshotManager(catalog, tableId.get(), tablePath, hadoopConf);
      }
    }

    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }
}
