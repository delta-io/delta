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

import io.delta.kernel.spark.unity.UnityCatalogClientFactory;
import io.delta.kernel.spark.utils.CatalogTableUtils;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory responsible for instantiating {@link DeltaSnapshotManager} implementations.
 *
 * <p>The factory centralises the decision of whether a table should use the traditional
 * filesystem-based snapshot manager or a catalog-backed implementation. Today all tables rely on
 * {@link PathBasedSnapshotManager}. Catalog-managed support will be integrated in a subsequent
 * change once the corresponding snapshot manager is implemented.
 */
public final class SnapshotManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManagerFactory.class);

  private SnapshotManagerFactory() {}

  /**
   * Creates an appropriate {@link DeltaSnapshotManager} based on the provided metadata.
   *
   * @param identifier Spark identifier for the table being resolved
   * @param tablePath canonical filesystem path to the table root
   * @param hadoopConf Hadoop configuration pre-populated with user options
   * @param catalogTable optional Spark {@link CatalogTable} descriptor when available
   * @param unityCatalogClient optional Unity Catalog client handle for catalog-managed tables
   * @return a snapshot manager implementation ready to serve table snapshots
   */
  public static DeltaSnapshotManager create(
      Identifier identifier,
      String tablePath,
      Configuration hadoopConf,
      Optional<CatalogTable> catalogTable,
      Optional<UnityCatalogClientFactory.UnityCatalogClient> unityCatalogClient) {

    requireNonNull(identifier, "identifier is null");
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");
    requireNonNull(catalogTable, "catalogTable optional is null");
    requireNonNull(unityCatalogClient, "unityCatalogClient optional is null");

    if (catalogTable.isPresent()
        && CatalogTableUtils.isUnityCatalogManagedTable(catalogTable.get())
        && unityCatalogClient.isPresent()) {
      LOG.debug(
          "Unity Catalog-managed table '{}' detected. Using CatalogManagedSnapshotManager.",
          identifier);
      return new CatalogManagedSnapshotManager(
          tablePath, catalogTable.get(), unityCatalogClient.get(), hadoopConf);
    }

    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  /**
   * Convenience overload for path-based tables without Spark catalog metadata.
   *
   * @param identifier Spark identifier for the table being resolved
   * @param tablePath canonical filesystem path to the table root
   * @param hadoopConf Hadoop configuration pre-populated with user options
   * @return a {@link PathBasedSnapshotManager} instance
   */
  public static DeltaSnapshotManager createForPath(
      Identifier identifier, String tablePath, Configuration hadoopConf) {
    return create(identifier, tablePath, hadoopConf, Optional.empty(), Optional.empty());
  }
}
