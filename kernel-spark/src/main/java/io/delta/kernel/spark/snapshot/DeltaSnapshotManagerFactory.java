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
import java.util.ServiceLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

/**
 * Factory for creating {@link DeltaSnapshotManager} instances.
 *
 * <p>Provides convenience methods mirroring PR #5617: {@link #fromPath} and {@link
 * #fromCatalogTable}. Internally leverages {@link CatalogWithManagedCommits} implementations
 * discovered via {@link ServiceLoader} to keep the kernel-side wiring catalog-agnostic.
 */
@Experimental
public final class DeltaSnapshotManagerFactory {

  private DeltaSnapshotManagerFactory() {}

  /** Create a path-based manager for filesystem tables. */
  public static DeltaSnapshotManager fromPath(String tablePath, Configuration hadoopConf) {
    requireNonNull(tablePath, "tablePath is null");
    requireNonNull(hadoopConf, "hadoopConf is null");
    return new PathBasedSnapshotManager(tablePath, hadoopConf);
  }

  /**
   * Create a manager for a catalog table. If a {@link CatalogWithManagedCommits} is available and
   * returns a table ID for the given properties, a {@link CatalogManagedSnapshotManager} is used;
   * otherwise fall back to {@link PathBasedSnapshotManager}.
   */
  public static DeltaSnapshotManager fromCatalogTable(
      CatalogTable catalogTable, SparkSession spark, Configuration hadoopConf) {
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(spark, "spark is null");
    requireNonNull(hadoopConf, "hadoopConf is null");

    Map<String, String> props =
        scala.collection.JavaConverters.mapAsJavaMap(catalogTable.properties());
    String tablePath = new Path(catalogTable.location()).toString();

    Optional<ManagedCatalogResolution> resolution = resolveManagedCatalog(props, spark);

    if (resolution.isPresent()) {
      ManagedCatalogResolution r = resolution.get();
      return new CatalogManagedSnapshotManager(r.catalog, r.tableId, tablePath, hadoopConf);
    }

    return fromPath(tablePath, hadoopConf);
  }

  /**
   * Legacy create method retained for callers that already have a {@link CatalogWithManagedCommits}
   * instance.
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

  private static Optional<ManagedCatalogResolution> resolveManagedCatalog(
      Map<String, String> props, SparkSession spark) {
    // Discover implementations via ServiceLoader to avoid Spark-specific dependencies
    ClassLoader cl = spark.sessionState().sharedState().jarClassLoader();
    if (cl == null) {
      cl = Thread.currentThread().getContextClassLoader();
    }
    ServiceLoader<CatalogWithManagedCommits> loader =
        ServiceLoader.load(CatalogWithManagedCommits.class, cl);
    for (CatalogWithManagedCommits catalog : loader) {
      try {
        Optional<String> tableId = catalog.extractTableId(props);
        if (tableId.isPresent()) {
          return Optional.of(new ManagedCatalogResolution(catalog, tableId.get()));
        }
      } catch (Exception ignored) {
        // Skip implementations that cannot parse these properties
      }
    }
    return Optional.empty();
  }

  private static final class ManagedCatalogResolution {
    private final CatalogWithManagedCommits catalog;
    private final String tableId;

    private ManagedCatalogResolution(CatalogWithManagedCommits catalog, String tableId) {
      this.catalog = catalog;
      this.tableId = tableId;
    }
  }
}
