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

package org.apache.spark.sql.delta.catalog;

import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark catalog plugin for Delta Lake tables that implements the Spark DataSource V2 Catalog API.
 *
 * To use this catalog, configure it in your Spark session:
 * <pre>{@code
 * // Scala example
 * val spark = SparkSession
 *   .builder()
 *   .appName("...")
 *   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 *   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *   .getOrCreate()
 *
 *
 * // Python example
 * spark = SparkSession \
 *   .builder \
 *   .appName("...") \
 *   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
 *   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
 *   .getOrCreate()
 * }</pre>
 *
 * <h2>Architecture and Delegation Logic</h2>
 *
 * This class sits in the delta-spark (unified) module and provides a single entry point for
 * Delta Lake catalog operations.
 *
 * <p>The unified module can access both implementations:</p>
 * <ul>
 *   <li>V1 connector: {@link DeltaTableV2} - Legacy connector using DeltaLog, full read/write support</li>
 *   <li>V2 connector: {@link SparkTable} - sparkV2 connector, read-only support</li>
 * </ul>
 *
 * <p>See {@link DeltaV2Mode} for V1 vs V2 connector definitions and enable mode configuration.</p>
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  /** Engine info string used in the Delta log commit when the kernel path is used. */
  static final String ENGINE_INFO = "kernel-spark-dsv2";

  /**
   * Creates a Delta table using the kernel-based commit path when STRICT or AUTO mode is enabled.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   *
   * <ul>
   *   <li>STRICT/AUTO + Delta provider: Commits version 0 via Delta Kernel, then finalizes the
   *       catalog entry via the delegate catalog.
   *   <li>Otherwise: Delegates to the V1 path in {@link AbstractDeltaCatalog}.
   * </ul>
   *
   * @param ident The identifier of the table to create.
   * @param schema The schema for the new table.
   * @param partitions Partition transforms for the table.
   * @param properties Table properties including location, provider, etc.
   * @return The created Table (SparkTable in STRICT mode, V1-created Table otherwise).
   */
  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    DeltaV2Mode mode = new DeltaV2Mode(spark().sessionState().conf());
    if (!shouldUseKernelCreatePath(mode, ident, properties)) {
      return super.createTable(ident, schema, partitions, properties);
    }

    // Resolve table location:
    // - UC tables: location is always in properties (user-specified or UC-assigned)
    // - Path-based tables: ident.name() IS the path (e.g., delta.`/tmp/table`)
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location == null) {
      location = properties.get("location");
    }
    if (location == null && isPathIdentifier(ident)) {
      location = ident.name();
    }

    // Filter out DSv2-specific keys that are not Delta table properties. This mirrors the
    // filtering in AbstractDeltaCatalog.createDeltaTable (lines 117-126).
    Map<String, String> tableProperties = filterDsv2Properties(properties);

    // Convert Spark schema to kernel schema
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);

    // Build Hadoop config with UC-vended cloud storage credentials from properties.
    // UCSingleCatalog injects temporary credentials (fs.s3a.access.key, etc.) into
    // properties before calling delegate.createTable().
    Map<String, String> fsOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (DeltaTableUtils.validDeltaTableHadoopPrefixes()
          .exists(prefix -> entry.getKey().startsWith(prefix))) {
        fsOptions.put(entry.getKey(), entry.getValue());
      }
    }
    Configuration hadoopConf =
        spark().sessionState().newHadoopConfWithOptions(ScalaUtils.toScalaMap(fsOptions));
    Engine engine = DefaultEngine.create(hadoopConf);

    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.createForNewTable(location, engine, properties, name(), spark());

    List<Column> partitionColumns = extractPartitionColumns(partitions);
    Optional<DataLayoutSpec> dataLayoutSpec =
        partitionColumns.isEmpty()
            ? Optional.empty()
            : Optional.of(DataLayoutSpec.partitioned(partitionColumns));

    Transaction txn =
        snapshotManager.buildCreateTableTransaction(
            kernelSchema, tableProperties, dataLayoutSpec, ENGINE_INFO);
    txn.commit(engine, CloseableIterable.emptyIterable());

    // Register the table with the catalog (UC delegate) and return a SparkTable.
    // For catalog-registered tables: register via delegate, then loadTable() to get a SparkTable
    // backed by the kernel snapshot. This exercises the full DSv2 path including
    // SnapshotManagerFactory (UCManagedTableSnapshotManager for managed, DeltaSnapshotManager
    // for external). If loadSnapshot() fails due to the createImpl/commitToUC gap (UC doesn't
    // know about version 0), this is where it surfaces.
    // For path-based tables (delta.`/path`): no catalog registration needed.
    if (!isPathIdentifier(ident)) {
      // Strip credentials from properties before passing to the UC delegate. The delegate needs
      // catalog coordination keys (location, provider, is_managed_location) but NOT transient
      // credentials (fs.s3a.init.access.key, fs.unitycatalog.auth.token, etc.).
      // In the V1 path, CatalogTable.properties never contains fs.* keys because
      // CreateDeltaTableCommand filters them via DeltaTableUtils.validDeltaTableHadoopPrefixes.
      createCatalogTable(ident, schema, partitions, filterCredentialProperties(properties));
      return loadTable(ident);
    }
    return loadPathTable(ident);
  }

  /**
   * Loads a Delta table that is registered in the catalog.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link SparkTable} (V2 connector)</li>
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)</li>
   * </ul>
   *
   * @param ident The identifier of the table in the catalog.
   * @param catalogTable The catalog table metadata containing table properties and location.
   * @return Table instance (SparkTable for V2, DeltaTableV2 for V1).
   */
  @Override
  public Table loadCatalogTable(Identifier ident, CatalogTable catalogTable) {
    return loadTableInternal(
        () -> new SparkTable(ident, catalogTable, new HashMap<>()),
        () -> super.loadCatalogTable(ident, catalogTable));
  }

  /**
   * Loads a Delta table directly from a path.
   * This is used for path-based table access where the identifier name is the table path.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link SparkTable} (V2 connector)</li>
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)</li>
   * </ul>
   *
   * @param ident The identifier whose name contains the path to the Delta table.
   * @return Table instance (SparkTable for V2, DeltaTableV2 for V1).
   */
  @Override
  public Table loadPathTable(Identifier ident) {
    return loadTableInternal(
        // delta.`/path/to/table`, where ident.name() is `/path/to/table`
        () -> new SparkTable(ident, ident.name()),
        () -> super.loadPathTable(ident));
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns true when all conditions for the kernel-based create path are met:
   *
   * <ol>
   *   <li>STRICT or AUTO mode is active
   *   <li>The provider is Delta
   *   <li>Either the delegate catalog is Unity Catalog (for catalog-registered tables), or the
   *       identifier is a path-based table (which skips catalog registration entirely).
   *       Spark's V2SessionCatalog cannot register Delta tables through its V2 createTable API,
   *       so non-path, non-UC tables must use the V1 path.
   * </ol>
   */
  private boolean shouldUseKernelCreatePath(
      DeltaV2Mode mode, Identifier ident, Map<String, String> properties) {
    return isCreatePathEnabledInMode(mode)
        && DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))
        && (isUnityCatalog() || isPathIdentifier(ident));
  }

  private static boolean isCreatePathEnabledInMode(DeltaV2Mode mode) {
    return mode.shouldCatalogReturnV2Tables() || "AUTO".equalsIgnoreCase(mode.getMode());
  }

  /**
   * Filters out DSv2-specific keys and filesystem options that are not actual Delta table
   * properties. This mirrors the filtering in {@code AbstractDeltaCatalog.createDeltaTable}.
   *
   * <p>UCSingleCatalog injects two families of transient keys into properties (all keys
   * originate from {@code UCHadoopConf} constants which carry the {@code fs.} prefix):
   * <ul>
   *   <li>{@code fs.*} — Hadoop filesystem credentials (e.g., {@code fs.s3a.init.access.key},
   *       {@code fs.unitycatalog.auth.token})
   *   <li>{@code option.fs.*} — same keys with Spark's {@code option.} prefix
   * </ul>
   * Both are caught by {@link DeltaTableUtils#validDeltaTableHadoopPrefixes} ({@code "fs."},
   * {@code "dfs."}) after stripping the {@code option.} prefix.
   */
  private static Map<String, String> filterDsv2Properties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.remove(TableCatalog.PROP_LOCATION);
    filtered.remove(TableCatalog.PROP_PROVIDER);
    filtered.remove(TableCatalog.PROP_COMMENT);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_EXTERNAL);
    filtered.remove("path");
    filtered.remove("option.path");
    // is_managed_location is a DSv2 coordination property, not a Delta table property
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    // Remove legacy ucTableId key; the new key (io.unitycatalog.tableId) is set by
    // UCCatalogManagedClient for managed tables.
    filtered.remove("ucTableId");
    // Remove filesystem options (UC-vended credentials) and their "option." prefixed variants
    // — these belong in the Hadoop config, not in the Delta table metadata persisted in the
    // commit log. All credential keys from UCSingleCatalog carry the "fs." prefix
    // (e.g., fs.s3a.init.access.key, fs.unitycatalog.auth.token) per UCHadoopConf constants.
    filtered.entrySet().removeIf(entry -> {
      String key = entry.getKey();
      String effectiveKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
      return DeltaTableUtils.validDeltaTableHadoopPrefixes()
          .exists(prefix -> effectiveKey.startsWith(prefix));
    });
    return filtered;
  }

  /**
   * Strips filesystem credential keys ({@code fs.*}, {@code dfs.*}, {@code option.fs.*},
   * {@code option.dfs.*}) from properties while keeping DSv2 catalog coordination keys
   * ({@code location}, {@code provider}, {@code is_managed_location}, etc.).
   *
   * <p>Used when passing properties to the UC delegate for catalog registration. The delegate
   * needs catalog keys to register the table, but must NOT receive transient credentials
   * (e.g., {@code fs.s3a.init.access.key}, {@code fs.unitycatalog.auth.token}).
   */
  private static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.entrySet().removeIf(entry -> {
      String key = entry.getKey();
      String effectiveKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
      return DeltaTableUtils.validDeltaTableHadoopPrefixes()
          .exists(prefix -> effectiveKey.startsWith(prefix));
    });
    return filtered;
  }

  /**
   * Extracts partition column names from identity transforms. Only identity transforms are
   * supported for partitioning (e.g., {@code PARTITIONED BY (col)}).
   */
  private static List<Column> extractPartitionColumns(Transform[] partitions) {
    List<Column> columns = new ArrayList<>();
    for (Transform partition : partitions) {
      if (partition.references().length > 0) {
        columns.add(new Column(partition.references()[0].describe()));
      }
    }
    return columns;
  }

  /**
   * Loads a table based on the {@link DeltaV2Mode} SQL configuration.
   *
   * <p>This method checks the configuration and delegates to the appropriate supplier:
   * <ul>
   *   <li>STRICT mode: Uses V2 connector (sparkV2 SparkTable) - for testing V2 capabilities</li>
   *   <li>NONE mode (default): Uses V1 connector (DeltaTableV2) - production default with full features</li>
   * </ul>
   *
   * <p>See {@link DeltaV2Mode} for detailed V1 vs V2 connector definitions.
   *
   * @param v2ConnectorSupplier Supplier for V2 connector (sparkV2 SparkTable) - used in STRICT mode
   * @param v1ConnectorSupplier Supplier for V1 connector (DeltaTableV2) - used in NONE mode (default)
   * @return Table instance from the selected supplier
   */
  private Table loadTableInternal(
      Supplier<Table> v2ConnectorSupplier,
      Supplier<Table> v1ConnectorSupplier) {
    DeltaV2Mode connectorMode = new DeltaV2Mode(spark().sessionState().conf());
    if (connectorMode.shouldCatalogReturnV2Tables()) {
      return v2ConnectorSupplier.get();
    } else {
      return v1ConnectorSupplier.get();
    }
  }
}
