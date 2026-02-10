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

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/**
 * A Spark catalog plugin for Delta Lake tables that implements the Spark DataSource V2 Catalog API.
 *
 * <p>To use this catalog, configure it in your Spark session:
 *
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
 * <p>This class sits in the delta-spark (unified) module and provides a single entry point for
 * Delta Lake catalog operations.
 *
 * <p>The unified module can access both implementations:
 *
 * <ul>
 *   <li>V1 connector: {@link DeltaTableV2} - Legacy connector using DeltaLog, full read/write
 *       support
 *   <li>V2 connector: {@link SparkTable} - sparkV2 connector, read-only support
 * </ul>
 *
 * <p>See {@link DeltaV2Mode} for V1 vs V2 connector definitions and enable mode configuration.
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  /** Engine info string used in the Delta log commit when the kernel path is used. */
  static final String ENGINE_INFO = "kernel-spark-dsv2";

  /**
   * Creates a Delta table using the kernel-based commit path when STRICT mode is enabled.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   *
   * <ul>
   *   <li>STRICT + Delta provider: Commits version 0 via Delta Kernel, then finalizes the catalog
   *       entry via the delegate catalog.
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
    if (!isStrictDeltaCreate(properties)) {
      return super.createTable(ident, schema, partitions, properties);
    }

    // --- STRICT mode kernel-based immediate create path ---

    // Determine table location from properties, falling back to the default warehouse path.
    String location = resolveTableLocation(ident, properties);

    // Filter out DSv2-specific keys that are not Delta table properties. This mirrors the
    // filtering in AbstractDeltaCatalog.createDeltaTable (lines 117-126).
    Map<String, String> tableProperties = filterDsv2Properties(properties);

    // Convert Spark schema to kernel schema
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);

    // Build kernel create-table transaction
    Configuration hadoopConf = spark().sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(location, kernelSchema, ENGINE_INFO);

    if (!tableProperties.isEmpty()) {
      builder = builder.withTableProperties(tableProperties);
    }

    // Extract partition columns from identity transforms
    List<Column> partitionColumns = extractPartitionColumns(partitions);
    if (!partitionColumns.isEmpty()) {
      builder = builder.withDataLayoutSpec(DataLayoutSpec.partitioned(partitionColumns));
    }

    // Commit version 0 of the Delta table
    Transaction txn = builder.build(engine);
    txn.commit(engine, CloseableIterable.emptyIterable());

    // Finalize the catalog entry via the delegate catalog (e.g., Unity Catalog).
    // Without this step, UC-managed tables would remain in a staged state and
    // TablesApi.getTable() would 404.
    // Skip for path-based tables (delta.`/path/to/table`) which are not catalog-managed.
    if (!isPathIdentifier(ident)) {
      createCatalogTable(ident, schema, partitions, properties);
    }

    // Load and return the table; in STRICT mode loadTable returns SparkTable.
    return loadTable(ident);
  }

  /**
   * Loads a Delta table that is registered in the catalog.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   *
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link SparkTable} (V2 connector)
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)
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
   * Loads a Delta table directly from a path. This is used for path-based table access where the
   * identifier name is the table path.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   *
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link SparkTable} (V2 connector)
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)
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
   *   <li>STRICT mode is active
   *   <li>The provider is Delta
   *   <li>The delegate catalog is Unity Catalog (the kernel path requires a delegate that supports
   *       direct table registration; Spark's V2SessionCatalog does not)
   * </ol>
   */
  private boolean isStrictDeltaCreate(Map<String, String> properties) {
    DeltaV2Mode mode = new DeltaV2Mode(spark().sessionState().conf());
    return mode.shouldCatalogReturnV2Tables()
        && DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))
        && isUnityCatalog();
  }

  /**
   * Resolves the table location from properties. Falls back to the session catalog's default
   * warehouse path when no explicit location is provided (managed table case).
   */
  private String resolveTableLocation(Identifier ident, Map<String, String> properties) {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location == null) {
      location = properties.get("location");
    }
    if (location != null) {
      return location;
    }
    // Managed table: compute the default warehouse location
    String dbName =
        ident.namespace().length > 0 ? ident.namespace()[ident.namespace().length - 1] : null;
    Option<String> dbOption = dbName != null ? Option.apply(dbName) : Option.<String>empty();
    TableIdentifier tableId = new TableIdentifier(ident.name(), dbOption);
    return spark().sessionState().catalog().defaultTablePath(tableId).toString();
  }

  /**
   * Filters out DSv2-specific keys that are not actual Delta table properties. This mirrors the
   * filtering in {@code AbstractDeltaCatalog.createDeltaTable}.
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
   *
   * <ul>
   *   <li>STRICT mode: Uses V2 connector (sparkV2 SparkTable) - for testing V2 capabilities
   *   <li>NONE mode (default): Uses V1 connector (DeltaTableV2) - production default with full
   *       features
   * </ul>
   *
   * <p>See {@link DeltaV2Mode} for detailed V1 vs V2 connector definitions.
   *
   * @param v2ConnectorSupplier Supplier for V2 connector (sparkV2 SparkTable) - used in STRICT mode
   * @param v1ConnectorSupplier Supplier for V1 connector (DeltaTableV2) - used in NONE mode
   *     (default)
   * @return Table instance from the selected supplier
   */
  private Table loadTableInternal(
      Supplier<Table> v2ConnectorSupplier, Supplier<Table> v1ConnectorSupplier) {
    DeltaV2Mode connectorMode = new DeltaV2Mode(spark().sessionState().conf());
    if (connectorMode.shouldCatalogReturnV2Tables()) {
      return v2ConnectorSupplier.get();
    } else {
      return v1ConnectorSupplier.get();
    }
  }

}
