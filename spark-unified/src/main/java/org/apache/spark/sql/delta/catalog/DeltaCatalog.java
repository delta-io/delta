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

import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.ddl.DeltaKernelDDL;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils$;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import scala.Option;

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

  /**
   * Creates a Delta table. Routes to Kernel-based V2 DDL when STRICT mode is on.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>STRICT + Delta provider: Kernel-based metadata-only CREATE TABLE (V2 path)</li>
   *   <li>NONE (default): V1 path via {@code AbstractDeltaCatalog.createTable()}</li>
   * </ul>
   *
   * @param ident The table identifier.
   * @param schema The table schema.
   * @param partitions The partition transforms.
   * @param properties The table properties.
   * @return The created Table instance.
   */
  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      java.util.Map<String, String> properties) {
    DeltaV2Mode mode = new DeltaV2Mode(spark().sessionState().conf());
    if (mode.shouldCatalogReturnV2Tables() && isDeltaProvider(properties)) {
      return createTableV2(ident, schema, partitions, properties);
    }
    return super.createTable(ident, schema, partitions, properties);
  }

  /**
   * V2 DDL path: writes commit 0 via Kernel, registers table name in catalog, returns SparkTable.
   */
  private Table createTableV2(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    String location = resolveLocation(ident, properties);

    // Build Hadoop conf with fs.* properties for credential propagation
    Configuration hadoopConf = buildHadoopConf(properties);
    Engine engine = DefaultEngine.create(hadoopConf);

    // Write commit 0 via Kernel (metadata-only, no data files)
    DeltaKernelDDL.createMetadataOnlyTable(engine, location, schema, partitions, properties);

    // Register table name in catalog (UC or session catalog)
    createCatalogTable(ident, schema, partitions, properties);

    // Return SparkTable directly from the known path.
    // We avoid loadTable(ident) because for UC-managed tables it triggers
    // UCManagedTableSnapshotManager which expects catalog-committed versions.
    return new SparkTable(ident, location);
  }

  /**
   * Resolves the table storage location from the identifier or properties.
   *
   * <p>Priority:
   * <ol>
   *   <li>Path-based identifier (delta.`/path`): use ident.name()</li>
   *   <li>Explicit location property: use properties.get("location")</li>
   *   <li>Default warehouse location: use Spark's defaultTablePath</li>
   * </ol>
   */
  private String resolveLocation(Identifier ident, Map<String, String> properties) {
    if (isPathIdentifier(ident)) {
      return ident.name();
    }
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location != null) {
      return location;
    }
    // Fall back to default table path from session catalog
    TableIdentifier tableId =
        new TableIdentifier(
            ident.name(),
            ident.namespace().length > 0
                ? Option.apply(ident.namespace()[ident.namespace().length - 1])
                : Option.empty());
    return spark().sessionState().catalog().defaultTablePath(tableId).toString();
  }

  /**
   * Builds a Hadoop Configuration from fs.* and dfs.* properties for credential propagation.
   * These properties are injected by UC's UCSingleCatalog and are needed for Kernel to access
   * cloud storage.
   */
  private Configuration buildHadoopConf(Map<String, String> properties) {
    // scalastyle:off deltahadoopconfiguration
    Configuration conf = spark().sessionState().newHadoopConf();
    // scalastyle:on deltahadoopconfiguration
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("fs.") || key.startsWith("dfs.")) {
        conf.set(key, entry.getValue());
      }
    }
    return conf;
  }

  /** Checks whether the provider in the table properties refers to Delta. */
  private boolean isDeltaProvider(Map<String, String> properties) {
    String provider = getProvider(properties);
    return DeltaSourceUtils$.MODULE$.isDeltaDataSourceName(provider);
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
