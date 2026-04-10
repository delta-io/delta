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
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.ddl.CreateTableBuilder;
import io.delta.spark.internal.v2.ddl.DDLRequestContext;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.types.StructType;
import scala.jdk.javaapi.CollectionConverters;

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

  /** Creates a Delta table via the DSv2 connector path. */
  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {

    // Step 1: Extract UC table info from DSv2 properties (if this is a UC catalog).
    Optional<UCTableInfo> ucInfo = extractUCInfoFromProperties(properties);

    // Step 2: Check DeltaV2Mode to decide V1 vs V2; fall back to V1 if V2 is not enabled.
    DeltaV2Mode connectorMode = new DeltaV2Mode(spark().sessionState().conf());
    if (!connectorMode.shouldUseDSv2ForCreateTable(ucInfo.isPresent())) {
      return super.createTable(ident, schema, partitions, properties);
    }

    // Step 3: Inject UC storage credentials into Hadoop conf so Kernel can write to the
    // staging path.
    Configuration hadoopConf = spark().sessionState().newHadoopConf();
    ucInfo.ifPresent(info -> injectStorageCredentials(properties, hadoopConf));

    // Step 4: Prepare DDLRequestContext (Spark-to-Kernel conversion).
    DDLRequestContext ddlRequestContext =
        CreateTableBuilder.prepare(
            ident, name(), schema, partitions, properties, hadoopConf, ucInfo);

    // Step 5: Build the Kernel transaction from the DDLRequestContext and commit
    // (metadata-only; no data rows for CREATE TABLE).
    try {
      Transaction txn = CreateTableBuilder.buildTransaction(ddlRequestContext);
      txn.commit(ddlRequestContext.engine(), CloseableIterable.emptyIterable());
    } catch (Exception e) {
      throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }

    // Step 6: Return a path-based SparkTable for the newly created table.
    return new SparkTable(ident, ddlRequestContext.tablePath());
  }

  /**
   * Extracts {@link UCTableInfo} from DSv2 properties when running under a Unity Catalog. Returns
   * empty if this is not a UC catalog or if the required {@code io.unitycatalog.tableId} and {@code
   * location} properties are missing.
   */
  private Optional<UCTableInfo> extractUCInfoFromProperties(Map<String, String> properties) {
    if (!isUnityCatalog()) {
      return Optional.empty();
    }
    String tableId = properties.get("io.unitycatalog.tableId");
    String tablePath = properties.get(TableCatalog.PROP_LOCATION);
    if (tableId == null || tablePath == null) {
      return Optional.empty();
    }
    UCCatalogConfig config = getUCCatalogConfig();
    return Optional.of(
        new UCTableInfo(
            tableId, tablePath, config.uri(), CollectionConverters.asJava(config.authConfig())));
  }

  /**
   * Looks up the {@link UCCatalogConfig} for this catalog from the Spark session's coordinated
   * commit configuration.
   *
   * @throws IllegalStateException if no UC configuration is found for this catalog name
   */
  private UCCatalogConfig getUCCatalogConfig() {
    scala.collection.immutable.Map<String, UCCatalogConfig> ucConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark());
    scala.Option<UCCatalogConfig> configOpt = ucConfigs.get(name());
    if (configOpt.isEmpty()) {
      throw new IllegalStateException(
          "Unity Catalog configuration not found for catalog '" + name() + "'");
    }
    return configOpt.get();
  }

  /**
   * Copies UC-vended storage credentials ({@code fs.*}, {@code dfs.*}) from DSv2 properties into
   * the Hadoop configuration so that Kernel's {@link io.delta.kernel.defaults.engine.DefaultEngine}
   * can authenticate when writing to the table's staging path.
   */
  private static void injectStorageCredentials(
      Map<String, String> properties, Configuration hadoopConf) {
    properties.forEach(
        (key, value) -> {
          if (key.startsWith("fs.") || key.startsWith("dfs.")) {
            hadoopConf.set(key, value);
          }
        });
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
