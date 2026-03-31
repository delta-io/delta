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

import io.delta.kernel.Meta;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.unitycatalog.UnityCatalogUtils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.ddl.CreateTableBuilder;
import io.delta.spark.internal.v2.ddl.DDLRequestContext;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger logger = LoggerFactory.getLogger(DeltaCatalog.class);

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

  private DeltaV2Mode v2Mode() {
    return new DeltaV2Mode(spark().sessionState().conf());
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
    if (v2Mode().shouldCatalogReturnV2Tables()) {
      return v2ConnectorSupplier.get();
    } else {
      return v1ConnectorSupplier.get();
    }
  }

  // ── CREATE TABLE (DSv2 + Kernel path) ─────────────────────────────
  //
  // For UC managed tables, UCSingleCatalog has already staged the table (via
  // createStagingTable REST API) and injected the staging metadata (tableId, location,
  // credentials) into the properties map BEFORE calling this method.
  //
  // The flow follows the staging-table protocol
  // (see https://github.com/delta-io/delta/issues/5118):
  //
  //   1. UCSingleCatalog       — allocate staging table, inject into properties (already done)
  //   2. txn.commit()          — Kernel writes 000.json to the staging location
  //   3. finalizeCreateInUC()  — promote staging → real managed table with all Class-B properties
  //
  // This ensures post-commit properties (delta.lastUpdateVersion, delta.lastCommitTimestamp,
  // protocol features, clustering columns) are set atomically during table creation.

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {

    if (!v2Mode().shouldUseKernelForCreateTable(isUnityCatalog(), properties)) {
      return super.createTable(ident, schema, partitions, properties);
    }

    Configuration hadoopConf = spark().sessionState().newHadoopConf();
    Optional<UCTableInfo> ucInfo = extractUCInfoFromProperties(properties);
    ucInfo.ifPresent(info -> injectStorageCredentials(properties, hadoopConf));

    DDLRequestContext request =
        CreateTableBuilder.prepare(ident, schema, partitions, properties, hadoopConf, ucInfo);
    try {
      Transaction txn = CreateTableBuilder.buildTransaction(request);
      TransactionCommitResult result =
          txn.commit(request.engine(), CloseableIterable.emptyIterable());

      if (ucInfo.isPresent()) {
        finalizeCreateInUC(ident, schema, request, result, ucInfo.get());
      }
    } catch (Exception e) {
      throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }

    if (ucInfo.isPresent()) {
      CatalogTable ct =
          CatalogTableFactory.buildUCCatalogTable(
              ident, name(), ucInfo.get().getTableId(), request.tablePath());
      return new SparkTable(ident, ct, new HashMap<>());
    }
    return new SparkTable(ident, request.tablePath());
  }

  /**
   * Extracts UC table metadata from properties that were pre-populated by {@code
   * UCSingleCatalog.stageManagedDeltaTableAndGetProps()}. The staging table ID and location are
   * injected into properties by the UC Spark connector before this method is reached.
   *
   * @return UC table info if properties contain staging metadata, empty otherwise
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
   * Promotes the staging table to a real UC managed table with post-commit "Class B" properties.
   *
   * <p>These properties are derived from the committed version-0 snapshot and include protocol
   * features, metadata configuration, clustering columns, {@code delta.lastUpdateVersion}, and
   * {@code delta.lastCommitTimestamp}. They are computed via {@link
   * UnityCatalogUtils#getPropertiesForCreate}.
   */
  private void finalizeCreateInUC(
      Identifier ident,
      StructType schema,
      DDLRequestContext request,
      TransactionCommitResult result,
      UCTableInfo ucInfo) {
    SnapshotImpl snapshot =
        (SnapshotImpl)
            result
                .getPostCommitSnapshot()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Missing post-commit snapshot for CREATE TABLE " + ident));

    Map<String, String> ucProps =
        UnityCatalogUtils.getPropertiesForCreate(request.engine(), snapshot);
    List<UCClient.ColumnDef> columns = toColumnDefs(schema);

    try (UCClient ucClient = buildUCClient(ucInfo.getUcUri(), ucInfo.getAuthConfig())) {
      ucClient.finalizeCreate(
          ident.name(), name(), ident.namespace()[0], ucInfo.getTablePath(), columns, ucProps);
      logger.info("Finalized UC table creation for {}", ident);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to finalize table creation for " + ident + " in Unity Catalog", e);
    }
  }

  private static List<UCClient.ColumnDef> toColumnDefs(StructType schema) {
    List<UCClient.ColumnDef> columns = new java.util.ArrayList<>();
    org.apache.spark.sql.types.StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      org.apache.spark.sql.types.StructField f = fields[i];
      columns.add(
          new UCClient.ColumnDef(
              f.name(),
              f.dataType().catalogString(),
              f.dataType().catalogString().toUpperCase(),
              f.dataType().json(),
              f.nullable(),
              i));
    }
    return columns;
  }

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

  private static UCClient buildUCClient(String ucUri, Map<String, String> authConfig) {
    Map<String, String> appVersions =
        new HashMap<>(UCTokenBasedRestClientFactory$.MODULE$.defaultAppVersionsAsJava());
    appVersions.put("Kernel", Meta.KERNEL_VERSION);
    appVersions.put("Delta V2 connector", "true");
    return UCTokenBasedRestClientFactory$.MODULE$.createUCClientWithVersions(
        ucUri, authConfig, appVersions);
  }

  /**
   * Injects filesystem credentials from properties into the Hadoop configuration. The UC Spark
   * connector ({@code UCSingleCatalog}) injects temporary credentials as {@code fs.*} properties.
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
}
