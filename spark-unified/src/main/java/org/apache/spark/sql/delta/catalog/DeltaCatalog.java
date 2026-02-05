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
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils$;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
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

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    if (!isDeltaProvider(properties) || !isStrictMode()) {
      return super.createTable(ident, schema, partitions, properties);
    }
    StagedTable staged = stageCreate(ident, schema, partitions, properties);
    staged.commitStagedChanges();
    return loadTable(ident);
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    if (!isDeltaProvider(properties) || !isStrictMode()) {
      return super.stageCreate(ident, schema, partitions, properties);
    }
    Map<String, String> safeProps = properties == null ? new HashMap<>() : new HashMap<>(properties);
    translateUcTableIdProperty(safeProps);
    if (CatalogTableUtils.isUnityCatalogManagedTableFromProperties(safeProps)) {
      String ucTableId = safeProps.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
      if (ucTableId == null || ucTableId.isEmpty()) {
        throw new IllegalArgumentException("UC table id is required for UC-managed create");
      }
      String stagingPath = getUcStagingPath(safeProps);
      if (stagingPath == null || stagingPath.isEmpty()) {
        throw new IllegalArgumentException("UC staging path is required for UC-managed create");
      }
      ResolvedCreateRequest request =
          ResolvedCreateRequest.forUCManagedCreate(
              ident,
              schema,
              partitions,
              safeProps,
              ucTableId,
              stagingPath);
      return new KernelStagedDeltaTable(spark(), ident, request);
    }
    if (isPathIdentifier(ident)) {
      ResolvedCreateRequest request =
          ResolvedCreateRequest.forPathCreate(ident, schema, partitions, safeProps);
      return new KernelStagedDeltaTable(spark(), ident, request);
    }
    return super.stageCreate(ident, schema, partitions, properties);
  }

  private boolean isStrictMode() {
    DeltaV2Mode connectorMode = new DeltaV2Mode(spark().sessionState().conf());
    return connectorMode.shouldCatalogReturnV2Tables();
  }

  private boolean isDeltaProvider(Map<String, String> properties) {
    String provider = getProvider(properties);
    return DeltaSourceUtils$.MODULE$.isDeltaDataSourceName(provider);
  }

  private String getProvider(Map<String, String> properties) {
    if (properties != null) {
      String provider = properties.get(TableCatalog.PROP_PROVIDER);
      if (provider != null) {
        return provider;
      }
    }
    return spark().sessionState().conf()
        .getConfString("spark.sql.sources.default", "parquet");
  }

  private static String getUcStagingPath(Map<String, String> properties) {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location != null) {
      return location;
    }
    return properties.get("location");
  }

  private static void translateUcTableIdProperty(Map<String, String> props) {
    String oldValue = props.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldValue != null && !oldValue.isEmpty()) {
      props.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldValue);
    }
  }
}
