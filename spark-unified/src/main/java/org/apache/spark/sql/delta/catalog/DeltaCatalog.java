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
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import java.util.HashMap;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

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
 *   <li>V2 connector: {@link SparkTable} - Kernel-backed connector, read-only support</li>
 * </ul>
 *
 * <p>See {@link DeltaSQLConf#V2_ENABLE_MODE} for V1 vs V2 connector definitions and enable mode configuration.</p>
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  /**
   * Loads a Delta table that is registered in the catalog.
   *
   * <p>Routing logic based on {@link DeltaSQLConf#V2_ENABLE_MODE}:
   * <ul>
   *   <li>STRICT: Returns Kernel {@link SparkTable} (V2 connector)</li>
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
        () -> {
          // If the table is catalog-owned (coordinated commits), fallback to V1 connector
          // because SparkTable doesn't support maxCatalogVersion yet.
          if (isCatalogOwned(catalogTable)) {
            return super.loadCatalogTable(ident, catalogTable);
          }
          try {
            return new SparkTable(ident, catalogTable, new HashMap<>());
          } catch (IllegalArgumentException e) {
            // Catch "Must provide maxCatalogVersion for catalogManaged tables"
            // This happens if the property exists in the Delta Log but not in the CatalogTable
            if (e.getMessage() != null && e.getMessage().contains("maxCatalogVersion")) {
              return super.loadCatalogTable(ident, catalogTable);
            }
            throw e;
          }
        },
        () -> super.loadCatalogTable(ident, catalogTable));
  }

  /**
   * Loads a Delta table directly from a path.
   * This is used for path-based table access where the identifier name is the table path.
   *
   * <p>Routing logic based on {@link DeltaSQLConf#V2_ENABLE_MODE}:
   * <ul>
   *   <li>STRICT: Returns Kernel {@link SparkTable} (V2 connector)</li>
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
        () -> {
          try {
            return new SparkTable(ident, ident.name());
          } catch (IllegalArgumentException e) {
            // Catch "Must provide maxCatalogVersion for catalogManaged tables"
            if (e.getMessage() != null && e.getMessage().contains("maxCatalogVersion")) {
              return super.loadPathTable(ident);
            }
            throw e;
          }
        },
        () -> super.loadPathTable(ident));
  }

  /**
   * Checks if the table is catalog-owned (coordinated commits).
   * Uses the "delta.coordinatedCommits.commitCoordinator-preview" property.
   */
  private boolean isCatalogOwned(CatalogTable catalogTable) {
    if (catalogTable == null) return false;
    // Scala Map.contains is accessible from Java
    return catalogTable.properties()
        .contains("delta.coordinatedCommits.commitCoordinator-preview");
  }

  /**
   * Loads a table based on the {@link DeltaSQLConf#V2_ENABLE_MODE} SQL configuration.
   *
   * <p>This method checks the configuration and delegates to the appropriate supplier:
   * <ul>
   *   <li>STRICT mode: Uses V2 connector (Kernel SparkTable) - for testing V2 capabilities</li>
   *   <li>NONE mode (default): Uses V1 connector (DeltaTableV2) - production default with full features</li>
   * </ul>
   *
   * <p>See {@link DeltaSQLConf#V2_ENABLE_MODE} for detailed V1 vs V2 connector definitions.
   *
   * @param v2ConnectorSupplier Supplier for V2 connector (Kernel SparkTable) - used in STRICT mode
   * @param v1ConnectorSupplier Supplier for V1 connector (DeltaTableV2) - used in NONE mode (default)
   * @return Table instance from the selected supplier
   */
  private Table loadTableInternal(
      Supplier<Table> v2ConnectorSupplier,
      Supplier<Table> v1ConnectorSupplier) {
    String mode =
        spark()
            .conf()
            .get(DeltaSQLConf.V2_ENABLE_MODE().key(),
                DeltaSQLConf.V2_ENABLE_MODE().defaultValueString());
    switch (mode.toUpperCase()) {
      case "STRICT":
        return v2ConnectorSupplier.get();
      default:
        return v1ConnectorSupplier.get();
    }
  }
}
