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

import io.delta.spark.internal.v2.catalog.DeltaKernelStagedCreateTable;
import io.delta.spark.internal.v2.catalog.SparkTable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.apache.spark.sql.types.StructType;

/**
 * Delta Lake catalog plugin for Spark's DataSource V2 catalog API.
 *
 * <p>Routing is controlled by {@link DeltaV2Mode}: in STRICT mode this catalog returns V2
 * {@link SparkTable} instances and routes supported DDLs through the Kernel-backed implementation;
 * otherwise it falls back to the V1 implementation.
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

  /**
   * CREATE TABLE routing for STRICT mode.
   *
   * <p>In STRICT mode, the catalog should not just return V2 tables; it must also create them via
   * Kernel (metadata-only) so commit provenance is unambiguous (commitInfo.engineInfo).
   *
   * <p>Non-STRICT modes fall back to the existing V1 create pipeline in {@link AbstractDeltaCatalog}.
   */
  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    DeltaV2Mode connectorMode = new DeltaV2Mode(spark().sessionState().conf());
    if (!connectorMode.shouldCatalogReturnV2Tables()) {
      return super.createTable(ident, schema, partitions, properties);
    }

    // STRICT mode: only intercept Delta tables.
    String provider = properties.get(TableCatalog.PROP_PROVIDER);
    if (provider == null || !DeltaSourceUtils.isDeltaDataSourceName(provider)) {
      return super.createTable(ident, schema, partitions, properties);
    }

    final String catalogName = name();

    if (isPathIdentifier(ident)) {
      // Path-based identifier: no catalog registration required.
      new DeltaKernelStagedCreateTable(
              spark(),
              catalogName,
              ident,
              ident.name(),
              schema,
              partitions,
              properties,
              null)
          .commitStagedChanges();
      return loadTable(ident);
    }

    // Catalog-based identifier: commit metadata via Kernel, then register in Spark catalog.
    scala.Tuple2<CatalogTable, String> spec =
        V2CreateTableHelper$.MODULE$.buildCatalogTableSpec(
            spark(), ident, schema, partitions, properties);
    CatalogTable tableDesc = spec._1();
    String tablePath = spec._2();

    new DeltaKernelStagedCreateTable(
            spark(),
            catalogName,
            ident,
            tablePath,
            schema,
            partitions,
            properties,
            () -> V2CreateTableHelper$.MODULE$.registerTable(spark(), tableDesc))
        .commitStagedChanges();

    return loadTable(ident);
  }
}
