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
import io.delta.spark.internal.v2.ddl.DDLRequest;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
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

  // ── CREATE TABLE (DSv2 + Kernel path) ─────────────────────────────

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {

    DeltaV2Mode mode = new DeltaV2Mode(spark().sessionState().conf());
    if (!mode.shouldUseKernelForCreateTable(isUnityCatalog(), properties)) {
      return super.createTable(ident, schema, partitions, properties);
    }

    // For UC-managed tables, pre-register to get the UC-assigned tableId and location.
    // TODO: If the Kernel commit below fails, this leaves a catalog entry with no Delta log
    //  (zombie table). Add cleanup/rollback (e.g. super.dropTable) on commit failure.
    Optional<UCTableInfo> ucInfo =
        isUnityCatalog()
            ? preRegisterAndExtractUCInfo(ident, schema, partitions, properties)
            : Optional.empty();

    // prepare
    DDLRequest request =
        CreateTableBuilder.prepare(
            ident, schema, partitions, properties, spark().sessionState().newHadoopConf(), ucInfo);

    // build + commit
    Transaction txn = CreateTableBuilder.buildTransaction(request);
    txn.commit(request.engine(), CloseableIterable.emptyIterable());

    // load via V2 connector
    return new SparkTable(ident, request.tablePath());
  }

  /**
   * Registers the table in the UC catalog via the V1 delegate path and extracts the UC-assigned
   * table metadata (tableId, managed location, ucUri, authConfig).
   *
   * <p>This is required because the UC committer needs the tableId to coordinate the Kernel
   * commit, and UC only assigns the tableId during table creation. So we must register first,
   * then commit the Delta log through the UC committer.
   *
   * @return UC table info if the delegate catalog is UC-backed, otherwise empty
   */
  private Optional<UCTableInfo> preRegisterAndExtractUCInfo(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    // Delegates to AbstractDeltaCatalog (V1 path) which registers in the UC catalog
    Table t = super.createTable(ident, schema, partitions, properties);
    if (!(t instanceof V1Table)) {
      throw new IllegalStateException(
          "Expected V1Table from delegate catalog, got: " + t.getClass().getName());
    }
    CatalogTable ct = ((V1Table) t).catalogTable();
    return UCUtils.extractTableInfo(ct, spark());
  }
}
