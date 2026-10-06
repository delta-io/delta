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

import io.delta.spark.internal.v2.catalog.DeltaV2Table;
import io.delta.spark.internal.v2.exception.TableNotFoundException;
import io.delta.spark.internal.v2.exception.TimestampOutOfRangeException;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import org.apache.spark.sql.delta.DeltaTableUtils$;
import org.apache.spark.sql.delta.DeltaTimeTravelSpec;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.delta.util.DateTimeUtils$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2ErrorInterop$;
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
 *   <li>V2 connector: {@link DeltaV2Table} - sparkV2 connector, read-only support</li>
 * </ul>
 *
 * <p>See {@link DeltaV2Mode} for V1 vs V2 connector definitions and enable mode configuration.</p>
 */
public class DeltaCatalog extends AbstractDeltaCatalog implements ChangelogSupport {

  /**
   * Loads a Delta table that is registered in the catalog.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link DeltaV2Table} (V2 connector)</li>
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)</li>
   * </ul>
   *
   * @param ident The identifier of the table in the catalog.
   * @param catalogTable The catalog table metadata containing table properties and location.
   * @return Table instance (DeltaV2Table for V2, DeltaTableV2 for V1).
   */
  @Override
  public Table loadCatalogTable(Identifier ident, CatalogTable catalogTable) {
    return loadTableInternal(
        () -> new DeltaV2Table(ident, catalogTable, new HashMap<>()),
        () -> super.loadCatalogTable(ident, catalogTable));
  }

  /**
   * Loads a Delta table directly from a path.
   * This is used for path-based table access where the identifier name is the table path.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>STRICT: Returns sparkV2 {@link DeltaV2Table} (V2 connector)</li>
   *   <li>NONE (default): Returns {@link DeltaTableV2} (V1 connector)</li>
   * </ul>
   *
   * @param ident The identifier whose name contains the path to the Delta table.
   * @return Table instance (DeltaV2Table for V2, DeltaTableV2 for V1).
   */
  @Override
  public Table loadPathTable(Identifier ident) {
    return loadTableInternal(
        // delta.`/path/to/table`, where ident.name() is `/path/to/table`.
        () -> pinPathTable(ident),
        () -> super.loadPathTable(ident));
  }

  /**
   * Builds a Kernel-backed V2 table for a path-based identifier, honoring an `@`-suffix time travel
   * spec if present.
   *
   * <p>Reuses the V1
   * {@link org.apache.spark.sql.delta.DeltaTableUtils#extractIfPathContainsTimeTravel} helper.
   *
   * @param ident The path identifier.
   * @return A {@link DeltaV2Table} pinned to the requested version/timestamp, or the latest
   *     snapshot.
   */
  private DeltaV2Table pinPathTable(Identifier ident) {
    scala.Tuple2<String, scala.Option<DeltaTimeTravelSpec>> resolved =
        DeltaTableUtils$.MODULE$.extractIfPathContainsTimeTravel(
            spark(), ident.name(), ScalaUtils.toScalaMap(new HashMap<>()));
    String realPath = resolved._1();
    scala.Option<DeltaTimeTravelSpec> timeTravelByPath = resolved._2();
    try {
      DeltaV2Table table = new DeltaV2Table(ident, realPath);
      if (timeTravelByPath.isEmpty()) {
        return table;
      }
      DeltaTimeTravelSpec spec = timeTravelByPath.get();
      if (spec.version().isDefined()) {
        return table.withVersion((Long) spec.version().get());
      }
      long timestampMicros = DateTimeUtils$.MODULE$.fromJavaTimestamp(
          spec.getTimestamp(spark().sessionState().conf()));
      return table.withTimestamp(timestampMicros);
    } catch (TableNotFoundException e) {
      DeltaV2ErrorInterop$.MODULE$.throwAsDeltaError(e);
      throw new IllegalStateException("unreachable");
    } catch (VersionNotFoundException e) {
      DeltaV2ErrorInterop$.MODULE$.throwAsDeltaError(e);
      throw new IllegalStateException("unreachable");
    } catch (TimestampOutOfRangeException e) {
      DeltaV2ErrorInterop$.MODULE$.throwAsDeltaError(e);
      throw new IllegalStateException("unreachable");
    }
  }

  /**
   * Loads a Delta table pinned to a specific version.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>V2 connector: pins a {@link DeltaV2Table}</li>
   *   <li>V1 connector (default): delegates to the {@code AbstractDeltaCatalog} time travel path</li>
   * </ul>
   *
   * @param ident The identifier of the table in the catalog.
   * @param version The table version to load.
   * @return Table instance pinned to {@code version}.
   */
  @Override
  public Table loadTable(Identifier ident, String version) {
    Table table = loadTable(ident);
    if (table instanceof DeltaV2Table) {
      try {
        return ((DeltaV2Table) table).withVersion(Long.parseLong(version));
      } catch (VersionNotFoundException e) {
        // throwAsDeltaError always throws; the trailing throw is unreachable but satisfies javac.
        DeltaV2ErrorInterop$.MODULE$.throwAsDeltaError(e);
        throw new IllegalStateException("unreachable");
      }
    }
    return super.loadTable(ident, version);
  }

  /**
   * Loads a Delta table pinned to the snapshot active at a specific timestamp.
   *
   * <p>Routing logic based on {@link DeltaV2Mode}:
   * <ul>
   *   <li>V2 connector: pins a {@link DeltaV2Table}.</li>
   *   <li>V1 connector (default): delegates to the {@code AbstractDeltaCatalog} time travel path</li>
   * </ul>
   *
   * @param ident The identifier of the table in the catalog.
   * @param timestamp The timestamp to load the table at, in microseconds since the epoch.
   * @return Table instance pinned to the snapshot active at {@code timestamp}.
   */
  @Override
  public Table loadTable(Identifier ident, long timestamp) {
    Table table = loadTable(ident);
    if (table instanceof DeltaV2Table) {
      try {
        return ((DeltaV2Table) table).withTimestamp(timestamp);
      } catch (TimestampOutOfRangeException e) {
        // throwAsDeltaError always throws; the trailing throw is unreachable but satisfies javac.
        DeltaV2ErrorInterop$.MODULE$.throwAsDeltaError(e);
        throw new IllegalStateException("unreachable");
      }
    }
    return super.loadTable(ident, timestamp);
  }

  /**
   * Loads a table based on the {@link DeltaV2Mode} SQL configuration.
   *
   * <p>This method checks the configuration and delegates to the appropriate supplier:
   * <ul>
   *   <li>STRICT mode: Uses V2 connector (sparkV2 DeltaV2Table) - for testing V2 capabilities</li>
   *   <li>NONE mode (default): Uses V1 connector (DeltaTableV2) - production default with full features</li>
   * </ul>
   *
   * <p>See {@link DeltaV2Mode} for detailed V1 vs V2 connector definitions.
   *
   * @param v2ConnectorSupplier Supplier for V2 connector (sparkV2 DeltaV2Table)
   *                            - used in STRICT mode
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
