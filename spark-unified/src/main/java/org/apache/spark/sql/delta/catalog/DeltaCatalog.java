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
import io.delta.spark.internal.v2.ddl.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaV2Mode;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark catalog plugin for Delta Lake tables that implements the Spark DataSource V2 Catalog API.
 *
 * <p>See {@link DeltaV2Mode} for V1 vs V2 connector definitions and enable mode configuration.
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  // ── table loading (V1/V2 routing) ──────────────────────────────────

  @Override
  public Table loadCatalogTable(Identifier ident, CatalogTable catalogTable) {
    return loadTableInternal(
        () -> new SparkTable(ident, catalogTable, new HashMap<>()),
        () -> super.loadCatalogTable(ident, catalogTable));
  }

  @Override
  public Table loadPathTable(Identifier ident) {
    return loadTableInternal(
        () -> new SparkTable(ident, ident.name()),
        () -> super.loadPathTable(ident));
  }

  private Table loadTableInternal(
      Supplier<Table> v2ConnectorSupplier, Supplier<Table> v1ConnectorSupplier) {
    DeltaV2Mode mode = new DeltaV2Mode(spark().sessionState().conf());
    return mode.shouldCatalogReturnV2Tables()
        ? v2ConnectorSupplier.get()
        : v1ConnectorSupplier.get();
  }

  // ── CREATE TABLE (DSv2 + Kernel + CCv2 path) ──────────────────────

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

    CreateTableContext ctx =
        new CreateTableContext(
            ident,
            schema,
            partitions,
            properties,
            spark().sessionState().newHadoopConf(),
            isUnityCatalog());

    // build
    PreparedCreateTableTxn prepared =
        new CreateTableTxnBuilder(ctx, this::preRegisterInCatalog).build();

    // commit
    CommittedTableTxn committed = TableCommitter.commit(prepared);

    // publish
    new CreateTablePublisher(ctx, this::updateCatalogProperties).publish(committed);

    // load
    return loadTable(ident);
  }

  // ── catalog callbacks (passed into builder / publisher) ────────────

  private CatalogTable preRegisterInCatalog(CreateTableContext ctx) {
    Table t =
        super.createTable(
            ctx.ident, CatalogV2Util.structTypeToV2Columns(ctx.schema), ctx.partitions,
            ctx.properties);
    if (t instanceof V1Table) return ((V1Table) t).catalogTable();
    throw new IllegalStateException(
        "Expected V1Table from delegate catalog, got: " + t.getClass().getName());
  }

  private void updateCatalogProperties(Identifier ident, TableChange[] changes) {
    super.alterTable(ident, changes);
  }
}
