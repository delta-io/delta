/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.ddl;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Meta;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import java.util.*;
import java.util.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;

/**
 * Builds a Kernel CREATE TABLE transaction for the DSv2 + Kernel + CCv2 path.
 *
 * <p>All table-type-specific logic (UC pre-registration, path resolution, committer selection) is
 * encapsulated here so that the orchestrator sees only {@code builder.build()}.
 */
public class CreateTableTxnBuilder implements TableTxnBuilder<PreparedCreateTableTxn> {

  /** DSv2-internal keys that should not be forwarded to Kernel. */
  private static final Set<String> FILTERED_PROPERTY_KEYS =
      Set.of(
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_EXTERNAL,
          TableCatalog.PROP_IS_MANAGED_LOCATION,
          "path",
          "option.path");

  private static final String ENGINE_INFO = "Delta-Spark-DSv2/" + Meta.KERNEL_VERSION;

  private final CreateTableContext ctx;

  /**
   * Callback to pre-register a table in the delegate catalog (UC). Accepts the context, returns the
   * CatalogTable with UC-assigned metadata (tableId, location).
   */
  private final Function<CreateTableContext, CatalogTable> preRegistrar;

  public CreateTableTxnBuilder(
      CreateTableContext ctx, Function<CreateTableContext, CatalogTable> preRegistrar) {
    this.ctx = requireNonNull(ctx, "ctx is null");
    this.preRegistrar = requireNonNull(preRegistrar, "preRegistrar is null");
  }

  @Override
  public PreparedCreateTableTxn build() {
    // resolve table path and optional UC metadata
    String tablePath;
    Optional<UCTableInfo> ucTableInfo;

    if (ctx.isUnityCatalog) {
      CatalogTable catalogTable = preRegistrar.apply(ctx);
      ucTableInfo = UCUtils.extractTableInfo(catalogTable, SparkSession.active());
      tablePath =
          ucTableInfo.map(UCTableInfo::getTablePath).orElse(catalogTable.location().toString());
    } else {
      tablePath = resolvePathFromProperties(ctx.ident, ctx.properties);
      ucTableInfo = Optional.empty();
    }

    // filter properties, convert schema, build layout spec
    Map<String, String> filteredProps = filterProperties(ctx.properties);
    StructType kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(ctx.schema);
    DataLayoutSpec layoutSpec = buildDataLayoutSpec(ctx.partitions);

    // create Kernel engine and transaction
    Engine engine = DefaultEngine.create(ctx.hadoopConf);

    CreateTableTransactionBuilder txnBuilder;
    if (ucTableInfo.isPresent()) {
      UCTableInfo info = ucTableInfo.get();
      UCCatalogManagedClient ucClient = createUCCatalogManagedClient(info);
      txnBuilder =
          ucClient.buildCreateTableTransaction(
              info.getTableId(), tablePath, kernelSchema, ENGINE_INFO);
    } else {
      txnBuilder = TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO);
    }

    if (!filteredProps.isEmpty()) {
      txnBuilder = txnBuilder.withTableProperties(filteredProps);
    }
    if (!layoutSpec.hasNoDataLayoutSpec()) {
      txnBuilder = txnBuilder.withDataLayoutSpec(layoutSpec);
    }

    Transaction transaction = txnBuilder.build(engine);
    return new PreparedCreateTableTxn(transaction, engine, tablePath);
  }

  // ── helpers (package-visible for testing) ──────────────────────────

  static Map<String, String> filterProperties(Map<String, String> properties) {
    Map<String, String> filtered = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!FILTERED_PROPERTY_KEYS.contains(entry.getKey())) {
        filtered.put(entry.getKey(), entry.getValue());
      }
    }
    return filtered;
  }

  static DataLayoutSpec buildDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return DataLayoutSpec.noDataLayout();
    }
    List<Column> partCols = new ArrayList<>();
    for (Transform t : partitions) {
      if (t instanceof IdentityTransform) {
        FieldReference ref = (FieldReference) ((IdentityTransform) t).ref();
        partCols.add(new Column(ref.fieldNames()[0]));
      }
    }
    return partCols.isEmpty()
        ? DataLayoutSpec.noDataLayout()
        : DataLayoutSpec.partitioned(partCols);
  }

  private static String resolvePathFromProperties(
      org.apache.spark.sql.connector.catalog.Identifier ident, Map<String, String> properties) {
    String loc = properties.get(TableCatalog.PROP_LOCATION);
    if (loc != null) return loc;
    loc = properties.get("location");
    if (loc != null) return loc;
    return ident.name();
  }

  private static UCCatalogManagedClient createUCCatalogManagedClient(UCTableInfo tableInfo) {
    Map<String, String> appVersions =
        UCTokenBasedRestClientFactory$.MODULE$.defaultAppVersionsAsJava();
    appVersions.put("Kernel", Meta.KERNEL_VERSION);
    appVersions.put("Delta V2 connector", "true");
    UCClient ucClient =
        UCTokenBasedRestClientFactory$.MODULE$.createUCClientWithVersions(
            tableInfo.getUcUri(), tableInfo.getAuthConfig(), appVersions);
    return new UCCatalogManagedClient(ucClient);
  }
}
