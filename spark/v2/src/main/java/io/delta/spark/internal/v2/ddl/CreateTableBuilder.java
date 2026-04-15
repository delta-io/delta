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

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Prepares and builds Kernel transactions for CREATE TABLE.
 *
 * <p>Stateless utility following the pattern of {@link TableManager} — all state flows through
 * {@link DDLRequestContext}. The two public entry points are {@link #prepare} (Spark→Kernel
 * conversion) and {@link #buildTransaction} (transaction construction).
 */
public final class CreateTableBuilder {

  private CreateTableBuilder() {}

  /** Convert Spark-level inputs into a Kernel-ready {@link DDLRequestContext}. */
  public static DDLRequestContext prepare(
      Identifier ident,
      String catalogName,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> properties,
      Configuration hadoopConf,
      Optional<UCTableInfo> ucTableInfo) {
    requireNonNull(ident);
    requireNonNull(sparkSchema);
    requireNonNull(properties);
    requireNonNull(hadoopConf);

    String tablePath =
        ucTableInfo
            .map(UCTableInfo::getTablePath)
            .orElseGet(() -> DDLUtils.resolveTablePath(properties, ident));

    Optional<String> comment = DDLUtils.extractComment(properties);
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    DataLayoutSpec dataLayoutSpec = DDLUtils.toDataLayoutSpec(partitions);
    DDLUtils.validateClusteringColumns(dataLayoutSpec, kernelSchema);

    CreateTableTransactionBuilder txnBuilder =
        ucTableInfo.isPresent()
            ? buildUCTransactionBuilder(
                ucTableInfo.get(),
                catalogName,
                String.join(".", ident.namespace()),
                ident.name(),
                kernelSchema)
            : TableManager.buildCreateTableTransaction(
                tablePath, kernelSchema, DDLUtils.ENGINE_INFO);

    return new DDLRequestContext(
        ident,
        tablePath,
        kernelSchema,
        DDLUtils.filterProperties(properties),
        comment,
        dataLayoutSpec,
        DefaultEngine.create(hadoopConf),
        ucTableInfo,
        txnBuilder);
  }

  /**
   * Applies optional table properties and data layout to the pre-resolved transaction builder, then
   * builds the final Kernel {@link Transaction}. This is separated from {@link #prepare} so callers
   * can inspect or modify the {@link DDLRequestContext} between preparation and building.
   */
  public static Transaction buildTransaction(DDLRequestContext request) {
    CreateTableTransactionBuilder builder = request.transactionBuilder();
    if (!request.properties().isEmpty()) {
      builder.withTableProperties(request.properties());
    }
    if (!request.dataLayoutSpec().hasNoDataLayoutSpec()) {
      builder.withDataLayoutSpec(request.dataLayoutSpec());
    }
    return builder.build(request.engine());
  }

  /**
   * Creates a {@link CreateTableTransactionBuilder} backed by Unity Catalog. This wires up the UC
   * REST client and {@link io.delta.kernel.unitycatalog.UCCatalogManagedCommitter} so that when the
   * transaction commits, the delta file is written to the staging location and the table is
   * finalized (promoted) in UC atomically via the committer's {@code createImpl} path.
   */
  private static CreateTableTransactionBuilder buildUCTransactionBuilder(
      UCTableInfo ucTableInfo,
      String catalogName,
      String schemaName,
      String tableName,
      io.delta.kernel.types.StructType kernelSchema) {
    io.delta.storage.commit.uccommitcoordinator.UCDeltaClient ucDeltaClient =
        io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils.getSharedUCDeltaClient(
            catalogName);
    return new UCCatalogManagedClient(ucDeltaClient)
        .buildCreateTableTransaction(
            ucTableInfo.getTableId(),
            ucTableInfo.getTablePath(),
            kernelSchema,
            DDLUtils.ENGINE_INFO,
            new UCTableIdentifier(catalogName, schemaName, tableName));
  }
}
