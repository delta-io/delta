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
package io.delta.spark.internal.v2.catalog;

import static java.util.Objects.requireNonNull;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.spark.internal.v2.utils.PartitionTransformUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SparkDDLPropertyUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.util.CatalogTableUtils;

/**
 * Kernel-backed staged table used to implement Delta DDL operations in the Spark DSv2 connector.
 *
 * <p>The constructor performs the planning step (resolve engine, schema/layout, filtered table
 * properties, and optional UC table info). {@link #commitStagedChanges()} executes the Kernel
 * transaction using the planned state and then invokes a {@link PostCommitAction} to make the
 * result visible to Spark catalogs when required.
 */
public final class DeltaKernelStagedDDLTable implements StagedTable {

  /** Engine info marker used for test verification. */
  public static final String ENGINE_INFO = "kernel-spark-dsv2";

  private final Identifier ident;
  private final String tablePath;
  private final org.apache.spark.sql.types.StructType sparkSchema;
  private final Transform[] partitions;
  private final Map<String, String> allTableProperties;
  private final DdlOperation operation;
  private final PostCommitAction postCommitAction;

  private final Engine engine;
  private final StructType kernelSchema;
  private final Optional<DataLayoutSpec> dataLayoutSpecOpt;
  private final Map<String, String> filteredTableProperties;

  private final Optional<UCTableInfo> ucTableInfoOpt;

  public DeltaKernelStagedDDLTable(
      SparkSession spark,
      String catalogName,
      Identifier ident,
      String tablePath,
      org.apache.spark.sql.types.StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> allTableProperties,
      DdlOperation operation,
      PostCommitAction postCommitAction) {
    requireNonNull(spark, "spark is null");
    requireNonNull(catalogName, "catalogName is null");
    this.ident = requireNonNull(ident, "ident is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.sparkSchema = requireNonNull(sparkSchema, "sparkSchema is null");
    this.partitions = requireNonNull(partitions, "partitions is null");
    this.allTableProperties = requireNonNull(allTableProperties, "allTableProperties is null");
    this.operation = requireNonNull(operation, "operation is null");
    this.postCommitAction = requireNonNull(postCommitAction, "postCommitAction is null");

    final Configuration hadoopConf = spark.sessionState().newHadoopConf();
    this.engine = DefaultEngine.create(hadoopConf);

    this.kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    this.dataLayoutSpecOpt = toDataLayoutSpec(partitions);

    final Map<String, String> filtered = filterTableProperties(allTableProperties);

    // Compatibility: the UC table ID property was renamed from `ucTableId` to
    // `io.unitycatalog.tableId`. Normalize old -> new, and drop the old key if both exist.
    String oldUcTableId = filtered.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldUcTableId != null && !oldUcTableId.isEmpty()) {
      filtered.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldUcTableId);
    }

    // Never persist test-only markers.
    filtered.remove("test.simulateUC");
    this.filteredTableProperties = Collections.unmodifiableMap(filtered);

    if (CatalogTableUtils.isUnityCatalogManagedTableFromProperties(this.filteredTableProperties)) {
      String ucTableId = filteredTableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
      if (ucTableId == null || ucTableId.isEmpty()) {
        throw new IllegalArgumentException(
            "UC-managed table is missing required table id property '"
                + UCCommitCoordinatorClient.UC_TABLE_ID_KEY
                + "'.");
      }
      UCCatalogConfig resolved = UCUtils.resolveCatalogConfig(spark, catalogName);
      this.ucTableInfoOpt =
          Optional.of(
              new UCTableInfo(ucTableId, tablePath, resolved.uri(), asJava(resolved.authConfig())));
    } else {
      this.ucTableInfoOpt = Optional.empty();
    }
  }

  @Override
  public String name() {
    return ident.name();
  }

  @Override
  public org.apache.spark.sql.types.StructType schema() {
    return sparkSchema;
  }

  @Override
  public Transform[] partitioning() {
    return partitions;
  }

  @Override
  public Map<String, String> properties() {
    return allTableProperties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }

  @Override
  public void commitStagedChanges() {
    // Metadata-only CREATE TABLE: commit without data actions.
    // When CTAS/RTAS is implemented, the same commit path will accept data actions from the writer.
    try {
      commitKernelTransaction(CloseableIterable.emptyIterable());
      postCommitAction.execute();
    } catch (Throwable t) {
      try {
        postCommitAction.abort(t);
      } catch (Throwable ignored) {
        // Best effort: preserve original failure.
      }
      throw t;
    }
  }

  private void commitKernelTransaction(CloseableIterable<Row> dataActions) {
    try {
      if (ucTableInfoOpt.isPresent()) {
        commitUCManagedCreate(ucTableInfoOpt.get(), dataActions);
      } else {
        commitFilesystemCreate(dataActions);
      }
    } catch (TableAlreadyExistsException tae) {
      // Spark's TableAlreadyExistsException extends AnalysisException (checked), but
      // commitStagedChanges() cannot declare checked exceptions. sneakyThrow bypasses this;
      // Spark's analyzer catches AnalysisException for proper error messaging.
      org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException sparkException =
          new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident);
      sparkException.initCause(tae);
      DeltaKernelStagedDDLTable.<RuntimeException>sneakyThrow(sparkException);
    }
  }

  @Override
  public void abortStagedChanges() {
    postCommitAction.abort(new RuntimeException("aborted"));
  }

  private void commitFilesystemCreate(CloseableIterable<Row> dataActions) {
    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO);
    commit(builder, dataActions);
  }

  private void commitUCManagedCreate(UCTableInfo tableInfo, CloseableIterable<Row> dataActions) {
    final TokenProvider tokenProvider = TokenProvider.create(tableInfo.getAuthConfig());
    try (UCClient ucClient = new UCTokenBasedRestClient(tableInfo.getUcUri(), tokenProvider)) {
      UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
      CreateTableTransactionBuilder builder =
          ucCatalogClient.buildCreateTableTransaction(
              tableInfo.getTableId(), tableInfo.getTablePath(), kernelSchema, ENGINE_INFO);
      commit(builder, dataActions);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private void commit(CreateTableTransactionBuilder builder, CloseableIterable<Row> dataActions) {
    if (!filteredTableProperties.isEmpty()) {
      builder = builder.withTableProperties(filteredTableProperties);
    }
    if (dataLayoutSpecOpt.isPresent()) {
      builder = builder.withDataLayoutSpec(dataLayoutSpecOpt.get());
    }
    Transaction txn = builder.build(engine);
    txn.commit(engine, dataActions);
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    requireNonNull(partitions, "partitions is null");
    if (partitions.length == 0) {
      return Optional.empty();
    }
    List<String> names = PartitionTransformUtils.extractPartitionColumnNames(partitions);
    List<io.delta.kernel.expressions.Column> cols = new ArrayList<>(names.size());
    for (String name : names) {
      cols.add(new io.delta.kernel.expressions.Column(name));
    }
    return Optional.of(DataLayoutSpec.partitioned(cols));
  }

  private static Map<String, String> filterTableProperties(Map<String, String> allTableProperties) {
    requireNonNull(allTableProperties, "allTableProperties is null");
    final Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> e : allTableProperties.entrySet()) {
      String key = e.getKey();
      if (key == null) {
        continue;
      }
      if (SparkDDLPropertyUtils.isSparkReservedPropertyKey(key)) {
        continue;
      }
      result.put(key, e.getValue());
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }
}
