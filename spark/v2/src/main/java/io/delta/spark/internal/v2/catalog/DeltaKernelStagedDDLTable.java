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

/**
 * Kernel-backed staged table used to implement Delta DDL operations in the Spark DSv2 connector.
 *
 * <p>This class is meant to be the single place where Spark DSv2 DDL entrypoints (for example,
 * {@code CREATE TABLE}, {@code REPLACE TABLE}, CTAS/RTAS) are translated into Delta Kernel
 * transactions. Higher-level catalog code resolves Spark inputs (identifier, location, schema,
 * partitioning, and properties), then delegates to this staged table to plan and commit the
 * corresponding Kernel operation.
 *
 * <ul>
 *   <li>The constructor performs the planning step (resolve Engine, schema, partition layout, and
 *       filtered table properties, plus optional UC table info).
 *   <li>{@link #commitStagedChanges()} executes the Kernel create-table transaction using the
 *       planned state, selecting the appropriate committer (filesystem vs UC/CCv2).
 * </ul>
 *
 * <p>Commit execution is expressed in terms of a Kernel transaction plus an iterable of data
 * actions; metadata-only DDL uses an empty iterable.
 */
public final class DeltaKernelStagedDDLTable implements StagedTable {

  /** Engine info marker used for test verification. */
  public static final String ENGINE_INFO = "kernel-spark-dsv2";

  private final Identifier ident;
  private final String tablePath;
  private final org.apache.spark.sql.types.StructType sparkSchema;
  private final Transform[] partitions;
  private final Map<String, String> allTableProperties;
  private final Runnable postCommitHook;

  private final Engine engine;
  private final StructType kernelSchema;
  private final Optional<DataLayoutSpec> dataLayoutSpecOpt;
  private final Map<String, String> filteredTableProperties;

  // UC-managed table info (optional). When present, commits go through UC's CatalogCommitter
  private final Optional<UCTableInfo> ucTableInfoOpt;

  /**
   * Create a Kernel-backed staged table for a Delta DDL operation.
   *
   * <p>The constructor performs the <em>planning</em> step: it converts Spark inputs into a
   * Kernel-ready plan and caches the resolved state needed for the eventual commit (engine, schema,
   * layout, filtered properties, and optional UC table info).
   *
   * <p>{@link #commitStagedChanges()} then uses this planned state to execute the Kernel
   * transaction. For catalog-based tables, callers may provide {@code postCommitHook} to register
   * the table in Spark's catalog after a successful commit.
   */
  public DeltaKernelStagedDDLTable(
      SparkSession spark,
      String catalogName,
      Identifier ident,
      String tablePath,
      org.apache.spark.sql.types.StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> allTableProperties,
      Runnable postCommitHook) {
    requireNonNull(spark, "spark is null");
    this.ident = requireNonNull(ident, "ident is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.sparkSchema = requireNonNull(sparkSchema, "sparkSchema is null");
    this.partitions = requireNonNull(partitions, "partitions is null");
    this.allTableProperties = requireNonNull(allTableProperties, "allTableProperties is null");
    this.postCommitHook = postCommitHook;

    final Configuration hadoopConf = spark.sessionState().newHadoopConf();
    this.engine = DefaultEngine.create(hadoopConf);

    this.kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    this.dataLayoutSpecOpt = toDataLayoutSpec(this.partitions);

    final Map<String, String> filteredProperties = filterTableProperties(this.allTableProperties);

    // Compatibility: the UC table ID property was renamed from `ucTableId` to
    // `io.unitycatalog.tableId`. Normalize old -> new, and drop the old key if both exist.
    String oldUcTableId = filteredProperties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldUcTableId != null && !oldUcTableId.isEmpty()) {
      filteredProperties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldUcTableId);
    }

    // Never persist test-only markers.
    filteredProperties.remove("test.simulateUC");
    this.filteredTableProperties = Collections.unmodifiableMap(filteredProperties);

    String ucTableId = filteredTableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId != null) {
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

  /**
   * Finalize the staged table creation.
   *
   * <p>This is the entrypoint Spark calls to commit the staged DDL. For metadata-only CREATE TABLE,
   * the commit is executed with an empty iterable of data actions. For CTAS/RTAS, the commit is
   * executed with the data actions produced by the writer.
   */
  @Override
  public void commitStagedChanges() {
    commitKernelTransaction(CloseableIterable.emptyIterable());
  }

  /**
   * Execute the Kernel create-table transaction using the planned state.
   *
   * <p>Flow:
   *
   * <ul>
   *   <li>Select commit coordination based on whether UC table info was resolved at plan time.
   *   <li>Build a Kernel create-table transaction builder (filesystem or UC/CCv2).
   *   <li>Apply table properties and layout spec.
   *   <li>Commit the transaction with the provided data actions (if any).
   *   <li>Run the optional post-commit hook (e.g., catalog registration).
   * </ul>
   */
  private void commitKernelTransaction(CloseableIterable<Row> dataActions) {
    try {
      // Choose commit coordination at execution time (filesystem vs UC/CCv2) based on plan-time UC
      // resolution. This keeps the commit path linear: pick builder -> apply plan -> commit
      // actions.
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

    if (postCommitHook != null) {
      // The two-phase approach (Kernel commit -> catalog registration) has inherent gaps:
      //
      // 1. TOCTOU: V2CreateTableHelper checks catalog existence before this commit. Another
      //    session could create the table between the check and the Kernel commit. The early
      //    check is best-effort to avoid the worse failure mode below.
      //
      // 2. Orphaned 000.json: if postCommitHook fails (e.g. HMS unavailable), the Kernel commit
      //    has already been written. The table exists on the filesystem but has no catalog entry.
      //    V1 handles this inside CreateDeltaTableCommand's single-command execution context.
      //
      // TODO: Consider adding compensating cleanup or an operator-visible warning for case 2.
      postCommitHook.run();
    }
  }

  @Override
  public void abortStagedChanges() {
    // No-op for metadata-only create.
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
      // Note: per UCCatalogManagedClient contract, a UC Tables API call may be required after
      // 000.json is committed to inform UC of successful create.
      commit(builder, dataActions);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  /** Apply planned properties/layout to the builder, build, and commit. */
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

  /** Convert Spark partition transforms to a Kernel {@link DataLayoutSpec}. */
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

  /**
   * Filter Spark-provided properties down to user/delta properties.
   *
   * <p>Matches the filtering done by V1 {@code AbstractDeltaCatalog#createDeltaTable}.
   */
  private static Map<String, String> filterTableProperties(Map<String, String> allTableProperties) {
    requireNonNull(allTableProperties, "allTableProperties is null");
    final Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> e : allTableProperties.entrySet()) {
      final String key = e.getKey();
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

  /**
   * Re-throw a checked exception without declaring it. Spark's {@code TableAlreadyExistsException}
   * extends {@code AnalysisException} (checked), but {@link StagedTable#commitStagedChanges()} does
   * not declare checked exceptions. We cannot wrap in {@code RuntimeException} because Spark's
   * analyzer catches {@code AnalysisException} specifically for proper error messaging.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }
}
