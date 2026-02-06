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

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;

/**
 * Kernel-backed staged table for metadata-only CREATE TABLE.
 *
 * <p>This class is scaffolding for future CTAS/RTAS support: it captures the planned table schema,
 * layout, and table properties, and can later be extended to implement {@code SupportsWrite}.
 *
 * <p>Today, {@link #commitStagedChanges()} commits only protocol + metadata (no data actions).
 */
public final class DeltaKernelStagedCreateTable implements StagedTable {

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
   * Create a Kernel-backed staged table for metadata-only {@code CREATE TABLE}.
   *
   * <p>The constructor performs the "planning" step and caches the resolved state needed for the
   * eventual commit:
   *
   * <ul>
   *   <li>Initializes a Kernel {@link Engine} using the session Hadoop configuration
   *   <li>Converts Spark schema to Kernel schema
   *   <li>Converts Spark partition transforms to a Kernel {@link DataLayoutSpec} (identity-only)
   *   <li>Filters Spark catalog properties down to user/Delta properties (V1-compatible), and
   *       translates legacy UC table id key to the current key
   *   <li>Detects UC-managed tables from properties and, when present, resolves {@link UCTableInfo}
   *       (UC URI + auth config) via {@link UCUtils}
   * </ul>
   *
   * <p>{@link #commitStagedChanges()} then uses this planned state to commit protocol+metadata to
   * the Delta log (no data actions). For catalog-based tables, callers may provide {@code
   * postCommitHook} to register the table in Spark's catalog after a successful Kernel commit.
   */
  public DeltaKernelStagedCreateTable(
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
    this.filteredTableProperties = filteredProperties;

    String ucTableId = filteredTableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId != null) {
      this.ucTableInfoOpt =
          Optional.of(UCUtils.buildTableInfo(ucTableId, tablePath, spark, catalogName));
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
    try {
      if (ucTableInfoOpt.isPresent()) {
        commitUCManagedCreate(ucTableInfoOpt.get());
      } else {
        commitFilesystemCreate();
      }
    } catch (TableAlreadyExistsException tae) {
      // Spark's TableAlreadyExistsException is checked in Java, but StagedTable doesn't declare
      // checked exceptions. Use a "sneaky throw" to preserve Spark semantics.
      throwSparkTableAlreadyExists(ident);
    }

    if (postCommitHook != null) {
      postCommitHook.run();
    }
  }

  @Override
  public void abortStagedChanges() {
    // No-op for metadata-only create. Future work: cleanup staging artifacts for CTAS/RTAS.
  }

  private void commitFilesystemCreate() {
    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO)
            .withTableProperties(filteredTableProperties);
    commitMetadataOnly(builder);
  }

  private void commitUCManagedCreate(UCTableInfo tableInfo) {
    final TokenProvider tokenProvider = TokenProvider.create(tableInfo.getAuthConfig());

    // The committer created by UCCatalogManagedClient holds onto the UCClient, so keep it open for
    // the duration of the transaction commit.
    try (UCClient ucClient = new UCTokenBasedRestClient(tableInfo.getUcUri(), tokenProvider)) {
      UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
      CreateTableTransactionBuilder builder =
          ucCatalogClient
              .buildCreateTableTransaction(
                  tableInfo.getTableId(), tableInfo.getTablePath(), kernelSchema, ENGINE_INFO)
              .withTableProperties(filteredTableProperties);
      // Note: per UCCatalogManagedClient contract, a UC Tables API call may be required after
      // 000.json is committed to inform UC of successful create.
      commitMetadataOnly(builder);
    } catch (java.io.IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private void commitMetadataOnly(CreateTableTransactionBuilder builder) {
    if (dataLayoutSpecOpt.isPresent()) {
      builder = builder.withDataLayoutSpec(dataLayoutSpecOpt.get());
    }
    Transaction txn = builder.build(engine);
    txn.commit(engine, CloseableIterable.emptyIterable());
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    requireNonNull(partitions, "partitions is null");
    if (partitions.length == 0) {
      return Optional.empty();
    }

    final List<Column> partitionCols = new ArrayList<>(partitions.length);
    for (Transform transform : partitions) {
      // Only support identity partitioning transforms (PARTITIONED BY col1, col2, ...).
      if (!(transform instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "Partitioning by expressions is not supported: " + transform.name());
      }
      NamedReference[] refs = transform.references();
      if (refs == null || refs.length != 1) {
        throw new IllegalArgumentException("Invalid partition transform: " + transform);
      }
      String[] fieldNames = refs[0].fieldNames();
      if (fieldNames == null || fieldNames.length != 1) {
        throw new UnsupportedOperationException(
            "Partition columns must be top-level columns: " + refs[0].describe());
      }
      partitionCols.add(new Column(fieldNames[0]));
    }
    return Optional.of(DataLayoutSpec.partitioned(partitionCols));
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
      switch (key) {
        case TableCatalog.PROP_LOCATION:
        case TableCatalog.PROP_PROVIDER:
        case TableCatalog.PROP_COMMENT:
        case TableCatalog.PROP_OWNER:
        case TableCatalog.PROP_EXTERNAL:
        case "path":
        case "option.path":
          continue;
        default:
          break;
      }
      result.put(key, e.getValue());
    }
    return result;
  }

  private static void throwSparkTableAlreadyExists(Identifier ident) {
    DeltaKernelStagedCreateTable.<RuntimeException>sneakyThrow(
        new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident));
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }
}
