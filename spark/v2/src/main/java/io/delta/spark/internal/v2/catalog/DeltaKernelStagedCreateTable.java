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
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
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
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;

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

  private final SparkSession spark;
  private final String catalogName;
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
  // (CCv2).
  private final Optional<String> ucTableIdOpt;
  private final Optional<UcConfig> ucConfigOpt;

  public DeltaKernelStagedCreateTable(
      SparkSession spark,
      String catalogName,
      Identifier ident,
      String tablePath,
      org.apache.spark.sql.types.StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> allTableProperties,
      Runnable postCommitHook) {
    this.spark = requireNonNull(spark, "spark is null");
    this.catalogName = catalogName; // may be null for path-based tables
    this.ident = requireNonNull(ident, "ident is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.sparkSchema = requireNonNull(sparkSchema, "sparkSchema is null");
    this.partitions = requireNonNull(partitions, "partitions is null");
    this.allTableProperties = requireNonNull(allTableProperties, "allTableProperties is null");
    this.postCommitHook = postCommitHook;

    final Configuration hadoopConf = spark.sessionState().newHadoopConf();
    this.engine = DefaultEngine.create(hadoopConf);

    this.kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    this.dataLayoutSpecOpt = toDataLayoutSpec(partitions);

    final Map<String, String> filtered = filterTableProperties(allTableProperties);
    translateUcTableIdProperty(filtered);
    // Never persist test-only markers.
    filtered.remove("test.simulateUC");
    this.filteredTableProperties = filtered;

    this.ucTableIdOpt =
        Optional.ofNullable(filteredTableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY));
    if (ucTableIdOpt.isPresent()) {
      this.ucConfigOpt = Optional.of(resolveUcConfig(spark, catalogName, tablePath));
    } else {
      this.ucConfigOpt = Optional.empty();
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
      if (ucTableIdOpt.isPresent()) {
        commitUCManagedCreate();
      } else {
        commitFileSystemManagedCreate();
      }
    } catch (TableAlreadyExistsException tae) {
      DeltaKernelStagedCreateTable.<RuntimeException>sneakyThrow(
          new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident));
    }

    if (postCommitHook != null) {
      postCommitHook.run();
    }
  }

  @Override
  public void abortStagedChanges() {
    // No-op for metadata-only create. Future work: cleanup staging artifacts for CTAS/RTAS.
  }

  private void commitFileSystemManagedCreate() {
    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO)
            .withTableProperties(filteredTableProperties);
    builder = withLayoutSpecIfPresent(builder);
    Transaction txn = builder.build(engine);
    txn.commit(engine, CloseableIterable.emptyIterable());
  }

  private void commitUCManagedCreate() {
    final UcConfig ucConfig = ucConfigOpt.orElseThrow();
    final TokenProvider tokenProvider = TokenProvider.create(ucConfig.authConfig);

    // The committer created by UCCatalogManagedClient holds onto the UCClient, so keep it open for
    // the duration of the transaction commit.
    try (UCClient ucClient = new UCTokenBasedRestClient(ucConfig.ucUri, tokenProvider)) {
      UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);
      CreateTableTransactionBuilder builder =
          ucCatalogClient
              .buildCreateTableTransaction(ucTableIdOpt.get(), tablePath, kernelSchema, ENGINE_INFO)
              .withTableProperties(filteredTableProperties);
      builder = withLayoutSpecIfPresent(builder);
      Transaction txn = builder.build(engine);
      txn.commit(engine, CloseableIterable.emptyIterable());
    } catch (java.io.IOException ioe) {
      throw new RuntimeException("Failed to close UC client after create table", ioe);
    }
  }

  private CreateTableTransactionBuilder withLayoutSpecIfPresent(CreateTableTransactionBuilder b) {
    return dataLayoutSpecOpt.map(b::withDataLayoutSpec).orElse(b);
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    if (partitions.length == 0) {
      return Optional.empty();
    }

    final List<Column> partitionCols = new ArrayList<>();
    for (Transform transform : partitions) {
      // Only support identity partitioning transforms (PARTITIONED BY col1, col2, ...).
      if (!"identity".equalsIgnoreCase(transform.name())) {
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
    final Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> e : allTableProperties.entrySet()) {
      final String key = e.getKey();
      if (key == null) {
        continue;
      }
      if (TableCatalog.PROP_LOCATION.equals(key)
          || TableCatalog.PROP_PROVIDER.equals(key)
          || TableCatalog.PROP_COMMENT.equals(key)
          || TableCatalog.PROP_OWNER.equals(key)
          || TableCatalog.PROP_EXTERNAL.equals(key)
          || "path".equals(key)
          || "option.path".equals(key)) {
        continue;
      }
      result.put(key, e.getValue());
    }
    return result;
  }

  /**
   * Translate the legacy UC table ID key to the new one, matching V1 behavior.
   *
   * <p>If both are set, drop the old one.
   */
  private static void translateUcTableIdProperty(Map<String, String> properties) {
    String oldValue = properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldValue != null && !oldValue.isEmpty()) {
      properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldValue);
    }
  }

  private static UcConfig resolveUcConfig(
      SparkSession spark, String catalogName, String tablePath) {
    if (catalogName == null || catalogName.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create UC-managed table at "
              + tablePath
              + ": catalogName is missing. Expected the catalog plugin name to resolve UC "
              + "configuration.");
    }

    scala.collection.immutable.Map<String, UCCatalogConfig> ucConfigs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);
    scala.Option<UCCatalogConfig> configOpt = ucConfigs.get(catalogName);
    if (configOpt.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create UC-managed table at "
              + tablePath
              + ": Unity Catalog configuration not found for catalog '"
              + catalogName
              + "'.");
    }

    final UCCatalogConfig config = configOpt.get();
    final String ucUri = config.uri();
    final java.util.Map<String, String> authConfig =
        scala.jdk.javaapi.CollectionConverters.asJava(config.authConfig());
    return new UcConfig(ucUri, authConfig);
  }

  private static final class UcConfig {
    private final String ucUri;
    private final java.util.Map<String, String> authConfig;

    private UcConfig(String ucUri, java.util.Map<String, String> authConfig) {
      this.ucUri = requireNonNull(ucUri, "ucUri is null");
      this.authConfig = requireNonNull(authConfig, "authConfig is null");
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }
}
