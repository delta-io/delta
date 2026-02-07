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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.CommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Kernel-backed staged table for metadata-only CREATE operations.
 *
 * <p>This uses the Delta Kernel create table transaction and commits empty actions to create
 * _delta_log/00000000000000000000.json.
 */
public final class KernelStagedDeltaTable implements StagedTable {

  private final SparkSession spark;
  private final Identifier ident;
  private final ResolvedCreateRequest request;
  private final Optional<String> ucCatalogName;
  private final Optional<UCClient> ucClient;

  public KernelStagedDeltaTable(
      SparkSession spark,
      Identifier ident,
      ResolvedCreateRequest request) {
    this(spark, ident, request, Optional.empty(), Optional.empty());
  }

  public KernelStagedDeltaTable(
      SparkSession spark,
      Identifier ident,
      ResolvedCreateRequest request,
      String ucCatalogName) {
    this(spark, ident, request, Optional.of(requireNonNull(ucCatalogName, "ucCatalogName is null")), Optional.empty());
  }

  public KernelStagedDeltaTable(
      SparkSession spark,
      Identifier ident,
      ResolvedCreateRequest request,
      UCClient ucClient) {
    this(spark, ident, request, Optional.empty(), Optional.of(ucClient));
  }

  private KernelStagedDeltaTable(
      SparkSession spark,
      Identifier ident,
      ResolvedCreateRequest request,
      Optional<String> ucCatalogName,
      Optional<UCClient> ucClient) {
    this.spark = requireNonNull(spark, "spark is null");
    this.ident = requireNonNull(ident, "ident is null");
    this.request = requireNonNull(request, "request is null");
    this.ucCatalogName = requireNonNull(ucCatalogName, "ucCatalogName is null");
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
  }

  @Override
  public String name() {
    return ident.name();
  }

  @Override
  public org.apache.spark.sql.types.StructType schema() {
    return request.getSchema();
  }

  @Override
  public Transform[] partitioning() {
    List<String> columns = request.getPartitionColumns();
    if (columns.isEmpty()) {
      return new Transform[0];
    }
    Transform[] transforms = new Transform[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      transforms[i] = Expressions.identity(columns.get(i));
    }
    return transforms;
  }

  @Override
  public Map<String, String> properties() {
    return request.getProperties();
  }

  @Override
  public java.util.Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }

  @Override
  public void commitStagedChanges() {
    Engine engine = DefaultEngine.create(spark.sessionState().newHadoopConf());
    if (request.isUCManaged() && ucClient.isEmpty()) {
      try (UCClient client = buildUcClient()) {
        commitWithClient(engine, client);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close UC client", e);
      }
      return;
    }
    commitWithClient(engine, ucClient.orElse(null));
  }

  @Override
  public void abortStagedChanges() {}

  private void commitWithClient(Engine engine, UCClient client) {
    CreateTableTransactionBuilder builder = createTransactionBuilder(engine, client);
    Transaction transaction = builder.build(engine);
    transaction.commit(engine, CloseableIterable.emptyIterable());
  }

  private CreateTableTransactionBuilder createTransactionBuilder(Engine engine, UCClient client) {
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(request.getSchema());
    if (request.isUCManaged()) {
      if (client == null) {
        throw new IllegalStateException("UC client required for UC-managed create");
      }
      UCCatalogManagedClient catalogManagedClient = new UCCatalogManagedClient(client);
      String resolvedPath = resolvePath(engine, request.getTablePath());
      CreateTableTransactionBuilder builder =
          catalogManagedClient.buildCreateTableTransaction(
              request.getUcTableId().orElseThrow(() -> new IllegalStateException("ucTableId missing")),
              resolvedPath,
              kernelSchema,
              buildEngineInfo());
      return applyTableProperties(builder).withDataLayoutSpec(request.getDataLayoutSpec());
    }
    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(
            request.getTablePath(), kernelSchema, buildEngineInfo());
    return applyTableProperties(builder).withDataLayoutSpec(request.getDataLayoutSpec());
  }

  private CreateTableTransactionBuilder applyTableProperties(
      CreateTableTransactionBuilder builder) {
    Map<String, String> props = new HashMap<>(request.getProperties());
    if (request.isUCManaged()) {
      props.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
      props.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    }
    if (!props.isEmpty()) {
      builder = builder.withTableProperties(props);
    }
    return builder;
  }

  private String buildEngineInfo() {
    try {
      String sparkVersion = spark.version();
      String deltaVersion = io.delta.package$.MODULE$.VERSION();
      return "Apache-Spark/" + sparkVersion + " Delta-Lake/" + deltaVersion;
    } catch (Exception e) {
      return "<unknown>";
    }
  }

  private static String resolvePath(Engine engine, String path) {
    try {
      return engine.getFileSystemClient().resolvePath(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to resolve path " + path, e);
    }
  }

  private UCClient buildUcClient() {
    String catalogName =
        ucCatalogName.orElseGet(
            () -> spark.sessionState().catalogManager().currentCatalog().name());
    CommitCoordinatorClient coordinator;
    try {
      coordinator = UCCommitCoordinatorBuilder$.MODULE$.buildForCatalog(spark, catalogName);
    } catch (IllegalArgumentException e) {
      scala.collection.immutable.Map<String, UCCatalogConfig> configs =
          UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);
      String available = String.join(", ", CollectionConverters.asJava(configs.keySet()));
      throw new IllegalArgumentException(
          "Unity Catalog configuration not found for catalog '" + catalogName
              + "'. Available UC catalogs: [" + available + "]",
          e);
    }
    if (!(coordinator instanceof UCCommitCoordinatorClient)) {
      throw new IllegalStateException(
          "Unexpected commit coordinator type for catalog '" + catalogName + "'");
    }
    return ((UCCommitCoordinatorClient) coordinator).ucClient;
  }
}
