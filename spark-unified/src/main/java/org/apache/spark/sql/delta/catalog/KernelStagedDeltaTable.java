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

package org.apache.spark.sql.delta.catalog;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.types.StructType;

/**
 * Staged table implementation for Kernel-backed CREATE TABLE.
 *
 * <p>This class encapsulates the metadata-only CREATE flow using Kernel create-table transactions,
 * with an extensible shape for CTAS/RTAS in future phases.
 */
final class KernelStagedDeltaTable implements StagedTable, SupportsWrite {

  private static final String ENGINE_INFO = "delta-spark-v2-connector";

  private final SparkSession spark;
  private final Identifier ident;
  private final ResolvedCreateRequest request;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final StructType schema;

  KernelStagedDeltaTable(SparkSession spark, Identifier ident, ResolvedCreateRequest request) {
    this.spark = requireNonNull(spark, "spark is null");
    this.ident = requireNonNull(ident, "ident is null");
    this.request = requireNonNull(request, "request is null");
    this.properties = request.properties();
    this.partitions = request.partitions();
    this.schema = request.schema();
  }

  @Override
  public void commitStagedChanges() {
    Engine engine = createEngine();
    try {
      CreateTableTransactionBuilder builder = buildCreateTableTransaction(engine);
      Transaction transaction = builder.build(engine);
      transaction.commit(engine, CloseableIterable.emptyIterable());
    } catch (io.delta.kernel.exceptions.TableAlreadyExistsException e) {
      throw QueryCompilationErrors.tableAlreadyExistsError(ident);
    }
  }

  @Override
  public void abortStagedChanges() {
    // No-op for phase 1: nothing to clean up for metadata-only create.
  }

  @Override
  public String name() {
    return ident.name();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Transform[] partitioning() {
    return partitions;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    // Advertise BATCH_WRITE to allow staged CTAS/RTAS in future. For phase 1,
    // newWriteBuilder throws a targeted error.
    return Collections.unmodifiableSet(EnumSet.of(TableCapability.BATCH_WRITE));
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    throw DeltaErrors.unsupportedWriteStagedTable(ident.toString());
  }

  private CreateTableTransactionBuilder buildCreateTableTransaction(Engine engine) {
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);
    CreateTableTransactionBuilder builder;

    if (request.isUCManaged()) {
      UCCatalogManagedClient ucClient = buildUcCatalogClient();
      builder =
          ucClient.buildCreateTableTransaction(
              request.ucTableId(), request.tablePath(), kernelSchema, ENGINE_INFO);
    } else {
      builder =
          io.delta.kernel.TableManager.buildCreateTableTransaction(
              request.tablePath(), kernelSchema, ENGINE_INFO);
    }

    if (!request.tableProperties().isEmpty()) {
      builder = builder.withTableProperties(request.tableProperties());
    }

    Optional<io.delta.kernel.transaction.DataLayoutSpec> layoutSpec = request.dataLayoutSpec();
    if (layoutSpec.isPresent()) {
      builder = builder.withDataLayoutSpec(layoutSpec.get());
    }

    return builder;
  }

  private Engine createEngine() {
    Map<String, String> fsOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      for (String prefix : DeltaTableUtils.validDeltaTableHadoopPrefixes()) {
        if (entry.getKey().startsWith(prefix)) {
          fsOptions.put(entry.getKey(), entry.getValue());
          break;
        }
      }
    }

    Configuration hadoopConf =
        spark
            .sessionState()
            .newHadoopConfWithOptions(ScalaUtils.toScalaMap(fsOptions));
    return DefaultEngine.create(hadoopConf);
  }

  private UCCatalogManagedClient buildUcCatalogClient() {
    scala.collection.immutable.Map<String, UCCatalogConfig> configs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);
    String catalogName = spark.sessionState().catalogManager().currentCatalog().name();
    scala.Option<UCCatalogConfig> configOpt = configs.get(catalogName);
    final UCCatalogConfig config;
    if (configOpt.isEmpty()) {
      // Fallback: if only one UC config exists, use it.
      if (configs.size() == 1) {
        config = configs.head()._2();
      } else {
        throw new IllegalArgumentException(
            "Unity Catalog configuration not found for catalog '" + catalogName + "'.");
      }
    } else {
      config = configOpt.get();
    }
    java.util.Map<String, String> authConfig =
        scala.jdk.javaapi.CollectionConverters.asJava(config.authConfig());
    TokenProvider tokenProvider = TokenProvider.create(authConfig);
    UCClient ucClient = new UCTokenBasedRestClient(config.uri(), tokenProvider);
    return new UCCatalogManagedClient(ucClient);
  }
}
