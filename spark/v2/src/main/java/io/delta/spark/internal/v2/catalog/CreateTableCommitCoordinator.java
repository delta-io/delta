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

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory$;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
import org.apache.spark.sql.types.StructType;

/**
 * Coordinates metadata-only CREATE TABLE commits for the Spark DSv2 connector using Delta Kernel.
 */
public final class CreateTableCommitCoordinator {

  private CreateTableCommitCoordinator() {}

  public static void commitCreateTableVersion0(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      SparkSession spark,
      String catalogName,
      String engineInfo,
      boolean isPathIdentifier,
      CloseableIterable<Row> dataActions) {
    String location = resolveLocation(ident, properties, isPathIdentifier);
    Map<String, String> tableProperties = filterDsv2Properties(properties);
    io.delta.kernel.types.StructType kernelSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(schema);
    Engine engine = createKernelEngine(spark, properties);

    CreateTableTransactionBuilder txnBuilder =
        buildCreateTableTransaction(
            spark, catalogName, location, kernelSchema, tableProperties, engineInfo);

    toDataLayoutSpec(partitions).ifPresent(txnBuilder::withDataLayoutSpec);

    Transaction txn = txnBuilder.build(engine);
    txn.commit(engine, dataActions);
  }

  /** Removes filesystem credential keys while preserving catalog coordination properties. */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    return filtered;
  }

  private static CreateTableTransactionBuilder buildCreateTableTransaction(
      SparkSession spark,
      String catalogName,
      String location,
      io.delta.kernel.types.StructType schema,
      Map<String, String> tableProperties,
      String engineInfo) {
    if (CatalogTableUtils.isUnityCatalogManagedTableFromProperties(tableProperties)) {
      scala.collection.immutable.Map<String, UCCatalogConfig> ucConfigs =
          UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);
      scala.Option<UCCatalogConfig> configOpt = ucConfigs.get(catalogName);
      if (configOpt.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot create UC client for catalog '" + catalogName + "'.");
      }

      UCCatalogConfig config = configOpt.get();
      UCCatalogManagedClient ucCatalogClient =
          new UCCatalogManagedClient(
              UCTokenBasedRestClientFactory$.MODULE$.createUCClient(
                  config.uri(), config.authConfig()));

      String ucTableId = tableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
      if (ucTableId == null || ucTableId.isEmpty()) {
        ucTableId = tableProperties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
      }
      if (ucTableId == null || ucTableId.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot create a UC managed table without property io.unitycatalog.tableId");
      }

      return ucCatalogClient
          .buildCreateTableTransaction(ucTableId, location, schema, engineInfo)
          .withTableProperties(tableProperties);
    }

    return TableManager.buildCreateTableTransaction(location, schema, engineInfo)
        .withTableProperties(tableProperties);
  }

  private static String resolveLocation(
      Identifier ident, Map<String, String> properties, boolean isPathIdentifier) {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location == null) {
      location = properties.get("location");
    }
    if (location == null && isPathIdentifier) {
      location = ident.name();
    }
    if (location == null) {
      throw new IllegalArgumentException("Unable to resolve location for CREATE TABLE " + ident);
    }
    return location;
  }

  private static Engine createKernelEngine(SparkSession spark, Map<String, String> properties) {
    Map<String, String> fsOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (isHadoopOption(key)) {
        String normalizedKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
        fsOptions.put(normalizedKey, entry.getValue());
      }
    }
    Configuration hadoopConf =
        spark
            .sessionState()
            .newHadoopConfWithOptions(
                io.delta.spark.internal.v2.utils.ScalaUtils.toScalaMap(fsOptions));
    return DefaultEngine.create(hadoopConf);
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    List<Column> partitionColumns = new ArrayList<>();
    for (Transform partition : partitions) {
      if (partition.references().length > 0) {
        partitionColumns.add(new Column(partition.references()[0].describe()));
      }
    }
    if (partitionColumns.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(DataLayoutSpec.partitioned(partitionColumns));
  }

  private static Map<String, String> filterDsv2Properties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.remove(TableCatalog.PROP_LOCATION);
    filtered.remove(TableCatalog.PROP_PROVIDER);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_EXTERNAL);
    filtered.remove("path");
    filtered.remove("option.path");
    filtered.remove("test.simulateUC");
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    String oldTableId = filtered.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldTableId != null) {
      filtered.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldTableId);
    }
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    return filtered;
  }

  private static boolean isHadoopOption(String key) {
    String effectiveKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
    return DeltaTableUtils.validDeltaTableHadoopPrefixes().exists(effectiveKey::startsWith);
  }
}
