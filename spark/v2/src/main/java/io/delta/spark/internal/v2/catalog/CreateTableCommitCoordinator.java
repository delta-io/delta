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

import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
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
import org.apache.spark.sql.types.StructType;

/**
 * Small helper for building and committing the kernel create-table transaction used by the new
 * managed Delta create path.
 */
public final class CreateTableCommitCoordinator {
  private static final String LOCATION = TableCatalog.PROP_LOCATION;
  private static final String PROVIDER = TableCatalog.PROP_PROVIDER;
  private static final String COMMENT = TableCatalog.PROP_COMMENT;
  private static final String OWNER = TableCatalog.PROP_OWNER;
  private static final String EXTERNAL = TableCatalog.PROP_EXTERNAL;
  private static final String IS_MANAGED_LOCATION = TableCatalog.PROP_IS_MANAGED_LOCATION;
  private static final String TABLE_TYPE = "table_type";

  private CreateTableCommitCoordinator() {}

  public static void commitCreateTableVersion0(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      SparkSession spark,
      String catalogName,
      String engineInfo) {
    String tablePath = resolveLocation(ident, properties);
    Engine kernelEngine = createKernelEngine(spark, properties);
    Optional<io.delta.spark.internal.v2.snapshot.unitycatalog.UCTableInfo> ucTableInfo =
        UCUtils.extractTableInfoForCreate(tablePath, properties, catalogName, spark);
    if (ucTableInfo.isEmpty()) {
      throw new IllegalArgumentException(
          "Managed CREATE TABLE routing expected Unity Catalog metadata for " + ident);
    }
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.forCreateTable(tablePath, kernelEngine, ucTableInfo);

    Transaction txn =
        snapshotManager.buildCreateTableTransaction(
            SchemaUtils.convertSparkSchemaToKernelSchema(schema),
            filterCreateTableProperties(properties),
            toDataLayoutSpec(partitions),
            engineInfo);
    txn.commit(kernelEngine, CloseableIterable.emptyIterable());
  }

  /**
   * Returns the properties that should be handed to the delegated catalog after the Delta log v0
   * commit succeeds.
   *
   * <p>This removes transient filesystem credentials and normalizes the UC table ID key.
   */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    normalizeUCTableId(filtered);
    filtered.entrySet().removeIf(entry -> isCredentialOrOptionKey(entry.getKey()));
    return filtered;
  }

  private static Map<String, String> filterCreateTableProperties(Map<String, String> properties) {
    Map<String, String> filtered = filterCredentialProperties(properties);
    filtered.remove(LOCATION);
    filtered.remove(PROVIDER);
    filtered.remove(COMMENT);
    filtered.remove(OWNER);
    filtered.remove(EXTERNAL);
    filtered.remove(IS_MANAGED_LOCATION);
    filtered.remove(TABLE_TYPE);
    filtered.remove("path");
    return filtered;
  }

  private static void normalizeUCTableId(Map<String, String> properties) {
    String tableId = properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (tableId != null && !tableId.isEmpty()) {
      properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, tableId);
    }
  }

  private static boolean isCredentialOrOptionKey(String key) {
    String normalizedKey =
        key.startsWith(TableCatalog.OPTION_PREFIX)
            ? key.substring(TableCatalog.OPTION_PREFIX.length())
            : key;
    return normalizedKey.startsWith("fs.")
        || normalizedKey.startsWith("dfs.")
        || normalizedKey.startsWith("option.fs.")
        || normalizedKey.startsWith("option.dfs.")
        || key.startsWith(TableCatalog.OPTION_PREFIX);
  }

  private static Engine createKernelEngine(SparkSession spark, Map<String, String> properties) {
    Map<String, String> hadoopOptions = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          String effectiveKey =
              key.startsWith(TableCatalog.OPTION_PREFIX)
                  ? key.substring(TableCatalog.OPTION_PREFIX.length())
                  : key;
          if (effectiveKey.startsWith("fs.") || effectiveKey.startsWith("dfs.")) {
            hadoopOptions.put(effectiveKey, value);
          }
        });
    Configuration hadoopConf =
        spark.sessionState().newHadoopConfWithOptions(ScalaUtils.toScalaMap(hadoopOptions));
    return DefaultEngine.create(hadoopConf);
  }

  private static Optional<DataLayoutSpec> toDataLayoutSpec(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return Optional.empty();
    }
    List<Column> partitionColumns = new ArrayList<>();
    for (Transform partition : partitions) {
      if (partition.references() != null && partition.references().length > 0) {
        partitionColumns.add(new Column(partition.references()[0].describe()));
      }
    }
    if (partitionColumns.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(DataLayoutSpec.partitioned(partitionColumns));
  }

  private static String resolveLocation(Identifier ident, Map<String, String> properties) {
    String location =
        firstNonEmpty(
            properties.get(LOCATION),
            properties.get("location"),
            properties.get("path"),
            properties.get(TableCatalog.OPTION_PREFIX + "path"));
    if (location == null && isPathIdentifier(ident)) {
      location = ident.name();
    }
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException("Unable to resolve CREATE TABLE location for " + ident);
    }
    return location;
  }

  private static String firstNonEmpty(String... values) {
    for (String value : values) {
      if (value != null && !value.isEmpty()) {
        return value;
      }
    }
    return null;
  }

  private static boolean isPathIdentifier(Identifier ident) {
    return ident.namespace().length == 1 && ident.namespace()[0].equalsIgnoreCase("delta");
  }
}
