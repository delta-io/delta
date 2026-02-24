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
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericColumnVector;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StringType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
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
import org.apache.spark.sql.types.StructType;

/**
 * Coordinator for CREATE TABLE version-0 commits using the DSv2/Kernel path.
 *
 * <p>This class centralizes CREATE TABLE commit mechanics so callers can keep catalog routing thin.
 * Callers can pass either empty actions (metadata-only create) or non-empty data actions (future
 * CTAS-style flows).
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
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);
    Engine engine = createKernelEngine(spark, properties);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.forCreateTable(location, engine, properties, catalogName, spark);
    Optional<DataLayoutSpec> dataLayoutSpec = toDataLayoutSpec(partitions);
    Transaction txn =
        snapshotManager.buildCreateTableTransaction(
            kernelSchema, tableProperties, dataLayoutSpec, engineInfo);
    txn.commit(engine, dataActions);
  }

  /**
   * Commits CREATE TABLE version 0 using pre-generated data action JSON from the legacy writer.
   *
   * <p>The JSON entries must be Delta log action lines (single-action JSON objects). This method
   * parses them into Kernel single-action rows and commits them alongside metadata/protocol built
   * by the CREATE TABLE transaction.
   */
  public static void commitCreateTableVersion0FromLegacyWriterActions(
      String location,
      StructType schema,
      List<String> partitionColumns,
      Map<String, String> properties,
      SparkSession spark,
      String catalogName,
      String engineInfo,
      List<String> dataActionJson) {
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException("Unable to resolve location for CREATE TABLE");
    }

    Map<String, String> createProperties = new HashMap<>(properties);
    createProperties.putIfAbsent(TableCatalog.PROP_LOCATION, location);
    Map<String, String> tableProperties = filterDsv2Properties(createProperties);
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(schema);
    Engine engine = createKernelEngine(spark, createProperties);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.forCreateTable(
            location, engine, createProperties, catalogName, spark);
    Optional<DataLayoutSpec> dataLayoutSpec = toDataLayoutSpec(partitionColumns);
    Transaction txn =
        snapshotManager.buildCreateTableTransaction(
            kernelSchema, tableProperties, dataLayoutSpec, engineInfo);
    txn.commit(engine, toDataActionRows(engine, dataActionJson));
  }

  /**
   * Strips filesystem credential keys ({@code fs.*}, {@code dfs.*}, {@code option.fs.*}, {@code
   * option.dfs.*}) from properties while keeping DSv2 catalog coordination keys.
   */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered
        .entrySet()
        .removeIf(
            entry -> {
              String key = entry.getKey();
              String effectiveKey =
                  key.startsWith("option.") ? key.substring("option.".length()) : key;
              return DeltaTableUtils.validDeltaTableHadoopPrefixes()
                  .exists(prefix -> effectiveKey.startsWith(prefix));
            });
    return filtered;
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
      String effectiveKey =
          entry.getKey().startsWith("option.")
              ? entry.getKey().substring("option.".length())
              : entry.getKey();
      if (DeltaTableUtils.validDeltaTableHadoopPrefixes()
          .exists(prefix -> effectiveKey.startsWith(prefix))) {
        fsOptions.put(effectiveKey, entry.getValue());
      }
    }
    Configuration hadoopConf =
        spark.sessionState().newHadoopConfWithOptions(ScalaUtils.toScalaMap(fsOptions));
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

  private static Optional<DataLayoutSpec> toDataLayoutSpec(List<String> partitionColumns) {
    if (partitionColumns == null || partitionColumns.isEmpty()) {
      return Optional.empty();
    }
    List<Column> kernelPartitionColumns = new ArrayList<>();
    for (String partitionColumn : partitionColumns) {
      if (partitionColumn != null && !partitionColumn.isEmpty()) {
        kernelPartitionColumns.add(new Column(partitionColumn));
      }
    }
    if (kernelPartitionColumns.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(DataLayoutSpec.partitioned(kernelPartitionColumns));
  }

  private static CloseableIterable<Row> toDataActionRows(
      Engine engine, List<String> dataActionJson) {
    if (dataActionJson == null || dataActionJson.isEmpty()) {
      return CloseableIterable.emptyIterable();
    }

    GenericColumnVector jsonVector = new GenericColumnVector(dataActionJson, StringType.STRING);
    io.delta.kernel.data.ColumnarBatch parsedActions =
        engine.getJsonHandler().parseJson(jsonVector, SingleAction.FULL_SCHEMA, Optional.empty());
    return CloseableIterable.inMemoryIterable(parsedActions.getRows());
  }

  /** Filters out DSv2-specific keys and filesystem options that are not Delta table properties. */
  private static Map<String, String> filterDsv2Properties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.remove(TableCatalog.PROP_LOCATION);
    filtered.remove(TableCatalog.PROP_PROVIDER);
    filtered.remove(TableCatalog.PROP_COMMENT);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_EXTERNAL);
    filtered.remove("path");
    filtered.remove("option.path");
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    filtered.remove("ucTableId");
    filtered
        .entrySet()
        .removeIf(
            entry -> {
              String key = entry.getKey();
              String effectiveKey =
                  key.startsWith("option.") ? key.substring("option.".length()) : key;
              return DeltaTableUtils.validDeltaTableHadoopPrefixes()
                  .exists(prefix -> effectiveKey.startsWith(prefix));
            });
    return filtered;
  }
}
