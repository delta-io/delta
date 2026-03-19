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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UnityCatalogUtils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCUtils;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
import org.apache.spark.sql.types.StructType;

/**
 * Coordinates metadata-only CREATE TABLE commits for the Spark DSv2 connector using Delta Kernel.
 */
public final class CreateTableCommitCoordinator {
  private static final String DEFAULT_TABLE_PROPERTY_PREFIX =
      "spark.databricks.delta.properties.defaults.";
  private static final String CHECKPOINT_POLICY_KEY = "delta.checkpointPolicy";
  private static final String CHECKPOINT_POLICY_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "checkpointPolicy";
  private static final String ENABLE_DELETION_VECTORS_KEY = "delta.enableDeletionVectors";
  private static final String ENABLE_DELETION_VECTORS_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "enableDeletionVectors";
  private static final String ENABLE_ROW_TRACKING_KEY = "delta.enableRowTracking";
  private static final String ENABLE_ROW_TRACKING_DEFAULT_KEY =
      DEFAULT_TABLE_PROPERTY_PREFIX + "enableRowTracking";

  private CreateTableCommitCoordinator() {}

  public static final class KernelCreateResult {
    final Engine engine;
    final SnapshotImpl postCommitSnapshot;

    private KernelCreateResult(Engine engine, SnapshotImpl postCommitSnapshot) {
      this.engine = engine;
      this.postCommitSnapshot = postCommitSnapshot;
    }
  }

  public static KernelCreateResult commitCreateTableVersion0(
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
    addCatalogManagedQoLDefaults(spark, tableProperties);
    StructType normalizedSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema);
    io.delta.kernel.types.StructType kernelSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            normalizedSchema);
    Engine engine = createKernelEngine(spark, properties);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.forCreateTable(
            location,
            engine,
            UCUtils.extractTableInfoForCreate(location, tableProperties, catalogName, spark));
    Transaction txn =
        snapshotManager.buildCreateTableTransaction(
            kernelSchema, tableProperties, toDataLayoutSpec(partitions), engineInfo);
    TransactionCommitResult commitResult = txn.commit(engine, dataActions);
    Snapshot postCommitSnapshot =
        commitResult
            .getPostCommitSnapshot()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Kernel metadata-only CREATE TABLE did not return a post-commit snapshot"));
    return new KernelCreateResult(engine, (SnapshotImpl) postCommitSnapshot);
  }

  /** Removes filesystem credential keys while preserving catalog coordination properties. */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    normalizeUcTableIdProperty(filtered);
    return filtered;
  }

  /**
   * Returns the catalog properties that should be published after a successful Kernel create.
   *
   * <p>We start from the original catalog registration properties and then overlay the Delta and UC
   * properties derived from the committed version-0 snapshot.
   */
  public static Map<String, String> buildCatalogRegistrationProperties(
      Map<String, String> properties, KernelCreateResult result) {
    Map<String, String> registrationProperties = filterCredentialProperties(properties);
    if (CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties)) {
      registrationProperties.putIfAbsent(
          TableCatalog.PROP_LOCATION, result.postCommitSnapshot.getPath());
      registrationProperties.putIfAbsent(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");
    }
    registrationProperties.putAll(
        UnityCatalogUtils.getPropertiesForCreate(result.engine, result.postCommitSnapshot));
    normalizeUcTableIdProperty(registrationProperties);
    return registrationProperties;
  }

  /**
   * Returns catalog partition transforms derived from the committed snapshot.
   *
   * <p>Spark's temporary clustering transform must not be forwarded to UC as a partitioning
   * transform.
   */
  public static Transform[] buildCatalogRegistrationPartitions(KernelCreateResult result) {
    return result.postCommitSnapshot.getPartitionColumnNames().stream()
        .map(Expressions::identity)
        .toArray(Transform[]::new);
  }

  /**
   * Returns catalog columns derived from the committed snapshot schema.
   *
   * <p>This keeps the UC registration schema aligned with the exact Delta schema that was committed
   * at version 0, including field metadata such as Spark's char/varchar annotations.
   */
  public static org.apache.spark.sql.connector.catalog.Column[] buildCatalogRegistrationColumns(
      KernelCreateResult result) {
    StructType sparkSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
            result.postCommitSnapshot.getSchema());
    return CatalogV2Util.structTypeToV2Columns(sparkSchema);
  }

  /**
   * Returns whether this CREATE TABLE shape can be represented by the Kernel metadata-only path.
   */
  public static boolean canRepresentWithKernel(Transform[] partitions) {
    for (Transform partition : partitions) {
      if (!"identity".equals(partition.name())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true when the Kernel create failure is a safe signal to retry on the V1 path instead of
   * surfacing the error directly.
   */
  public static boolean shouldFallbackToV1Create(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof UnsupportedTableFeatureException) {
        return true;
      }
    }
    return false;
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

  /**
   * Mirrors the V1 catalog-managed create path by seeding the QoL defaults that Spark enables for
   * new UC managed tables unless the user or session defaults already specified them.
   */
  private static void addCatalogManagedQoLDefaults(
      SparkSession spark, Map<String, String> properties) {
    if (!CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties)) {
      return;
    }

    putIfUnsetByTableOrSession(
        spark,
        properties,
        ENABLE_DELETION_VECTORS_KEY,
        ENABLE_DELETION_VECTORS_DEFAULT_KEY,
        "true");
    putIfUnsetByTableOrSession(
        spark, properties, CHECKPOINT_POLICY_KEY, CHECKPOINT_POLICY_DEFAULT_KEY, "v2");
    putIfUnsetByTableOrSession(
        spark, properties, ENABLE_ROW_TRACKING_KEY, ENABLE_ROW_TRACKING_DEFAULT_KEY, "true");
  }

  private static void putIfUnsetByTableOrSession(
      SparkSession spark,
      Map<String, String> properties,
      String tablePropertyKey,
      String sessionDefaultKey,
      String value) {
    if (!properties.containsKey(tablePropertyKey)
        && spark.conf().get(sessionDefaultKey, null) == null) {
      properties.put(tablePropertyKey, value);
    }
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
    filtered.remove(TableCatalog.PROP_COMMENT);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_EXTERNAL);
    filtered.remove("path");
    filtered.remove("option.path");
    filtered.remove("test.simulateUC");
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    normalizeUcTableIdProperty(filtered);
    filtered.entrySet().removeIf(entry -> isHadoopOption(entry.getKey()));
    return filtered;
  }

  private static void normalizeUcTableIdProperty(Map<String, String> properties) {
    String oldTableId = properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    if (oldTableId != null) {
      properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldTableId);
    }
  }

  private static boolean isHadoopOption(String key) {
    String effectiveKey = key.startsWith("option.") ? key.substring("option.".length()) : key;
    return DeltaTableUtils.validDeltaTableHadoopPrefixes().exists(effectiveKey::startsWith);
  }
}
