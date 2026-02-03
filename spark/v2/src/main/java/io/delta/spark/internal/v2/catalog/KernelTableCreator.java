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
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.coordinatedcommits.UCCatalogConfig;
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$;
import org.apache.spark.sql.delta.util.CatalogTableUtils;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Kernel-backed CREATE TABLE helper for the Spark V2 connector.
 *
 * <p>Phase 1 constraints:
 *
 * <ul>
 *   <li>Path-based identifiers (delta.`/path`) and UC-managed catalog tables
 *   <li>Partitioning by identity columns only
 * </ul>
 */
public final class KernelTableCreator {

  private static final String ENGINE_INFO = "delta-spark-v2-connector";

  private KernelTableCreator() {}

  /**
   * Creates a new Delta table by committing metadata-only actions via Kernel.
   *
   * <p>This is a thin wrapper around a unified create request/executor, limited to metadata-only
   * CREATE in phase 1 (path-based or UC-managed).
   */
  public static SparkTable createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    Map<String, String> rawProperties = properties == null ? Collections.emptyMap() : properties;
    ResolvedCreateRequest request;
    if (isUCManagedCreate(rawProperties)) {
      request = ResolvedCreateRequest.forCreateUC(ident, schema, partitions, rawProperties);
    } else {
      request = ResolvedCreateRequest.forCreate(ident, schema, partitions, rawProperties);
    }
    return execute(request);
  }

  private static SparkTable execute(ResolvedCreateRequest request) {
    requireNonNull(request, "request is null");

    if (request.operation != CreateOperation.CREATE) {
      throw new UnsupportedOperationException("V2 CREATE TABLE supports only CREATE in phase 1");
    }

    if (request.isUCManaged) {
      return executeUCManagedCreate(request);
    }
    return executePathBasedCreate(request);
  }

  private static SparkTable executePathBasedCreate(ResolvedCreateRequest request) {
    String tablePath = request.tablePath;
    Engine engine = createEngine();

    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(request.schema);

    CreateTableTransactionBuilder builder =
        TableManager.buildCreateTableTransaction(tablePath, kernelSchema, ENGINE_INFO);

    if (request.properties != null && !request.properties.isEmpty()) {
      builder = builder.withTableProperties(request.properties);
    }

    Optional<DataLayoutSpec> dataLayoutSpec =
        buildDataLayoutSpec(request.partitions, request.schema);
    if (dataLayoutSpec.isPresent()) {
      builder = builder.withDataLayoutSpec(dataLayoutSpec.get());
    }

    builder.build(engine).commit(engine, CloseableIterable.emptyIterable());
    return new SparkTable(request.ident, tablePath);
  }

  private static SparkTable executeUCManagedCreate(ResolvedCreateRequest request) {
    Engine engine = createEngine();
    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(request.schema);

    UCClient ucClient = null;
    try {
      ucClient = createUCClient(request);
      UCCatalogManagedClient ucCatalogClient = new UCCatalogManagedClient(ucClient);

      CreateTableTransactionBuilder builder =
          ucCatalogClient.buildCreateTableTransaction(
              request.ucTableId, request.tablePath, kernelSchema, ENGINE_INFO);

      if (request.properties != null && !request.properties.isEmpty()) {
        builder = builder.withTableProperties(request.properties);
      }

      Optional<DataLayoutSpec> dataLayoutSpec =
          buildDataLayoutSpec(request.partitions, request.schema);
      if (dataLayoutSpec.isPresent()) {
        builder = builder.withDataLayoutSpec(dataLayoutSpec.get());
      }

      builder.build(engine).commit(engine, CloseableIterable.emptyIterable());
      // TODO: UC-managed CREATE requires registering the table via the UC Tables API after commit.
      return new SparkTable(request.ident, request.tablePath);
    } finally {
      if (ucClient != null) {
        try {
          ucClient.close();
        } catch (Exception ignored) {
          // Best effort; commit already completed.
        }
      }
    }
  }

  /** Builds a DefaultEngine using the active Spark session's Hadoop conf. */
  private static Engine createEngine() {
    SparkSession spark = SparkSession.active();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    return DefaultEngine.create(hadoopConf);
  }

  /**
   * Builds a Kernel DataLayoutSpec from Spark partition transforms.
   *
   * <p>Supports identity transforms only; non-identity transforms fail fast.
   */
  private static Optional<DataLayoutSpec> buildDataLayoutSpec(
      Transform[] partitions, StructType schema) {
    if (partitions == null || partitions.length == 0) {
      return Optional.empty();
    }

    requireNonNull(schema, "schema is null");
    Set<String> schemaFields = new HashSet<>(Arrays.asList(schema.fieldNames()));
    Set<String> seenPartitionCols = new HashSet<>();

    List<Column> partitionColumns = new ArrayList<>();
    for (Transform transform : partitions) {
      if (!(transform instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "V2 CREATE TABLE supports only identity partition columns");
      }

      NamedReference[] refs = transform.references();
      if (refs == null || refs.length != 1) {
        throw new IllegalArgumentException("Partition transform must reference exactly one column");
      }
      NamedReference ref = refs[0];
      String[] fieldNames = ref.fieldNames();
      if (fieldNames.length != 1) {
        throw new IllegalArgumentException("Partition columns must be top-level columns");
      }
      String fieldName = fieldNames[0];
      if (!schemaFields.contains(fieldName)) {
        throw new IllegalArgumentException(
            "Partition column does not exist in schema: " + fieldName);
      }
      if (!seenPartitionCols.add(fieldName)) {
        throw new IllegalArgumentException("Duplicate partition column: " + fieldName);
      }
      partitionColumns.add(new Column(fieldName));
    }

    return Optional.of(DataLayoutSpec.partitioned(partitionColumns));
  }

  /** Returns true if the identifier represents a path-based Delta table (delta.`/path`). */
  private static boolean isPathIdentifier(Identifier ident) {
    if (ident.namespace().length != 1 || !"delta".equalsIgnoreCase(ident.namespace()[0])) {
      return false;
    }
    String path = ident.name();
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    Path hadoopPath = new Path(path);
    return hadoopPath.isAbsolute() || hadoopPath.toUri().getScheme() != null;
  }

  private static boolean isUCManagedCreate(Map<String, String> properties) {
    return CatalogTableUtils.isUnityCatalogManagedTableFromProperties(properties);
  }

  private static UCClient createUCClient(ResolvedCreateRequest request) {
    requireNonNull(request.ucUri, "ucUri is null");
    requireNonNull(request.ucAuthConfig, "ucAuthConfig is null");
    TokenProvider tokenProvider = TokenProvider.create(request.ucAuthConfig);
    return new UCTokenBasedRestClient(request.ucUri, tokenProvider);
  }

  private static String resolveTablePath(Identifier ident, Map<String, String> properties) {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    if (location == null || location.trim().isEmpty()) {
      location = properties.get("location");
    }
    if (location != null && !location.trim().isEmpty()) {
      return location;
    }

    SparkSession spark = SparkSession.active();
    String[] namespace = ident.namespace();
    String database = namespace.length == 0 ? null : namespace[namespace.length - 1];
    TableIdentifier tableId = new TableIdentifier(ident.name(), Option.apply(database));
    return spark.sessionState().catalog().defaultTablePath(tableId).toString();
  }

  private static String resolveUCTableId(Map<String, String> properties) {
    String ucTableId = properties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId == null || ucTableId.trim().isEmpty()) {
      ucTableId = properties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    }
    if (ucTableId == null || ucTableId.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing UC table id in table properties");
    }
    return ucTableId;
  }

  private static UCCatalogConfig resolveUCCatalogConfig(SparkSession spark) {
    scala.collection.immutable.Map<String, UCCatalogConfig> configs =
        UCCommitCoordinatorBuilder$.MODULE$.getCatalogConfigMap(spark);
    String catalogName = spark.sessionState().catalogManager().currentCatalog().name();
    scala.Option<UCCatalogConfig> configOpt = configs.get(catalogName);
    if (configOpt.isEmpty()) {
      throw new IllegalArgumentException(
          "Unity Catalog configuration not found for catalog '" + catalogName + "'");
    }
    return configOpt.get();
  }

  private static Map<String, String> normalizeTableProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> filtered = new HashMap<>(properties);
    filtered.remove(TableCatalog.PROP_LOCATION);
    filtered.remove(TableCatalog.PROP_PROVIDER);
    filtered.remove(TableCatalog.PROP_OWNER);
    filtered.remove(TableCatalog.PROP_COMMENT);
    filtered.remove(TableCatalog.PROP_IS_MANAGED_LOCATION);
    filtered.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);
    return filtered;
  }

  private enum CreateOperation {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE,
    CREATE_TABLE_LIKE
  }

  private static final class ResolvedCreateRequest {
    private final Identifier ident;
    private final String tablePath;
    private final StructType schema;
    private final Transform[] partitions;
    private final Map<String, String> properties;
    private final CreateOperation
        operation; // CREATE | REPLACE | CREATE_OR_REPLACE | CREATE_TABLE_LIKE
    // Future fields (phase 2+): currently unused placeholders.
    private final Map<String, String> writeOptions;
    private final boolean tableByPath;
    private final Object sourceTableOpt;
    private final boolean isUCManaged;
    private final String ucTableId;
    private final String ucUri;
    private final Map<String, String> ucAuthConfig;

    private ResolvedCreateRequest(
        Identifier ident,
        String tablePath,
        StructType schema,
        Transform[] partitions,
        Map<String, String> properties,
        CreateOperation operation,
        Map<String, String> writeOptions,
        boolean tableByPath,
        Object sourceTableOpt,
        boolean isUCManaged,
        String ucTableId,
        String ucUri,
        Map<String, String> ucAuthConfig) {
      this.ident = ident;
      this.tablePath = tablePath;
      this.schema = schema;
      this.partitions = partitions;
      this.properties = properties;
      this.operation = operation;
      this.writeOptions = writeOptions;
      this.tableByPath = tableByPath;
      this.sourceTableOpt = sourceTableOpt;
      this.isUCManaged = isUCManaged;
      this.ucTableId = ucTableId;
      this.ucUri = ucUri;
      this.ucAuthConfig = ucAuthConfig;
    }

    /**
     * Factory method for creating a CREATE request.
     *
     * @param ident The identifier of the table.
     * @param schema The schema of the table.
     * @param partitions The partitions of the table.
     * @param properties The properties of the table.
     * @return The resolved create request.
     */
    private static ResolvedCreateRequest forCreate(
        Identifier ident,
        StructType schema,
        Transform[] partitions,
        Map<String, String> properties) {
      requireNonNull(ident, "ident is null");
      requireNonNull(schema, "schema is null");

      if (!isPathIdentifier(ident)) {
        throw new UnsupportedOperationException(
            "V2 CREATE TABLE supports only path-based tables (delta.`/path`)");
      }

      return new ResolvedCreateRequest(
          ident,
          ident.name(),
          schema,
          partitions,
          normalizeTableProperties(properties),
          CreateOperation.CREATE,
          /* writeOptions = */ null,
          /* tableByPath = */ true,
          /* sourceTableOpt = */ null,
          /* isUCManaged = */ false,
          /* ucTableId = */ null,
          /* ucUri = */ null,
          /* ucAuthConfig = */ null);
    }

    private static ResolvedCreateRequest forCreateUC(
        Identifier ident,
        StructType schema,
        Transform[] partitions,
        Map<String, String> properties) {
      requireNonNull(ident, "ident is null");
      requireNonNull(schema, "schema is null");

      if (!isUCManagedCreate(properties)) {
        throw new UnsupportedOperationException(
            "V2 CREATE TABLE supports UC-managed tables only when UC properties are present");
      }

      SparkSession spark = SparkSession.active();
      UCCatalogConfig config = resolveUCCatalogConfig(spark);
      Map<String, String> authConfig = CollectionConverters.asJava(config.authConfig());

      return new ResolvedCreateRequest(
          ident,
          resolveTablePath(ident, properties),
          schema,
          partitions,
          normalizeTableProperties(properties),
          CreateOperation.CREATE,
          /* writeOptions = */ null,
          /* tableByPath = */ false,
          /* sourceTableOpt = */ null,
          /* isUCManaged = */ true,
          /* ucTableId = */ resolveUCTableId(properties),
          /* ucUri = */ config.uri(),
          /* ucAuthConfig = */ authConfig);
    }
  }
}
