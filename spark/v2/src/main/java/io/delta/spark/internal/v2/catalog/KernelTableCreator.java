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
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Kernel-backed CREATE TABLE helper for the Spark V2 connector.
 *
 * <p>Phase 1 constraints:
 *
 * <ul>
 *   <li>Path-based identifiers only (delta.`/path`)
 *   <li>Partitioning by identity columns only
 * </ul>
 */
public final class KernelTableCreator {

  private static final String ENGINE_INFO = "delta-spark-v2-connector";

  private KernelTableCreator() {}

  /**
   * Creates a new Delta table by committing metadata-only actions via Kernel.
   *
   * <p>This is a thin wrapper around a unified create request/executor, limited to path-based
   * CREATE in phase 1.
   */
  public static SparkTable createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    ResolvedCreateRequest request =
        ResolvedCreateRequest.forCreate(ident, schema, partitions, properties);
    return execute(request);
  }

  private static SparkTable execute(ResolvedCreateRequest request) {
    requireNonNull(request, "request is null");

    if (request.operation != CreateOperation.CREATE) {
      throw new UnsupportedOperationException("V2 CREATE TABLE supports only CREATE in phase 1");
    }

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
        throw new IllegalArgumentException(
            "Partition transform must reference exactly one column");
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
    private final CreateOperation operation; // CREATE | REPLACE | CREATE_OR_REPLACE | CREATE_TABLE_LIKE
    // Future fields (phase 2+): currently unused placeholders.
    private final Map<String, String> writeOptions;
    private final boolean tableByPath;
    private final Object sourceTableOpt;
    private final boolean isUCManaged;

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
        boolean isUCManaged) {
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
          properties,
          CreateOperation.CREATE,
          /* writeOptions = */ null,
          /* tableByPath = */ true,
          /* sourceTableOpt = */ null,
          /* isUCManaged = */ false);
    }
  }
}
