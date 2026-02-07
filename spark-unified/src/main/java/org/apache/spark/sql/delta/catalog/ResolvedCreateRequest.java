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

import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Normalized create table request for the kernel-backed DSv2 catalog.
 *
 * <p>This class validates and normalizes CREATE TABLE inputs so the execution layer can
 * operate on a stable, future-extensible request object.
 */
final class ResolvedCreateRequest {

  enum Operation {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE,
    CREATE_TABLE_LIKE,
    CTAS,
    RTAS
  }

  private static final Set<String> RESERVED_TABLE_PROPERTIES =
      Set.of(
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_COMMENT,
          TableCatalog.PROP_OWNER,
          TableCatalog.PROP_EXTERNAL,
          "path",
          "option.path",
          "test.simulateUC",
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY,
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD);

  private final Identifier ident;
  private final String tablePath;
  private final StructType schema;
  private final Transform[] partitions;
  private final Map<String, String> properties;
  private final Map<String, String> tableProperties;
  private final Optional<DataLayoutSpec> dataLayoutSpec;
  private final Operation operation;
  private final boolean ucManaged;
  private final String ucTableId;
  private final String ucStagingPath;

  private ResolvedCreateRequest(
      Identifier ident,
      String tablePath,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      Map<String, String> tableProperties,
      Optional<DataLayoutSpec> dataLayoutSpec,
      Operation operation,
      boolean ucManaged,
      String ucTableId,
      String ucStagingPath) {
    this.ident = requireNonNull(ident, "ident is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.partitions = requireNonNull(partitions, "partitions is null");
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.tableProperties = Collections.unmodifiableMap(new HashMap<>(tableProperties));
    this.dataLayoutSpec = requireNonNull(dataLayoutSpec, "dataLayoutSpec is null");
    this.operation = requireNonNull(operation, "operation is null");
    this.ucManaged = ucManaged;
    this.ucTableId = ucTableId;
    this.ucStagingPath = ucStagingPath;
  }

  static ResolvedCreateRequest forPathCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      boolean caseSensitiveAnalysis) {
    requireNonNull(ident, "ident is null");
    String tablePath = ident.name();
    if (!new Path(tablePath).isAbsolute()) {
      throw new IllegalArgumentException("Path-based identifier must be absolute: " + tablePath);
    }
    Map<String, String> safeProperties = copyProperties(properties);
    List<String> partitionColumns =
        extractIdentityPartitionColumns(partitions, schema, caseSensitiveAnalysis);
    Optional<DataLayoutSpec> dataLayoutSpec = buildDataLayoutSpec(partitionColumns);
    Map<String, String> tableProperties = filterTableProperties(safeProperties);
    return new ResolvedCreateRequest(
        ident,
        tablePath,
        schema,
        defaultPartitions(partitions),
        safeProperties,
        tableProperties,
        dataLayoutSpec,
        Operation.CREATE,
        false,
        null,
        null);
  }

  static ResolvedCreateRequest forUCManagedCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      String ucTableId,
      String ucStagingPath,
      boolean caseSensitiveAnalysis) {
    requireNonNull(ucTableId, "ucTableId is null");
    requireNonNull(ucStagingPath, "ucStagingPath is null");
    if (ucTableId.isEmpty()) {
      throw new IllegalArgumentException("Unity Catalog table ID must be non-empty");
    }
    if (ucStagingPath.isEmpty()) {
      throw new IllegalArgumentException("Unity Catalog staging path must be non-empty");
    }
    Map<String, String> safeProperties = copyProperties(properties);
    List<String> partitionColumns =
        extractIdentityPartitionColumns(partitions, schema, caseSensitiveAnalysis);
    Optional<DataLayoutSpec> dataLayoutSpec = buildDataLayoutSpec(partitionColumns);
    Map<String, String> tableProperties = filterTableProperties(safeProperties);
    return new ResolvedCreateRequest(
        ident,
        ucStagingPath,
        schema,
        defaultPartitions(partitions),
        safeProperties,
        tableProperties,
        dataLayoutSpec,
        Operation.CREATE,
        true,
        ucTableId,
        ucStagingPath);
  }

  static ResolvedCreateRequest forReplace() {
    throw new UnsupportedOperationException("REPLACE TABLE is not supported in phase 1");
  }

  static ResolvedCreateRequest forCreateOrReplace() {
    throw new UnsupportedOperationException("CREATE OR REPLACE TABLE is not supported in phase 1");
  }

  static ResolvedCreateRequest forCreateTableLike() {
    throw new UnsupportedOperationException("CREATE TABLE LIKE is not supported in phase 1");
  }

  static ResolvedCreateRequest forCTAS() {
    throw new UnsupportedOperationException("CTAS is not supported in phase 1");
  }

  Identifier ident() {
    return ident;
  }

  String tablePath() {
    return tablePath;
  }

  StructType schema() {
    return schema;
  }

  Transform[] partitions() {
    return partitions;
  }

  Map<String, String> properties() {
    return properties;
  }

  Map<String, String> tableProperties() {
    return tableProperties;
  }

  Optional<DataLayoutSpec> dataLayoutSpec() {
    return dataLayoutSpec;
  }

  Operation operation() {
    return operation;
  }

  boolean isUCManaged() {
    return ucManaged;
  }

  String ucTableId() {
    return ucTableId;
  }

  String ucStagingPath() {
    return ucStagingPath;
  }

  private static Map<String, String> copyProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    return new HashMap<>(properties);
  }

  private static Transform[] defaultPartitions(Transform[] partitions) {
    return partitions == null ? new Transform[0] : partitions;
  }

  private static Map<String, String> filterTableProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (RESERVED_TABLE_PROPERTIES.contains(key)) {
        continue;
      }
      filtered.put(key, entry.getValue());
    }
    return filtered;
  }

  private static Optional<DataLayoutSpec> buildDataLayoutSpec(List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      return Optional.empty();
    }
    List<Column> kernelColumns =
        partitionColumns.stream().map(Column::new).collect(Collectors.toList());
    return Optional.of(DataLayoutSpec.partitioned(kernelColumns));
  }

  private static List<String> extractIdentityPartitionColumns(
      Transform[] partitions, StructType schema, boolean caseSensitiveAnalysis) {
    if (partitions == null || partitions.length == 0) {
      return Collections.emptyList();
    }

    Set<String> schemaColumns = new HashSet<>();
    for (StructField field : schema.fields()) {
      schemaColumns.add(normalize(field.name(), caseSensitiveAnalysis));
    }

    Set<String> seen = new HashSet<>();
    List<String> duplicates = new ArrayList<>();
    List<String> partitionColumns = new ArrayList<>();
    for (Transform transform : partitions) {
      if (!(transform instanceof IdentityTransform)) {
        throw DeltaErrors.operationNotSupportedException("Partitioning by expressions");
      }
      NamedReference ref = transform.references()[0];
      String[] fieldNames = ref.fieldNames();
      if (fieldNames.length != 1) {
        throw DeltaErrors.operationNotSupportedException("Partitioning by expressions");
      }
      String columnName = fieldNames[0];
      String normalized = normalize(columnName, caseSensitiveAnalysis);
      if (!schemaColumns.contains(normalized)) {
        throw DeltaErrors.partitionColumnNotFoundException(columnName, schema.toAttributes());
      }
      if (!seen.add(normalized)) {
        duplicates.add(columnName);
      } else {
        partitionColumns.add(columnName);
      }
    }

    if (!duplicates.isEmpty()) {
      throw DeltaErrors.foundDuplicateColumnsException(
          "partition columns", String.join(",", duplicates));
    }

    return partitionColumns;
  }

  private static String normalize(String name, boolean caseSensitiveAnalysis) {
    return caseSensitiveAnalysis ? name : name.toLowerCase(Locale.ROOT);
  }
}
