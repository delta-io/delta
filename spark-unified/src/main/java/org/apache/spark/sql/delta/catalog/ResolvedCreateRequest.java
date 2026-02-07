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

import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Normalized create-table request used for routing and Kernel-backed execution.
 *
 * <p>This consolidates Spark's DDL inputs into a stable, validated representation
 * that can be used by both metadata-only and data-writing create flows.
 */
public final class ResolvedCreateRequest {

  public enum Operation {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE,
    CTAS,
    RTAS,
    LIKE
  }

  private static final Set<String> RESERVED_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  TableCatalog.PROP_LOCATION,
                  TableCatalog.PROP_PROVIDER,
                  TableCatalog.PROP_COMMENT,
                  TableCatalog.PROP_OWNER,
                  TableCatalog.PROP_EXTERNAL,
                  "path",
                  "option.path")));

  private final Identifier ident;
  private final String tablePath;
  private final StructType schema;
  private final List<String> partitionColumns;
  private final Map<String, String> properties;
  private final Operation operation;
  private final boolean isUCManaged;
  private final Optional<String> ucTableId;
  private final Optional<String> ucStagingPath;
  private final DataLayoutSpec dataLayoutSpec;

  private ResolvedCreateRequest(
      Identifier ident,
      String tablePath,
      StructType schema,
      List<String> partitionColumns,
      Map<String, String> properties,
      Operation operation,
      boolean isUCManaged,
      Optional<String> ucTableId,
      Optional<String> ucStagingPath,
      DataLayoutSpec dataLayoutSpec) {
    this.ident = requireNonNull(ident, "ident is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.partitionColumns = Collections.unmodifiableList(new ArrayList<>(partitionColumns));
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.operation = requireNonNull(operation, "operation is null");
    this.isUCManaged = isUCManaged;
    this.ucTableId = requireNonNull(ucTableId, "ucTableId is null");
    this.ucStagingPath = requireNonNull(ucStagingPath, "ucStagingPath is null");
    this.dataLayoutSpec = requireNonNull(dataLayoutSpec, "dataLayoutSpec is null");
  }

  public static ResolvedCreateRequest forPathCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    requireNonNull(ident, "ident is null");
    String tablePath = ident.name();
    validateAbsolutePath(tablePath);
    List<String> partitionColumns = extractIdentityPartitionColumns(partitions);
    validatePartitionColumns(schema, partitionColumns);
    Map<String, String> filtered = filterTableProperties(properties);
    DataLayoutSpec dataLayoutSpec = buildDataLayoutSpec(partitionColumns);
    return new ResolvedCreateRequest(
        ident,
        tablePath,
        schema,
        partitionColumns,
        filtered,
        Operation.CREATE,
        false,
        Optional.empty(),
        Optional.empty(),
        dataLayoutSpec);
  }

  public static ResolvedCreateRequest forUCManagedCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      String ucTableId,
      String ucStagingPath) {
    requireNonNull(ucTableId, "ucTableId is null");
    requireNonNull(ucStagingPath, "ucStagingPath is null");
    validateAbsolutePath(ucStagingPath);
    List<String> partitionColumns = extractIdentityPartitionColumns(partitions);
    validatePartitionColumns(schema, partitionColumns);
    Map<String, String> filtered = filterTableProperties(properties);
    DataLayoutSpec dataLayoutSpec = buildDataLayoutSpec(partitionColumns);
    return new ResolvedCreateRequest(
        ident,
        ucStagingPath,
        schema,
        partitionColumns,
        filtered,
        Operation.CREATE,
        true,
        Optional.of(ucTableId),
        Optional.of(ucStagingPath),
        dataLayoutSpec);
  }

  public Identifier getIdent() {
    return ident;
  }

  public String getTablePath() {
    return tablePath;
  }

  public StructType getSchema() {
    return schema;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Operation getOperation() {
    return operation;
  }

  public boolean isUCManaged() {
    return isUCManaged;
  }

  public Optional<String> getUcTableId() {
    return ucTableId;
  }

  public Optional<String> getUcStagingPath() {
    return ucStagingPath;
  }

  public DataLayoutSpec getDataLayoutSpec() {
    return dataLayoutSpec;
  }

  private static Map<String, String> filterTableProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>();
    if (properties == null) {
      return filtered;
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (!RESERVED_PROPERTIES.contains(key)) {
        filtered.put(key, entry.getValue());
      }
    }
    return filtered;
  }

  private static List<String> extractIdentityPartitionColumns(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return Collections.emptyList();
    }
    List<String> columns = new ArrayList<>(partitions.length);
    for (Transform transform : partitions) {
      if (!"identity".equalsIgnoreCase(transform.name())) {
        throw new IllegalArgumentException(
            "Only identity partition transforms are supported in DSv2 DDL");
      }
      NamedReference[] refs = transform.references();
      if (refs.length != 1) {
        throw new IllegalArgumentException(
            "Identity partition transform must reference exactly one column");
      }
      String[] names = refs[0].fieldNames();
      if (names.length != 1) {
        throw new IllegalArgumentException(
            "Identity partition transform must reference a top-level column");
      }
      columns.add(names[0]);
    }
    return columns;
  }

  private static void validateAbsolutePath(String path) {
    if (!new Path(path).isAbsolute()) {
      throw new IllegalArgumentException("Table path must be absolute: " + path);
    }
  }

  private static void validatePartitionColumns(StructType schema, List<String> partitionColumns) {
    requireNonNull(schema, "schema is null");
    if (partitionColumns.isEmpty()) {
      return;
    }
    Set<String> schemaFields = new HashSet<>();
    for (StructField field : schema.fields()) {
      schemaFields.add(field.name().toLowerCase(Locale.ROOT));
    }
    Set<String> seen = new HashSet<>();
    for (String column : partitionColumns) {
      String key = column.toLowerCase(Locale.ROOT);
      if (!schemaFields.contains(key)) {
        throw new IllegalArgumentException("Partition column not found in schema: " + column);
      }
      if (!seen.add(key)) {
        throw new IllegalArgumentException("Duplicate partition column: " + column);
      }
    }
  }

  private static DataLayoutSpec buildDataLayoutSpec(List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      return DataLayoutSpec.noDataLayout();
    }
    List<Column> columns = new ArrayList<>(partitionColumns.size());
    for (String column : partitionColumns) {
      columns.add(new Column(column));
    }
    return DataLayoutSpec.partitioned(columns);
  }
}
