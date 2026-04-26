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

package io.delta.storage.commit.uccommitcoordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.delta.model.ArrayType;
import io.unitycatalog.client.delta.model.DataSourceFormat;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.MapType;
import io.unitycatalog.client.delta.model.SecurableType;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.model.TableType;
import io.unitycatalog.client.delta.serde.DeltaTypeModule;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TableInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Converts legacy UC table responses into the DRC {@code LoadTableResponse} model.
 *
 * <p>When {@code loadTable} cannot use the Delta REST Catalog tables API, it calls the legacy UC
 * tables API instead. This adapter converts that legacy response into the DRC response shape.
 */
final class UCLegacyLoadTableAdapter {
  private UCLegacyLoadTableAdapter() {}

  // Spark persists legacy type_json with camelCase field names. The DRC SDK models are generated
  // for kebab-case wire JSON, so the legacy fallback uses a separate mapper for type_json parsing.
  private static final ObjectMapper TYPE_JSON_MAPPER = createTypeJsonMapper();

  private static ObjectMapper createTypeJsonMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new DeltaTypeModule());
    mapper.addMixIn(ArrayType.class, CamelCaseArrayMixin.class);
    mapper.addMixIn(MapType.class, CamelCaseMapMixin.class);
    return mapper;
  }

  private abstract static class CamelCaseArrayMixin {
    @JsonProperty("elementType")
    abstract DeltaType getElementType();

    @JsonSetter("elementType")
    abstract void setElementType(DeltaType value);

    @JsonProperty("containsNull")
    abstract Boolean getContainsNull();

    @JsonSetter("containsNull")
    abstract void setContainsNull(Boolean value);
  }

  private abstract static class CamelCaseMapMixin {
    @JsonProperty("keyType")
    abstract DeltaType getKeyType();

    @JsonSetter("keyType")
    abstract void setKeyType(DeltaType value);

    @JsonProperty("valueType")
    abstract DeltaType getValueType();

    @JsonSetter("valueType")
    abstract void setValueType(DeltaType value);

    @JsonProperty("valueContainsNull")
    abstract Boolean getValueContainsNull();

    @JsonSetter("valueContainsNull")
    abstract void setValueContainsNull(Boolean value);
  }

  /**
   * Loads a table through the legacy UC tables API and converts it to a DRC-style response.
   *
   * <p>The legacy API does not return unbackfilled commits here, so this method always returns an
   * empty commit list.
   */
  static LoadTableResponse loadTableViaLegacyApi(
      TablesApi tablesApi,
      String catalog,
      String schema,
      String table) throws IOException, ApiException {
    String fullName = String.format("%s.%s.%s", catalog, schema, table);
    TableInfo tableInfo = tablesApi.getTable(fullName, true, true);
    LoadTableResponse response = new LoadTableResponse();
    response.setMetadata(toDeltaTableMetadata(tableInfo));
    // Legacy getTable returns metadata only. It does not include the DRC unbackfilled commits.
    response.setCommits(Collections.emptyList());
    return response;
  }

  /**
   * Converts the legacy table format to the DRC enum.
   *
   * <p>The generated DRC enum parser maps unknown strings to
   * {@code UNKNOWN_DEFAULT_OPEN_API} instead of failing, so this helper treats missing and unknown
   * legacy values as invalid input and fails explicitly.
   */
  private static DataSourceFormat toDeltaDataSourceFormat(TableInfo tableInfo) throws IOException {
    if (tableInfo.getDataSourceFormat() == null) {
      throw new IOException("Legacy table is missing data_source_format.");
    }
    String dataSourceFormatValue = tableInfo.getDataSourceFormat().getValue();
    DataSourceFormat dataSourceFormat = DataSourceFormat.fromValue(dataSourceFormatValue);
    if (dataSourceFormat == DataSourceFormat.UNKNOWN_DEFAULT_OPEN_API) {
      throw new IOException(
          String.format("Unsupported legacy data_source_format %s.", dataSourceFormatValue));
    }
    return dataSourceFormat;
  }

  /**
   * Converts the legacy table type to the DRC enum.
   *
   * <p>The generated DRC enum parser maps unknown strings to
   * {@code UNKNOWN_DEFAULT_OPEN_API} instead of failing, so this helper treats missing and unknown
   * legacy values as invalid input and fails explicitly.
   */
  private static TableType toDeltaTableType(TableInfo tableInfo) throws IOException {
    if (tableInfo.getTableType() == null) {
      throw new IOException("Legacy table is missing table_type.");
    }
    String tableTypeValue = tableInfo.getTableType().getValue();
    TableType tableType = TableType.fromValue(tableTypeValue);
    if (tableType == TableType.UNKNOWN_DEFAULT_OPEN_API) {
      throw new IOException(String.format("Unsupported legacy table_type %s.", tableTypeValue));
    }
    return tableType;
  }

  /**
   * Parses the legacy {@code table_id} as a UUID.
   *
   * <p>This keeps malformed legacy response data on the same {@code IOException} path as the rest
   * of the adapter instead of leaking {@code IllegalArgumentException} from
   * {@link UUID#fromString(String)}.
   */
  private static UUID parseTableUuid(TableInfo tableInfo) throws IOException {
    String tableId = requireField(tableInfo.getTableId(), "table_id");
    try {
      return UUID.fromString(tableId);
    } catch (IllegalArgumentException e) {
      throw new IOException(String.format("Invalid legacy table_id %s.", tableId), e);
    }
  }

  /**
   * Returns a required legacy field.
   *
   * <p>This helper turns missing legacy response fields into explicit {@code IOException}s so the
   * fallback fails loudly instead of continuing with invented defaults or later null dereferences.
   */
  private static <T> T requireField(T value, String fieldName) throws IOException {
    if (value == null) {
      throw new IOException(String.format("Legacy table is missing %s.", fieldName));
    }
    return value;
  }

  /**
   * Converts legacy {@link TableInfo} into DRC {@code TableMetadata}.
   */
  private static TableMetadata toDeltaTableMetadata(TableInfo tableInfo) throws IOException {
    Objects.requireNonNull(tableInfo, "tableInfo must not be null");

    TableMetadata metadata = new TableMetadata();
    metadata.setDataSourceFormat(toDeltaDataSourceFormat(tableInfo));
    metadata.setTableType(toDeltaTableType(tableInfo));
    metadata.setTableUuid(parseTableUuid(tableInfo));
    metadata.setLocation(requireField(tableInfo.getStorageLocation(), "storage_location"));
    metadata.setCreatedTime(requireField(tableInfo.getCreatedAt(), "created_at"));
    metadata.setUpdatedTime(requireField(tableInfo.getUpdatedAt(), "updated_at"));

    List<ColumnInfo> columns = requireField(tableInfo.getColumns(), "columns");
    // The legacy getTable API used here only returns table securables.
    metadata.setSecurableType(SecurableType.TABLE);
    metadata.setColumns(toDeltaStructType(columns));
    metadata.setPartitionColumns(toPartitionColumns(columns));
    metadata.setProperties(
        tableInfo.getProperties() != null ? tableInfo.getProperties() : Collections.emptyMap());
    return metadata;
  }

  /**
   * Converts legacy UC columns into a DRC struct schema.
   */
  private static StructType toDeltaStructType(List<ColumnInfo> columns) throws IOException {
    StructType structType = new StructType();
    List<StructField> fields = new ArrayList<>();
    for (ColumnInfo column : columns) {
      fields.add(toStructField(column));
    }
    structType.setFields(fields);
    return structType;
  }

  /**
   * Parses one legacy UC column's Spark-format {@code type_json} into a DRC {@link StructField}.
   *
   * <p>Some legacy writers stored the full Spark {@code StructField} JSON, while older Kernel/Flink
   * paths stored only the field's data-type JSON and kept name/nullability on {@link ColumnInfo}.
   * Support both shapes so the fallback can read existing tables without inventing defaults.
   */
  private static StructField toStructField(ColumnInfo column) throws IOException {
    if (column.getTypeJson() == null || column.getTypeJson().isEmpty()) {
      throw new IOException(
          String.format("Legacy column %s is missing type_json.", column.getName()));
    }

    JsonProcessingException fieldParseException = null;
    try {
      StructField field = TYPE_JSON_MAPPER.readValue(
          column.getTypeJson(),
          StructField.class);
      if (field.getName() != null && field.getType() != null && field.getNullable() != null) {
        return field;
      }
    } catch (JsonProcessingException e) {
      fieldParseException = e;
    }

    try {
      StructField field = new StructField();
      field.setName(requireField(column.getName(), "column name"));
      field.setType(TYPE_JSON_MAPPER.readValue(column.getTypeJson(), DeltaType.class));
      field.setNullable(
          requireField(column.getNullable(), "nullable for column " + column.getName()));
      field.setMetadata(Collections.emptyMap());
      return field;
    } catch (JsonProcessingException e) {
      IOException parseException = new IOException(
          String.format("Failed to parse legacy column type JSON for column %s: %s",
              column.getName(), column.getTypeJson()),
          e);
      if (fieldParseException != null) {
        parseException.addSuppressed(fieldParseException);
      }
      throw parseException;
    }
  }

  /**
   * Returns the partition column names in partition order from the legacy column list.
   */
  private static List<String> toPartitionColumns(List<ColumnInfo> columns) throws IOException {
    List<ColumnInfo> partitionColumns = columns.stream()
        .filter(column -> column.getPartitionIndex() != null)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .collect(Collectors.toList());
    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnInfo column = partitionColumns.get(i);
      if (column.getPartitionIndex() != i) {
        throw new IOException(
            String.format(
                "Legacy table has invalid partition_index %s on column %s, expected %s.",
                column.getPartitionIndex(),
                column.getName(),
                i));
      }
    }

    // Legacy UC stores partition order on each column, so rebuild the ordered name list here.
    return partitionColumns.stream()
        .map(ColumnInfo::getName)
        .collect(Collectors.toList());
  }
}
