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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TableInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

final class UCDeltaRestCatalogUtils {

  private UCDeltaRestCatalogUtils() {}

  static LoadTableResponse loadTableViaLegacyApi(
      TablesApi tablesApi,
      ObjectMapper objectMapper,
      String catalog,
      String schema,
      String table) throws IOException, ApiException {
    final String fullName = String.format("%s.%s.%s", catalog, schema, table);
    TableInfo tableInfo = tablesApi.getTable(fullName, true, true);
    LoadTableResponse response = new LoadTableResponse();
    response.setMetadata(toDeltaTableMetadata(tableInfo, objectMapper));
    response.setCommits(Collections.emptyList());
    return response;
  }

  private static io.unitycatalog.client.delta.model.TableMetadata toDeltaTableMetadata(
      TableInfo tableInfo,
      ObjectMapper objectMapper) throws IOException {
    io.unitycatalog.client.delta.model.TableMetadata metadata =
        new io.unitycatalog.client.delta.model.TableMetadata();
    metadata.setEtag("");
    metadata.setDataSourceFormat(io.unitycatalog.client.delta.model.DataSourceFormat.fromValue(
        tableInfo.getDataSourceFormat() != null ? tableInfo.getDataSourceFormat().getValue() : null));
    metadata.setTableType(io.unitycatalog.client.delta.model.TableType.fromValue(
        tableInfo.getTableType() != null ? tableInfo.getTableType().getValue() : null));
    metadata.setTableUuid(UUID.fromString(tableInfo.getTableId()));
    metadata.setLocation(tableInfo.getStorageLocation());
    metadata.setCreatedTime(tableInfo.getCreatedAt() != null ? tableInfo.getCreatedAt() : 0L);
    metadata.setUpdatedTime(tableInfo.getUpdatedAt() != null ? tableInfo.getUpdatedAt() : 0L);
    metadata.setSecurableType(io.unitycatalog.client.delta.model.SecurableType.TABLE);
    metadata.setColumns(toDeltaStructType(tableInfo.getColumns(), objectMapper));
    metadata.setPartitionColumns(toPartitionColumns(tableInfo.getColumns()));
    metadata.setProperties(
        tableInfo.getProperties() != null ? tableInfo.getProperties() : Collections.emptyMap());
    return metadata;
  }

  private static io.unitycatalog.client.delta.model.StructType toDeltaStructType(
      List<ColumnInfo> columns,
      ObjectMapper objectMapper) throws IOException {
    io.unitycatalog.client.delta.model.StructType structType =
        new io.unitycatalog.client.delta.model.StructType();
    if (columns == null) {
      structType.setFields(Collections.emptyList());
      return structType;
    }

    List<io.unitycatalog.client.delta.model.StructField> fields = new ArrayList<>();
    for (ColumnInfo column : columns) {
      io.unitycatalog.client.delta.model.StructField field =
          new io.unitycatalog.client.delta.model.StructField();
      field.setName(column.getName());
      field.setNullable(column.getNullable() == null || column.getNullable());
      field.setMetadata(Collections.emptyMap());
      field.setType(toDeltaType(column, objectMapper));
      fields.add(field);
    }
    structType.setFields(fields);
    return structType;
  }

  private static io.unitycatalog.client.delta.model.DeltaType toDeltaType(
      ColumnInfo column,
      ObjectMapper objectMapper) throws IOException {
    if (column.getTypeJson() != null && !column.getTypeJson().isEmpty()) {
      try {
        return objectMapper.readValue(
            column.getTypeJson(),
            io.unitycatalog.client.delta.model.DeltaType.class);
      } catch (JsonProcessingException e) {
        throw new IOException(
            String.format("Failed to parse legacy column type JSON for column %s: %s",
                column.getName(), column.getTypeJson()),
            e);
      }
    }

    if (column.getTypeText() != null && !column.getTypeText().isEmpty()) {
      io.unitycatalog.client.delta.model.PrimitiveType primitiveType =
          new io.unitycatalog.client.delta.model.PrimitiveType();
      primitiveType.setType(column.getTypeText());
      return primitiveType;
    }

    throw new IOException(
        String.format("Legacy column %s is missing both type_json and type_text.", column.getName()));
  }

  private static List<String> toPartitionColumns(List<ColumnInfo> columns) {
    if (columns == null) {
      return Collections.emptyList();
    }

    return columns.stream()
        .filter(column -> column.getPartitionIndex() != null)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .map(ColumnInfo::getName)
        .collect(Collectors.toList());
  }
}
