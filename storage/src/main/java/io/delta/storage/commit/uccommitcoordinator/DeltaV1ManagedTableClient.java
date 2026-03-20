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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaV1Api;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.DeltaV1ConfigResponse;
import io.unitycatalog.client.model.DeltaV1UpdateTableRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Adapter over the generated UC DeltaV1Api for the managed-table flow. */
public class DeltaV1ManagedTableClient {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final DeltaV1Api deltaV1Api;
  private DeltaV1ConfigResponse config;

  public DeltaV1ManagedTableClient(String baseUri, Map<String, String> authConfig) {
    TokenProvider tokenProvider = TokenProvider.create(authConfig);
    ApiClient apiClient = ApiClientBuilder.create()
        .uri(baseUri)
        .tokenProvider(tokenProvider)
        .build();
    this.deltaV1Api = new DeltaV1Api(apiClient);
  }

  public ConfigResponse getConfig() {
    ensureConfigured();
    return new ConfigResponse(config.getEndpoints(), config.getProtocolVersion());
  }

  public LoadTableResponse loadTable(
      String catalog,
      String schema,
      String table,
      Long startVersion,
      Long endVersion) throws IOException {
    ensureConfigured();
    try {
      return fromGenerated(
          deltaV1Api.deltaV1LoadTable(catalog, schema, table, startVersion, endVersion));
    } catch (ApiException e) {
      throw toIOException("loadTable", e);
    }
  }

  public LoadTableResponse updateTable(
      String catalog, String schema, String table, UpdateTableRequest request) throws IOException {
    ensureConfigured();
    try {
      return fromGenerated(
          deltaV1Api.deltaV1UpdateTable(catalog, schema, table, toGenerated(request)));
    } catch (ApiException e) {
      throw toIOException("updateTable", e);
    }
  }

  private DeltaV1UpdateTableRequest toGenerated(UpdateTableRequest request) {
    DeltaV1UpdateTableRequest generated = new DeltaV1UpdateTableRequest()
        .assertTableId(request.getAssertTableId())
        .assertEtag(request.getAssertEtag())
        .setProperties(request.getSetProperties())
        .removeProperties(request.getRemoveProperties())
        .comment(request.getComment())
        .columns(request.getColumns())
        .latestBackfilledVersion(request.getLatestBackfilledVersion());
    if (request.getCommitInfo() != null) {
      generated.commitInfo(
          new DeltaCommitInfo()
              .version(request.getCommitInfo().getVersion())
              .timestamp(request.getCommitInfo().getTimestamp())
              .fileName(request.getCommitInfo().getFileName())
              .fileSize(request.getCommitInfo().getFileSize())
              .fileModificationTimestamp(request.getCommitInfo().getFileModificationTimestamp()));
    }
    return generated;
  }

  private LoadTableResponse fromGenerated(io.unitycatalog.client.model.DeltaV1LoadTableResponse response) {
    LoadTableResponse mapped = new LoadTableResponse();
    if (response == null) {
      return mapped;
    }
    if (response.getTableInfo() != null) {
      TableInfo tableInfo = new TableInfo();
      tableInfo.setTableId(response.getTableInfo().getTableId());
      tableInfo.setStorageLocation(response.getTableInfo().getStorageLocation());
      tableInfo.setProperties(response.getTableInfo().getProperties());
      mapped.setTableInfo(tableInfo);
    }
    mapped.setEtag(response.getEtag());
    mapped.setLatestTableVersion(response.getLatestTableVersion());
    if (response.getCommits() != null) {
      List<CommitInfo> commits = new ArrayList<>();
      for (DeltaCommitInfo commitInfo : response.getCommits()) {
        commits.add(
            new CommitInfo()
                .setVersion(commitInfo.getVersion())
                .setTimestamp(commitInfo.getTimestamp())
                .setFileName(commitInfo.getFileName())
                .setFileSize(commitInfo.getFileSize())
                .setFileModificationTimestamp(commitInfo.getFileModificationTimestamp()));
      }
      mapped.setCommits(commits);
    }
    return mapped;
  }

  private IOException toIOException(String operation, ApiException e) {
    return new IOException(
        "delta/v1 " + operation + " failed (HTTP " + e.getCode() + "): " + e.getResponseBody(),
        e);
  }

  private void ensureConfigured() {
    if (config != null) {
      return;
    }
    try {
      config = deltaV1Api.deltaV1Config(null, null);
    } catch (ApiException e) {
      throw new IllegalStateException("Failed to discover delta/v1 API", e);
    }
  }

  public static class TableInfo {
    private String tableId;
    private String storageLocation;
    private Map<String, String> properties;

    public String getTableId() {
      return tableId;
    }

    public void setTableId(String tableId) {
      this.tableId = tableId;
    }

    public String getStorageLocation() {
      return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
      this.storageLocation = storageLocation;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }

  public static class CommitInfo {
    private Long version;
    private Long timestamp;
    private String fileName;
    private Long fileSize;
    private Long fileModificationTimestamp;

    public Long getVersion() {
      return version;
    }

    public CommitInfo setVersion(Long version) {
      this.version = version;
      return this;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public CommitInfo setTimestamp(Long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public String getFileName() {
      return fileName;
    }

    public CommitInfo setFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public Long getFileSize() {
      return fileSize;
    }

    public CommitInfo setFileSize(Long fileSize) {
      this.fileSize = fileSize;
      return this;
    }

    public Long getFileModificationTimestamp() {
      return fileModificationTimestamp;
    }

    public CommitInfo setFileModificationTimestamp(Long fileModificationTimestamp) {
      this.fileModificationTimestamp = fileModificationTimestamp;
      return this;
    }
  }

  public static class LoadTableResponse {
    private TableInfo tableInfo;
    private String etag;
    private Long latestTableVersion;
    private List<CommitInfo> commits;

    public TableInfo getTableInfo() {
      return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    public String getEtag() {
      return etag;
    }

    public void setEtag(String etag) {
      this.etag = etag;
    }

    public Long getLatestTableVersion() {
      return latestTableVersion;
    }

    public void setLatestTableVersion(Long latestTableVersion) {
      this.latestTableVersion = latestTableVersion;
    }

    public List<CommitInfo> getCommits() {
      return commits;
    }

    public void setCommits(List<CommitInfo> commits) {
      this.commits = commits;
    }
  }

  public static class UpdateTableRequest {
    private String assertTableId;
    private String assertEtag;
    private Map<String, String> setProperties;
    private List<String> removeProperties;
    private String comment;
    private List<ColumnInfo> columns;
    private CommitInfo commitInfo;
    private Long latestBackfilledVersion;

    public String getAssertTableId() {
      return assertTableId;
    }

    public UpdateTableRequest setAssertTableId(String assertTableId) {
      this.assertTableId = assertTableId;
      return this;
    }

    public String getAssertEtag() {
      return assertEtag;
    }

    public UpdateTableRequest setAssertEtag(String assertEtag) {
      this.assertEtag = assertEtag;
      return this;
    }

    public Map<String, String> getSetProperties() {
      return setProperties;
    }

    public UpdateTableRequest setSetProperties(Map<String, String> setProperties) {
      this.setProperties = setProperties;
      return this;
    }

    public List<String> getRemoveProperties() {
      return removeProperties;
    }

    public UpdateTableRequest setRemoveProperties(List<String> removeProperties) {
      this.removeProperties = removeProperties;
      return this;
    }

    public String getComment() {
      return comment;
    }

    public UpdateTableRequest setComment(String comment) {
      this.comment = comment;
      return this;
    }

    public List<ColumnInfo> getColumns() {
      return columns;
    }

    public UpdateTableRequest setColumns(List<ColumnInfo> columns) {
      this.columns = columns;
      return this;
    }

    public CommitInfo getCommitInfo() {
      return commitInfo;
    }

    public UpdateTableRequest setCommitInfo(CommitInfo commitInfo) {
      this.commitInfo = commitInfo;
      return this;
    }

    public Long getLatestBackfilledVersion() {
      return latestBackfilledVersion;
    }

    public UpdateTableRequest setLatestBackfilledVersion(Long latestBackfilledVersion) {
      this.latestBackfilledVersion = latestBackfilledVersion;
      return this;
    }
  }

  public static class ConfigResponse {
    private final List<String> endpoints;
    private final Integer protocolVersion;

    public ConfigResponse(List<String> endpoints, Integer protocolVersion) {
      this.endpoints = endpoints;
      this.protocolVersion = protocolVersion;
    }

    public List<String> getEndpoints() {
      return endpoints;
    }

    public Integer getProtocolVersion() {
      return protocolVersion;
    }
  }

  public static List<ColumnInfo> schemaStringToColumns(String schemaString) {
    if (schemaString == null || schemaString.isEmpty()) {
      return null;
    }
    try {
      JsonNode schema = OBJECT_MAPPER.readTree(schemaString);
      JsonNode fields = schema.get("fields");
      if (fields == null || !fields.isArray()) {
        return null;
      }
      List<ColumnInfo> columns = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        JsonNode field = fields.get(i);
        JsonNode typeNode = field.get("type");
        ColumnInfo columnInfo = new ColumnInfo()
            .name(field.get("name").asText())
            .nullable(field.path("nullable").asBoolean(true))
            .position(i)
            .typeJson(typeNode.toString())
            .typeText(typeNodeToCatalogString(typeNode))
            .typeName(typeNodeToTypeName(typeNode));
        columns.add(columnInfo);
      }
      return columns;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to convert schemaString to UC columns", e);
    }
  }

  private static ColumnTypeName typeNodeToTypeName(JsonNode typeNode) {
    String typeName = primitiveTypeName(typeNode);
    switch (typeName) {
      case "boolean":
        return ColumnTypeName.BOOLEAN;
      case "byte":
        return ColumnTypeName.BYTE;
      case "short":
        return ColumnTypeName.SHORT;
      case "integer":
      case "int":
        return ColumnTypeName.INT;
      case "long":
        return ColumnTypeName.LONG;
      case "float":
        return ColumnTypeName.FLOAT;
      case "double":
        return ColumnTypeName.DOUBLE;
      case "date":
        return ColumnTypeName.DATE;
      case "timestamp":
        return ColumnTypeName.TIMESTAMP;
      case "timestamp_ntz":
        return ColumnTypeName.TIMESTAMP_NTZ;
      case "string":
        return ColumnTypeName.STRING;
      case "binary":
        return ColumnTypeName.BINARY;
      case "decimal":
        return ColumnTypeName.DECIMAL;
      case "interval":
        return ColumnTypeName.INTERVAL;
      case "array":
        return ColumnTypeName.ARRAY;
      case "struct":
        return ColumnTypeName.STRUCT;
      case "map":
        return ColumnTypeName.MAP;
      case "char":
        return ColumnTypeName.CHAR;
      case "null":
        return ColumnTypeName.NULL;
      default:
        return ColumnTypeName.USER_DEFINED_TYPE;
    }
  }

  private static String typeNodeToCatalogString(JsonNode typeNode) {
    if (typeNode == null || typeNode.isNull()) {
      return "null";
    }
    if (typeNode.isTextual()) {
      return typeNode.asText();
    }
    String typeName = primitiveTypeName(typeNode);
    switch (typeName) {
      case "decimal":
        return "decimal(" + typeNode.path("precision").asInt() + "," + typeNode.path("scale").asInt() + ")";
      case "array":
        return "array<" + typeNodeToCatalogString(typeNode.get("elementType")) + ">";
      case "map":
        return "map<"
            + typeNodeToCatalogString(typeNode.get("keyType"))
            + ","
            + typeNodeToCatalogString(typeNode.get("valueType"))
            + ">";
      case "struct":
        List<String> fieldStrings = new ArrayList<>();
        JsonNode fields = typeNode.get("fields");
        if (fields != null && fields.isArray()) {
          for (JsonNode field : fields) {
            fieldStrings.add(
                field.get("name").asText() + ":" + typeNodeToCatalogString(field.get("type")));
          }
        }
        return "struct<" + String.join(",", fieldStrings) + ">";
      default:
        return typeName;
    }
  }

  private static String primitiveTypeName(JsonNode typeNode) {
    if (typeNode == null || typeNode.isNull()) {
      return "null";
    }
    return typeNode.isTextual() ? typeNode.asText() : typeNode.path("type").asText("unknown");
  }
}
