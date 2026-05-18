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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CreateTableRequest;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.DataSourceFormat;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.DeltaProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StagingTableResponse;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableType;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.ConfigurationApi;
import io.unitycatalog.client.delta.model.ArrayType;
import io.unitycatalog.client.delta.model.CatalogConfig;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.MapType;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.serde.DeltaTypeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Token-based REST client implementation for UC Delta Rest Catalog API operations.
 */
public class UCDeltaTokenBasedRestClient
    extends UCTokenBasedRestClient
    implements UCDeltaClient {

  private static final Logger LOG = LoggerFactory.getLogger(UCDeltaTokenBasedRestClient.class);
  private static final ObjectMapper DELTA_TYPE_OBJECT_MAPPER =
      JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build()
          .registerModule(new DeltaTypeModule());
  private static final ObjectMapper DELTA_SCHEMA_OBJECT_MAPPER =
      createDeltaSchemaObjectMapper();

  private static ObjectMapper createDeltaSchemaObjectMapper() {
    ObjectMapper mapper =
        JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build();
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

  private boolean supportsUCDeltaRestCatalogApi;
  private volatile boolean closed;
  private io.unitycatalog.client.delta.api.TablesApi deltaTablesApi;

  private static final int HTTP_NOT_FOUND = 404;
  private static final String UC_DELTA_API_PROTOCOL_VERSION = "1.0";
  // Endpoint identifiers advertised by the UC Delta Rest Catalog API /config endpoint, not
  // concrete URLs.
  private static final List<String> REQUIRED_UC_DELTA_API_ENDPOINT_IDS =
      Collections.singletonList("GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}");

  protected static class UCDeltaRestCatalogApiSupport {
    private final boolean supportsTableApis;

    UCDeltaRestCatalogApiSupport(boolean supportsTableApis) {
      this.supportsTableApis = supportsTableApis;
    }
  }

  public UCDeltaTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    super(baseUri, tokenProvider, appVersions);
  }

  public UCDeltaTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      String catalog) {
    super(baseUri, tokenProvider, appVersions, catalog);
    Objects.requireNonNull(catalog, "catalog must not be null");
    configureUCDeltaRestCatalogApi(catalog);
  }

  private void configureUCDeltaRestCatalogApi(String catalog) {
    initializeUCDeltaRestCatalogApi(
        getUCDeltaRestCatalogApiSupport(getApiClient(), catalog));
  }

  private void initializeUCDeltaRestCatalogApi(
      UCDeltaRestCatalogApiSupport ucDeltaRestCatalogApiSupport) {
    Objects.requireNonNull(
        ucDeltaRestCatalogApiSupport, "ucDeltaRestCatalogApiSupport must not be null");
    this.supportsUCDeltaRestCatalogApi = ucDeltaRestCatalogApiSupport.supportsTableApis;
    this.deltaTablesApi = ucDeltaRestCatalogApiSupport.supportsTableApis
        ? new io.unitycatalog.client.delta.api.TablesApi(getApiClient())
        : null;
  }

  protected static UCDeltaRestCatalogApiSupport getUCDeltaRestCatalogApiSupport(
      ApiClient apiClient,
      String catalog) {
    Objects.requireNonNull(apiClient, "apiClient must not be null");
    Objects.requireNonNull(catalog, "catalog must not be null");
    try {
      CatalogConfig config =
          new ConfigurationApi(apiClient).getConfig(catalog, UC_DELTA_API_PROTOCOL_VERSION);
      List<String> endpoints = config == null ? null : config.getEndpoints();
      return new UCDeltaRestCatalogApiSupport(
          endpoints != null && endpoints.containsAll(REQUIRED_UC_DELTA_API_ENDPOINT_IDS));
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        LOG.warn(
            "UC Delta Rest Catalog API config endpoint is unavailable for catalog {}. "
                + "UC Delta Rest Catalog API will be disabled.",
            catalog,
            e);
        return new UCDeltaRestCatalogApiSupport(false);
      }
      throw new IllegalArgumentException(
          String.format(
              "Failed to determine UC Delta Rest Catalog API support for catalog %s (HTTP %s): %s",
              catalog,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  @Override
  public boolean supportsUCDeltaRestCatalogApi() {
    return supportsUCDeltaRestCatalogApi;
  }

  /**
   * Ensures the client has not been closed. Must be called before any API operation.
   */
  protected final void ensureUCDeltaClientOpen() {
    if (closed) {
      throw new IllegalStateException("UCDeltaTokenBasedRestClient has been closed.");
    }
  }

  private void ensureUCDeltaRestCatalogApiSupported(String operation) {
    ensureUCDeltaClientOpen();
    if (!supportsUCDeltaRestCatalogApi) {
      throw new UnsupportedOperationException(
          operation + " requires UC Delta Rest Catalog API support.");
    }
  }

  /**
   * Loads one table from Unity Catalog.
   *
   * <p>This uses the UC Delta Rest Catalog API. Callers that need legacy UC loadTable behavior
   * should use the existing catalog path instead of this API-only method.
   */
  @Override
  public AbstractMetadata loadTable(
      String catalog,
      String schema,
      String table) throws IOException {
    ensureUCDeltaRestCatalogApiSupported("loadTable");
    Objects.requireNonNull(catalog, "catalog must not be null.");
    Objects.requireNonNull(schema, "schema must not be null.");
    Objects.requireNonNull(table, "table must not be null.");

    try {
      io.unitycatalog.client.delta.model.LoadTableResponse response =
          deltaTablesApi.loadTable(catalog, schema, table);
      if (response == null || response.getMetadata() == null) {
        throw new IOException(
            String.format(
                "Malformed UC Delta Rest Catalog API loadTable response for table %s.%s.%s: "
                    + "missing table metadata.",
                catalog,
                schema,
                table));
      }
      if (response.getMetadata().getColumns() == null) {
        throw new IOException(
            String.format(
                "Malformed UC Delta Rest Catalog API loadTable response for table %s.%s.%s: "
                    + "missing table schema columns.",
                catalog,
                schema,
                table));
      }
      return toTableMetadata(table, response.getMetadata());
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to load table %s.%s.%s via UC Delta Rest Catalog API (HTTP %s): %s",
              catalog,
              schema,
              table,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  /**
   * Creates a Delta staging table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  @Override
  public StagingTableResponse createStagingTable(
      String catalog,
      String schema,
      String table) throws IOException {
    ensureUCDeltaRestCatalogApiSupported("createStagingTable");
    Objects.requireNonNull(catalog, "catalog must not be null.");
    Objects.requireNonNull(schema, "schema must not be null.");
    Objects.requireNonNull(table, "table must not be null.");

    try {
      return toStagingTableResponse(deltaTablesApi.createStagingTable(
          catalog,
          schema,
          new CreateStagingTableRequest().name(table)));
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to create staging table %s.%s.%s via UC Delta Rest Catalog API (HTTP %s): %s",
              catalog,
              schema,
              table,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  /**
   * Finalizes a Delta table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  @Override
  public AbstractMetadata createTable(
      String catalog,
      String schema,
      CreateTableRequest request) throws IOException {
    ensureUCDeltaRestCatalogApiSupported("createTable");
    Objects.requireNonNull(catalog, "catalog must not be null.");
    Objects.requireNonNull(schema, "schema must not be null.");
    Objects.requireNonNull(request, "request must not be null.");

    try {
      io.unitycatalog.client.delta.model.LoadTableResponse response =
          deltaTablesApi.createTable(catalog, schema, toSdkCreateTableRequest(request));
      return response == null ? null : toTableMetadata(request.getName(), response.getMetadata());
    } catch (ApiException e) {
      String table = request.getName() != null ? request.getName() : "<unknown>";
      throw new IOException(
          String.format(
              "Failed to create table %s.%s.%s via UC Delta Rest Catalog API (HTTP %s): %s",
              catalog,
              schema,
              table,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  private static TableMetadataAdapter toTableMetadata(String tableName, TableMetadata metadata) {
    if (metadata == null) {
      return null;
    }
    return new TableMetadataAdapter(tableName, metadata);
  }

  private static StagingTableResponse toStagingTableResponse(
      io.unitycatalog.client.delta.model.StagingTableResponse response) {
    if (response == null) {
      return null;
    }
    return new StagingTableResponse(
        response.getTableId(),
        toTableType(response.getTableType()),
        response.getLocation(),
        toProtocol(response.getRequiredProtocol()),
        toProtocol(response.getSuggestedProtocol()),
        response.getRequiredProperties(),
        response.getSuggestedProperties());
  }

  private static DeltaProtocol toProtocol(
      io.unitycatalog.client.delta.model.StagingTableResponseRequiredProtocol protocol) {
    if (protocol == null) {
      return null;
    }
    return new DeltaProtocol()
        .minReaderVersion(protocol.getMinReaderVersion())
        .minWriterVersion(protocol.getMinWriterVersion())
        .readerFeatures(protocol.getReaderFeatures())
        .writerFeatures(protocol.getWriterFeatures());
  }

  private static DeltaProtocol toProtocol(
      io.unitycatalog.client.delta.model.StagingTableResponseSuggestedProtocol protocol) {
    if (protocol == null) {
      return null;
    }
    return new DeltaProtocol()
        .readerFeatures(protocol.getReaderFeatures())
        .writerFeatures(protocol.getWriterFeatures());
  }

  private static io.unitycatalog.client.delta.model.CreateTableRequest toSdkCreateTableRequest(
      CreateTableRequest request) {
    io.unitycatalog.client.delta.model.CreateTableRequest sdkRequest =
        new io.unitycatalog.client.delta.model.CreateTableRequest();
    sdkRequest
        .name(request.getName())
        .location(request.getLocation())
        .tableType(toSdkTableType(request.getTableType()))
        .dataSourceFormat(toSdkDataSourceFormat(request.getDataSourceFormat()))
        .comment(request.getComment())
        .columns(toSdkStructType(request.getSchemaString()))
        .partitionColumns(request.getPartitionColumns())
        .protocol(toSdkDeltaProtocol(request.getProtocol()))
        .properties(request.getProperties());
    sdkRequest.lastCommitTimestampMs(request.getLastCommitTimestampMs());
    return sdkRequest;
  }

  private static TableType toTableType(
      io.unitycatalog.client.delta.model.TableType tableType) {
    if (tableType == null) {
      return null;
    }
    switch (tableType) {
      case MANAGED:
        return TableType.MANAGED;
      case EXTERNAL:
        return TableType.EXTERNAL;
      default:
        throw new IllegalArgumentException("Unsupported UC Delta table type: " + tableType);
    }
  }

  private static io.unitycatalog.client.delta.model.TableType toSdkTableType(
      TableType tableType) {
    if (tableType == null) {
      return null;
    }
    switch (tableType) {
      case MANAGED:
        return io.unitycatalog.client.delta.model.TableType.MANAGED;
      case EXTERNAL:
        return io.unitycatalog.client.delta.model.TableType.EXTERNAL;
      default:
        throw new IllegalArgumentException("Unsupported UC Delta table type: " + tableType);
    }
  }

  private static io.unitycatalog.client.delta.model.DataSourceFormat toSdkDataSourceFormat(
      DataSourceFormat format) {
    if (format == null) {
      return null;
    }
    switch (format) {
      case DELTA:
        return io.unitycatalog.client.delta.model.DataSourceFormat.DELTA;
      case ICEBERG:
        return io.unitycatalog.client.delta.model.DataSourceFormat.ICEBERG;
      default:
        throw new IllegalArgumentException("Unsupported UC Delta data source format: " + format);
    }
  }

  private static io.unitycatalog.client.delta.model.StructType toSdkStructType(
      String schemaString) {
    if (schemaString == null) {
      return null;
    }
    try {
      return DELTA_TYPE_OBJECT_MAPPER.treeToValue(
          normalizeDeltaSchemaJson(DELTA_TYPE_OBJECT_MAPPER.readTree(schemaString)),
          io.unitycatalog.client.delta.model.StructType.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize Delta schema JSON.", e);
    }
  }

  // Spark stores array/map schema fields in camelCase; the UC Delta API SDK model uses the wire
  // names from the OpenAPI spec. Normalize only those field names before binding to SDK models.
  private static JsonNode normalizeDeltaSchemaJson(JsonNode node) {
    if (node == null) {
      return null;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        normalizeDeltaSchemaJson(child);
      }
    } else if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      renameField(object, "elementType", "element-type");
      renameField(object, "containsNull", "contains-null");
      renameField(object, "keyType", "key-type");
      renameField(object, "valueType", "value-type");
      renameField(object, "valueContainsNull", "value-contains-null");
      for (JsonNode child : object) {
        normalizeDeltaSchemaJson(child);
      }
    }
    return node;
  }

  private static void renameField(ObjectNode object, String from, String to) {
    if (object.has(from)) {
      if (!object.has(to)) {
        object.set(to, object.get(from));
      }
      object.remove(from);
    }
  }

  private static io.unitycatalog.client.delta.model.DeltaProtocol toSdkDeltaProtocol(
      DeltaProtocol protocol) {
    if (protocol == null) {
      return null;
    }
    return new io.unitycatalog.client.delta.model.DeltaProtocol()
        .minReaderVersion(protocol.getMinReaderVersion())
        .minWriterVersion(protocol.getMinWriterVersion())
        .readerFeatures(protocol.getReaderFeatures())
        .writerFeatures(protocol.getWriterFeatures());
  }

  /**
   * Adapts UC Delta SDK table metadata to Delta's storage-level metadata interface.
   *
   * <p>The UC SDK schema stays hidden behind this client implementation. Callers see the schema
   * through Delta's storage-level schema JSON.
   */
  public static final class TableMetadataAdapter implements AbstractMetadata {
    private final String tableName;
    private final TableMetadata delegate;
    private final String schemaString;

    private TableMetadataAdapter(String tableName, TableMetadata delegate) {
      this.tableName = Objects.requireNonNull(tableName, "tableName must not be null.");
      this.delegate = Objects.requireNonNull(delegate, "delegate must not be null.");
      this.schemaString = toSchemaString(
          Objects.requireNonNull(delegate.getColumns(), "UC Delta table schema is missing."));
    }

    public String getTableType() {
      return delegate.getTableType() == null ? null : delegate.getTableType().getValue();
    }

    public UUID getTableUuid() {
      return delegate.getTableUuid();
    }

    public String getLocation() {
      return delegate.getLocation();
    }

    @Override
    public String getId() {
      return delegate.getTableUuid() == null ? null : delegate.getTableUuid().toString();
    }

    @Override
    public String getName() {
      return tableName;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public String getProvider() {
      return delegate.getDataSourceFormat() == null
          ? null
          : delegate.getDataSourceFormat().getValue().toLowerCase(Locale.ROOT);
    }

    @Override
    public Map<String, String> getFormatOptions() {
      return Collections.emptyMap();
    }

    @Override
    public String getSchemaString() {
      return schemaString;
    }

    @Override
    public List<String> getPartitionColumns() {
      return delegate.getPartitionColumns();
    }

    @Override
    public Map<String, String> getConfiguration() {
      return delegate.getProperties();
    }

    @Override
    public Long getCreatedTime() {
      return delegate.getCreatedTime();
    }
  }

  private static String toSchemaString(
      io.unitycatalog.client.delta.model.StructType schema) {
    try {
      return DELTA_SCHEMA_OBJECT_MAPPER.writeValueAsString(schema);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize UC Delta schema.", e);
    }
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    this.deltaTablesApi = null;
    super.close();
  }
}
