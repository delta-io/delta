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

import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.TableIdentifier;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo;
import io.delta.storage.commit.uccommitcoordinator.exceptions.CredentialFetchFailedException;
import io.delta.storage.commit.uccommitcoordinator.exceptions.NoSuchTableException;
import io.delta.storage.commit.uccommitcoordinator.exceptions.UnsupportedTableFormatException;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.MetastoresApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.AddCommitUpdate;
import io.unitycatalog.client.delta.model.AssertTableUUID;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.client.delta.model.SetPartitionColumnsUpdate;
import io.unitycatalog.client.delta.model.SetPropertiesUpdate;
import io.unitycatalog.client.delta.model.SetProtocolUpdate;
import io.unitycatalog.client.delta.model.SetSchemaUpdate;
import io.unitycatalog.client.delta.model.SetTableCommentUpdate;
import io.unitycatalog.client.delta.model.StagingTableResponse;
import io.unitycatalog.client.delta.model.StagingTableResponseRequiredProtocol;
import io.unitycatalog.client.delta.model.StagingTableResponseSuggestedProtocol;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.model.UniformMetadata;
import io.unitycatalog.client.delta.model.UniformMetadataIceberg;
import io.unitycatalog.client.delta.model.UpdateTableRequest;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.TableOperation;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;

/**
 * A REST client implementation of {@link UCDeltaClient} that uses the UC Delta REST API for
 * all table lifecycle and commit coordination operations.
 *
 * <p>This client uses {@code io.unitycatalog.client.delta.api.TablesApi} for Delta-specific
 * table operations (load, create, update) and {@link MetastoresApi} for metastore queries.
 *
 * @see UCDeltaClient
 */
public class UCDeltaTokenBasedRestClient implements UCDeltaClient {

  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_NOT_FOUND = 404;

  private TablesApi deltaTablesApi;
  private MetastoresApi metastoresApi;
  private final ApiClient apiClient;
  private final String baseUri;
  private final TokenProvider tokenProvider;
  private final Map<String, String> appVersions;
  private final boolean credentialRenewalEnabled;
  private final boolean credentialScopedFsEnabled;
  private final Supplier<Configuration> hadoopConfSupplier;

  public UCDeltaTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    this(baseUri, tokenProvider, appVersions, false, false, null);
  }

  /**
   * @param hadoopConfSupplier called once per request so engine-level changes are picked up;
   *                           {@code null} defaults to {@code () -> new Configuration()}.
   */
  public UCDeltaTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      boolean credentialRenewalEnabled,
      boolean credentialScopedFsEnabled,
      Supplier<Configuration> hadoopConfSupplier) {
    Objects.requireNonNull(baseUri, "baseUri must not be null");
    Objects.requireNonNull(tokenProvider, "tokenProvider must not be null");
    Objects.requireNonNull(appVersions, "appVersions must not be null");

    ApiClientBuilder builder = ApiClientBuilder.create()
        .uri(baseUri)
        .tokenProvider(tokenProvider);

    appVersions.forEach((name, version) -> {
      if (version != null) {
        builder.addAppVersion(name, version);
      }
    });

    this.apiClient = builder.build();
    this.deltaTablesApi = new TablesApi(this.apiClient);
    this.metastoresApi = new MetastoresApi(this.apiClient);
    this.baseUri = baseUri;
    this.tokenProvider = tokenProvider;
    this.appVersions = appVersions;
    this.credentialRenewalEnabled = credentialRenewalEnabled;
    this.credentialScopedFsEnabled = credentialScopedFsEnabled;
    this.hadoopConfSupplier = hadoopConfSupplier != null ? hadoopConfSupplier : Configuration::new;
  }

  /**
   * Factory for callers that can't depend on {@code io.unitycatalog.client} directly: pass
   * a flat {@code authConfigs} map ({@code type} + provider-specific keys) and the factory
   * constructs the {@link TokenProvider} internally.
   */
  public static UCDeltaTokenBasedRestClient create(
      String baseUri,
      Map<String, String> authConfigs,
      Map<String, String> appVersions,
      boolean credentialRenewalEnabled,
      boolean credentialScopedFsEnabled,
      Supplier<Configuration> hadoopConfSupplier) {
    Objects.requireNonNull(authConfigs, "authConfigs must not be null");
    if (authConfigs.isEmpty()) {
      throw new IllegalArgumentException(
          "authConfigs must not be empty; expected at least a 'type' key plus the keys " +
              "required by that TokenProvider type.");
    }
    return new UCDeltaTokenBasedRestClient(
        baseUri,
        TokenProvider.create(authConfigs),
        appVersions,
        credentialRenewalEnabled,
        credentialScopedFsEnabled,
        hadoopConfSupplier);
  }

  /** Fresh builder per call: scheme depends on the table's location, hadoopConf is live. */
  private UCCredentialHadoopConfs.Builder newCredBuilder(String scheme) {
    return UCCredentialHadoopConfs.builder(baseUri, scheme)
        .tokenProvider(tokenProvider)
        .apiClient(apiClient)
        .enableCredentialRenewal(credentialRenewalEnabled)
        .enableCredentialScopedFs(credentialScopedFsEnabled)
        .addAppVersions(appVersions)
        .hadoopConf(hadoopConfSupplier.get());
  }

  private static String schemeOf(String location) {
    int colon = location.indexOf(':');
    return colon > 0 ? location.substring(0, colon) : "";
  }

  private Map<String, String> fetchTableCredentials(
      String catalog, String schema, String table, String location) throws ApiException {
    UCCredentialHadoopConfs.Builder b = newCredBuilder(schemeOf(location));
    try {
      return b.buildForTable(catalog, schema, table, TableOperation.READ_WRITE, location);
    } catch (ApiException rw) {
      return b.buildForTable(catalog, schema, table, TableOperation.READ, location);
    } catch (IllegalArgumentException malformed) {
      // UC Hadoop's response validator (DeltaStorageCredentialUtil.requireSingleCloudConfig)
      // throws when the scheme has no cloud cred (e.g. file://). Treat as no creds.
      return Collections.emptyMap();
    }
  }

  private void ensureOpen() {
    if (deltaTablesApi == null || metastoresApi == null) {
      throw new IllegalStateException("UCDeltaTokenBasedRestClient has been closed.");
    }
  }

  // ===========================
  // UCClient Implementation
  // ===========================

  @Override
  public String getMetastoreId() throws IOException {
    ensureOpen();
    try {
      GetMetastoreSummaryResponse response = metastoresApi.summary();
      return response.getMetastoreId();
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to get metastore ID (HTTP %s): %s",
              e.getCode(), e.getResponseBody()), e);
    }
  }

  @Override
  public void commit(
      String tableId,
      URI tableUri,
      TableIdentifier tableIdentifier,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      Optional<AbstractMetadata> oldMetadata,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol,
      Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    ensureOpen();
    Objects.requireNonNull(tableId, "tableId must not be null");
    Objects.requireNonNull(tableIdentifier, "tableIdentifier must not be null");

    UpdateTableRequest request = new UpdateTableRequest();
    request.addRequirementsItem(new AssertTableUUID()
        .type("assert-table-uuid")
        .uuid(UUID.fromString(tableId)));

    commit.ifPresent(c -> {
      AddCommitUpdate addCommit = new AddCommitUpdate()
          .action("add-commit")
          .commit(toSDKDeltaCommit(c));
      uniform.ifPresent(u -> addCommit.uniform(toSDKUniformMetadata(u)));
      request.addUpdatesItem(addCommit);
    });

    lastKnownBackfilledVersion.ifPresent(v ->
        request.addUpdatesItem(new SetLatestBackfilledVersionUpdate()
            .action("set-latest-backfilled-version")
            .latestPublishedVersion(v)));

    if (oldMetadata.isPresent()
        && newMetadata.isPresent()
        && !Objects.equals(oldMetadata.get(), newMetadata.get())) {
      addMetadataUpdates(request, oldMetadata.get(), newMetadata.get());
    }
    if (oldProtocol.isPresent()
        && newProtocol.isPresent()
        && !Objects.equals(oldProtocol.get(), newProtocol.get())) {
      request.addUpdatesItem(new SetProtocolUpdate()
          .action("set-protocol")
          .protocol(toSDKDeltaProtocol(newProtocol.get())));
    }

    String catalog = tableIdentifier.getNamespace()[0];
    String schema = tableIdentifier.getNamespace()[1];
    String table = tableIdentifier.getName();

    try {
      deltaTablesApi.updateTable(catalog, schema, table, request);
    } catch (ApiException e) {
      handleUpdateTableException(e, catalog, schema, table);
    }
  }

  @Override
  public GetCommitsResponse getCommits(
      String tableId,
      URI tableUri,
      TableIdentifier tableIdentifier,
      Optional<Long> startVersion,
      Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException {
    throw new UnsupportedOperationException(
        "getCommits is not yet supported by UCDeltaTokenBasedRestClient. " +
            "A separate PR will add this once the tableIdentifier mapping is available.");
  }

  @Override
  public void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<ColumnDef> columns,
      Map<String, String> properties) throws CommitFailedException {
    ensureOpen();
    Objects.requireNonNull(tableName, "tableName must not be null");
    Objects.requireNonNull(catalogName, "catalogName must not be null");
    Objects.requireNonNull(schemaName, "schemaName must not be null");
    Objects.requireNonNull(storageLocation, "storageLocation must not be null");
    Objects.requireNonNull(columns, "columns must not be null");
    Objects.requireNonNull(properties, "properties must not be null");

    CreateTableRequest sdkRequest = new CreateTableRequest()
        .name(tableName)
        .location(storageLocation)
        .properties(properties);

    if (!columns.isEmpty()) {
      sdkRequest.columns(UCDeltaSchemaConverter.toUCStructType(columns));
    }

    try {
      deltaTablesApi.createTable(catalogName, schemaName, sdkRequest);
    } catch (ApiException e) {
      throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          String.format("Failed to finalize table %s.%s.%s (HTTP %s): %s",
              catalogName, schemaName, tableName, e.getCode(), e.getResponseBody()),
          e);
    }
  }

  @Override
  public void close() throws IOException {
    this.deltaTablesApi = null;
    this.metastoresApi = null;
  }

  // ===========================
  // UCDeltaClient Implementation
  // ===========================

  @Override
  public TableInfo loadTable(TableIdentifier tableIdentifier) throws IOException {
    ensureOpen();
    Objects.requireNonNull(tableIdentifier, "tableIdentifier must not be null");
    String[] namespace = tableIdentifier.getNamespace();
    if (namespace == null || namespace.length != 2) {
      throw new IllegalArgumentException(
          "UC tableIdentifier must have a 2-component namespace [catalog, schema]; got " +
              (namespace == null ? "null" : java.util.Arrays.toString(namespace)));
    }
    String catalog = namespace[0];
    String schema = namespace[1];
    String table = tableIdentifier.getName();

    try {
      return toTableInfo(
          deltaTablesApi.loadTable(catalog, schema, table), catalog, schema, table);
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        throw new NoSuchTableException(
            String.format("Table %s.%s.%s not found in Unity Catalog", catalog, schema, table), e);
      }
      // UC encodes non-Delta-format errors as HTTP 400 with error.type =
      // "UnsupportedTableFormatException"; substring-match the body to avoid coupling to an
      // ErrorResponse parser.
      String body = e.getResponseBody();
      if (body != null && body.contains("UnsupportedTableFormatException")) {
        throw new UnsupportedTableFormatException(
            String.format("Table %s.%s.%s is not in Delta format; the Delta REST API cannot "
                + "serve it. Body: %s", catalog, schema, table, body),
            e);
      }
      throw new IOException(
          String.format("Failed to load table %s.%s.%s (HTTP %s): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()), e);
    }
  }

  @Override
  public UCDeltaModels.StagingTableInfo createStagingTable(
      String catalog, String schema, String table) throws IOException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");

    try {
      CreateStagingTableRequest request = new CreateStagingTableRequest().name(table);
      StagingTableResponse response =
          deltaTablesApi.createStagingTable(catalog, schema, request);
      return toStagingTableInfo(response);
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to create staging table %s.%s.%s (HTTP %s): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()), e);
    }
  }

  @Override
  public AbstractMetadata createTable(
      String catalog,
      String schema,
      String name,
      String location,
      UCDeltaModels.TableType tableType,
      String comment,
      List<String> partitionColumns,
      UCDeltaModels.DeltaProtocol protocol,
      Map<String, String> properties) throws IOException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(name, "name must not be null");

    try {
      CreateTableRequest sdkRequest = new CreateTableRequest()
          .name(name)
          .location(location);
      if (tableType != null) {
        sdkRequest.tableType(
            io.unitycatalog.client.delta.model.TableType.fromValue(tableType.name()));
      }
      if (comment != null) {
        sdkRequest.comment(comment);
      }
      if (partitionColumns != null && !partitionColumns.isEmpty()) {
        sdkRequest.partitionColumns(partitionColumns);
      }
      if (protocol != null) {
        sdkRequest.protocol(toSDKDeltaProtocol(protocol));
      }
      if (properties != null && !properties.isEmpty()) {
        sdkRequest.properties(properties);
      }

      LoadTableResponse response =
          deltaTablesApi.createTable(catalog, schema, sdkRequest);
      return new DeltaTableMetadata(name, response.getMetadata());
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to create table %s.%s.%s (HTTP %s): %s",
              catalog, schema, name, e.getCode(), e.getResponseBody()), e);
    }
  }

  // ===========================
  // Response Conversion Methods
  // ===========================

  private TableInfo toTableInfo(
      LoadTableResponse response, String catalog, String schema, String name) throws IOException {
    TableMetadata m = response.getMetadata();
    String location = m.getLocation();
    if (location == null) {
      throw new IOException("UC returned null location for table " + name);
    }
    UUID ucTableId = m.getTableUuid();
    if (ucTableId == null) {
      throw new IOException("UC returned null table ID for table " + name);
    }
    if (m.getTableType() == null) {
      throw new IOException("UC returned null table type for table " + name);
    }
    UCDeltaModels.TableType tableType =
        UCDeltaModels.TableType.valueOf(m.getTableType().getValue());
    DeltaTableMetadata adapted = new DeltaTableMetadata(name, m);
    Map<String, String> storageProps;
    try {
      storageProps = fetchTableCredentials(catalog, schema, name, location);
    } catch (ApiException e) {
      // Surface as a typed failure so callers with a fallback (e.g. server-side planning) can
      // recover. The exception carries the catalog-side TableInfo (with empty storageProperties)
      // so the caller can still build a CatalogTable.
      TableInfo withoutCreds = new TableInfo(
          ucTableId, tableType, location, adapted, Collections.emptyMap());
      throw new CredentialFetchFailedException(
          String.format("Credential fetch failed for table %s.%s.%s (HTTP %s): %s",
              catalog, schema, name, e.getCode(), e.getResponseBody()),
          e, withoutCreds);
    }
    return new TableInfo(ucTableId, tableType, location, adapted, storageProps);
  }

  private UCDeltaModels.StagingTableInfo toStagingTableInfo(StagingTableResponse r) {
    UCDeltaModels.TableType tableType = null;
    if (r.getTableType() != null) {
      tableType = UCDeltaModels.TableType.valueOf(r.getTableType().getValue());
    }

    return new UCDeltaModels.StagingTableInfo(
        r.getTableId(),
        tableType,
        r.getLocation(),
        toDeltaProtocol(r.getRequiredProtocol()),
        toDeltaProtocol(r.getSuggestedProtocol()),
        r.getRequiredProperties(),
        r.getSuggestedProperties());
  }

  private UCDeltaModels.DeltaProtocol toDeltaProtocol(StagingTableResponseRequiredProtocol p) {
    if (p == null) {
      return null;
    }
    UCDeltaModels.DeltaProtocol protocol = new UCDeltaModels.DeltaProtocol()
        .minReaderVersion(p.getMinReaderVersion())
        .minWriterVersion(p.getMinWriterVersion());
    if (p.getReaderFeatures() != null) {
      protocol.readerFeatures(p.getReaderFeatures());
    }
    if (p.getWriterFeatures() != null) {
      protocol.writerFeatures(p.getWriterFeatures());
    }
    return protocol;
  }

  private UCDeltaModels.DeltaProtocol toDeltaProtocol(StagingTableResponseSuggestedProtocol p) {
    if (p == null) {
      return null;
    }
    UCDeltaModels.DeltaProtocol protocol = new UCDeltaModels.DeltaProtocol();
    if (p.getReaderFeatures() != null) {
      protocol.readerFeatures(p.getReaderFeatures());
    }
    if (p.getWriterFeatures() != null) {
      protocol.writerFeatures(p.getWriterFeatures());
    }
    return protocol;
  }

  // ===========================
  // SDK Conversion Methods
  // ===========================

  private DeltaCommit toSDKDeltaCommit(Commit c) {
    Objects.requireNonNull(c, "commit must not be null");
    Objects.requireNonNull(c.getFileStatus(), "commit fileStatus must not be null");
    return new DeltaCommit()
        .version(c.getVersion())
        .timestamp(c.getCommitTimestamp())
        .fileName(c.getFileStatus().getPath().getName())
        .fileSize(c.getFileStatus().getLen())
        .fileModificationTimestamp(c.getFileStatus().getModificationTime());
  }

  private DeltaProtocol toSDKDeltaProtocol(AbstractProtocol p) {
    DeltaProtocol protocol = new DeltaProtocol()
        .minReaderVersion(p.getMinReaderVersion())
        .minWriterVersion(p.getMinWriterVersion());
    if (p.getReaderFeatures() != null && !p.getReaderFeatures().isEmpty()) {
      protocol.readerFeatures(new ArrayList<>(p.getReaderFeatures()));
    }
    if (p.getWriterFeatures() != null && !p.getWriterFeatures().isEmpty()) {
      protocol.writerFeatures(new ArrayList<>(p.getWriterFeatures()));
    }
    return protocol;
  }

  private UniformMetadata toSDKUniformMetadata(
      io.delta.storage.commit.uniform.UniformMetadata uniform) {
    UniformMetadata ucUniform = new UniformMetadata();
    uniform.getIcebergMetadata().ifPresent(iceberg -> {
      UniformMetadataIceberg ucIceberg = new UniformMetadataIceberg()
          .metadataLocation(iceberg.getMetadataLocation())
          .convertedDeltaVersion(iceberg.getConvertedDeltaVersion())
          .convertedDeltaTimestamp(parseTimestampToEpochMs(
              iceberg.getConvertedDeltaTimestamp()));
      iceberg.getBaseConvertedDeltaVersion().ifPresent(
          ucIceberg::baseConvertedDeltaVersion);
      ucUniform.iceberg(ucIceberg);
    });
    return ucUniform;
  }

  /**
   * Parses a timestamp string to epoch milliseconds. Handles both numeric strings (already epoch
   * millis) and ISO-8601 datetime strings (e.g. "2025-01-04T03:13:11.423Z").
   */
  private Long parseTimestampToEpochMs(String timestamp) {
    if (timestamp == null) {
      return null;
    }
    try {
      return Long.parseLong(timestamp);
    } catch (NumberFormatException e) {
      return java.time.Instant.parse(timestamp).toEpochMilli();
    }
  }

  /**
   * Compares old and new metadata, adding the appropriate UC SDK update items to the request for
   * any fields that changed.
   */
  private void addMetadataUpdates(
      UpdateTableRequest request,
      AbstractMetadata oldMetadata,
      AbstractMetadata newMetadata) {
    if (!Objects.equals(oldMetadata.getSchemaString(), newMetadata.getSchemaString())) {
      request.addUpdatesItem(new SetSchemaUpdate()
          .action("set-columns")
          .columns(UCDeltaSchemaConverter.parseSchemaString(newMetadata.getSchemaString())));
    }
    if (!Objects.equals(oldMetadata.getPartitionColumns(), newMetadata.getPartitionColumns())) {
      request.addUpdatesItem(new SetPartitionColumnsUpdate()
          .action("set-partition-columns")
          .partitionColumns(newMetadata.getPartitionColumns()));
    }
    if (!Objects.equals(oldMetadata.getDescription(), newMetadata.getDescription())) {
      request.addUpdatesItem(new SetTableCommentUpdate()
          .action("set-table-comment")
          .comment(newMetadata.getDescription()));
    }

    Map<String, String> oldConfig = oldMetadata.getConfiguration() != null
        ? oldMetadata.getConfiguration() : Collections.emptyMap();
    Map<String, String> newConfig = newMetadata.getConfiguration() != null
        ? newMetadata.getConfiguration() : Collections.emptyMap();

    if (!Objects.equals(oldConfig, newConfig)) {
      Map<String, String> toSet = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : newConfig.entrySet()) {
        if (!Objects.equals(entry.getValue(), oldConfig.get(entry.getKey()))) {
          toSet.put(entry.getKey(), entry.getValue());
        }
      }
      if (!toSet.isEmpty()) {
        request.addUpdatesItem(new SetPropertiesUpdate()
            .action("set-properties")
            .updates(toSet));
      }

      List<String> toRemove = new ArrayList<>();
      for (String key : oldConfig.keySet()) {
        if (!newConfig.containsKey(key)) {
          toRemove.add(key);
        }
      }
      if (!toRemove.isEmpty()) {
        request.addUpdatesItem(new RemovePropertiesUpdate()
            .action("remove-properties")
            .removals(toRemove));
      }
    }
  }

  // ===========================
  // Exception Handling
  // ===========================

  private void handleUpdateTableException(
      ApiException e, String catalog, String schema, String table)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    int statusCode = e.getCode();
    String responseBody = e.getResponseBody();

    switch (statusCode) {
      case HTTP_CONFLICT:
        throw new CommitFailedException(
            true /* retryable */,
            true /* conflict */,
            String.format("Update conflict for %s.%s.%s: %s",
                catalog, schema, table, responseBody),
            e);
      case HTTP_NOT_FOUND:
        throw new InvalidTargetTableException(
            String.format("Table not found %s.%s.%s: %s",
                catalog, schema, table, responseBody));
      default:
        throw new IOException(
            String.format("Failed to update table %s.%s.%s (HTTP %s): %s",
                catalog, schema, table, statusCode, responseBody), e);
    }
  }

  // ===========================
  // Inner Classes
  // ===========================

  /**
   * Adapts a UC SDK {@link TableMetadata} to {@link AbstractMetadata}.
   */
  private static final class DeltaTableMetadata implements AbstractMetadata {

    private final String name;
    private final TableMetadata m;

    DeltaTableMetadata(String name, TableMetadata m) {
      this.name = name;
      this.m = m;
    }

    @Override
    public String getId() {
      // UC's loadTable response carries the UC table_uuid (exposed via TableInfo.getTableId),
      // not the Delta Metadata.id. The Delta id only lives in the Delta log Metadata action and
      // is never sent to UC; callers that need it must read the log.
      return null;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public String getProvider() {
      return m.getDataSourceFormat() != null ? m.getDataSourceFormat().getValue() : null;
    }

    @Override
    public Map<String, String> getFormatOptions() {
      return Collections.emptyMap();
    }

    @Override
    public String getSchemaString() {
      return UCDeltaSchemaConverter.serializeSchema(m.getColumns());
    }

    @Override
    public List<String> getPartitionColumns() {
      return m.getPartitionColumns() != null
          ? m.getPartitionColumns() : Collections.emptyList();
    }

    @Override
    public Map<String, String> getConfiguration() {
      return m.getProperties() != null ? m.getProperties() : Collections.emptyMap();
    }

    @Override
    public Long getCreatedTime() {
      return m.getCreatedTime();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DeltaTableMetadata)) return false;
      DeltaTableMetadata that = (DeltaTableMetadata) o;
      return Objects.equals(getId(), that.getId())
          && Objects.equals(getName(), that.getName())
          && Objects.equals(getProvider(), that.getProvider())
          && Objects.equals(getSchemaString(), that.getSchemaString())
          && Objects.equals(getPartitionColumns(), that.getPartitionColumns())
          && Objects.equals(getConfiguration(), that.getConfiguration())
          && Objects.equals(getCreatedTime(), that.getCreatedTime());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getId(), getName(), getProvider(), getSchemaString(),
          getPartitionColumns(), getConfiguration(), getCreatedTime());
    }
  }
}
