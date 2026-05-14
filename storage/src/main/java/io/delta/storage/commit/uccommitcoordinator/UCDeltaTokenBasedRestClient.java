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
import io.delta.storage.commit.CoordinatedCommitsUtils;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.TableIdentifier;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.MetastoresApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.AddCommitUpdate;
import io.unitycatalog.client.delta.model.AssertEtag;
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
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.model.TableRequirement;
import io.unitycatalog.client.delta.model.TableUpdate;
import io.unitycatalog.client.delta.model.UniformMetadata;
import io.unitycatalog.client.delta.model.UniformMetadataIceberg;
import io.unitycatalog.client.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.client.delta.model.UpdateTableRequest;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

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
import java.util.concurrent.ConcurrentHashMap;

/**
 * A REST client implementation of {@link UCDeltaClient} that uses the UC Delta REST Catalog API
 * for all table lifecycle and commit coordination operations.
 *
 * <p>This client uses {@code io.unitycatalog.client.delta.api.TablesApi} for Delta-specific
 * table operations (load, create, update) and {@link MetastoresApi} for metastore queries.
 *
 * @see UCDeltaClient
 */
public class UCDeltaTokenBasedRestClient implements UCDeltaClient {

  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_NOT_FOUND = 404;

  private final Map<String, TableIdentifier> tableIdToIdentifier = new ConcurrentHashMap<>();

  private TablesApi deltaTablesApi;
  private MetastoresApi metastoresApi;

  /**
   * Constructs a new UCDeltaTokenBasedRestClient.
   *
   * @param baseUri       The base URI of the Unity Catalog server
   * @param tokenProvider The TokenProvider to use for authentication
   * @param appVersions   A map of application name to version string for telemetry
   */
  public UCDeltaTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
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

    ApiClient apiClient = builder.build();
    this.deltaTablesApi = new TablesApi(apiClient);
    this.metastoresApi = new MetastoresApi(apiClient);
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

    tableIdToIdentifier.put(tableId, tableIdentifier);

    UCDeltaModels.UpdateTableRequest request = new UCDeltaModels.UpdateTableRequest()
        .addRequirementsItem(UCDeltaModels.TableRequirement.assertTableUuid(
            UUID.fromString(tableId)));

    commit.ifPresent(c -> request.addUpdatesItem(
        UCDeltaModels.TableUpdate.addCommit(toUCDeltaCommit(c), uniform.orElse(null))));
    lastKnownBackfilledVersion.ifPresent(v ->
        request.addUpdatesItem(UCDeltaModels.TableUpdate.setLatestBackfilledVersion(v)));

    if (oldMetadata.isPresent()
        && newMetadata.isPresent()
        && oldMetadata.get() != newMetadata.get()) {
      addMetadataUpdates(request, oldMetadata.get(), newMetadata.get());
    }
    if (oldProtocol.isPresent()
        && newProtocol.isPresent()
        && oldProtocol.get() != newProtocol.get()) {
      request.addUpdatesItem(
          UCDeltaModels.TableUpdate.setProtocolUpdate(toUCDeltaProtocol(newProtocol.get())));
    }

    String catalog = tableIdentifier.getNamespace()[0];
    String schema = tableIdentifier.getNamespace()[1];
    String table = tableIdentifier.getName();
    updateTable(catalog, schema, table, request);
  }

  @Override
  public GetCommitsResponse getCommits(
      String tableId,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException {
    ensureOpen();
    Objects.requireNonNull(tableId, "tableId must not be null");
    Objects.requireNonNull(tableUri, "tableUri must not be null");

    TableIdentifier tableIdentifier = tableIdToIdentifier.get(tableId);
    if (tableIdentifier == null) {
      throw new IllegalStateException(
          "No TableIdentifier cached for tableId " + tableId + ". " +
          "commit() must be called before getCommits() so the mapping is established.");
    }

    String catalog = tableIdentifier.getNamespace()[0];
    String schema = tableIdentifier.getNamespace()[1];
    String table = tableIdentifier.getName();

    try {
      LoadTableResponse response = deltaTablesApi.loadTable(catalog, schema, table);

      Path commitBasePath = CoordinatedCommitsUtils.commitDirPath(
          CoordinatedCommitsUtils.logDirPath(new Path(tableUri)));
      long start = startVersion.orElse(0L);
      long end = endVersion.orElse(Long.MAX_VALUE);

      List<Commit> commits = new ArrayList<>();
      if (response.getCommits() != null) {
        for (DeltaCommit dc : response.getCommits()) {
          if (dc.getVersion() >= start && dc.getVersion() <= end) {
            commits.add(fromDeltaCommit(dc, commitBasePath));
          }
        }
      }

      long latestVersion = response.getLatestTableVersion() != null
          ? response.getLatestTableVersion() : -1L;
      return new GetCommitsResponse(commits, latestVersion);
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        throw new InvalidTargetTableException(
            String.format("Table not found %s.%s.%s: %s",
                catalog, schema, table, e.getResponseBody()));
      }
      throw new IOException(
          String.format("Failed to get commits for %s.%s.%s (HTTP %s): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()), e);
    }
  }

  @Override
  public void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<ColumnDef> columns,
      Map<String, String> properties) throws CommitFailedException {
    throw new UnsupportedOperationException(
        "UCDeltaTokenBasedRestClient uses the Delta REST Catalog API. " +
        "Use createStagingTable() + createTable() instead.");
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
  public AbstractMetadata loadTable(
      String catalog, String schema, String table) throws IOException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");

    try {
      LoadTableResponse response = deltaTablesApi.loadTable(catalog, schema, table);
      return toAbstractMetadata(response.getMetadata());
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to load table %s.%s.%s (HTTP %s): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()), e);
    }
  }

  @Override
  public UCDeltaModels.StagingTableResponse createStagingTable(
      String catalog, String schema, String table) throws IOException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");

    try {
      CreateStagingTableRequest request = new CreateStagingTableRequest().name(table);
      StagingTableResponse response =
          deltaTablesApi.createStagingTable(catalog, schema, request);
      return toDeltaStagingTableResponse(response);
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
      CreateTableRequest request) throws IOException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(request, "request must not be null");

    try {
      LoadTableResponse response = deltaTablesApi.createTable(catalog, schema, request);
      return toAbstractMetadata(response.getMetadata());
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to create table in %s.%s (HTTP %s): %s",
              catalog, schema, e.getCode(), e.getResponseBody()), e);
    }
  }

  private AbstractMetadata updateTable(
      String catalog,
      String schema,
      String table,
      UCDeltaModels.UpdateTableRequest request)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    ensureOpen();
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(request, "request must not be null");

    try {
      UpdateTableRequest ucRequest = toUCUpdateTableRequest(request);
      LoadTableResponse response =
          deltaTablesApi.updateTable(catalog, schema, table, ucRequest);
      return toAbstractMetadata(response.getMetadata());
    } catch (ApiException e) {
      handleUpdateTableException(e, catalog, schema, table);
      throw new IllegalStateException("unreachable");
    }
  }

  // ===========================
  // Response Conversion Methods
  // ===========================

  /**
   * Converts a UC {@link TableMetadata} to Delta's {@link AbstractMetadata}.
   */
  private AbstractMetadata toAbstractMetadata(TableMetadata m) {
    return new AbstractMetadata() {
      @Override
      public String getId() {
        return m.getTableUuid() != null ? m.getTableUuid().toString() : null;
      }

      @Override
      public String getName() {
        return null;
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
        return m.getColumns() != null ? m.getColumns().toString() : null;
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
    };
  }

  /**
   * Converts a UC SDK {@link StagingTableResponse} to Delta's
   * {@link UCDeltaModels.StagingTableResponse}.
   */
  private UCDeltaModels.StagingTableResponse toDeltaStagingTableResponse(
      StagingTableResponse r) {
    UCDeltaModels.TableType tableType = null;
    if (r.getTableType() != null) {
      tableType = UCDeltaModels.TableType.valueOf(r.getTableType().getValue());
    }

    return new UCDeltaModels.StagingTableResponse(
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
    return new UCDeltaModels.DeltaProtocol()
        .minReaderVersion(p.getMinReaderVersion())
        .minWriterVersion(p.getMinWriterVersion())
        .readerFeatures(p.getReaderFeatures())
        .writerFeatures(p.getWriterFeatures());
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
  // Commit Helper Methods
  // ===========================

  /**
   * Converts a Delta {@link Commit} to a {@link UCDeltaModels.DeltaCommit}.
   */
  private UCDeltaModels.DeltaCommit toUCDeltaCommit(Commit c) {
    Objects.requireNonNull(c, "commit must not be null");
    Objects.requireNonNull(c.getFileStatus(), "commit fileStatus must not be null");

    return new UCDeltaModels.DeltaCommit()
        .version(c.getVersion())
        .timestamp(c.getCommitTimestamp())
        .fileName(c.getFileStatus().getPath().getName())
        .fileSize(c.getFileStatus().getLen())
        .fileModificationTimestamp(c.getFileStatus().getModificationTime());
  }

  /**
   * Converts a UC SDK {@link DeltaCommit} to a Delta {@link Commit}.
   */
  private Commit fromDeltaCommit(DeltaCommit dc, Path commitBasePath) {
    FileStatus fileStatus = new FileStatus(
        dc.getFileSize(),
        false /* isdir */,
        0 /* block_replication */,
        0 /* blocksize */,
        dc.getFileModificationTimestamp(),
        new Path(commitBasePath, dc.getFileName()));
    return new Commit(dc.getVersion(), fileStatus, dc.getTimestamp());
  }

  /**
   * Converts a Delta {@link AbstractProtocol} to a {@link UCDeltaModels.DeltaProtocol}.
   */
  private UCDeltaModels.DeltaProtocol toUCDeltaProtocol(AbstractProtocol p) {
    UCDeltaModels.DeltaProtocol protocol = new UCDeltaModels.DeltaProtocol()
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

  /**
   * Compares old and new metadata, adding the appropriate {@link UCDeltaModels.TableUpdate}
   * items to the request for any fields that changed.
   */
  private void addMetadataUpdates(
      UCDeltaModels.UpdateTableRequest request,
      AbstractMetadata oldMetadata,
      AbstractMetadata newMetadata) {
    if (!Objects.equals(oldMetadata.getSchemaString(), newMetadata.getSchemaString())) {
      request.addUpdatesItem(UCDeltaModels.TableUpdate.setColumns(newMetadata.getSchemaString()));
    }
    if (!Objects.equals(oldMetadata.getPartitionColumns(), newMetadata.getPartitionColumns())) {
      request.addUpdatesItem(
          UCDeltaModels.TableUpdate.setPartitionColumnsUpdate(newMetadata.getPartitionColumns()));
    }
    if (!Objects.equals(oldMetadata.getDescription(), newMetadata.getDescription())) {
      request.addUpdatesItem(
          UCDeltaModels.TableUpdate.setTableComment(newMetadata.getDescription()));
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
        request.addUpdatesItem(UCDeltaModels.TableUpdate.setProperties(toSet));
      }

      List<String> toRemove = new ArrayList<>();
      for (String key : oldConfig.keySet()) {
        if (!newConfig.containsKey(key)) {
          toRemove.add(key);
        }
      }
      if (!toRemove.isEmpty()) {
        request.addUpdatesItem(UCDeltaModels.TableUpdate.removeProperties(toRemove));
      }
    }
  }

  // ===========================
  // Request Conversion Methods
  // ===========================

  /**
   * Converts Delta's {@link UCDeltaModels.UpdateTableRequest} to UC SDK's
   * {@link UpdateTableRequest}.
   */
  private UpdateTableRequest toUCUpdateTableRequest(
      UCDeltaModels.UpdateTableRequest request) {
    UpdateTableRequest ucRequest = new UpdateTableRequest();

    List<TableRequirement> ucRequirements = new ArrayList<>();
    for (UCDeltaModels.TableRequirement req : request.getRequirements()) {
      ucRequirements.add(toUCTableRequirement(req));
    }
    ucRequest.setRequirements(ucRequirements);

    List<TableUpdate> ucUpdates = new ArrayList<>();
    for (UCDeltaModels.TableUpdate update : request.getUpdates()) {
      ucUpdates.add(toUCTableUpdate(update));
    }
    ucRequest.setUpdates(ucUpdates);

    return ucRequest;
  }

  private TableRequirement toUCTableRequirement(UCDeltaModels.TableRequirement req) {
    switch (req.getType()) {
      case ASSERT_TABLE_UUID:
        return new AssertTableUUID()
            .type("assert-table-uuid")
            .uuid(req.getUuid());
      case ASSERT_ETAG:
        return new AssertEtag()
            .type("assert-etag")
            .etag(req.getEtag());
      default:
        throw new IllegalArgumentException("Unknown requirement type: " + req.getType());
    }
  }

  private TableUpdate toUCTableUpdate(UCDeltaModels.TableUpdate update) {
    switch (update.getAction()) {
      case SET_PROPERTIES:
        return new SetPropertiesUpdate()
            .action("set-properties")
            .updates(update.getPropertyUpdates());

      case REMOVE_PROPERTIES:
        return new RemovePropertiesUpdate()
            .action("remove-properties")
            .removals(update.getPropertyRemovals());

      case SET_PROTOCOL:
        return new SetProtocolUpdate()
            .action("set-protocol")
            .protocol(toSDKDeltaProtocol(update.getProtocol()));

      case SET_COLUMNS:
        return new SetSchemaUpdate()
            .action("set-columns")
            .columns(parseSchemaString(update.getSchemaString()));

      case SET_PARTITION_COLUMNS:
        return new SetPartitionColumnsUpdate()
            .action("set-partition-columns")
            .partitionColumns(update.getPartitionColumns());

      case SET_TABLE_COMMENT:
        return new SetTableCommentUpdate()
            .action("set-table-comment")
            .comment(update.getComment());

      case ADD_COMMIT:
        AddCommitUpdate addCommit = new AddCommitUpdate()
            .action("add-commit")
            .commit(toSDKDeltaCommit(update.getCommit()));
        if (update.getUniform() != null) {
          addCommit.uniform(toUCUniformMetadata(update.getUniform()));
        }
        return addCommit;

      case SET_LATEST_BACKFILLED_VERSION:
        return new SetLatestBackfilledVersionUpdate()
            .action("set-latest-backfilled-version")
            .latestPublishedVersion(update.getLatestPublishedVersion());

      case UPDATE_METADATA_SNAPSHOT_VERSION:
        return new UpdateSnapshotVersionUpdate()
            .action("update-metadata-snapshot-version")
            .lastCommitVersion(update.getLastCommitVersion())
            .lastCommitTimestampMs(update.getLastCommitTimestampMs());

      default:
        throw new IllegalArgumentException("Unknown update action: " + update.getAction());
    }
  }

  private DeltaProtocol toSDKDeltaProtocol(UCDeltaModels.DeltaProtocol p) {
    DeltaProtocol ucProtocol = new DeltaProtocol()
        .minReaderVersion(p.getMinReaderVersion())
        .minWriterVersion(p.getMinWriterVersion());
    if (!p.getReaderFeatures().isEmpty()) {
      ucProtocol.readerFeatures(p.getReaderFeatures());
    }
    if (!p.getWriterFeatures().isEmpty()) {
      ucProtocol.writerFeatures(p.getWriterFeatures());
    }
    return ucProtocol;
  }

  private DeltaCommit toSDKDeltaCommit(UCDeltaModels.DeltaCommit c) {
    return new DeltaCommit()
        .version(c.getVersion())
        .timestamp(c.getTimestamp())
        .fileName(c.getFileName())
        .fileSize(c.getFileSize())
        .fileModificationTimestamp(c.getFileModificationTimestamp());
  }

  private UniformMetadata toUCUniformMetadata(
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
   * Parses a timestamp string to epoch milliseconds. Handles both numeric strings
   * (already epoch millis) and ISO-8601 datetime strings (e.g. "2025-01-04T03:13:11.423Z").
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
   * Parses a Delta schema string into a UC SDK {@link StructType}.
   * Not yet implemented — callers should be aware this will throw.
   */
  private StructType parseSchemaString(String schemaString) {
    // TODO: implement full Delta schema string -> StructType conversion
    throw new UnsupportedOperationException(
        "Delta schema string to StructType conversion is not yet implemented.");
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
}
