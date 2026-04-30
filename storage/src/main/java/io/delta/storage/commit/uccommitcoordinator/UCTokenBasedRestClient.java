/*
 * Copyright (2021) The Delta Lake Project Authors.
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
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.IcebergMetadata;
import io.delta.storage.commit.uniform.UniformMetadata;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CredentialOperation;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CredentialsResponse;
import io.unitycatalog.client.JSON;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.api.MetastoresApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.ConfigurationApi;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CatalogConfig;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.DeltaCommitMetadataProperties;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.DeltaGetCommitsResponse;
import io.unitycatalog.client.model.DeltaMetadata;
import io.unitycatalog.client.model.DeltaUniform;
import io.unitycatalog.client.model.DeltaUniformIceberg;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import io.unitycatalog.client.model.TableType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.*;

/**
 * A REST client implementation of {@link UCClient} for interacting with Unity Catalog's commit
 * coordination service. This client uses the Unity Catalog SDK with TokenProvider-based
 * authentication for managing Delta table commits and metadata.
 *
 * <p>The client handles the following primary operations:
 * <ul>
 *   <li>Retrieving metastore information</li>
 *   <li>Committing changes to Delta tables</li>
 *   <li>Fetching unbackfilled commit histories</li>
 *   <li>Loading table metadata through the UC Delta Rest Catalog API</li>
 *   <li>Getting table and path credentials through the UC Delta Rest Catalog API</li>
 * </ul>
 *
 * <p>All requests are authenticated using a TokenProvider that generates Bearer tokens dynamically.
 * The client uses the Unity Catalog SDK's {@link DeltaCommitsApi} and {@link MetastoresApi} for
 * API interactions.
 *
 * <p>Usage example:
 * <pre>{@code
 * TokenProvider tokenProvider = ... // Create or configure TokenProvider
 * try (UCTokenBasedRestClient client = new UCTokenBasedRestClient(baseUri, tokenProvider, Map.of())) {
 *     String metastoreId = client.getMetastoreId();
 *     // Perform operations with the client...
 * }
 * }</pre>
 *
 * @see UCClient
 * @see Commit
 * @see GetCommitsResponse
 * @see TokenProvider
 */
public class UCTokenBasedRestClient implements UCClient {

  private static final Logger LOG = LoggerFactory.getLogger(UCTokenBasedRestClient.class);

  private final boolean supportsUCDeltaRestCatalogApi;
  private final boolean supportsUCDeltaRestCatalogTemporaryPathCredentials;
  private DeltaCommitsApi deltaCommitsApi;
  private MetastoresApi metastoresApi;
  private TablesApi tablesApi;
  private io.unitycatalog.client.delta.api.TablesApi deltaTablesApi;
  private TemporaryCredentialsApi deltaTemporaryCredentialsApi;

  // HTTP status codes for error handling
  private static final int HTTP_BAD_REQUEST = 400;
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_TOO_MANY_REQUESTS = 429;
  private static final String UC_DELTA_API_PROTOCOL_VERSION = "1.0";
  // Endpoint identifiers advertised by the UC Delta Rest Catalog API /config endpoint, not
  // concrete URLs.
  private static final List<String> REQUIRED_UC_DELTA_API_ENDPOINT_IDS =
      Arrays.asList(
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
          "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials");
  private static final String UC_DELTA_TEMPORARY_PATH_CREDENTIALS_ENDPOINT_ID =
      "GET /v1/temporary-path-credentials";

  /**
   * Constructs a new UCTokenBasedRestClient with the specified base URI, TokenProvider,
   * and application version information for telemetry.
   *
   * @param baseUri The base URI of the Unity Catalog server
   * @param tokenProvider The TokenProvider to use for authentication
   * @param appVersions A map of application name to version string
   *                    (e.g. {@code "Delta" -> "4.0.0"}). Each entry is
   *                    registered for User-Agent telemetry. May be empty.
   */
  public UCTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    this(buildApiClient(baseUri, tokenProvider, appVersions),
        new UCDeltaRestCatalogApiSupport(true, true));
  }

  /**
   * Constructs a new UCTokenBasedRestClient and probes whether the catalog supports UC Delta Rest
   * Catalog API.
   *
   * <p>This constructor issues a network request to the UC Delta Rest Catalog API config endpoint.
   *
   * <p>When the config endpoint is absent or does not advertise the required UC Delta Rest Catalog
   * API endpoints, this client reports {@link #supportsUCDeltaRestCatalogApi()} as false. UC Delta
   * Rest Catalog API table methods then throw {@link UnsupportedOperationException}.
   */
  public UCTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      String catalog) {
    this(buildApiClient(baseUri, tokenProvider, appVersions), catalog);
  }

  private UCTokenBasedRestClient(ApiClient apiClient, String catalog) {
    this(apiClient, getUCDeltaRestCatalogApiSupport(apiClient, catalog));
  }

  private static ApiClient buildApiClient(
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

    return builder.build();
  }

  private static UCDeltaRestCatalogApiSupport getUCDeltaRestCatalogApiSupport(
      ApiClient apiClient,
      String catalog) {
    Objects.requireNonNull(apiClient, "apiClient must not be null");
    Objects.requireNonNull(catalog, "catalog must not be null");
    try {
      CatalogConfig config =
          new ConfigurationApi(apiClient).getConfig(catalog, UC_DELTA_API_PROTOCOL_VERSION);
      List<String> endpoints = config == null ? null : config.getEndpoints();
      return new UCDeltaRestCatalogApiSupport(
          endpoints != null && endpoints.containsAll(REQUIRED_UC_DELTA_API_ENDPOINT_IDS),
          endpoints != null && endpoints.contains(UC_DELTA_TEMPORARY_PATH_CREDENTIALS_ENDPOINT_ID));
    } catch (ApiException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        LOG.warn(
            "UC Delta Rest Catalog API config endpoint is unavailable for catalog {}. "
                + "UC Delta Rest Catalog API will be disabled.",
            catalog,
            e);
        return new UCDeltaRestCatalogApiSupport(false, false);
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

  /**
   * Builds a client around an existing UC {@link ApiClient}.
   *
   * <p>If {@code supportsUCDeltaRestCatalogApi} is false, UC Delta Rest Catalog API table methods
   * are unsupported. Commit-coordinator methods continue to use the legacy UC APIs.
   */
  private UCTokenBasedRestClient(
      ApiClient apiClient,
      UCDeltaRestCatalogApiSupport ucDeltaRestCatalogApiSupport) {
    Objects.requireNonNull(apiClient, "apiClient must not be null");
    Objects.requireNonNull(
        ucDeltaRestCatalogApiSupport, "ucDeltaRestCatalogApiSupport must not be null");
    this.supportsUCDeltaRestCatalogApi = ucDeltaRestCatalogApiSupport.supportsTableApis;
    this.supportsUCDeltaRestCatalogTemporaryPathCredentials =
        ucDeltaRestCatalogApiSupport.supportsTemporaryPathCredentials;
    this.deltaCommitsApi = new DeltaCommitsApi(apiClient);
    this.metastoresApi = new MetastoresApi(apiClient);
    this.tablesApi = new TablesApi(apiClient);
    this.deltaTablesApi = supportsUCDeltaRestCatalogApi
        ? new io.unitycatalog.client.delta.api.TablesApi(apiClient)
        : null;
    this.deltaTemporaryCredentialsApi = supportsUCDeltaRestCatalogApi
        ? new TemporaryCredentialsApi(apiClient)
        : null;
  }

  private static class UCDeltaRestCatalogApiSupport {
    private final boolean supportsTableApis;
    private final boolean supportsTemporaryPathCredentials;

    UCDeltaRestCatalogApiSupport(
        boolean supportsTableApis,
        boolean supportsTemporaryPathCredentials) {
      this.supportsTableApis = supportsTableApis;
      this.supportsTemporaryPathCredentials = supportsTemporaryPathCredentials;
    }
  }

  @Override
  public boolean supportsUCDeltaRestCatalogApi() {
    return supportsUCDeltaRestCatalogApi;
  }

  /**
   * Ensures the client has not been closed. Must be called before any API operation.
   */
  private void ensureOpen() {
    if (deltaCommitsApi == null || metastoresApi == null || tablesApi == null) {
      throw new IllegalStateException("UCTokenBasedRestClient has been closed.");
    }
  }

  private void ensureUCDeltaRestCatalogApiSupported(String operation) {
    ensureOpen();
    if (!supportsUCDeltaRestCatalogApi) {
      throw new UnsupportedOperationException(
          operation + " requires UC Delta Rest Catalog API support.");
    }
  }

  private void ensureUCDeltaRestCatalogTemporaryPathCredentialsSupported(String operation) {
    ensureOpen();
    if (!supportsUCDeltaRestCatalogTemporaryPathCredentials) {
      throw new UnsupportedOperationException(
          operation + " requires UC Delta Rest Catalog API temporary path credentials support.");
    }
  }

  @Override
  public String getMetastoreId() throws IOException {
    ensureOpen();
    try {
      GetMetastoreSummaryResponse response = metastoresApi.summary();
      return response.getMetastoreId();
    } catch (ApiException e) {
      throw new IOException(
          String.format("Failed to get metastore ID (HTTP %s): ", e.getCode()), e);
    }
  }

  @Override
  public void commit(
      String tableId,
      URI tableUri,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform
  ) throws IOException, CommitFailedException, UCCommitCoordinatorException {
    ensureOpen();
    Objects.requireNonNull(tableId, "tableId must not be null.");
    Objects.requireNonNull(tableUri, "tableUri must not be null.");

    // Build the DeltaCommit request using SDK models
    DeltaCommit deltaCommit = new DeltaCommit()
        .tableId(tableId)
        .tableUri(tableUri.toString());

    // Add commit info if present
    commit.ifPresent(c -> deltaCommit.commitInfo(toDeltaCommitInfo(c)));

    // Add latest backfilled version if present
    lastKnownBackfilledVersion.ifPresent(deltaCommit::latestBackfilledVersion);

    // Add metadata if present
    newMetadata.ifPresent(m -> deltaCommit.metadata(toDeltaMetadata(m)));

    // Add uniform metadata if present
    uniform.flatMap(u -> u.getIcebergMetadata().map(this::toDeltaUniformIceberg))
        .ifPresent(iceberg -> deltaCommit.uniform(new DeltaUniform().iceberg(iceberg)));

    // Note: protocol and disown are not part of the DeltaCommit schema in the Unity Catalog
    // OpenAPI spec. They are intentionally not sent.

    try {
      deltaCommitsApi.commit(deltaCommit);
    } catch (ApiException e) {
      handleCommitException(e);
    }
  }

  @Override
  public GetCommitsResponse getCommits(
      String tableId,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException {
    ensureOpen();
    Objects.requireNonNull(tableId, "tableId must not be null.");
    Objects.requireNonNull(tableUri, "tableUri must not be null.");

    // Build the DeltaGetCommits request using SDK models
    DeltaGetCommits request = new DeltaGetCommits()
        .tableId(tableId)
        .tableUri(tableUri.toString())
        .startVersion(startVersion.orElse(0L));

    endVersion.ifPresent(request::endVersion);

    try {
      DeltaGetCommitsResponse response = deltaCommitsApi.getCommits(request);
      return toGetCommitsResponse(response, tableUri);
    } catch (ApiException e) {
      int statusCode = e.getCode();
      String responseBody = e.getResponseBody();

      if (statusCode == HTTP_NOT_FOUND) {
        throw new InvalidTargetTableException(
            String.format("Invalid Target Table (HTTP %s) due to: %s", statusCode, responseBody));
      } else {
        throw new IOException(
            String.format("Unexpected getCommits failure (HTTP %s): due to: %s", statusCode,
                responseBody), e);
      }
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
      return response == null ? null : toTableMetadata(response.getMetadata());
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
   * Gets temporary credentials for one table through the UC Delta Rest Catalog API.
   *
   * <p>This method is only supported when this client has the UC Delta Rest Catalog API.
   */
  @Override
  public CredentialsResponse getTableCredentials(
      CredentialOperation operation,
      String catalog,
      String schema,
      String table) throws IOException {
    ensureUCDeltaRestCatalogApiSupported("getTableCredentials");
    Objects.requireNonNull(operation, "operation must not be null.");
    Objects.requireNonNull(catalog, "catalog must not be null.");
    Objects.requireNonNull(schema, "schema must not be null.");
    Objects.requireNonNull(table, "table must not be null.");

    try {
      return toCredentialsResponse(deltaTemporaryCredentialsApi.getTableCredentials(
          toSdkCredentialOperation(operation),
          catalog,
          schema,
          table));
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to get table credentials for %s.%s.%s (HTTP %s): %s",
              catalog,
              schema,
              table,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  /**
   * Gets temporary credentials for one path through the UC Delta credential APIs.
   *
   * <p>This method is only supported when the UC Delta Rest Catalog API config advertises
   * temporary path credentials. It is used for raw path-based Delta access, not named table access.
   */
  @Override
  public CredentialsResponse getTemporaryPathCredentials(
      String location,
      CredentialOperation operation) throws IOException {
    ensureUCDeltaRestCatalogTemporaryPathCredentialsSupported("getTemporaryPathCredentials");
    Objects.requireNonNull(location, "location must not be null.");
    Objects.requireNonNull(operation, "operation must not be null.");

    try {
      return toCredentialsResponse(deltaTemporaryCredentialsApi.getTemporaryPathCredentials(
          location,
          toSdkCredentialOperation(operation)));
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to get path credentials for %s (HTTP %s): %s",
              location,
              e.getCode(),
              e.getResponseBody()),
          e);
    }
  }

  private static TableMetadataAdapter toTableMetadata(TableMetadata metadata) {
    if (metadata == null) {
      return null;
    }
    return new TableMetadataAdapter(metadata);
  }

  private static CredentialsResponse toCredentialsResponse(
      io.unitycatalog.client.delta.model.CredentialsResponse response) {
    if (response == null) {
      return null;
    }
    List<UCDeltaModels.StorageCredential> credentials = new ArrayList<>();
    if (response.getStorageCredentials() != null) {
      for (io.unitycatalog.client.delta.model.StorageCredential credential :
          response.getStorageCredentials()) {
        credentials.add(toStorageCredential(credential));
      }
    }
    return new CredentialsResponse(credentials);
  }

  private static UCDeltaModels.StorageCredential toStorageCredential(
      io.unitycatalog.client.delta.model.StorageCredential credential) {
    if (credential == null) {
      return null;
    }
    return new UCDeltaModels.StorageCredential(
        credential.getPrefix(),
        toCredentialOperation(credential.getOperation()),
        toStorageCredentialConfig(credential.getConfig()),
        credential.getExpirationTimeMs());
  }

  private static UCDeltaModels.StorageCredentialConfig toStorageCredentialConfig(
      io.unitycatalog.client.delta.model.StorageCredentialConfig config) {
    if (config == null) {
      return null;
    }
    return new UCDeltaModels.StorageCredentialConfig(
        config.getS3AccessKeyId(),
        config.getS3SecretAccessKey(),
        config.getS3SessionToken(),
        config.getAzureSasToken(),
        config.getGcsOauthToken());
  }

  private static CredentialOperation toCredentialOperation(
      io.unitycatalog.client.delta.model.CredentialOperation operation) {
    if (operation == null) {
      return null;
    }
    switch (operation) {
      case READ:
        return CredentialOperation.READ;
      case READ_WRITE:
        return CredentialOperation.READ_WRITE;
      default:
        throw new IllegalArgumentException("Unsupported UC Delta credential operation: " + operation);
    }
  }

  private static io.unitycatalog.client.delta.model.CredentialOperation toSdkCredentialOperation(
      CredentialOperation operation) {
    switch (operation) {
      case READ:
        return io.unitycatalog.client.delta.model.CredentialOperation.READ;
      case READ_WRITE:
        return io.unitycatalog.client.delta.model.CredentialOperation.READ_WRITE;
      default:
        throw new IllegalArgumentException("Unsupported UC Delta credential operation: " + operation);
    }
  }

  /**
   * Adapts UC Delta SDK table metadata to Delta's storage-level metadata interface.
   *
   * <p>The UC SDK schema stays hidden behind this OSS-only client implementation. DRC-aware
   * callers can access the raw UC SDK schema through {@link #getSchema()} and convert it directly
   * to their local schema representation.
   */
  public static final class TableMetadataAdapter implements AbstractMetadata {
    private final TableMetadata delegate;
    private volatile String cachedSchemaJson;

    private TableMetadataAdapter(TableMetadata delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate must not be null.");
    }

    public StructType getSchema() {
      return delegate.getColumns();
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
      return null;
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
      if (cachedSchemaJson == null && delegate.getColumns() != null) {
        try {
          cachedSchemaJson = JSON.getDefault().getMapper().writeValueAsString(delegate.getColumns());
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to serialize UC Delta schema to JSON.", e);
        }
      }
      return cachedSchemaJson;
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

  @Override
  public void close() throws IOException {
    // Nulling out the API instances makes them eligible for GC. Once garbage collected,
    // the underlying connection pool is freed and destroyed.
    this.deltaCommitsApi = null;
    this.metastoresApi = null;
    this.tablesApi = null;
    this.deltaTablesApi = null;
    this.deltaTemporaryCredentialsApi = null;
  }

  /**
   * Converts a Delta {@link Commit} to a Unity Catalog SDK {@link DeltaCommitInfo}.
   *
   * @param commit The Delta commit to convert
   * @return The converted DeltaCommitInfo
   */
  private DeltaCommitInfo toDeltaCommitInfo(Commit commit) {
    if (commit == null) {
      throw new IllegalArgumentException("commit cannot be null");
    }
    if (commit.getFileStatus() == null) {
      throw new IllegalArgumentException("commit.getFileStatus() cannot be null");
    }

    return new DeltaCommitInfo()
        .version(commit.getVersion())
        .timestamp(commit.getCommitTimestamp())
        .fileName(commit.getFileStatus().getPath().getName())
        .fileSize(commit.getFileStatus().getLen())
        .fileModificationTimestamp(commit.getFileStatus().getModificationTime());
  }

  /**
   * Converts a Delta {@link IcebergMetadata} to a Unity Catalog SDK
   * {@link DeltaUniformIceberg}.
   *
   * <p>Field mapping (Delta internal -> OpenAPI snake_case):
   * <ul>
   *   <li>metadataLocation -> metadata_location</li>
   *   <li>convertedDeltaVersion -> converted_delta_version</li>
   *   <li>convertedDeltaTimestamp -> converted_delta_timestamp</li>
   *   <li>baseConvertedDeltaVersion -> base_converted_delta_version (optional)</li>
   * </ul>
   */
  private DeltaUniformIceberg toDeltaUniformIceberg(IcebergMetadata iceberg) {
    return new DeltaUniformIceberg()
        .metadataLocation(URI.create(iceberg.getMetadataLocation()))
        .convertedDeltaVersion(iceberg.getConvertedDeltaVersion())
        .convertedDeltaTimestamp(iceberg.getConvertedDeltaTimestamp())
        .baseConvertedDeltaVersion(iceberg.getBaseConvertedDeltaVersion().orElse(null));
  }

  /**
   * Converts an {@link AbstractMetadata} to a Unity Catalog SDK {@link DeltaMetadata}.
   *
   * @param metadata The abstract metadata to convert
   * @return The converted DeltaMetadata
   */
  private DeltaMetadata toDeltaMetadata(AbstractMetadata metadata) {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null");
    }

    DeltaMetadata deltaMetadata = new DeltaMetadata()
        .description(metadata.getDescription());

    // Set properties if available
    if (metadata.getConfiguration() != null && !metadata.getConfiguration().isEmpty()) {
      DeltaCommitMetadataProperties properties = new DeltaCommitMetadataProperties()
          .properties(metadata.getConfiguration());
      deltaMetadata.properties(properties);
    }

    // Schema conversion is not directly supported as the SDK expects ColumnInfos
    // which requires parsing the schema string. For now, we skip schema conversion.
    // If needed, implement schema string parsing to ColumnInfos.

    return deltaMetadata;
  }

  /**
   * Converts a Unity Catalog SDK {@link DeltaGetCommitsResponse} to a Delta
   * {@link GetCommitsResponse}.
   *
   * @param response The SDK response to convert
   * @param tableUri The table URI for constructing file paths
   * @return The converted GetCommitsResponse
   */
  private GetCommitsResponse toGetCommitsResponse(DeltaGetCommitsResponse response, URI tableUri) {
    Path basePath = CoordinatedCommitsUtils.commitDirPath(
        CoordinatedCommitsUtils.logDirPath(new Path(tableUri)));

    List<Commit> commits = new ArrayList<>();
    for (DeltaCommitInfo commitInfo : response.getCommits()) {
      commits.add(fromDeltaCommitInfo(commitInfo, basePath));
    }

    return new GetCommitsResponse(commits, response.getLatestTableVersion());
  }

  /**
   * Converts a Unity Catalog SDK {@link DeltaCommitInfo} to a Delta {@link Commit}.
   *
   * @param commitInfo The SDK commit info to convert
   * @param basePath   The base path for constructing file paths
   * @return The converted Commit
   */
  private Commit fromDeltaCommitInfo(DeltaCommitInfo commitInfo, Path basePath) {
    FileStatus fileStatus = new FileStatus(
        commitInfo.getFileSize(),
        false /* isdir */,
        0 /* block_replication */,
        0 /* blocksize */,
        commitInfo.getFileModificationTimestamp(),
        new Path(basePath, commitInfo.getFileName()));

    return new Commit(commitInfo.getVersion(), fileStatus, commitInfo.getTimestamp());
  }

  // ===========================
  // Exception Handling Methods
  // ===========================

  @Override
  public void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<UCClient.ColumnDef> columns,
      Map<String, String> properties) throws CommitFailedException {
    ensureOpen();
    Objects.requireNonNull(tableName, "tableName must not be null.");
    Objects.requireNonNull(catalogName, "catalogName must not be null.");
    Objects.requireNonNull(schemaName, "schemaName must not be null.");
    Objects.requireNonNull(storageLocation, "storageLocation must not be null.");
    Objects.requireNonNull(columns, "columns must not be null.");
    Objects.requireNonNull(properties, "properties must not be null.");

    List<ColumnInfo> ucColumns = columns.stream()
        .map(c -> {
          ColumnTypeName typeName;
          try {
            typeName = ColumnTypeName.fromValue(c.getTypeName());
          } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                "Unknown column type '" + c.getTypeName() + "' for column '" + c.getName() + "'");
          }
          return new ColumnInfo()
              .name(c.getName())
              .typeName(typeName)
              .typeText(c.getTypeText())
              .typeJson(c.getTypeJson())
              .nullable(c.isNullable())
              .position(c.getPosition());
        })
        .collect(java.util.stream.Collectors.toList());

    CreateTable request = new CreateTable()
        .name(tableName)
        .catalogName(catalogName)
        .schemaName(schemaName)
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(ucColumns)
        .storageLocation(storageLocation)
        .properties(properties);

    try {
      tablesApi.createTable(request);
    } catch (ApiException e) {
      throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          String.format(
              "Failed to finalize table creation for %s.%s.%s (HTTP %s): %s",
              catalogName, schemaName, tableName, e.getCode(), e.getResponseBody()),
          e);
    }
  }

  /**
   * Handles {@link ApiException} from commit operations by converting to appropriate Delta
   * exceptions.
   *
   * @param e The API exception to handle
   * @throws CommitFailedException        If the commit failed due to various reasons
   * @throws UCCommitCoordinatorException If there's a UC-specific error
   * @throws IOException                  If there's an unexpected error
   */
  private void handleCommitException(ApiException e)
      throws CommitFailedException, UCCommitCoordinatorException {
    int statusCode = e.getCode();
    String responseBody = e.getResponseBody();

    switch (statusCode) {
      case HTTP_BAD_REQUEST:
        throw new CommitFailedException(
            false /* retryable */,
            false /* conflict */,
            "Invalid commit parameters: " + responseBody,
            e);
      case HTTP_NOT_FOUND:
        throw new InvalidTargetTableException("Invalid Target Table: " + responseBody);
      case HTTP_CONFLICT:
        throw new CommitFailedException(
            true /* retryable */,
            true /* conflict */,
            "Commit conflict: " + responseBody,
            e);
      case HTTP_TOO_MANY_REQUESTS:
        throw new CommitLimitReachedException("Backfilled commits limit reached: " + responseBody);
      default:
        throw new CommitFailedException(
            true /* retryable */,
            false /* conflict */,
            "Unexpected commit failure (HTTP " + statusCode + "): " + responseBody,
            e);
    }
  }
}
