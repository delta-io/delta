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
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.AddCommitUpdate;
import io.unitycatalog.client.delta.model.AssertTableUUID;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.client.delta.model.TableRequirement;
import io.unitycatalog.client.delta.model.TableUpdate;
import io.unitycatalog.client.delta.model.UniformMetadataIceberg;
import io.unitycatalog.client.delta.model.UpdateTableRequest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link UCClient} implementation that routes coordinated commit operations through
 * the Delta REST Catalog API ({@code updateTable} / {@code loadTable} endpoints) instead
 * of the legacy {@code DeltaCommitsApi}.
 *
 * <p>Unlike the legacy API which addresses tables by {@code tableId} (UUID), this
 * implementation uses the three-part table name ({@code catalogName.schemaName.tableName})
 * extracted from the table configuration map.
 */
public class UCDeltaRestClient implements UCClient {

  // Table property keys (must match UCTableProperties in the UC Spark connector)
  static final String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  static final String UC_CATALOG_NAME_KEY = "io.unitycatalog.catalogName";
  static final String UC_SCHEMA_NAME_KEY = "io.unitycatalog.schemaName";
  static final String UC_TABLE_NAME_KEY = "io.unitycatalog.tableName";

  private static final String DELTA_REST_BASE_PATH = "/api/2.1/unity-catalog/delta";

  // HTTP status codes
  private static final int HTTP_BAD_REQUEST = 400;
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  private TablesApi deltaTablesApi;
  private final String baseUri;

  public UCDeltaRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions) {
    Objects.requireNonNull(baseUri, "baseUri must not be null");
    Objects.requireNonNull(tokenProvider, "tokenProvider must not be null");
    Objects.requireNonNull(appVersions, "appVersions must not be null");

    this.baseUri = baseUri;

    ApiClientBuilder builder = ApiClientBuilder.create()
        .uri(URI.create(baseUri))
        .tokenProvider(tokenProvider)
        .basePath(DELTA_REST_BASE_PATH);

    appVersions.forEach((name, version) -> {
      if (version != null) {
        builder.addAppVersion(name, version);
      }
    });

    ApiClient apiClient = builder.build();
    this.deltaTablesApi = new TablesApi(apiClient);
  }

  private void ensureOpen() {
    if (deltaTablesApi == null) {
      throw new IllegalStateException("UCDeltaRestClient has been closed.");
    }
  }

  // ---- UCClient interface ----

  @Override
  public String getMetastoreId() throws IOException {
    ensureOpen();
    // The Delta REST API has no metastore concept. Return a deterministic key
    // derived from the server URI so the caching model in UCCommitCoordinatorBuilder
    // still works (one client per distinct UC server).
    return "delta-rest:" + baseUri;
  }

  /**
   * Legacy commit method (by tableId). Delegates to the tableConf-based overload
   * by constructing a minimal conf map. Prefer the tableConf overload when
   * catalog/schema/table names are available.
   */
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
    throw new UnsupportedOperationException(
        "UCDeltaRestClient requires table name properties in tableConf. " +
        "Use the commit(Map<String, String> tableConf, ...) overload instead.");
  }

  /**
   * Legacy getCommits method (by tableId). Not supported — use the tableConf overload.
   */
  @Override
  public GetCommitsResponse getCommits(
      String tableId,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion
  ) throws IOException, UCCommitCoordinatorException {
    throw new UnsupportedOperationException(
        "UCDeltaRestClient requires table name properties in tableConf. " +
        "Use the getCommits(Map<String, String> tableConf, ...) overload instead.");
  }

  /**
   * Commit changes via the Delta REST Catalog API's {@code updateTable} endpoint.
   *
   * <p>Builds an {@link UpdateTableRequest} with:
   * <ul>
   *   <li>{@link AssertTableUUID} requirement (validates table hasn't been replaced)</li>
   *   <li>{@link AddCommitUpdate} containing the commit info and optional uniform metadata</li>
   *   <li>{@link SetLatestBackfilledVersionUpdate} if backfill version is provided</li>
   * </ul>
   */
  public void commit(
      Map<String, String> tableConf,
      URI tableUri,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform
  ) throws IOException, CommitFailedException, UCCommitCoordinatorException {
    ensureOpen();

    String catalogName = requireTableProp(tableConf, UC_CATALOG_NAME_KEY, "catalogName");
    String schemaName = requireTableProp(tableConf, UC_SCHEMA_NAME_KEY, "schemaName");
    String tableName = requireTableProp(tableConf, UC_TABLE_NAME_KEY, "tableName");
    String tableId = tableConf.get(UC_TABLE_ID_KEY);

    // Build requirements
    List<TableRequirement> requirements = new ArrayList<>();
    if (tableId != null) {
      requirements.add(new AssertTableUUID().uuid(UUID.fromString(tableId)));
    }

    // Build updates
    List<TableUpdate> updates = new ArrayList<>();

    // AddCommitUpdate (if commit is present)
    commit.ifPresent(c -> {
      AddCommitUpdate addCommit = new AddCommitUpdate()
          .commit(toDeltaCommit(c));

      // Attach uniform metadata to the commit update
      uniform.flatMap(u -> u.getIcebergMetadata().map(this::toDeltaUniformIceberg))
          .ifPresent(iceberg -> addCommit.uniform(
              new io.unitycatalog.client.delta.model.UniformMetadata().iceberg(iceberg)));

      updates.add(addCommit);
    });

    // SetLatestBackfilledVersionUpdate
    lastKnownBackfilledVersion.ifPresent(v ->
        updates.add(new SetLatestBackfilledVersionUpdate().latestPublishedVersion(v)));

    UpdateTableRequest request = new UpdateTableRequest()
        .requirements(requirements)
        .updates(updates);

    try {
      deltaTablesApi.updateTable(catalogName, schemaName, tableName, request);
    } catch (ApiException e) {
      handleCommitException(e);
    }
  }

  /**
   * Get unbackfilled commits via the Delta REST Catalog API's {@code loadTable} endpoint.
   * Filters the returned commits by startVersion/endVersion client-side.
   */
  public GetCommitsResponse getCommits(
      Map<String, String> tableConf,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion
  ) throws IOException, UCCommitCoordinatorException {
    ensureOpen();

    String catalogName = requireTableProp(tableConf, UC_CATALOG_NAME_KEY, "catalogName");
    String schemaName = requireTableProp(tableConf, UC_SCHEMA_NAME_KEY, "schemaName");
    String tableName = requireTableProp(tableConf, UC_TABLE_NAME_KEY, "tableName");

    try {
      LoadTableResponse response = deltaTablesApi.loadTable(catalogName, schemaName, tableName);
      return toGetCommitsResponse(response, tableUri, startVersion, endVersion);
    } catch (ApiException e) {
      int statusCode = e.getCode();
      if (statusCode == HTTP_NOT_FOUND) {
        throw new InvalidTargetTableException(
            String.format("Table not found (HTTP %s): %s", statusCode, e.getResponseBody()));
      }
      throw new IOException(
          String.format("getCommits failed (HTTP %s): %s", statusCode, e.getResponseBody()), e);
    }
  }

  @Override
  public void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<ColumnDef> columns,
      Map<String, String> properties
  ) throws CommitFailedException {
    ensureOpen();
    Objects.requireNonNull(tableName, "tableName must not be null.");
    Objects.requireNonNull(catalogName, "catalogName must not be null.");
    Objects.requireNonNull(schemaName, "schemaName must not be null.");

    CreateTableRequest request = new CreateTableRequest()
        .name(tableName)
        .location(storageLocation)
        .properties(properties);

    try {
      deltaTablesApi.createTable(catalogName, schemaName, request);
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

  @Override
  public void close() throws IOException {
    this.deltaTablesApi = null;
  }

  // ---- Conversion helpers ----

  private DeltaCommit toDeltaCommit(Commit commit) {
    return new DeltaCommit()
        .version(commit.getVersion())
        .timestamp(commit.getCommitTimestamp())
        .fileName(commit.getFileStatus().getPath().getName())
        .fileSize(commit.getFileStatus().getLen())
        .fileModificationTimestamp(commit.getFileStatus().getModificationTime());
  }

  private UniformMetadataIceberg toDeltaUniformIceberg(IcebergMetadata iceberg) {
    // The IcebergMetadata stores convertedDeltaTimestamp as an ISO-8601 string,
    // but the Delta REST API model expects a Long (epoch millis).
    Long timestampMillis = null;
    String tsStr = iceberg.getConvertedDeltaTimestamp();
    if (tsStr != null) {
      timestampMillis = java.time.Instant.parse(tsStr).toEpochMilli();
    }
    return new UniformMetadataIceberg()
        .metadataLocation(iceberg.getMetadataLocation())
        .convertedDeltaVersion(iceberg.getConvertedDeltaVersion())
        .convertedDeltaTimestamp(timestampMillis);
  }

  private GetCommitsResponse toGetCommitsResponse(
      LoadTableResponse response,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion) {

    Path basePath = CoordinatedCommitsUtils.commitDirPath(
        CoordinatedCommitsUtils.logDirPath(new Path(tableUri)));

    List<DeltaCommit> rawCommits = response.getCommits();
    if (rawCommits == null) {
      rawCommits = new ArrayList<>();
    }

    long latestTableVersion = response.getLatestTableVersion() != null
        ? response.getLatestTableVersion() : -1L;

    // Filter by version range (loadTable returns all unbackfilled commits)
    List<Commit> filtered = rawCommits.stream()
        .filter(c -> !startVersion.isPresent() || c.getVersion() >= startVersion.get())
        .filter(c -> !endVersion.isPresent() || c.getVersion() <= endVersion.get())
        .map(c -> fromDeltaCommit(c, basePath))
        .collect(Collectors.toList());

    return new GetCommitsResponse(filtered, latestTableVersion);
  }

  private Commit fromDeltaCommit(DeltaCommit commitInfo, Path basePath) {
    FileStatus fileStatus = new FileStatus(
        commitInfo.getFileSize(),
        false /* isdir */,
        0 /* block_replication */,
        0 /* blocksize */,
        commitInfo.getFileModificationTimestamp(),
        new Path(basePath, commitInfo.getFileName()));

    return new Commit(commitInfo.getVersion(), fileStatus, commitInfo.getTimestamp());
  }

  // ---- Error handling ----

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
        throw new CommitLimitReachedException(
            "Backfilled commits limit reached: " + responseBody);
      default:
        throw new CommitFailedException(
            true /* retryable */,
            false /* conflict */,
            "Unexpected commit failure (HTTP " + statusCode + "): " + responseBody,
            e);
    }
  }

  // ---- Utilities ----

  private static String requireTableProp(
      Map<String, String> tableConf, String key, String displayName) {
    String value = tableConf.get(key);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(
          "Delta REST API requires " + displayName + " (key: " + key + ") " +
          "in the table configuration. Ensure the table was created with " +
          "deltaRestApi.enabled=true so that name properties are stored.");
    }
    return value;
  }
}
