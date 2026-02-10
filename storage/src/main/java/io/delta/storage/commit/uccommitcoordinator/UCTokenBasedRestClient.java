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
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.api.MetastoresApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.DeltaCommitMetadataProperties;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.DeltaGetCommitsResponse;
import io.unitycatalog.client.model.DeltaMetadata;
import io.unitycatalog.client.model.DeltaUniform;
import io.unitycatalog.client.model.DeltaUniformIceberg;
import io.unitycatalog.client.model.GetMetastoreSummaryResponse;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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
 * </ul>
 *
 * <p>All requests are authenticated using a TokenProvider that generates Bearer tokens dynamically.
 * The client uses the Unity Catalog SDK's {@link DeltaCommitsApi} and {@link MetastoresApi} for
 * API interactions.
 *
 * <p>Usage example:
 * <pre>{@code
 * TokenProvider tokenProvider = ... // Create or configure TokenProvider
 * try (UCTokenBasedRestClient client = new UCTokenBasedRestClient(baseUri, tokenProvider,...)) {
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

  private final DeltaCommitsApi deltaCommitsApi;
  private final MetastoresApi metastoresApi;

  // HTTP status codes for error handling
  private static final int HTTP_BAD_REQUEST = 400;
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  /**
   * Constructs a new UCTokenBasedRestClient with the specified base URI, TokenProvider,
   * and version information for telemetry.
   *
   * @param baseUri The base URI of the Unity Catalog server
   * @param tokenProvider The TokenProvider to use for authentication
   * @param deltaVersion The Delta version string (can be null)
   * @param sparkVersion The Spark version string (can be null)
   * @param scalaVersion The Scala version string (can be null)
   */
  public UCTokenBasedRestClient(
      String baseUri,
      TokenProvider tokenProvider,
      String deltaVersion,
      String sparkVersion,
      String scalaVersion) {
    Objects.requireNonNull(baseUri, "baseUri must not be null");
    Objects.requireNonNull(tokenProvider, "tokenProvider must not be null");

    ApiClientBuilder builder = ApiClientBuilder.create()
        .uri(baseUri)
        .tokenProvider(tokenProvider);

    // Add version information for telemetry
    if (deltaVersion != null) {
      builder.addAppVersion("Delta", deltaVersion);
    }
    if (sparkVersion != null) {
      builder.addAppVersion("Spark", sparkVersion);
    }
    if (scalaVersion != null) {
      builder.addAppVersion("Scala", scalaVersion);
    }
    // Add the Java version
    String javaVersion = System.getProperty("java.version");
    if(javaVersion != null && !javaVersion.isEmpty()) {
      builder.addAppVersion("Java", javaVersion);
    }

    ApiClient apiClient = builder.build();
    this.deltaCommitsApi = new DeltaCommitsApi(apiClient);
    this.metastoresApi = new MetastoresApi(apiClient);
  }

  @Override
  public String getMetastoreId() throws IOException {
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
    // Validate required parameters
    Objects.requireNonNull(tableId, "tableId must not be null.");
    Objects.requireNonNull(tableUri, "tableUri must not be null.");

    // Build the DeltaCommit request using SDK models
    DeltaCommit deltaCommit = new DeltaCommit()
        .tableId(tableId)
        .tableUri(tableUri.toString());

    // Add commit info if present
    commit.ifPresent(c -> deltaCommit.commitInfo(toDeltaCommitInfo(c, disown)));

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
    // Validate required parameters
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

  @Override
  public void close() throws IOException {
    // The Unity Catalog SDK's ApiClient doesn't require explicit closing
    // as it manages HTTP connections internally
  }

  /**
   * Converts a Delta {@link Commit} to a Unity Catalog SDK {@link DeltaCommitInfo}.
   *
   * @param commit         The Delta commit to convert
   * @param isDisownCommit Whether this is a disown commit
   * @return The converted DeltaCommitInfo
   */
  private DeltaCommitInfo toDeltaCommitInfo(Commit commit, boolean isDisownCommit) {
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
    // Note: isDisownCommit is not directly supported in the SDK's DeltaCommitInfo model
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
   * </ul>
   */
  private DeltaUniformIceberg toDeltaUniformIceberg(IcebergMetadata iceberg) {
    return new DeltaUniformIceberg()
        .metadataLocation(URI.create(iceberg.getMetadataLocation()))
        .convertedDeltaVersion(iceberg.getConvertedDeltaVersion())
        .convertedDeltaTimestamp(iceberg.getConvertedDeltaTimestamp());
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
