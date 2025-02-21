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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.CoordinatedCommitsUtils;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.CommitInfo;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.CommitRequest;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.GetCommitsRequest;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.GetMetastoreSummaryResponse;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.RestGetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.Metadata;
import io.delta.storage.commit.uccommitcoordinator.UCRestClientPayload.Protocol;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A REST client implementation of [[UCClient]] for interacting with Unity Catalog's commit
 * coordination service. This client uses token-based authentication to make HTTP requests to the
 * Unity Catalog API for managing Delta table commits and metadata.
 *
 * <p>The client handles the following primary operations:
 * <ul>
 *   <li>Retrieving metastore information</li>
 *   <li>Committing changes to Delta tables</li>
 *   <li>Fetching unbackfilled commit histories</li>
 * </ul>
 *
 * <p>All requests are authenticated using a Bearer token and communicate using JSON payloads.
 * The client automatically handles JSON serialization/deserialization and HTTP header management.
 *
 * <p>Usage example:
 * <pre>{@code
 * try (UCTokenBasedRestClient client = new UCTokenBasedRestClient(baseUri, token)) {
 *     String metastoreId = client.getMetastoreId();
 *     // Perform operations with the client...
 * }
 * }</pre>
 *
 * @see UCClient
 * @see Commit
 * @see GetCommitsResponse
 */
public class UCTokenBasedRestClient implements UCClient {
  private final String baseUri;
  private final ObjectMapper mapper;
  private final CloseableHttpClient httpClient;

  public UCTokenBasedRestClient(String baseUri, String token) {
    this.baseUri = resolve(baseUri, "/api/2.1/unity-catalog");
    this.mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
      .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    this.httpClient =
      HttpClientBuilder
        .create()
        .setDefaultHeaders(Arrays.asList(
          // Authorization header: Provides the Bearer token for authentication
          new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token),
          // Accept header: Indicates that the client expects JSON responses from the server
          new BasicHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType()),
          // Content-Type header: Indicates that the client sends JSON payloads to the server
          new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())))
        .build();
  }

  private static String resolve(String baseUri, String child) {
    // Ensures baseUri doesn't end with '/'
    if (baseUri.endsWith("/")) {
      baseUri = baseUri.substring(0, baseUri.length() - 1);
    }
    // Ensures child starts with '/'
    if (!child.startsWith("/")) {
      child = "/" + child;
    }
    return baseUri + child;
  }

  private <T> String toJson(T object) throws JsonProcessingException {
    return mapper.writeValueAsString(object);
  }

  private <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
    return mapper.readValue(json, clazz);
  }

  private static class HttpError extends Throwable {
    final int statusCode;
    final String responseBody;

    HttpError(int statusCode, String responseBody) {
      super("HTTP Error " + statusCode + ": " + responseBody);
      this.statusCode = statusCode;
      this.responseBody = responseBody;
    }
  }

  private <T> T executeHttpRequest(
    HttpUriRequest request,
    Object payload,
    Class<T> responseClass) throws IOException, HttpError {

    if (payload != null && request instanceof HttpEntityEnclosingRequestBase) {
      String jsonPayload = toJson(payload);
      ((HttpEntityEnclosingRequestBase) request).setEntity(
        new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
    }

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      String responseBody = EntityUtils.toString(response.getEntity());

      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
        return fromJson(responseBody, responseClass);
      } else {
        throw new HttpError(statusCode, responseBody);
      }
    } catch (JsonProcessingException e) {
      throw new IOException("Failed to parse response", e);
    }
  }

  @Override
  public String getMetastoreId() throws IOException {
    URI uri = URI.create(resolve(baseUri, "/metastore_summary"));
    HttpGet request = new HttpGet(uri);

    try {
      GetMetastoreSummaryResponse response =
        executeHttpRequest(request, null, GetMetastoreSummaryResponse.class);
      return response.metastoreId;
    } catch (HttpError e) {
      throw new IOException("Failed to get metastore ID (HTTP " + e.statusCode + "): " +
        e.responseBody);
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
      Optional<AbstractProtocol> newProtocol
  ) throws IOException, CommitFailedException, UCCommitCoordinatorException {
    // Validate required parameters
    Objects.requireNonNull(tableId, "tableId must not be null.");
    Objects.requireNonNull(tableUri, "tableUri must not be null.");

    // Create commit request payload
    CommitRequest commitRequest = new CommitRequest();
    commitRequest.tableId = tableId;
    commitRequest.tableUri = tableUri.toString();
    commit.ifPresent(c -> commitRequest.commitInfo = CommitInfo.fromCommit(c, disown));
    lastKnownBackfilledVersion.ifPresent(version ->
      commitRequest.latestBackfilledVersion = version);
    newMetadata.ifPresent(m -> commitRequest.metadata = Metadata.fromAbstractMetadata(m));
    newProtocol.ifPresent(p -> commitRequest.protocol = Protocol.fromAbstractProtocol(p));

    URI uri = URI.create(resolve(baseUri, "/delta/commits"));
    HttpPost request = new HttpPost(uri);

    try {
      executeHttpRequest(request, commitRequest, Void.class);
    } catch (HttpError e) {
      switch (e.statusCode) {
        case HttpStatus.SC_BAD_REQUEST:
          throw new CommitFailedException(
            false /* retryable */,
            false /* conflict */,
            "Invalid commit parameters: " + e.responseBody,
            e);
        case HttpStatus.SC_NOT_FOUND:
          throw new InvalidTargetTableException("Invalid Target Table: " + e.responseBody);
        case HttpStatus.SC_CONFLICT:
          throw new CommitFailedException(
            true /* retryable */,
            true /* conflict */,
            "Commit conflict: " + e.responseBody,
            e);
        case 429:
          throw new CommitLimitReachedException("Backfilled commits limit reached: " +
            e.responseBody);
        default:
          throw new CommitFailedException(
            true /* retryable */,
            false /* conflict */,
            "Unexpected commit failure (HTTP " + e.statusCode + "): " + e.responseBody,
            e);
      }
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

    GetCommitsRequest getCommitsRequest = new GetCommitsRequest();
    getCommitsRequest.tableId = tableId;
    getCommitsRequest.tableUri = tableUri.toString();
    getCommitsRequest.startVersion = startVersion.orElse(0L);
    endVersion.ifPresent(v -> getCommitsRequest.endVersion = v);

    // Create a custom HttpGet that allows body
    URI uri = URI.create(resolve(baseUri, "/delta/commits"));
    HttpEntityEnclosingRequestBase httpRequest = new HttpEntityEnclosingRequestBase() {
      @Override
      public String getMethod() {
        return "GET";
      }
    };
    httpRequest.setURI(uri);

    try {
      RestGetCommitsResponse restGetCommitsResponse =
        executeHttpRequest(httpRequest, getCommitsRequest, RestGetCommitsResponse.class);
      Path basePath = CoordinatedCommitsUtils.commitDirPath(CoordinatedCommitsUtils.logDirPath(
        new Path(tableUri)));
      List<Commit> commits = new ArrayList<>();
      if (restGetCommitsResponse.commits != null) {
        for (CommitInfo commitInfo : restGetCommitsResponse.commits) {
          commits.add(CommitInfo.toCommit(commitInfo, basePath));
        }
      }
      return new GetCommitsResponse(commits, restGetCommitsResponse.latestTableVersion);
    } catch (HttpError e) {
      // Handle response based on status code
      switch (e.statusCode) {
        case HttpStatus.SC_NOT_FOUND:
          throw new InvalidTargetTableException("Invalid Target Table due to: " + e.responseBody);
        default:
          throw new IOException("Unexpected getCommits failure (HTTP " + e.statusCode +
            "): due to: " + e.responseBody);
      }
    }
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }
}
