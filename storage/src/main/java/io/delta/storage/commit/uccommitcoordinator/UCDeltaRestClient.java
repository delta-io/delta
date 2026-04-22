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

import io.delta.storage.annotation.Unstable;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.AddCommitUpdate;
import io.unitycatalog.client.delta.model.AssertEtag;
import io.unitycatalog.client.delta.model.AssertTableUUID;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.TableRequirement;
import io.unitycatalog.client.delta.model.TableUpdate;
import io.unitycatalog.client.delta.model.UpdateTableRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP implementation of {@link UCDeltaClient} backed by the UC SDK's Delta REST Catalog
 * {@code TablesApi}.
 *
 * <p>The {@link ApiClient} is shared -- it is owned by the UC catalog plugin (via the UC-side
 * {@code DeltaRestClientProvider}) and supplies the HTTP connection pool, auth refresh cycle,
 * and telemetry headers. This class does not own the {@code ApiClient} and must not close it.
 */
@Unstable
public class UCDeltaRestClient implements UCDeltaClient {

  private static final Logger LOG = LoggerFactory.getLogger(UCDeltaRestClient.class);

  private final TablesApi tablesApi;
  private final TemporaryCredentialsApi credentialsApi;

  /**
   * Constructs a client wrapping a shared {@link ApiClient}. The caller (typically
   * {@code AbstractDeltaCatalog.loadTable}) builds this only on the DRC path, i.e. when the
   * UC-side {@code DeltaRestClientProvider.getDeltaTablesApi()} returned a present
   * {@code Optional}. The apiClient is not closed by this class.
   */
  public UCDeltaRestClient(ApiClient apiClient) {
    Objects.requireNonNull(apiClient, "apiClient must not be null");
    this.tablesApi = new TablesApi(apiClient);
    this.credentialsApi = new TemporaryCredentialsApi(apiClient);
    // Load-bearing invariant: this class does not own the ApiClient. A leak of the shared
    // HTTP pool is the single worst failure mode here; this log exists so that mystery
    // leaks can be triaged by matching identity hashes across the lifetime of the JVM.
    LOG.info("UCDeltaRestClient bound to shared ApiClient (identityHash={}) at baseUri={}",
        System.identityHashCode(apiClient), apiClient.getBaseUri());
  }

  @Override
  public LoadTableResponse loadTable(String catalog, String schema, String table)
      throws IOException {
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");
    try {
      return tablesApi.loadTable(catalog, schema, table);
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to load DRC table %s.%s.%s (HTTP %d): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()),
          e);
    }
  }

  @Override
  public CredentialsResponse getTableCredentials(
      String catalog,
      String schema,
      String table,
      CredentialOperation operation) throws IOException {
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(operation, "operation must not be null");
    try {
      return credentialsApi.getTableCredentials(operation, catalog, schema, table);
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "Failed to fetch DRC credentials for %s.%s.%s op=%s (HTTP %d): %s",
              catalog, schema, table, operation, e.getCode(), e.getResponseBody()),
          e);
    }
  }

  @Override
  public LoadTableResponse commit(
      String catalog,
      String schema,
      String table,
      DeltaCommit commit,
      UUID tableUuid,
      Optional<String> etag,
      List<TableUpdate> metadataUpdates) throws IOException {
    Objects.requireNonNull(catalog, "catalog must not be null");
    Objects.requireNonNull(schema, "schema must not be null");
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(commit, "commit must not be null");
    Objects.requireNonNull(tableUuid, "tableUuid must not be null");
    Objects.requireNonNull(etag, "etag Optional must not be null (use Optional.empty())");
    Objects.requireNonNull(metadataUpdates, "metadataUpdates must not be null (use empty list)");

    UpdateTableRequest request = buildCommitRequest(commit, tableUuid, etag, metadataUpdates);
    try {
      return tablesApi.updateTable(catalog, schema, table, request);
    } catch (ApiException e) {
      throw new IOException(
          String.format(
              "DRC commit failed for %s.%s.%s (HTTP %d): %s",
              catalog, schema, table, e.getCode(), e.getResponseBody()),
          e);
    }
  }

  /**
   * Package-private for tests: assembles the {@link UpdateTableRequest} body used by
   * {@link #commit}. Exposed separately so tests can assert {@code requirements} and
   * {@code updates} list composition without standing up an HTTP server.
   */
  static UpdateTableRequest buildCommitRequest(
      DeltaCommit commit,
      UUID tableUuid,
      Optional<String> etag,
      List<TableUpdate> metadataUpdates) {
    Objects.requireNonNull(commit, "commit must not be null");
    Objects.requireNonNull(tableUuid, "tableUuid must not be null");
    Objects.requireNonNull(etag, "etag Optional must not be null (use Optional.empty())");
    Objects.requireNonNull(metadataUpdates, "metadataUpdates must not be null (use empty list)");

    List<TableRequirement> requirements = new ArrayList<>();
    requirements.add(new AssertTableUUID().uuid(tableUuid));
    etag.ifPresent(e -> requirements.add(new AssertEtag().etag(e)));

    List<TableUpdate> updates = new ArrayList<>();
    updates.add(new AddCommitUpdate().commit(commit));
    updates.addAll(metadataUpdates);

    return new UpdateTableRequest().requirements(requirements).updates(updates);
  }
}
