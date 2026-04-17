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
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.TableDescriptor;
import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Extended UC client interface for Delta table operations. Adds DRC-aware
 * createTable and typed commit/getCommits overloads on top of the legacy
 * {@link UCClient} interface.
 *
 * <p>Implementations choose DRC or legacy internally based on whether the
 * DRC TablesApi is available.
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Cast a UCClient to UCDeltaClient. Works because UCTokenBasedRestClientFactory
   * creates UCTokenBasedDeltaRestClient (which implements UCDeltaClient).
   */
  static UCDeltaClient fromLegacyClient(UCClient ucClient) {
    if (ucClient instanceof UCDeltaClient) {
      return (UCDeltaClient) ucClient;
    }
    if (ucClient instanceof UCTokenBasedRestClient) {
      // Wrap in the DRC subclass with DRC disabled.
      // This handles where the factory creates plain
      // UCTokenBasedRestClient (not the DeltaRest subclass).
      return new UCTokenBasedDeltaRestClient(
          ((UCTokenBasedRestClient) ucClient).apiClient, false);
    }
    throw new IllegalArgumentException(
        "UCClient is not a UCDeltaClient: "
        + (ucClient == null ? "null" : ucClient.getClass().getName()));
  }

  /**
   * Response from loadTable -- carries commits, etag, and table metadata.
   *
   * <p>DRC path: protocol/metadata are populated from the DRC API response.
   * Legacy/no-DRC path: protocol/metadata are null (caller reads from delta log).
   */
  class LoadTableResponse {
    private final GetCommitsResponse commitsResponse;
    private final String etag;
    private final String location;
    private final String tableId;
    private final String tableType;
    private final java.util.Map<String, String> properties;
    private final AbstractProtocol protocol;
    private final AbstractMetadata metadata;
    private final java.util.Map<String, String> credentials;

    /** Full constructor. */
    public LoadTableResponse(
        GetCommitsResponse commitsResponse, String etag,
        String location, String tableId, String tableType,
        java.util.Map<String, String> properties,
        AbstractProtocol protocol, AbstractMetadata metadata,
        java.util.Map<String, String> credentials) {
      this.commitsResponse = commitsResponse;
      this.etag = etag;
      this.location = location;
      this.tableId = tableId;
      this.tableType = tableType;
      this.properties = properties;
      this.protocol = protocol;
      this.metadata = metadata;
      this.credentials = credentials;
    }

    /** Commit-path constructor -- no protocol, metadata, or credentials. */
    public LoadTableResponse(
        GetCommitsResponse commitsResponse, String etag,
        String location, String tableId, String tableType,
        java.util.Map<String, String> properties) {
      this(commitsResponse, etag, location, tableId, tableType,
          properties, null, null, null);
    }

    public GetCommitsResponse getCommitsResponse() { return commitsResponse; }
    public String getEtag() { return etag; }
    public String getLocation() { return location; }
    public String getTableId() { return tableId; }
    public String getTableType() { return tableType; }
    public java.util.Map<String, String> getProperties() { return properties; }
    /** DRC protocol, or null if loaded via commit path. */
    public AbstractProtocol getProtocol() { return protocol; }
    /** DRC metadata, or null if loaded via commit path. */
    public AbstractMetadata getMetadata() { return metadata; }
    /**
     * Temporary storage credentials as Hadoop-style properties
     * (e.g. fs.s3a.access.key, fs.s3a.secret.key). Empty map if
     * credential vending is unavailable or not needed.
     */
    public java.util.Map<String, String> getCredentials() {
      return credentials != null ? credentials : java.util.Collections.emptyMap();
    }
  }

  /**
   * Load table from UC. Returns the full table state: commits + etag + metadata.
   * DRC path: single RPC. Legacy path: calls getTable + getCommits.
   * Callers use this instead of getCommits when they need the etag.
   */
  LoadTableResponse loadTable(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Long> startVersion, Optional<Long> endVersion)
      throws IOException;

  /** Create table with native protocol, schema, domain metadata (DRC or legacy). */
  void createTable(
      String catalog, String schema, String table, String location,
      AbstractMetadata metadata, AbstractProtocol protocol,
      boolean isManaged, String dataSourceFormat,
      List<? extends AbstractDomainMetadata> domainMetadata)
      throws CommitFailedException;

  /** Commit (Spark V1 path: from TableDescriptor). */
  void commit(
      TableDescriptor tableDesc, Path logPath,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException;

  /** Commit (Kernel V2 path: explicit catalog/schema/table). */
  void commit(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException;

}
