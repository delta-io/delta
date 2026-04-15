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
import io.delta.storage.commit.TableDescriptor;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.unitycatalog.client.ApiClient;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * No-DRC subclass. Implements UCDeltaClient using legacy UC APIs.
 * loadTable/createTable use the shared legacy helpers in the base class.
 * commit/getCommits delegate to the base class UCClient methods.
 * Compiled when building without -DdeltaRestCatalog=true (released UC).
 */
public class UCTokenBasedDeltaRestClient
    extends UCTokenBasedRestClient implements UCDeltaClient {

  public UCTokenBasedDeltaRestClient(ApiClient apiClient, boolean drcEnabled) {
    super(apiClient);
  }

  public UCTokenBasedDeltaRestClient(
      String uri,
      io.unitycatalog.client.auth.TokenProvider tokenProvider,
      java.util.Map<String, String> appVersions,
      boolean drcEnabled) {
    super(uri, tokenProvider, appVersions);
  }


  @Override
  public void createTable(
      String catalog, String schema, String table, String location,
      AbstractMetadata metadata, AbstractProtocol protocol,
      boolean isManaged, String dataSourceFormat,
      List<? extends io.delta.storage.commit.actions.AbstractDomainMetadata> domainMetadata)
      throws CommitFailedException {
    createTableLegacy(catalog, schema, table, location,
        metadata, protocol, isManaged);
  }

  @Override
  public void commit(
      TableDescriptor tableDesc, Path logPath,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException {
    boolean metadataChanged = oldMetadata.isPresent() && newMetadata.isPresent()
        && !oldMetadata.get().equals(newMetadata.get());
    boolean protocolChanged = oldProtocol.isPresent() && newProtocol.isPresent()
        && !oldProtocol.get().equals(newProtocol.get());
    if ((metadataChanged || protocolChanged) && !disown) {
      throw new CommitFailedException(false, false,
          "Metadata/protocol changes require Delta REST Catalog but DRC " +
          "is not available. Build with -DdeltaRestCatalog=true.");
    }
    try {
      super.commit(extractTableId(tableDesc),
          CoordinatedCommitsUtils.getTablePath(logPath).toUri(),
          commit, lastKnownBackfilledVersion, disown,
          Optional.empty(), Optional.empty(), uniform);
    } catch (UCCommitCoordinatorException e) {
      throw new CommitFailedException(true, false, e.getMessage(), e);
    }
  }

  @Override
  public void commit(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<io.delta.storage.commit.uniform.UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException {
    boolean metadataChanged = oldMetadata.isPresent() && newMetadata.isPresent()
        && !oldMetadata.get().equals(newMetadata.get());
    boolean protocolChanged = oldProtocol.isPresent() && newProtocol.isPresent()
        && !oldProtocol.get().equals(newProtocol.get());
    if ((metadataChanged || protocolChanged) && !disown) {
      throw new CommitFailedException(false, false,
          "Metadata/protocol changes require Delta REST Catalog but DRC " +
          "is not available. Build with -DdeltaRestCatalog=true.");
    }
    try {
      super.commit(ucTableId, tableUri, commit, lastKnownBackfilledVersion,
          disown, Optional.empty(), Optional.empty(), uniform);
    } catch (UCCommitCoordinatorException e) {
      throw new CommitFailedException(true, false, e.getMessage(), e);
    }
  }

  @Override
  public UCDeltaClient.LoadTableResponse loadTable(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Long> startVersion, Optional<Long> endVersion) throws IOException {
    GetCommitsResponse commits;
    try {
      commits = super.getCommits(ucTableId, tableUri, startVersion, endVersion);
    } catch (UCCommitCoordinatorException e) {
      throw new IOException(e);
    }
    return new UCDeltaClient.LoadTableResponse(
        commits, null, null, ucTableId, null, null);
  }

  private static String extractTableId(TableDescriptor tableDesc) {
    Map<String, String> tableConf = tableDesc.getTableConf();
    String id = tableConf.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (id == null) {
      throw new IllegalStateException("UC Table ID not found in " + tableConf);
    }
    return id;
  }
}
