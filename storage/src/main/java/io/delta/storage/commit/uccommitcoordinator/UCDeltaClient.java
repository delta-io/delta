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
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.LoadTableResponse;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * Delta-specific UC client surface layered on top of the legacy {@link UCClient}.
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Loads table metadata via the Delta REST Catalog API.
   */
  LoadTableResponse loadTable(
      String catalog,
      String schema,
      String table) throws IOException;

  /**
   * Vends temporary storage credentials for a table via the Delta REST Catalog API.
   */
  CredentialsResponse getTableCredentials(
      CredentialOperation operation,
      String catalog,
      String schema,
      String table) throws IOException;

  /**
   * Placeholder for the future DRC createTable path.
   */
  default void createTable(
      String catalog,
      String schema,
      String table,
      String location,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      boolean isManaged) throws CommitFailedException {
    throw new UnsupportedOperationException("DRC createTable is not implemented yet.");
  }

  /**
   * Placeholder for the future name-based DRC commit path.
   */
  default void commit(
      String catalog,
      String schema,
      String table,
      String tableId,
      URI tableUri,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol,
      Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    throw new UnsupportedOperationException("DRC commit is not implemented yet.");
  }

  /**
   * Placeholder for the future name-based DRC getCommits path.
   */
  default GetCommitsResponse getCommits(
      String catalog,
      String schema,
      String table,
      String tableId,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException {
    throw new UnsupportedOperationException("DRC getCommits is not implemented yet.");
  }
}
