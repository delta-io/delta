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

import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.actions.AbstractMetadata;

import java.io.IOException;

/**
 * Extended interface for interacting with Unity Catalog for Delta-specific operations.
 *
 * This interface extends {@link UCClient} to provide additional Delta-specific
 * functionality beyond the base commit coordination operations, including
 * table lifecycle management through the UC Delta Rest Catalog API.
 *
 * Implementations should handle Delta-specific concerns while delegating core
 * commit coordination to the underlying {@link UCClient} contract.
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Loads a Delta table from Unity Catalog through the UC Delta Rest Catalog API.
   */
  default AbstractMetadata loadTable(
      String catalog,
      String schema,
      String table) throws IOException {
    throw new UnsupportedOperationException(
        "loadTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Creates a Delta staging table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  default UCDeltaModels.StagingTableResponse createStagingTable(
      String catalog,
      String schema,
      String table) throws IOException {
    throw new UnsupportedOperationException(
        "createStagingTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Finalizes a staged Delta table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  default AbstractMetadata createTable(
      String catalog,
      String schema,
      UCDeltaModels.CreateTableRequest request) throws IOException {
    throw new UnsupportedOperationException(
        "createTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Updates a Delta table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  default AbstractMetadata updateTable(
      String catalog,
      String schema,
      String table,
      UCDeltaModels.UpdateTableRequest request)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    throw new UnsupportedOperationException(
        "updateTable requires UC Delta Rest Catalog API support.");
  }
}
