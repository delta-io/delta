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

import io.delta.storage.commit.TableIdentifier;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StagingTableInfo;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo;
import java.io.IOException;
import java.net.URI;

/**
 * Extends {@link UCClient} with Delta table lifecycle operations backed by the UC Delta API
 * (load, stage, and create tables).
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Loads a table's metadata from Unity Catalog.
   *
   * @param tableIdentifier catalog + schema namespace and table name
   * @return the table's {@link TableInfo}, carrying the catalog-supplied storage location and
   *         metadata
   * @throws IOException on network or API errors
   */
  TableInfo loadTable(TableIdentifier tableIdentifier) throws IOException;

  /**
   * Reserves a staging slot for a new Delta table. The returned response contains the table ID,
   * storage location, and protocol/property requirements that the caller must honor when
   * finalizing the table with {@link #createTable}.
   *
   * @param tableIdentifier catalog + schema namespace and table name
   * @return a {@link StagingTableInfo} with the reserved table details
   * @throws IOException on network or API errors
   */
  StagingTableInfo createStagingTable(TableIdentifier tableIdentifier) throws IOException;

  /**
   * Finalizes a previously staged Delta table, making it visible in the catalog. Mirrors the
   * shape of {@link UCClient#commit}: the catalog-level identity is conveyed by
   * {@code tableId} / {@code tableUri} / {@code tableIdentifier}; the Delta-log shape is
   * conveyed by an {@link AbstractMetadata} (description, schemaString, partitionColumns,
   * configuration) and an {@link AbstractProtocol} (min reader/writer versions, reader/writer
   * features).
   *
   * @param tableUri               the storage location of the staged table
   * @param tableIdentifier        catalog + schema namespace and table name
   * @param tableType              MANAGED or EXTERNAL
   * @param metadata               the Delta metadata to register; only {@code description},
   *                               {@code schemaString}, {@code partitionColumns}, and
   *                               {@code configuration} are consumed
   * @param protocol               the Delta protocol the table will be created with
   * @param lastCommitTimestampMs  Delta-log timestamp of the initial commit; UC stores this on
   *                               the catalog entry
   * @return the newly created table's {@link TableInfo}
   * @throws IOException on network or API errors
   */
  TableInfo createTable(
      URI tableUri,
      TableIdentifier tableIdentifier,
      UCDeltaModels.TableType tableType,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      long lastCommitTimestampMs) throws IOException;
}
