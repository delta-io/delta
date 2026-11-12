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
import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.CommitReport;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StagingTableInfo;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo;
import io.delta.storage.commit.uniform.UniformMetadata;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

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
   * Finalizes a previously staged Delta table, or registers an EXTERNAL Delta table, making it
   * visible in the catalog. Mirrors the
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
   * @param domainMetadata         the snapshot's domain-metadata entries (e.g. clustering,
   *                               row-tracking); pass an empty list when none are set
   * @param lastCommitTimestampMs  Delta-log timestamp of the initial commit; UC stores this on
   *                               the catalog entry
   * @param lastCommitVersion      Delta-log version of the registered snapshot. It is 0 for a
   *                               new table and can be higher when an EXTERNAL table is
   *                               registered over an existing Delta log. Recording a version
   *                               above 0 is best effort: the table can be created with UC
   *                               still at version 0
   * @param uniformMetadata        UniForm Iceberg metadata generated for the initial snapshot;
   *                               empty when the table was not created with UniForm enabled
   * @return the newly created table's {@link TableInfo}
   * @throws IOException on network or API errors
   */
  TableInfo createTable(
      URI tableUri,
      TableIdentifier tableIdentifier,
      UCDeltaModels.TableType tableType,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      List<AbstractDomainMetadata> domainMetadata,
      long lastCommitTimestampMs,
      long lastCommitVersion,
      Optional<UniformMetadata> uniformMetadata) throws IOException;

  /**
   * Updates the catalog entry of an EXTERNAL table to match a committed Delta snapshot, without
   * a commit. The update replaces the entry's columns, partition columns, comment, properties,
   * protocol and UC-modeled domain metadata, removes the properties that the commit removed, and
   * records {@code commitVersion} and {@code commitTimestampMs}.
   *
   * <p>The update is skipped when the table no longer exists, when the catalog entry's location
   * is not {@code tableUri} (the name now refers to another table), or when the entry already
   * records {@code commitVersion} or a later version (a newer commit already updated it). The update is conditional on the entry's
   * etag; on an etag conflict the entry is reloaded and the update retried once.
   *
   * @param tableUri          storage location of the committed Delta table
   * @param tableIdentifier   catalog + schema namespace and table name
   * @param oldMetadata       the Delta metadata before the commit; only its {@code
   *                          configuration} is consumed, to find removed properties
   * @param newMetadata       the Delta metadata after the commit; {@code description}, {@code
   *                          schemaString}, {@code partitionColumns}, and {@code configuration}
   *                          are consumed
   * @param newProtocol       the Delta protocol after the commit
   * @param domainMetadata    the snapshot's domain-metadata entries plus the commit's removals;
   *                          entries for domains UC does not model are ignored
   * @param commitVersion     the committed Delta version
   * @param commitTimestampMs the committed Delta version's timestamp, in epoch milliseconds
   * @return {@code true} if the entry was updated, {@code false} if the update was skipped
   * @throws IOException on network or API errors, including a second etag conflict
   */
  boolean updateTableMetadata(
      URI tableUri,
      TableIdentifier tableIdentifier,
      AbstractMetadata oldMetadata,
      AbstractMetadata newMetadata,
      AbstractProtocol newProtocol,
      List<AbstractDomainMetadata> domainMetadata,
      long commitVersion,
      long commitTimestampMs) throws IOException;

  /**
   * Reports post-commit telemetry for a table (file/row counts, byte counts, file-size
   * histogram) to the catalog.
   *
   * @param tableId         catalog-side table id (UC's `table_uuid`)
   * @param tableIdentifier catalog + schema namespace and table name
   * @param report          per-commit metrics payload
   * @throws IOException on network or API errors
   */
  void reportMetrics(
      String tableId,
      TableIdentifier tableIdentifier,
      CommitReport report) throws IOException;
}
