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
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.StagingTableResponse;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for interacting with the Unity Catalog.
 *
 * This interface defines the contract for operations related to the Unity Catalog,
 * including retrieving the metastore ID, and adding new commits to Delta tables where UC
 * acts as the Commit Coordinator and similarly retrieving unbackfilled commits.
 *
 * Implementations of this interface should handle the specifics of connecting to and
 * communicating with the Unity Catalog, including any necessary authentication and
 * request handling.
 */
public interface UCClient extends AutoCloseable {

  /**
   * Returns whether this client can use UC Delta Rest Catalog API.
   */
  default boolean supportsUCDeltaRestCatalogApi() {
    return false;
  }

  /**
   * Retrieves the metastore ID associated with this Unity Catalog instance.
   *
   * @return A String representing the unique identifier of the metastore
   * @throws IOException if there's an error in retrieving the metastore ID
   */
  String getMetastoreId() throws IOException;

  /**
   * Commits new changes to a Delta table using the Unity Catalog as the Commit Coordinator.
   *
   * This method is responsible for committing changes to a Delta table, including new data,
   * metadata updates, and protocol changes. It interacts with the Unity Catalog to ensure
   * proper coordination and consistency of the commit process.
   * Note: At least one of commit or lastKnownBackfilledVersion must be present.
   *
   * @param tableId The unique identifier of the Delta table.
   * @param tableUri The URI of the storage location of the table. e.g. s3://bucket/path/to/table
   *                 (and not s3://bucket/path/to/table/_delta_log).
   *                 If the tableId exists but the tableUri is different from the one previously
   *                 registered (e.g., if the table as moved), the request will fail.
   * @param commit An Optional containing the Commit object with the changes to be committed.
   *               If empty, it indicates that no new data is being added in this commit.
   * @param lastKnownBackfilledVersion An Optional containing the last known backfilled version
   *                                   of the table. This value serves as a hint to the UC about the
   *                                   most recent version that has been successfully backfilled.
   *                                   UC can use this information to optimize its internal state
   *                                   management by cleaning up tracking information for backfilled
   *                                   commits up to this version.
   *                                   If not provided (Optional.empty()), UC will rely on its
   *                                   current state without any additional cleanup hints.
   * @param disown A boolean flag indicating whether to disown the table after commit.
   *               If true, the coordinator will release ownership of the table after the commit.
   * @param newMetadata An Optional containing new metadata to be applied to the table.
   *                    If present, the table's metadata will be updated atomically with the commit.
   * @param newProtocol An Optional containing a new protocol version to be applied to the table.
   *                    If present, the table's protocol will be updated atomically with the commit.
   * @param uniform An Optional containing UniForm metadata for Delta Universal Format support.
   *                If present, this metadata will be used by UC to manage format conversions
   *                (e.g., Iceberg, Hudi).
   * @throws IOException if there's an error during the commit process, such as network issues.
   * @throws CommitFailedException if the commit fails due to conflicts or other logical errors.
   * @throws UCCommitCoordinatorException if there's an error specific to the Unity Catalog
   *         commit coordination process.
   */
  void commit(
      String tableId,
      URI tableUri,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform
  ) throws IOException, CommitFailedException, UCCommitCoordinatorException;

  /**
   * Retrieves the unbackfilled commits for a Delta table within a specified version range.
   *
   * @param tableId The unique identifier of the Delta table.
   * @param tableUri The URI of the storage location of the table. e.g. s3://bucket/path/to/table
   *                 (and not s3://bucket/path/to/table/_delta_log).
   *                 If the tableId exists but the tableUri is different from the one previously
   *                 registered (e.g., if the table as moved), the request will fail.
   * @param startVersion An Optional containing the start version of the range of commits to
   *                     retrieve.
   * @param endVersion An Optional containing the end version of the range of commits to retrieve.
   * @return A GetCommitsResponse object containing the unbackfilled commits within the specified
   *         version range. If all commits are backfilled, the response will contain an empty list.
   *         The response also contains the last known backfilled version of the table. If no
   *         commits are ratified via UC, the lastKnownBackfilledVersion will be -1.
   * @throws IOException if there's an error during the commit process, such as network issues.
   * @throws UCCommitCoordinatorException if there's an error specific to the Unity Catalog such as
   *                                      the table not being found.
   */
  GetCommitsResponse getCommits(
      String tableId,
      URI tableUri,
      Optional<Long> startVersion,
      Optional<Long> endVersion) throws IOException, UCCommitCoordinatorException;

  /** Column definition for registering table schema with Unity Catalog. */
  class ColumnDef {
    private final String name;
    private final String typeName;
    private final String typeText;
    private final String typeJson;
    private final boolean nullable;
    private final int position;

    public ColumnDef(
        String name, String typeName, String typeText, String typeJson,
        boolean nullable, int position) {
      this.name = name;
      this.typeName = typeName;
      this.typeText = typeText;
      this.typeJson = typeJson;
      this.nullable = nullable;
      this.position = position;
    }

    public String getName() { return name; }
    public String getTypeName() { return typeName; }
    public String getTypeText() { return typeText; }
    public String getTypeJson() { return typeJson; }
    public boolean isNullable() { return nullable; }
    public int getPosition() { return position; }
  }

  /**
   * Promotes a staging table to a real managed table in Unity Catalog. This is the correct API
   * for version 0 (CREATE); for version 1+ (WRITE), use {@link #commit} instead.
   *
   * @param tableName table name (relative to parent schema)
   * @param catalogName parent catalog name in Unity Catalog
   * @param schemaName parent schema name in Unity Catalog
   * @param storageLocation the storage root URL for the table
   * @param columns column definitions for the table schema
   * @param properties properties to persist in UC (protocol features, metadata config, etc.)
   * @throws CommitFailedException if there is a network or server error during finalization
   */
  void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<ColumnDef> columns,
      Map<String, String> properties) throws CommitFailedException;

  /**
   * Loads a Delta table from Unity Catalog through the UC Delta Rest Catalog API.
   *
   * <p>Implementations that do not support UC Delta Rest Catalog API should use the default
   * implementation, which fails loudly so callers do not accidentally route UC Delta Rest Catalog
   * API operations through a legacy-only client.
   */
  default LoadTableResponse loadTable(
      String catalog,
      String schema,
      String table) throws IOException {
    throw new UnsupportedOperationException(
        "loadTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Gets temporary storage credentials for a table through the UC Delta Rest Catalog API.
   *
   * <p>Implementations that do not support UC Delta Rest Catalog API should use the default
   * implementation, which fails loudly so callers do not accidentally treat legacy UC clients as
   * credential-aware Delta clients.
   */
  default CredentialsResponse getTableCredentials(
      CredentialOperation operation,
      String catalog,
      String schema,
      String table) throws IOException {
    throw new UnsupportedOperationException(
        "getTableCredentials requires UC Delta Rest Catalog API support.");
  }

  /**
   * Gets temporary storage credentials for a cloud path through the UC Delta Rest Catalog API.
   *
   * <p>Unlike table credentials, path credentials are not scoped by catalog/schema/table because
   * raw path access such as {@code delta.`s3://bucket/table`} bypasses catalog resolution.
   * External table creates also use this path before the table exists in Unity Catalog.
   */
  default CredentialsResponse getTemporaryPathCredentials(
      CredentialOperation operation,
      String location) throws IOException {
    throw new UnsupportedOperationException(
        "getTemporaryPathCredentials requires UC Delta Rest Catalog API temporary path "
            + "credentials support.");
  }

  /**
   * Creates a Delta staging table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  default StagingTableResponse createStagingTable(
      String catalog,
      String schema,
      String table) throws IOException {
    throw new UnsupportedOperationException(
        "createStagingTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Finalizes a staged Delta table in Unity Catalog through the UC Delta Rest Catalog API.
   */
  default LoadTableResponse createTable(
      String catalog,
      String schema,
      CreateTableRequest request) throws IOException {
    throw new UnsupportedOperationException(
        "createTable requires UC Delta Rest Catalog API support.");
  }

  /**
   * Closes any resources used by this client.
   * This method should be called to properly release resources such as network
   * connections (e.g., HTTPClient) when the client is no longer needed.
   * Once this method is called, the client should not be used to perform any operations.
   *
   * @throws IOException if there's an error while closing resources
   */
  @Override
  void close() throws IOException;
}
