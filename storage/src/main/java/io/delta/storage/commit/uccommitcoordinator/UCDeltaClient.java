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
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.TableUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Client interface for the Unity Catalog Delta REST Catalog (DRC) API. The DRC endpoints live
 * under {@code /api/2.1/unity-catalog/delta/v1/...} on the UC server and expose Delta-native
 * metadata (structured protocol, Delta {@code StructType} columns, domain metadata) without the
 * JSON-roundtrip overhead of the legacy UC API.
 *
 * <p>Implementations wrap the UC SDK's {@code io.unitycatalog.client.delta.api.TablesApi} and
 * share the {@link io.unitycatalog.client.ApiClient} owned by a UC catalog plugin -- the client
 * does not own that {@code ApiClient} and must not close it.
 *
 * <p>The interface grows one method per PR in the DRC rollout; callers should not assume
 * unsupported methods are merely unimplemented.
 */
@Unstable
public interface UCDeltaClient {

  /**
   * Loads a table via the DRC
   * {@code GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}} endpoint.
   *
   * <p>The response carries {@link LoadTableResponse#getMetadata() metadata}
   * (schema as UC {@code StructField} POJOs, structured protocol, properties, etag),
   * {@link LoadTableResponse#getCommits() unbackfilled commits}, and
   * {@link LoadTableResponse#getLatestTableVersion() latest-table-version}. The UC
   * {@code StructField} list is converted to a Spark {@code StructType} by
   * {@code DeltaRestSchemaConverter} in the delta-spark module.
   *
   * @param catalog catalog name
   * @param schema schema name
   * @param table table name (relative to the parent schema)
   * @return the DRC load-table response
   * @throws IOException on network error or a non-success HTTP response from the server
   */
  LoadTableResponse loadTable(String catalog, String schema, String table) throws IOException;

  /**
   * Vends temporary cloud-storage credentials for a table via DRC
   * {@code GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials}.
   *
   * <p>The returned {@link CredentialsResponse#getStorageCredentials() storage-credentials} list
   * carries one or more {@link io.unitycatalog.client.delta.model.StorageCredential} entries,
   * each keyed by a storage path prefix and operation scope. Callers pick the most specific
   * credential (longest-matching prefix) and inject its config into per-query Hadoop/Spark
   * properties -- never into the session-global config, so concurrent queries with different
   * principals do not clobber each other.
   *
   * @param catalog catalog name
   * @param schema schema name
   * @param table table name
   * @param operation {@link CredentialOperation#READ} for queries, {@link
   *     CredentialOperation#READ_WRITE} for writes
   * @return the credentials response
   * @throws IOException on network error or a non-success HTTP response
   */
  CredentialsResponse getTableCredentials(
      String catalog,
      String schema,
      String table,
      CredentialOperation operation) throws IOException;

  /**
   * Submits a CCv2 commit via DRC
   * {@code POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}}.
   *
   * <p>The request body carries:
   * <ul>
   *   <li>{@code requirements}: always an {@code AssertTableUUID} with {@code tableUuid}, and
   *       an {@code AssertEtag} when {@code etag.isPresent()}; etag is optional in v1 per the
   *       Apr-17 clarifications and the concurrency gap is a known tradeoff until Epic 7;</li>
   *   <li>{@code updates}: a single {@code AddCommitUpdate} wrapping {@link DeltaCommit},
   *       followed by the caller-supplied {@code metadataUpdates} in order. The
   *       {@code AddCommitUpdate} always comes first so the server processes the commit before
   *       applying the metadata mutations within the same atomic RPC.</li>
   * </ul>
   *
   * <p>{@link DeltaRestMetadataDiff} produces the {@code metadataUpdates} list from old/new
   * metadata, protocol, schema, and partition-column snapshots. Pass
   * {@link java.util.Collections#emptyList()} for a pure data commit.
   *
   * @param catalog catalog name
   * @param schema schema name
   * @param table table name
   * @param commit the Delta commit to append
   * @param tableUuid table UUID from an earlier {@link #loadTable}; used as the
   *     {@code AssertTableUUID} requirement
   * @param etag optional etag from an earlier load; when present the server rejects the commit
   *     if the table's etag has advanced between load and commit
   * @param metadataUpdates additional {@code TableUpdate}s to include in the same request
   * @return the DRC update-table response carrying the refreshed metadata and etag
   * @throws IOException on network error, HTTP non-success, or UC rejection (etag or UUID
   *     mismatch, rate limit, resource exhaustion, etc.)
   */
  LoadTableResponse commit(
      String catalog,
      String schema,
      String table,
      DeltaCommit commit,
      UUID tableUuid,
      Optional<String> etag,
      List<TableUpdate> metadataUpdates) throws IOException;
}
