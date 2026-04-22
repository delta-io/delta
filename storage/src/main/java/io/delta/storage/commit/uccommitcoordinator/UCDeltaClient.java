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
import io.unitycatalog.client.delta.model.LoadTableResponse;

import java.io.IOException;

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
}
