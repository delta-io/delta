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
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Unified Delta-Spark client for Unity Catalog communication across both the catalog path
 * ({@code loadTable} / {@code createTable} / list / drop / rename) and the commit path
 * ({@code commit} / staging credentials). Collapses the two historical clients --
 * {@code UCProxy} (catalog, Scala) and {@code UCClient} (commit, Java) -- behind one
 * interface so that one HTTP connection, one auth context, and one in-memory view of table
 * state are shared.
 *
 * <p><b>Routing contract.</b> Whether a call routes to the Delta REST Catalog (DRC) v1
 * endpoints or the legacy UC endpoints is decided inside the implementation on every call.
 * Callers must not branch on {@link #isDRCEnabled()} to pick between code paths -- that is
 * the exact pattern this interface exists to prevent. {@code isDRCEnabled()} is for
 * observability and for softening UC-managed kill switches on the DRC path.
 *
 * <p><b>Signature discipline.</b> Every method parameter and every return type is a Delta
 * storage type ({@link AbstractMetadata}, {@link AbstractProtocol}, {@link Commit}) or a
 * Delta-owned DTO ({@link UCLoadTableResponse} and siblings). No {@code
 * io.unitycatalog.client.*} type leaks through this interface so that non-Spark consumers
 * (Kernel, Flink) can depend on it without pulling in the UC SDK.
 *
 * <p><b>Threading.</b> Implementations must be safe for concurrent use by Spark task
 * threads within one {@code SparkSession}; one instance is shared across all of them via
 * the session-scoped registry.
 *
 * <p><b>Extends {@link UCClient} for migration.</b> Everything that already holds a
 * {@code UCClient} reference -- most importantly {@code UCCommitCoordinatorClient} --
 * can be handed a {@code UCDeltaClient} without a refactor. The inherited path-based
 * {@code commit} / {@code getCommits} / {@code finalizeCreate} / {@code getMetastoreId}
 * are the LEGACY surface; the methods added here are the DRC surface. The DRC
 * {@link #commit} is an overload of the legacy {@code commit}: different parameter
 * shape, different semantics (intent-based vs full-replacement), different return type
 * ({@link UCLoadTableResponse} vs void). Callers choose which shape matches their need.
 *
 * <p><b>Stability: evolving.</b> Method set grows per-PR during DRC rollout; consumers
 * outside Delta should not implement this interface directly.
 */
public interface UCDeltaClient extends UCClient {

  // ---------------------------------------------------------------------------
  // Read
  // ---------------------------------------------------------------------------

  /**
   * Loads a table's full metadata state from Unity Catalog.
   *
   * <p>DRC path: {@code GET /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}}. The response
   * carries structured metadata, unbackfilled commits, the server etag (optional in v1),
   * and {@code latest-table-version}. The response's {@code protocol} field has been
   * removed from the DRC spec; callers see a protocol derived from the
   * {@code delta.minReaderVersion} / {@code delta.minWriterVersion} / {@code
   * delta.feature.*} properties on the returned metadata.
   *
   * <p>Legacy path: not routed through this client -- legacy load goes through
   * {@code UCProxy.loadTable} directly on the catalog side. Implementations throw
   * {@link UnsupportedOperationException} when invoked with DRC disabled; catalog-side
   * wiring must gate via {@link #isDRCEnabled()} before calling.
   */
  UCLoadTableResponse loadTable(String catalog, String schema, String table)
      throws IOException;

  /**
   * Paginated listing of tables in a schema. Page size is advisory (server may cap it).
   */
  UCListTablesResponse listTables(
      String catalog, String schema, Optional<String> pageToken, Optional<Integer> maxResults)
      throws IOException;

  // ---------------------------------------------------------------------------
  // Create (two-phase managed table flow)
  // ---------------------------------------------------------------------------

  /**
   * Phase 1: allocate a UUID, a storage location, vended credentials, and a required
   * protocol / required properties envelope for a new managed table. The caller writes the
   * initial {@code 0.json} commit to the vended location before calling
   * {@link #createTable}.
   */
  UCCreateStagingTableResponse createStagingTable(
      String catalog, String schema, String name) throws IOException;

  /**
   * Phase 2: promote a staged managed table to a registered table with schema, protocol,
   * properties, and (optional) domain metadata. The initial commit written during phase 1
   * is referenced by {@code initialCommit}.
   *
   * <p>{@code properties} MUST be the raw {@code metaData.configuration} map -- server-
   * derived keys ({@code delta.feature.*}, {@code delta.minReaderVersion}, {@code
   * delta.lastUpdateVersion}, {@code clusteringColumns}, {@code is_managed_location})
   * MUST NOT appear. Implementations enforce this at the boundary and fail loud.
   */
  UCLoadTableResponse createTable(
      String catalog,
      String schema,
      String name,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      List<UCDomainMetadata> domainMetadata,
      Commit initialCommit) throws IOException;

  // ---------------------------------------------------------------------------
  // Write
  // ---------------------------------------------------------------------------

  /**
   * Commits to a registered managed table. Bundles the staged commit and any metadata
   * updates into a single atomic RPC.
   *
   * <p>The implementation computes intent-based diffs between {@code oldMetadata} and
   * {@code newMetadata} across four axes: description (comment), partition columns,
   * schema, and raw properties. {@code protocol} is full-replacement when
   * {@code oldProtocol != newProtocol}. Domain metadata is per-domain intent-based using
   * {@code oldDomainMetadata} and {@code newDomainMetadata}.
   *
   * <p>Requirements emitted:
   * <ul>
   *   <li>{@code assert-table-uuid} -- always.</li>
   *   <li>{@code assert-etag} -- only when {@code etag} is present. Absent etag is
   *       accepted in v1 (concurrency gap documented in design clarifications §3).</li>
   * </ul>
   *
   * <p>The returned {@link UCLoadTableResponse} reflects the post-commit state including
   * the new server etag.
   */
  UCLoadTableResponse commit(
      String catalog,
      String schema,
      String table,
      Optional<String> etag,
      Commit commit,
      AbstractMetadata oldMetadata,
      AbstractMetadata newMetadata,
      AbstractProtocol oldProtocol,
      AbstractProtocol newProtocol,
      List<UCDomainMetadata> oldDomainMetadata,
      List<UCDomainMetadata> newDomainMetadata) throws IOException;

  // ---------------------------------------------------------------------------
  // Admin
  // ---------------------------------------------------------------------------

  void dropTable(String catalog, String schema, String table) throws IOException;

  void renameTable(String catalog, String schema, String oldName, String newName)
      throws IOException;

  // ---------------------------------------------------------------------------
  // Credentials
  // ---------------------------------------------------------------------------

  /**
   * Vends storage credentials scoped to a registered table for the given operation. READ
   * returns read-only credentials; READ_WRITE returns credentials that permit writes. The
   * returned credentials have a server-assigned TTL.
   */
  UCCredentialsResponse getTableCredentials(
      String catalog, String schema, String table, UCCredentialOperation operation)
      throws IOException;

  /**
   * Vends storage credentials for the location returned by {@link #createStagingTable}
   * before the table is promoted. Used during phase 1 to write the initial commit.
   */
  UCCredentialsResponse getStagingTableCredentials(String tableId) throws IOException;

  /**
   * Vends credentials scoped to a raw path (used for external tables and cross-table
   * operations like RESTORE).
   */
  UCCredentialsResponse getPathCredentials(String location, UCCredentialOperation operation)
      throws IOException;

  // ---------------------------------------------------------------------------
  // Introspection / lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} when DRC routing is active. Resolved on every call from the
   * underlying provider so that flag flips take effect immediately without client
   * reconstruction. Use for logging, metrics, and softening UC-managed kill switches --
   * NOT for picking between code paths at the caller.
   */
  boolean isDRCEnabled();

  /**
   * Releases resources owned by this wrapper. Does NOT close the underlying UC SDK
   * {@code ApiClient}; that HTTP pool is owned by the UC catalog plugin and outlives any
   * single {@code UCDeltaClient} instance.
   */
  @Override
  void close() throws IOException;
}
