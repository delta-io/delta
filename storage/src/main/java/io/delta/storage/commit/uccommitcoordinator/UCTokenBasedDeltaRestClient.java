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
import io.delta.storage.commit.TableDescriptor;
import io.delta.storage.commit.actions.AbstractDomainMetadata;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.DeltaRestClientProvider;
import io.unitycatalog.client.delta.api.TablesApi;
// NOTE: io.unitycatalog.client.delta.model.LoadTableResponse is fully-qualified below because
// it collides with UCDeltaClient.LoadTableResponse (inner class) in this subclass's scope.
import io.unitycatalog.client.delta.model.TableMetadata;

import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production {@link UCDeltaClient} implementation. Extends {@link UCTokenBasedRestClient} so
 * that the legacy {@link UCClient} methods (commit, getCommits, getMetastoreId, finalizeCreate,
 * close) come for free -- {@link UCCommitCoordinatorClient} continues to hold a
 * {@code UCClient} field and does not need to change signature until the commit-path PR wires
 * the typed overloads.
 *
 * <p>The DRC-specific methods added by {@link UCDeltaClient} route through the
 * {@link DeltaRestClientProvider}'s {@code TablesApi} -- resolved on every call so mid-session
 * flag flips (via the UC-side {@code spark.sql.catalog.<catalog>.deltaRestApi.enabled}) take
 * effect immediately without reconstructing the client.
 *
 * <p><b>Method scope in this PR.</b> Only {@link #loadTable(String, String, String, String,
 * URI, Optional, Optional)} is wired. {@link #createTable} and the typed {@link #commit}
 * overloads throw {@link UnsupportedOperationException} until the PRs that wire them land:
 * createTable with the post-v1 CREATE PR, typed commits with the etag-plumbing and intent-diff
 * PRs. Legacy {@code UCClient.commit} (inherited) continues to service the existing commit
 * path unchanged; on DRC-enabled tables, that legacy path remains in use until the typed
 * overrides land.
 */
public final class UCTokenBasedDeltaRestClient extends UCTokenBasedRestClient
    implements UCDeltaClient {

  private static final Logger LOG = LoggerFactory.getLogger(UCTokenBasedDeltaRestClient.class);

  private final DeltaRestClientProvider drcProvider;
  /** Last-logged routing decision -- logs on transitions, not just the first call. */
  private final AtomicReference<Boolean> lastLoggedRoute = new AtomicReference<>(null);
  private final AtomicLong drcLoadTableCount = new AtomicLong();
  private final AtomicLong legacyLoadTableCount = new AtomicLong();

  /**
   * Constructs a DRC-aware client that shares an {@link ApiClient} with the UC catalog plugin.
   * Legacy UC API calls go through the inherited {@link UCTokenBasedRestClient} machinery;
   * DRC calls go through {@code drcProvider.getDeltaTablesApi()}. Both ultimately talk to the
   * same UC server via the same HTTP pool.
   */
  public UCTokenBasedDeltaRestClient(ApiClient apiClient, DeltaRestClientProvider drcProvider) {
    super(apiClient);
    this.drcProvider = Objects.requireNonNull(drcProvider, "drcProvider");
  }

  @Override
  public boolean isDRCEnabled() {
    return drcProvider.getDeltaTablesApi().isPresent();
  }

  @Override
  public UCDeltaClient.LoadTableResponse loadTable(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Long> startVersion, Optional<Long> endVersion) throws IOException {
    Objects.requireNonNull(catalog, "catalog"); require(!catalog.isEmpty(), "catalog empty");
    Objects.requireNonNull(schema, "schema");   require(!schema.isEmpty(),  "schema empty");
    Objects.requireNonNull(table, "table");     require(!table.isEmpty(),   "table empty");

    final Optional<TablesApi> api = drcProvider.getDeltaTablesApi();
    if (api.isPresent()) {
      logRoutingTransition(true);
      drcLoadTableCount.incrementAndGet();
      return loadTableDRC(api.get(), catalog, schema, table);
    } else {
      logRoutingTransition(false);
      legacyLoadTableCount.incrementAndGet();
      // Legacy load = getTable + getCommits composition. Ships alongside the typed commit
      // overloads in a follow-up PR; for now, the catalog layer reaches UCProxy directly
      // when DRC is off and never invokes this method.
      throw new UnsupportedOperationException(
          "UCDeltaClient.loadTable legacy fallback not yet wired; caller must gate on "
              + "isDRCEnabled() until the commit-path PR lands the getTable+getCommits "
              + "composition.");
    }
  }

  private UCDeltaClient.LoadTableResponse loadTableDRC(
      TablesApi api, String catalog, String schema, String table) throws IOException {
    final io.unitycatalog.client.delta.model.LoadTableResponse resp;
    try {
      resp = api.loadTable(catalog, schema, table);
    } catch (ApiException e) {
      throw translateApiException(e, catalog, schema, table);
    }
    require(resp != null,
        "DRC loadTable returned a null response for " + fqn(catalog, schema, table));
    require(resp.getLatestTableVersion() != null,
        "DRC loadTable response missing required 'latest-table-version'");
    final TableMetadata meta = resp.getMetadata();
    require(meta != null, "DRC loadTable response missing required 'metadata'");
    require(meta.getTableUuid() != null,
        "DRC loadTable response missing required 'metadata.table-uuid'");
    require(meta.getColumns() != null && meta.getColumns().getFields() != null,
        "DRC loadTable response missing required 'metadata.columns'");
    // Commit translation lands in the etag-plumbing PR. Fail loud if the server returns
    // unbackfilled commits today -- silent stale snapshots are worse than a surfaced gap.
    require(resp.getCommits() == null || resp.getCommits().isEmpty(),
        "DRC loadTable response carried " + (resp.getCommits() == null
            ? 0 : resp.getCommits().size())
            + " unbackfilled commits; commit translation is not yet wired");

    final DRCMetadataAdapter adapter = new DRCMetadataAdapter(
        meta.getTableUuid().toString(),
        /* name */ table,
        /* description */ null,
        meta.getColumns().getFields(),
        meta.getPartitionColumns() != null
            ? meta.getPartitionColumns() : Collections.emptyList(),
        meta.getProperties() != null ? meta.getProperties() : Collections.emptyMap(),
        meta.getCreatedTime());

    // GetCommitsResponse constructor takes (commits, latestTableVersion). Pass an empty list
    // here -- unbackfilled-commit translation is tracked by the require above.
    final GetCommitsResponse commitsResponse = new GetCommitsResponse(
        Collections.emptyList(), resp.getLatestTableVersion());

    return new UCDeltaClient.LoadTableResponse(
        commitsResponse,
        meta.getEtag(),
        meta.getLocation(),
        meta.getTableUuid().toString(),
        meta.getTableType() != null ? meta.getTableType().toString() : null,
        adapter.getConfiguration(),
        /* protocol  -- derived from log in SnapshotManagement */ null,
        adapter,
        /* credentials -- separate RPC in v1, inlined in a later PR */ null);
  }

  // -------------------- createTable + commits (not yet wired) --------------------

  @Override
  public void createTable(
      String catalog, String schema, String table, String location,
      AbstractMetadata metadata, AbstractProtocol protocol,
      boolean isManaged, String dataSourceFormat,
      List<? extends AbstractDomainMetadata> domainMetadata) throws CommitFailedException {
    throw new UnsupportedOperationException(
        "UCDeltaClient.createTable lands with the CREATE-TABLE PR (post-v1 stack).");
  }

  @Override
  public void commit(
      TableDescriptor tableDesc, Path logPath,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion, boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform, Optional<String> etag)
      throws IOException, CommitFailedException {
    throw new UnsupportedOperationException(
        "UCDeltaClient.commit (Spark V1) lands with the commit-path PR.");
  }

  @Override
  public void commit(
      String catalog, String schema, String table, String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion, boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform, Optional<String> etag)
      throws IOException, CommitFailedException {
    throw new UnsupportedOperationException(
        "UCDeltaClient.commit (Kernel V2) lands with the commit-path PR.");
  }

  // -------------------- helpers --------------------

  private void logRoutingTransition(boolean drc) {
    final Boolean prev = lastLoggedRoute.getAndSet(drc);
    if (prev == null) {
      LOG.info("UCTokenBasedDeltaRestClient routing resolved: {}", drc ? "DRC" : "LEGACY");
    } else if (prev.booleanValue() != drc) {
      LOG.info("UCTokenBasedDeltaRestClient routing transition: {} -> {}",
          prev ? "DRC" : "LEGACY", drc ? "DRC" : "LEGACY");
    }
  }

  private static IOException translateApiException(ApiException e, String c, String s, String t) {
    final int code = e.getCode();
    final String body = Optional.ofNullable(e.getResponseBody()).orElse("");
    final String fqn = fqn(c, s, t);
    if (code == 404) {
      return new FileNotFoundException(
          "DRC loadTable: table not found " + fqn + ": " + body);
    }
    return new IOException(String.format(
        "DRC loadTable failed for %s (HTTP %d): %s", fqn, code, body), e);
  }

  private static String fqn(String c, String s, String t) { return c + "." + s + "." + t; }

  private static void require(boolean cond, String msg) {
    if (!cond) throw new IllegalStateException(msg);
  }
}
