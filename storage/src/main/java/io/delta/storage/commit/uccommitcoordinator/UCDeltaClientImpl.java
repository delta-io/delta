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
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.DeltaRestClientProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.TableMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production implementation of {@link UCDeltaClient}. All routing between the legacy UC
 * API and the Delta REST Catalog (DRC) API lives here -- callers always call the
 * interface and get the right behavior transparently.
 *
 * <p>Routing is re-evaluated on every call via
 * {@link DeltaRestClientProvider#getDeltaTablesApi()} so that flipping the UC-side flag
 * {@code spark.sql.catalog.<catalog>.deltaRestApi.enabled} takes effect immediately.
 * The client holds the provider rather than a cached {@code TablesApi} reference for
 * this reason.
 *
 * <p><b>v1 scope.</b> Only {@link #loadTable(String, String, String)} is wired as a new
 * DRC method. Every other new DRC method throws {@link UnsupportedOperationException}
 * with a pointer to the PR where it lands. The catalog-side wiring (future PR) gates on
 * {@link #isDRCEnabled()} before invoking {@code loadTable}; flag-off loads still route
 * through {@code UCProxy} on the Scala side -- the legacy {@code loadTable} path has no
 * storage-side implementation to route to.
 *
 * <p><b>Legacy {@link UCClient} methods.</b> Because {@link UCDeltaClient} extends
 * {@code UCClient}, this impl must honor the inherited path-based methods
 * ({@code getMetastoreId}, {@code commit}, {@code getCommits}, {@code finalizeCreate}).
 * They are delegated to a {@link UCClient} injected at construction so that existing
 * commit-path callers ({@code UCCommitCoordinatorClient}) can receive a
 * {@code UCDeltaClient} without a refactor. When no legacy client is supplied the
 * delegated methods throw {@link UnsupportedOperationException}; full legacy-to-DRC
 * re-routing inside {@code commit} lands in the commit-path PR.
 *
 * <p><b>Internal.</b> Not part of the interface contract; consumers must depend on
 * {@link UCDeltaClient} only.
 */
public final class UCDeltaClientImpl implements UCDeltaClient {

  private static final Logger LOG = LoggerFactory.getLogger(UCDeltaClientImpl.class);

  private final DeltaRestClientProvider provider;
  /** Absent during DRC-only tests; present once callers wire legacy plumbing through. */
  private final Optional<UCClient> legacyClient;

  /**
   * Last-logged routing decision. We log on TRANSITIONS so that a long-lived client
   * outliving a flag flip surfaces the change in logs. Null means we have not logged yet;
   * {@code true}/{@code false} records the last observed routing direction.
   */
  private final AtomicReference<Boolean> lastLoggedRoute = new AtomicReference<>(null);

  // Minimum viable path-taken telemetry per design clarifications §8. Exposed via
  // getters below for future metrics wiring.
  private final AtomicLong drcLoadTableCount = new AtomicLong();
  private final AtomicLong legacyLoadTableCount = new AtomicLong();

  /**
   * DRC-only constructor. Inherited {@link UCClient} methods throw
   * {@link UnsupportedOperationException}; use the two-arg constructor to wire legacy
   * plumbing.
   */
  public UCDeltaClientImpl(DeltaRestClientProvider provider) {
    this(provider, null);
  }

  /**
   * Full constructor. The injected {@code legacyClient} receives all inherited
   * {@link UCClient} method calls; new DRC methods on this class route independently
   * via {@link DeltaRestClientProvider}.
   */
  public UCDeltaClientImpl(DeltaRestClientProvider provider, UCClient legacyClient) {
    this.provider = Objects.requireNonNull(provider, "provider");
    this.legacyClient = Optional.ofNullable(legacyClient);
    LOG.debug("UCDeltaClientImpl constructed (legacyClientPresent={}); routing resolved "
        + "per call.", this.legacyClient.isPresent());
  }

  // ---------------------------------------------------------------------------
  // Introspection
  // ---------------------------------------------------------------------------

  @Override
  public boolean isDRCEnabled() {
    return provider.getDeltaTablesApi().isPresent();
  }

  // ---------------------------------------------------------------------------
  // Read
  // ---------------------------------------------------------------------------

  @Override
  public UCLoadTableResponse loadTable(String catalog, String schema, String table)
      throws IOException {
    requireNonEmpty(catalog, "catalog");
    requireNonEmpty(schema, "schema");
    requireNonEmpty(table, "table");

    Optional<TablesApi> api = provider.getDeltaTablesApi();
    if (api.isPresent()) {
      logRoutingTransition(true);
      drcLoadTableCount.incrementAndGet();
      return loadTableDRC(api.get(), catalog, schema, table);
    } else {
      logRoutingTransition(false);
      legacyLoadTableCount.incrementAndGet();
      // Catalog-side wiring must gate on isDRCEnabled() before invoking -- legacy loads
      // flow through UCProxy directly. Throwing here (rather than silently doing nothing)
      // surfaces a forgotten gate as a loud failure.
      throw new UnsupportedOperationException(
          "UCDeltaClientImpl.loadTable is DRC-only. Flag-off loads must route through "
              + "UCProxy at the catalog layer; caller must gate on isDRCEnabled() before "
              + "invocation.");
    }
  }

  private UCLoadTableResponse loadTableDRC(
      TablesApi api, String catalog, String schema, String table) throws IOException {
    final LoadTableResponse resp;
    try {
      resp = api.loadTable(catalog, schema, table);
    } catch (ApiException e) {
      throw translateApiException(e, catalog, schema, table);
    }

    require(resp != null, "DRC loadTable returned a null response for "
        + fqn(catalog, schema, table));
    require(resp.getLatestTableVersion() != null,
        "DRC loadTable response missing required 'latest-table-version'");

    final TableMetadata meta = resp.getMetadata();
    require(meta != null, "DRC loadTable response missing required 'metadata'");
    require(meta.getTableUuid() != null,
        "DRC loadTable response missing required 'metadata.table-uuid'");
    require(meta.getColumns() != null && meta.getColumns().getFields() != null,
        "DRC loadTable response missing required 'metadata.columns'");

    // Commit translation lands in the commit-path PR. Fail loud the instant a server
    // upgrade returns unbackfilled commits so the oversight surfaces immediately rather
    // than producing a silent stale snapshot.
    require(resp.getCommits() == null || resp.getCommits().isEmpty(),
        "DRC loadTable returned " + (resp.getCommits() == null ? 0 : resp.getCommits().size())
            + " unbackfilled commits; commit translation not yet implemented");

    final DRCMetadataAdapter adapter = new DRCMetadataAdapter(
        meta.getTableUuid().toString(),
        /* name */ table,
        /* description */ null,
        meta.getColumns().getFields(),
        meta.getPartitionColumns() != null
            ? meta.getPartitionColumns() : Collections.emptyList(),
        meta.getProperties() != null
            ? meta.getProperties() : Collections.emptyMap(),
        meta.getCreatedTime());

    // Protocol was removed from TableMetadata in the DRC spec (delta.yaml). It is derived
    // from the flat delta.feature.* / delta.minReaderVersion / delta.minWriterVersion
    // properties. Writer features from delta.feature.X=supported keys are populated;
    // reader features are intentionally left empty and come from the Delta commit log
    // via SnapshotManagement -- populating them here would silently force reader-feature
    // support for writer-only features (appendOnly, invariants, etc.).
    final AbstractProtocol protocol = deriveProtocolFromProperties(adapter.getConfiguration());

    return new UCLoadTableResponse(
        meta.getTableUuid().toString(),
        adapter,
        protocol,
        /* unbackfilled commits -- filled by commit-path PR */ new ArrayList<>(),
        resp.getLatestTableVersion(),
        Optional.ofNullable(meta.getEtag()));
  }

  // ---------------------------------------------------------------------------
  // Unimplemented (wired in later PRs)
  // ---------------------------------------------------------------------------

  @Override
  public UCListTablesResponse listTables(
      String catalog, String schema, Optional<String> pageToken, Optional<Integer> maxResults) {
    throw notYetImplemented("listTables");
  }

  @Override
  public UCCreateStagingTableResponse createStagingTable(
      String catalog, String schema, String name) {
    throw notYetImplemented("createStagingTable");
  }

  @Override
  public UCLoadTableResponse createTable(
      String catalog,
      String schema,
      String name,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      List<UCDomainMetadata> domainMetadata,
      Commit initialCommit) {
    throw notYetImplemented("createTable");
  }

  @Override
  public UCLoadTableResponse commit(
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
      List<UCDomainMetadata> newDomainMetadata) {
    throw notYetImplemented("commit");
  }

  @Override
  public void dropTable(String catalog, String schema, String table) {
    throw notYetImplemented("dropTable");
  }

  @Override
  public void renameTable(String catalog, String schema, String oldName, String newName) {
    throw notYetImplemented("renameTable");
  }

  @Override
  public UCCredentialsResponse getTableCredentials(
      String catalog, String schema, String table, UCCredentialOperation operation) {
    throw notYetImplemented("getTableCredentials");
  }

  @Override
  public UCCredentialsResponse getStagingTableCredentials(String tableId) {
    throw notYetImplemented("getStagingTableCredentials");
  }

  @Override
  public UCCredentialsResponse getPathCredentials(
      String location, UCCredentialOperation operation) {
    throw notYetImplemented("getPathCredentials");
  }

  // ---------------------------------------------------------------------------
  // Legacy UCClient methods -- delegate to injected UCClient or fail loud
  // ---------------------------------------------------------------------------

  @Override
  public String getMetastoreId() throws IOException {
    return requireLegacy("getMetastoreId").getMetastoreId();
  }

  @Override
  public void commit(
      String tableId,
      URI tableUri,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform)
      throws IOException, CommitFailedException, UCCommitCoordinatorException {
    requireLegacy("commit(tableId, tableUri, ...)").commit(
        tableId, tableUri, commit, lastKnownBackfilledVersion, disown,
        newMetadata, newProtocol, uniform);
  }

  @Override
  public GetCommitsResponse getCommits(
      String tableId, URI tableUri, Optional<Long> startVersion, Optional<Long> endVersion)
      throws IOException, UCCommitCoordinatorException {
    return requireLegacy("getCommits").getCommits(
        tableId, tableUri, startVersion, endVersion);
  }

  @Override
  public void finalizeCreate(
      String tableName,
      String catalogName,
      String schemaName,
      String storageLocation,
      List<UCClient.ColumnDef> columns,
      Map<String, String> properties) throws CommitFailedException {
    requireLegacy("finalizeCreate").finalizeCreate(
        tableName, catalogName, schemaName, storageLocation, columns, properties);
  }

  @Override
  public void close() throws IOException {
    try {
      if (legacyClient.isPresent()) {
        legacyClient.get().close();
      }
    } finally {
      LOG.debug("UCDeltaClientImpl closed. DRC loadTable count={}, legacy count={}",
          drcLoadTableCount.get(), legacyLoadTableCount.get());
    }
  }

  // ---------------------------------------------------------------------------
  // Observability / test hooks
  // ---------------------------------------------------------------------------

  /** Minimum viable path-taken counter per clarifications §8; exposed for metrics. */
  public long getDRCLoadTableCount() { return drcLoadTableCount.get(); }
  public long getLegacyLoadTableCount() { return legacyLoadTableCount.get(); }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void logRoutingTransition(boolean drc) {
    final Boolean prev = lastLoggedRoute.getAndSet(drc);
    if (prev == null) {
      LOG.info("UCDeltaClientImpl routing resolved: {}", drc ? "DRC" : "LEGACY");
    } else if (prev.booleanValue() != drc) {
      LOG.info("UCDeltaClientImpl routing transition: {} -> {}",
          prev ? "DRC" : "LEGACY", drc ? "DRC" : "LEGACY");
    }
  }

  private static IOException translateApiException(
      ApiException e, String c, String s, String t) {
    final int code = e.getCode();
    final String body = Optional.ofNullable(e.getResponseBody()).orElse("");
    final String fqn = fqn(c, s, t);
    if (code == 404) {
      return new FileNotFoundException("DRC loadTable: table not found " + fqn + ": " + body);
    }
    // 409/429/500 collapse to IOException in v1. Typed mappings
    // (UpdateRequirementConflictException, ResourceExhaustedException,
    // CommitStateUnknownException) arrive with the commit-path PR where they're
    // actionable to callers; loadTable's 409/500 are advisory.
    return new IOException(String.format(
        "DRC loadTable failed for %s (HTTP %d): %s", fqn, code, body), e);
  }

  /**
   * Derives an {@link AbstractProtocol} from the flat {@code delta.feature.*} property
   * map. Writer features are populated from {@code delta.feature.X=supported} entries;
   * reader features are intentionally left empty (see the class-level note on the DRC
   * spec's protocol removal and the read-side routing via SnapshotManagement).
   *
   * <p>Fails loud if a {@code delta.feature.*} key carries a status other than
   * {@code "supported"}. Silent acceptance would downgrade an advertised feature.
   */
  private static AbstractProtocol deriveProtocolFromProperties(
      java.util.Map<String, String> props) {
    final int minR = Integer.parseInt(props.getOrDefault("delta.minReaderVersion", "1"));
    final int minW = Integer.parseInt(props.getOrDefault("delta.minWriterVersion", "2"));
    final Set<String> writerFeatures = new HashSet<>();
    for (java.util.Map.Entry<String, String> e : props.entrySet()) {
      final String k = e.getKey();
      if (!k.startsWith("delta.feature.")) continue;
      final String status = e.getValue();
      require("supported".equals(status),
          "Unknown delta.feature status '" + status + "' for key '" + k
              + "' (expected 'supported'); refusing to guess classification");
      writerFeatures.add(k.substring("delta.feature.".length()));
    }
    return new AbstractProtocol() {
      @Override public int getMinReaderVersion() { return minR; }
      @Override public int getMinWriterVersion() { return minW; }
      @Override public Set<String> getReaderFeatures() { return Collections.emptySet(); }
      @Override public Set<String> getWriterFeatures() {
        return Collections.unmodifiableSet(writerFeatures);
      }
    };
  }

  private static String fqn(String c, String s, String t) { return c + "." + s + "." + t; }

  private static void require(boolean cond, String msg) {
    if (!cond) throw new IllegalStateException(msg);
  }

  private static void requireNonEmpty(String v, String name) {
    Objects.requireNonNull(v, name);
    if (v.isEmpty()) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
  }

  private static UnsupportedOperationException notYetImplemented(String method) {
    return new UnsupportedOperationException(
        "UCDeltaClient." + method + " is not yet implemented in the DRC impl; "
            + "coming in a later PR.");
  }

  private UCClient requireLegacy(String method) {
    return legacyClient.orElseThrow(() -> new UnsupportedOperationException(
        "UCDeltaClient." + method + " requires a legacy UCClient; construct "
            + "UCDeltaClientImpl with the two-arg constructor to enable it."));
  }
}
