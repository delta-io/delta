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

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Extended UC client interface for Delta table operations. Adds DRC-aware {@link #loadTable},
 * {@link #createTable}, and typed {@link #commit} overloads on top of the legacy
 * {@link UCClient} interface. Implementations choose DRC or legacy internally based on whether
 * the UC-side {@code DeltaRestClientProvider} exposes a {@code TablesApi} -- callers never
 * branch on the flag.
 *
 * <p>Extends {@code UCClient} so that the existing {@code UCCommitCoordinatorClient} can hold
 * a single field ({@code UCClient ucClient}) that is polymorphically either a legacy impl or a
 * DRC-aware one, without a refactor of the commit-coordinator constructor. The richer DRC
 * methods added here are invoked by callers that know they have a {@code UCDeltaClient}
 * (wired via {@code UCDeltaClientProvider}); the inherited {@code UCClient} methods continue
 * to service the legacy commit path unchanged.
 *
 * <p>This interface is the full skeleton -- every RPC method any caller will reach for the DRC
 * integration lives here. Only the read path ({@link #loadTable}) has a production
 * implementation in this stack; {@link #createTable} and the typed {@link #commit} overloads
 * throw {@link UnsupportedOperationException} from the impl until the PR that wires each one
 * lands. Freezing the shape in one reviewable PR lets reviewers evaluate the architecture
 * before any commit-path logic lands.
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Reports whether this client would currently route the next DRC-specific RPC through DRC
   * endpoints. Resolved against the underlying {@code DeltaRestClientProvider.getDeltaTablesApi()}
   * on every call, so mid-session flag flips take effect immediately.
   *
   * <p><b>Allowed uses:</b> logging, metric tagging, tests, and softening UC-managed kill
   * switches on the DRC path (clarifications §2). <b>Not allowed:</b> picking between DRC and
   * legacy code paths at the caller layer -- the whole point of this interface is that the
   * routing is internal.
   */
  boolean isDRCEnabled();

  /**
   * Casts or wraps a {@link UCClient} into a {@link UCDeltaClient}. Used by
   * {@code UCCommitCoordinatorBuilder} when it receives a plain {@code UCClient} from the
   * legacy factory but the rest of the stack wants the richer contract.
   *
   * <p>Callers that have already obtained a {@code UCDeltaClient} (via
   * {@code UCDeltaClientProvider}) do not need to go through this helper.
   *
   * <p>The wrapping path for a legacy {@code UCTokenBasedRestClient} is implemented in the
   * PR that ships {@code UCTokenBasedDeltaRestClient} -- throwing
   * {@link IllegalArgumentException} for any other {@code UCClient} subclass makes an
   * accidental-legacy-path silently-succeeds scenario surface as a loud error.
   */
  static UCDeltaClient fromLegacyClient(UCClient ucClient) {
    if (ucClient instanceof UCDeltaClient) {
      return (UCDeltaClient) ucClient;
    }
    throw new IllegalArgumentException(
        "UCClient is not a UCDeltaClient: "
            + (ucClient == null ? "null" : ucClient.getClass().getName())
            + ". The DRC-aware wrapping path for UCTokenBasedRestClient lands alongside "
            + "UCTokenBasedDeltaRestClient in a follow-up PR.");
  }

  /**
   * Response from {@link #loadTable(String, String, String, String, URI, Optional, Optional)}.
   * Carries the full table state -- commits + etag + metadata + credentials -- so that a
   * single DRC RPC can hydrate a caller that would otherwise have needed
   * {@code getTable} + {@code getCommits} + {@code getCredentials} separately.
   *
   * <p>On the DRC path all fields are populated. On the legacy path (which this interface can
   * cover via {@link #fromLegacyClient}), {@code protocol}, {@code metadata}, and
   * {@code credentials} are {@code null} because the legacy APIs don't return them at load
   * time -- the caller reads them from the Delta log instead. The nullability mix is
   * intentional per Yili's POC (#6575); documented at the accessors.
   */
  final class LoadTableResponse {
    private final GetCommitsResponse commitsResponse;
    private final String etag;
    private final String location;
    private final String tableId;
    private final String tableType;
    private final Map<String, String> properties;
    private final AbstractProtocol protocol;
    private final AbstractMetadata metadata;
    private final Map<String, String> credentials;

    /** Full DRC-path constructor. */
    public LoadTableResponse(
        GetCommitsResponse commitsResponse, String etag,
        String location, String tableId, String tableType,
        Map<String, String> properties,
        AbstractProtocol protocol, AbstractMetadata metadata,
        Map<String, String> credentials) {
      this.commitsResponse = commitsResponse;
      this.etag = etag;
      this.location = location;
      this.tableId = tableId;
      this.tableType = tableType;
      this.properties = properties;
      this.protocol = protocol;
      this.metadata = metadata;
      this.credentials = credentials;
    }

    /** Legacy / commit-path constructor -- no protocol, metadata, or credentials. */
    public LoadTableResponse(
        GetCommitsResponse commitsResponse, String etag,
        String location, String tableId, String tableType,
        Map<String, String> properties) {
      this(commitsResponse, etag, location, tableId, tableType,
          properties, null, null, null);
    }

    public GetCommitsResponse getCommitsResponse() { return commitsResponse; }
    public String getEtag() { return etag; }
    public String getLocation() { return location; }
    public String getTableId() { return tableId; }
    public String getTableType() { return tableType; }
    public Map<String, String> getProperties() { return properties; }
    /** DRC protocol, or {@code null} if loaded via commit path. */
    public AbstractProtocol getProtocol() { return protocol; }
    /** DRC metadata, or {@code null} if loaded via commit path. */
    public AbstractMetadata getMetadata() { return metadata; }
    /**
     * Temporary storage credentials as Hadoop-style properties (e.g. {@code fs.s3a.access.key},
     * {@code fs.s3a.secret.key}). Empty map if credential vending is unavailable or not needed.
     */
    public Map<String, String> getCredentials() {
      return credentials != null ? credentials : Collections.emptyMap();
    }
  }

  /**
   * Loads a table's full state from UC. Returns commits + etag + metadata + credentials in one
   * call on the DRC path; on the legacy path, combines {@code getTable} and {@code getCommits}.
   * Callers use this instead of {@link UCClient#getCommits(String, URI, Optional, Optional)}
   * whenever they need the etag -- the DRC commit path asserts etag where present.
   */
  LoadTableResponse loadTable(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Long> startVersion, Optional<Long> endVersion)
      throws IOException;

  /**
   * Creates a table in UC with structured protocol, schema, and domain metadata. Replaces the
   * legacy {@link UCClient#finalizeCreate} path (which only supports the flat
   * {@link UCClient.ColumnDef} schema representation) for DRC-aware callers. Legacy callers
   * continue to use {@code finalizeCreate} unchanged via inheritance.
   */
  void createTable(
      String catalog, String schema, String table, String location,
      AbstractMetadata metadata, AbstractProtocol protocol,
      boolean isManaged, String dataSourceFormat,
      List<? extends AbstractDomainMetadata> domainMetadata)
      throws CommitFailedException;

  /**
   * Commits via the Spark V1 {@link TableDescriptor} path. Same contract as
   * {@link UCClient#commit(String, URI, Optional, Optional, boolean, Optional, Optional, Optional)}
   * but carries the extra DRC-required context: the old metadata and old protocol observed at
   * load time (for intent-based diffing), and the optional etag (for the DRC
   * {@code assert-etag} requirement per clarifications §3).
   */
  void commit(
      TableDescriptor tableDesc, Path logPath,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException;

  /**
   * Commits via the Kernel V2 path: explicit catalog / schema / table identifiers rather than
   * a {@link TableDescriptor}. Same contract as the Spark V1 overload otherwise.
   */
  void commit(
      String catalog, String schema, String table,
      String ucTableId, URI tableUri,
      Optional<Commit> commit, Optional<Long> lastKnownBackfilledVersion,
      boolean disown,
      Optional<AbstractMetadata> oldMetadata, Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> oldProtocol, Optional<AbstractProtocol> newProtocol,
      Optional<UniformMetadata> uniform,
      Optional<String> etag)
      throws IOException, CommitFailedException;
}
