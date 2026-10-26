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

package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code CatalogManagedTable} provides functionality for interacting with tables managed by a
 * catalog. It supports:
 *
 * <ul>
 *   <li>loading existing tables from a catalog via the UC Delta-Tables API, and
 *   <li>committing table changes back to the CCv2 catalog.
 * </ul>
 *
 * <p>Snapshot loading and commit coordination go through the UC Delta-Tables API (via {@link
 * UCDeltaTokenBasedRestClient}). Table creation, lookup, and credential vending are still served by
 * the {@link UnityCatalog} catalog over the classic UC table APIs, so the backing UC server must
 * expose both surfaces.
 */
public class CatalogManagedTable extends AbstractKernelTable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManagedTable.class);

  private final URI endpoint;
  private final UnityCatalog.AuthMode authMode;
  private String token;
  private URI oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;

  protected transient UCClient ucClient;
  protected transient UCCatalogManagedClient catalogManagedClient;

  private UCTableIdentifier getUcTableIdentifier() {
    Preconditions.checkArgument(
        catalog instanceof UnityCatalog, "Catalog-managed tables require a UnityCatalog catalog");
    return ((UnityCatalog) catalog).toUcTableIdentifier(tableId);
  }

  public CatalogManagedTable(
      DeltaCatalog catalog, String tableId, Map<String, String> conf, URI endpoint, String token) {
    this(catalog, tableId, conf, null, null, endpoint, token);
  }

  public CatalogManagedTable(
      DeltaCatalog catalog,
      String tableId,
      Map<String, String> conf,
      StructType schema,
      List<String> partitionColumns,
      URI endpoint,
      String token) {
    super(catalog, tableId, conf, schema, partitionColumns);
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkNotNull(token);
    this.endpoint = endpoint;
    this.token = token;
    this.authMode = UnityCatalog.AuthMode.token;
  }

  public CatalogManagedTable(
      DeltaCatalog catalog,
      String tableId,
      Map<String, String> conf,
      StructType schema,
      List<String> partitionColumns,
      URI endpoint,
      URI oauthUri,
      String oauthClientId,
      String oauthClientSecret) {
    super(catalog, tableId, conf, schema, partitionColumns);
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkNotNull(oauthUri);
    Preconditions.checkNotNull(oauthClientId);
    Preconditions.checkNotNull(oauthClientSecret);
    this.endpoint = endpoint;
    this.authMode = UnityCatalog.AuthMode.oauth;
    this.oauthUri = oauthUri;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
  }

  @Override
  public void open() {
    if (ucClient == null) {
      Map<String, String> ucConfig =
          UnityCatalog.buildUcConfig(
              endpoint, authMode, token, oauthUri, oauthClientId, oauthClientSecret);
      ucClient =
          new UCDeltaTokenBasedRestClient(ucConfig, /* hadoopConfSupplier = */ Configuration::new);
    }
    if (catalogManagedClient == null) {
      catalogManagedClient = new UCCatalogManagedClient(ucClient);
    }
    super.open();
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    return withRetry(
        () ->
            catalogManagedClient.loadSnapshot(
                /* engine */ getEngine(),
                /* ucTableId */ tableUUID,
                /* tablePath */ tablePath.toString(),
                /* ucTableIdentifier */ getUcTableIdentifier(),
                /* versionOpt */ Optional.empty(),
                /* timestampOpt */ Optional.empty()));
  }

  @Override
  public Optional<Snapshot> commit(
      CloseableIterable<Row> actions, String appId, long txnId, Map<String, String> properties) {
    // TODO remove this when CatalogManaged client supports update properties.
    //      currently updating properties from outside encounters
    //      "A table's Delta metadata can only be changed from a cluster or warehouse"
    if (!properties.isEmpty()) {
      LOG.warn("properties will be ignored");
    }
    return super.commit(actions, appId, txnId, Map.of());
  }

  @Override
  protected CreateTableTransactionBuilder buildCreateTableTransaction() {
    // Pass the table identifier so the UC committer finalizes (promotes) the staging table when
    // the version 0 commit is written, rather than relying on a separate catalog registration.
    return catalogManagedClient.buildCreateTableTransaction(
        tableUUID, tablePath.toString(), schema, ENGINE_INFO, getUcTableIdentifier());
  }

  @Override
  protected boolean versionExists(Long version) {
    try {
      catalogManagedClient.loadCommitRange(
          /* engine */ getEngine(),
          /* ucTableId */ getTableUUID(),
          /* tablePath */ getTablePath().toString(),
          /* ucTableIdentifier */ getUcTableIdentifier(),
          /* startVersionOpt */ Optional.of(version),
          /* startTimestampOpt */ Optional.empty(),
          /* endVersionOpt */ Optional.empty(),
          /* endTimestampOpt */ Optional.empty());
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
