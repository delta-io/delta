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

import static io.delta.kernel.internal.tablefeatures.TableFeatures.*;
import static io.delta.kernel.unitycatalog.UCCatalogManagedClient.UC_TABLE_ID_KEY;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code CatalogManagedTable} provides functionality for interacting with tables managed by a
 * catalog. It supports:
 *
 * <ul>
 *   <li>loading existing tables from a catalog via the UC Open API, and
 *   <li>committing table changes back to the CCv2 catalog.
 * </ul>
 */
public class CatalogManagedTable extends AbstractKernelTable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManagedTable.class);
  public static final Map<String, String> CATALOG_MANAGED_REQUIRED_FEATURES_CONF =
      Stream.of(
              CATALOG_MANAGED_RW_FEATURE,
              DELETION_VECTORS_RW_FEATURE,
              DOMAIN_METADATA_W_FEATURE,
              IN_COMMIT_TIMESTAMP_W_FEATURE,
              ROW_TRACKING_W_FEATURE,
              CHECKPOINT_V2_RW_FEATURE,
              VACUUM_PROTOCOL_CHECK_RW_FEATURE)
          .map(feature -> String.format("delta.feature.%s", feature.featureName()))
          .collect(Collectors.toMap(key -> key, key -> "supported"));

  private final URI endpoint;
  private final UnityCatalog.AuthMode authMode;
  private String token;
  private URI oauthUri;
  private String oauthClientId;
  private String oauthClientSecret;

  protected transient UCClient ucClient;
  protected transient UCCatalogManagedClient catalogManagedClient;

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
      Map<String, String> tokenProviderConf =
          UnityCatalog.buildTokenProviderConf(
              authMode, token, oauthUri, oauthClientId, oauthClientSecret);
      ucClient =
          new UCTokenBasedRestClient(
              endpoint.toString(),
              TokenProvider.create(tokenProviderConf),
              VersionHelper.appVersions());
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
  protected void createDeltaTable() {
    conf.update(Map.of(UC_TABLE_ID_KEY, tableUUID));
    super.createDeltaTable();
  }

  @Override
  protected CreateTableTransactionBuilder buildCreateTableTransaction() {
    return catalogManagedClient.buildCreateTableTransaction(
        tableUUID, tablePath.toString(), schema, ENGINE_INFO);
  }

  @Override
  protected Map<String, String> extraConf() {
    // Make sure the table supports table features needed by CatalogManaged.
    return CATALOG_MANAGED_REQUIRED_FEATURES_CONF;
  }

  @Override
  protected boolean versionExists(Long version) {
    try {
      catalogManagedClient.loadCommitRange(
          /* engine */ getEngine(),
          /* ucTableId */ getTableUUID(),
          /* tablePath */ getTablePath().toString(),
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
