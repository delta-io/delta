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

package io.delta.flink.table;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.*;
import static io.delta.kernel.unitycatalog.UCCatalogManagedClient.UC_TABLE_ID_KEY;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedCommitter;
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
 * A {@code CCv2Table} provides functionality for interacting with tables managed by a CCv2 catalog.
 * It supports:
 *
 * <ul>
 *   <li>loading existing tables from a catalog via the UC Open API, and
 *   <li>committing table changes back to the CCv2 catalog.
 * </ul>
 *
 * <p><strong>Note:</strong> {@code CCv2Table} does not support creating new tables. Instances must
 * reference an existing catalog table; attempts to create or initialize new tables through this
 * interface are not supported.
 */
public class CatalogManagedTable extends AbstractKernelTable {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManagedTable.class);
  public static final Map<String, String> CCV2_FEATURES_CONF =
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
  private final String token;

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
  }

  protected transient UCClient ucClient;
  protected transient UCCatalogManagedClient ccv2Client;

  @Override
  public void open() {
    if (ucClient == null) {
      ucClient =
          new UCTokenBasedRestClient(
              endpoint.toString(),
              TokenProvider.create(Map.of("type", "static", "token", token)),
              Map.of());
    }
    if (ccv2Client == null) {
      ccv2Client = new UCCatalogManagedClient(ucClient);
    }
    super.open();
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    return withRetry(
        () ->
            ccv2Client.loadSnapshot(
                getEngine(), tableUUID, tablePath.toString(), Optional.empty(), Optional.empty()));
  }

  @Override
  public Optional<Snapshot> commit(
      CloseableIterable<Row> actions, String appId, long txnId, Map<String, String> properties) {
    // TODO remove this when CCv2 client supports update properties.
    //      currently updating properties from outside encounters
    //      "A table's Delta metadata can only be changed from a cluster or warehouse"
    return super.commit(actions, appId, txnId, Map.of());
  }

  @Override
  protected void createDeltaTable() {
    conf.update(Map.of(UC_TABLE_ID_KEY, tableUUID));
    super.createDeltaTable();
  }

  @Override
  protected CreateTableTransactionBuilder buildCreateTableTransaction() {
    UCCatalogManagedCommitter committer =
        new UCCatalogManagedCommitter(ucClient, tableUUID, tablePath.toString());
    return TableManager.buildCreateTableTransaction(tablePath.toString(), schema, ENGINE_INFO)
        .withCommitter(committer);
  }

  @Override
  protected Map<String, String> extraConf() {
    // Make sure the table supports CCv2.
    return CCV2_FEATURES_CONF;
  }
}
