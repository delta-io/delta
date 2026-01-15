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

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
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
public class CCv2Table extends AbstractKernelTable {

  private static Logger LOG = LoggerFactory.getLogger(CCv2Table.class);

  private final URI endpoint;
  private final String token;

  public CCv2Table(
      DeltaCatalog catalog, String tableId, Map<String, String> conf, URI endpoint, String token) {
    super(catalog, tableId, conf);
    Preconditions.checkNotNull(endpoint);
    Preconditions.checkNotNull(token);
    this.endpoint = endpoint;
    this.token = token;
  }

  protected transient UCCatalogManagedClient ccv2Client;

  @Override
  public void open() {
    if (ccv2Client == null) {
      UCClient storageClient =
          new UCTokenBasedRestClient(
              endpoint.toString(), TokenProvider.create(Map.of("type", "static", "token", token)));
      ccv2Client = new UCCatalogManagedClient(storageClient);
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
}
