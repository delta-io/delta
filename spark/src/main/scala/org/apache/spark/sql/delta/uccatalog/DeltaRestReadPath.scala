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

package org.apache.spark.sql.delta.uccatalog

import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCDeltaRestClient}
import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.delta.DeltaRestClientProvider
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, V1Table}

/**
 * Composes the DRC read path from its pieces: `DeltaRestProviderLookup` (walks the catalog
 * chain) and `DeltaRestTableLoader` (builds `V1Table` from the DRC response). Exists as a
 * standalone object so `AbstractDeltaCatalog.loadTable` does not have to choreograph the logic
 * inline, and so the "flag off -> don't construct an HTTP client" invariant (the concern that
 * a future regression swaps `flatMap` for `map` or adds an eager side effect before the
 * `isPresent` check) can be asserted in a unit test with a client factory that throws on
 * invocation.
 */
private[delta] object DeltaRestReadPath {

  /** Default factory: real HTTP client bound to the shared `ApiClient`. */
  private[uccatalog] val DefaultClientFactory: ApiClient => UCDeltaClient =
    apiClient => new UCDeltaRestClient(apiClient)

  /**
   * Attempts to load a Delta table via the DRC. Returns `Some(v1Table)` when:
   * - the catalog or its delegate chain contains a `DeltaRestClientProvider`, AND
   * - that provider's `getDeltaTablesApi()` is present (DRC flag on).
   *
   * Returns `None` otherwise. In the `None` cases the `clientFactory` is NOT invoked -- callers
   * can rely on this to avoid unwanted HTTP side effects on the flag-off path.
   */
  def tryLoad(
      catalog: CatalogPlugin,
      catalogName: String,
      ident: Identifier,
      clientFactory: ApiClient => UCDeltaClient = DefaultClientFactory): Option[V1Table] = {
    DeltaRestProviderLookup.findProvider(catalog).flatMap { provider =>
      val apiOpt = provider.getDeltaTablesApi
      if (apiOpt.isPresent) {
        val client = clientFactory(provider.getApiClient)
        Some(DeltaRestTableLoader.load(catalogName, ident, client))
      } else {
        None
      }
    }
  }
}
