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

import io.unitycatalog.client.delta.DeltaRestClientProvider
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, DelegatingCatalogExtension}

/**
 * Pure helper that walks a `CatalogPlugin`'s delegate chain and returns the first catalog that
 * implements `DeltaRestClientProvider`. Extracted from `AbstractDeltaCatalog` so the chain-walk
 * behaviour can be unit-tested in isolation, without needing a live Spark session or a real
 * Delta catalog.
 *
 * The chain can take several shapes depending on how the catalog plugin is composed:
 * - `this -> DeltaCatalog -> UCProxy` when Delta is on the classpath and interposed between
 *   `UCSingleCatalog` and `UCProxy` (production OSS);
 * - `this -> UCProxy` when Delta is absent and the UC plugin wraps UCProxy directly;
 * - `UCSingleCatalog` itself when a consumer holds the outermost catalog (UCSingleCatalog
 *   also implements DeltaRestClientProvider per UC PR 1509);
 * - arbitrary wrappers interposed by Databricks-side or third-party adapters.
 *
 * The walk descends through `DelegatingCatalogExtension.delegate` (reflection) and stops when
 * it (a) finds a provider, (b) hits a non-delegating, non-provider catalog, or (c) exceeds a
 * defensive depth limit to guard against accidental cycles.
 */
private[delta] object DeltaRestProviderLookup {

  /** Defensive cap on chain depth; in practice chains are 0-3 deep. */
  private[uccatalog] val MAX_CHAIN_DEPTH: Int = 16

  /**
   * Returns the first `DeltaRestClientProvider` reachable by walking `catalog`'s delegate chain,
   * or `None` if the chain does not contain one.
   */
  def findProvider(catalog: CatalogPlugin): Option[DeltaRestClientProvider] = {
    if (catalog == null) return None
    var current: Any = catalog
    var depth = 0
    while (current != null && depth < MAX_CHAIN_DEPTH) {
      current match {
        case p: DeltaRestClientProvider => return Some(p)
        case d: DelegatingCatalogExtension => current = readDelegate(d)
        case _ => current = null
      }
      depth += 1
    }
    None
  }

  private def readDelegate(d: DelegatingCatalogExtension): Any = {
    val field = classOf[DelegatingCatalogExtension].getDeclaredField("delegate")
    field.setAccessible(true)
    field.get(d)
  }
}
