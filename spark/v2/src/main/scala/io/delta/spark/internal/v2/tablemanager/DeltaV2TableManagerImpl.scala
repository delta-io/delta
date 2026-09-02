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
package io.delta.spark.internal.v2.tablemanager

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Process-cached [[DeltaV2TableManager]] composite.
 *
 * Placeholder: inherits default trait stubs. The real implementation (snapshot lifecycle and
 * freshness control) is added in a follow-up layer.
 */
private[tablemanager] class DeltaV2TableManagerImpl(
    val cacheKey: DeltaV2CacheKey,
    val initialCatalogTableOpt: Option[CatalogTable])
    extends DeltaV2TableManager {

  /** The table's data directory (parent of `_delta_log`), fully qualified. */
  def tablePath: Path = cacheKey.path.getParent
}
