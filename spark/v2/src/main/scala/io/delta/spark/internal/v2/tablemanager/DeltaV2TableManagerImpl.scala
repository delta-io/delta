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

import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Process-cached [[DeltaV2TableManager]] composite.
 *
 * Placeholder: inherits default trait stubs. The real implementation (snapshot lifecycle and
 * freshness control) is added in a follow-up layer.
 *
 * @param qualifiedTableDataPath the fully-qualified table data directory (parent of `_delta_log`).
 * @param sessionInvariantFsOptions filesystem-prefixed credential options (`fs.*`, `dfs.*`) that
 *   were used to resolve the table path. Retained for downstream engine construction.
 * @param initialCatalogTableOpt the catalog table supplied by the first caller that loaded this
 *   entry, if any.
 */
private[tablemanager] class DeltaV2TableManagerImpl(
    val qualifiedTableDataPath: Path,
    val sessionInvariantFsOptions: Map[String, String],
    val initialCatalogTableOpt: Option[CatalogTable])
    extends DeltaV2TableManager {

  /** The table's data directory, fully qualified. */
  def tablePath: Path = qualifiedTableDataPath

  // Placeholder until snapshot lifecycle is implemented.
  override def snapshotManager(): DeltaV2SnapshotManager =
    throw new UnsupportedOperationException("snapshotManager not yet implemented")
}
