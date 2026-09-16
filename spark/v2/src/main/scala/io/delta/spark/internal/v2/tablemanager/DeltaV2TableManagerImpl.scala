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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.delta.storage.LogStoreProvider
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.spark.internal.v2.kernel.KernelContext
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Process-cached [[DeltaV2TableManager]] implementation.
 *
 * Reuses a table-scoped snapshot manager backed by the table's [[KernelContext]].
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
    extends DeltaV2TableManager
    with LogStoreProvider
{

  /** The table's data directory, fully qualified. */
  def tablePath: Path = qualifiedTableDataPath

  /** Used to read and write physical log files and checkpoints. */
  override private[v2] lazy val logStore = createLogStore(SparkSession.active)

  override private[v2] lazy val kernelContext = KernelContext(sessionInvariantFsOptions, logStore)

  private val latestCatalogTable =
    new AtomicReference[CatalogTable](initialCatalogTableOpt.orNull)
  private val cachedSnapshotManagerRef = new AtomicReference[CachedSnapshotManager]()

  /**
   * Returns the one cached manager owned by this composite. Construction remains lazy so creating
   * or retiring an unused table manager does not materialize a Kernel context on an eviction
   * thread. Concurrent first callers may create candidates, but only the CAS winner is retained;
   * unused candidates have not loaded snapshots and own no resources.
   */
  private def getOrCreateCachedSnapshotManager(): CachedSnapshotManager = {
    val existing = cachedSnapshotManagerRef.get()
    if (existing != null) {
      return existing
    }
    val candidate = new CachedSnapshotManager(tablePath, kernelContext, latestCatalogTable)
    if (cachedSnapshotManagerRef.compareAndSet(null, candidate)) {
      candidate
    } else {
      cachedSnapshotManagerRef.get()
    }
  }

  override private[v2] def snapshotManager(
      catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager = {
    // Catalog metadata is latest-wins best-effort state. A refresh captures one atomic value and
    // uses it consistently while selecting its path-based or catalog-managed uncached delegate.
    latestCatalogTable.set(catalogTableOpt.orNull)
    getOrCreateCachedSnapshotManager()
  }

  override def retire(): Unit = {
    Option(cachedSnapshotManagerRef.get()).foreach(_.retire())
  }
}
