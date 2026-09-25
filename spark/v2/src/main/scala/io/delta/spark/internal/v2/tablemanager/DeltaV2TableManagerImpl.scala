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

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.storage.LogStoreProvider
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.spark.internal.v2.kernel.KernelContext
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory
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
 */
private[tablemanager] class DeltaV2TableManagerImpl(
    val qualifiedTableDataPath: Path,
    val sessionInvariantFsOptions: Map[String, String])
    extends DeltaV2TableManager
    with LogStoreProvider {

  /** The table's data directory, fully qualified. */
  def tablePath: Path = qualifiedTableDataPath

  /** Used to read and write physical log files and checkpoints. */
  override private[v2] val logStore = createLogStore(SparkSession.active)

  override private[v2] val kernelContext = KernelContext(sessionInvariantFsOptions, logStore)

  private val cachedSnapshotManager = new CachedSnapshotManager(tablePath, kernelContext)

  override private[v2] def snapshotManager(
      catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager = {
    // Until the remaining consumers supply query context, preserve the pre-cache behavior and
    // construct one uncached manager from this request's catalog metadata. The shared cached
    // manager is activated only after every snapshot consumer carries its query context.
    SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      catalogTableOpt.toJava)
  }

  override private[v2] def queryContextSnapshotManager: DeltaV2SnapshotManager =
    cachedSnapshotManager

  override def retire(): Unit = cachedSnapshotManager.retire()
}
