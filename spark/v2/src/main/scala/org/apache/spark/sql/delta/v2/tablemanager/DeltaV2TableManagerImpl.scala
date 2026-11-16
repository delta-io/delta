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
package org.apache.spark.sql.delta.v2.tablemanager

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.delta.{DeltaSingleCommit, Snapshot}
import org.apache.spark.sql.delta.storage.{ClosableIterator, LogStoreProvider}
import org.apache.spark.sql.delta.storage.ClosableIterator.ClosableWrapper
import org.apache.spark.sql.delta.v2.DeltaV2Logging
import org.apache.spark.sql.delta.v2.interop.{
  DeltaV2QueryContext,
  DeltaV2SingleCommit,
  DeltaV2SnapshotManager
}
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam
import org.apache.spark.sql.delta.v2.kernel.KernelContext
import org.apache.hadoop.fs.Path
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.exceptions.{
  CommitRangeNotFoundException,
  StartVersionNotFoundException
}
import io.delta.kernel.internal.{DeltaHistoryManager => KernelDeltaHistoryManager}

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
    with DeltaV2Logging
    with LogStoreProvider
{

  /** The table's data directory, fully qualified. */
  def tablePath: Path = qualifiedTableDataPath

  /** Used to read and write physical log files and checkpoints. */
  override private[v2] val logStore = createLogStore(SparkSession.active)

  override private[v2] val kernelContext = KernelContext(sessionInvariantFsOptions, logStore)

  private val latestCatalogTable =
    new AtomicReference[CatalogTable](initialCatalogTableOpt.orNull)
  private val cachedSnapshotManager =
    new CachedSnapshotManager(tablePath, kernelContext, latestCatalogTable)
  private val maxStartVersionAttempts = 3

  override private[v2] def snapshotManager(
      catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager = {
    // Catalog metadata is latest-wins best-effort state. A refresh captures one atomic value and
    // uses it consistently while selecting its path-based or catalog-managed uncached delegate.
    latestCatalogTable.set(catalogTableOpt.orNull)
    cachedSnapshotManager
  }

  override private[v2] def getSingleCommitIterator(
      startVersion: Long,
      endVersion: Option[Long],
      queryContext: DeltaV2QueryContext,
      failOnDataLoss: Boolean)
      : ClosableIterator[DeltaSingleCommit] = {
    val kernelEngine = kernelContext.getDefaultEngine()
    val hadoopConf = kernelContext.materializeHadoopConf()
    var commitRange: KernelCommitRange = null
    var earliestVersionToFetch = startVersion
    var attempt = 1
    while (commitRange == null) {
      try {
        commitRange = cachedSnapshotManager.getTableChanges(
          kernelEngine,
          GetTableChangesParam(earliestVersionToFetch, endVersion, false, failOnDataLoss),
          queryContext)
      } catch {
        case e: StartVersionNotFoundException =>
          if (failOnDataLoss || !e.getEarliestAvailableVersion.isPresent ||
              attempt >= maxStartVersionAttempts) {
            throw e
          }
          earliestVersionToFetch = e.getEarliestAvailableVersion.get()
          logWarning(
            s"Start version commit $startVersion no longer exists for table $tablePath " +
              s"(attempt $attempt/$maxStartVersionAttempts). Skipping to earliest available " +
              s"version $earliestVersionToFetch because failOnDataLoss is false.")
        case _: CommitRangeNotFoundException =>
          return new ClosableWrapper[DeltaSingleCommit](Iterator.empty).toClosable
      }
      attempt += 1
    }
    val kernelCommitFiles = commitRange.getCommitFiles()
    new ClosableIterator[DeltaSingleCommit] {
      override def hasNext: Boolean = kernelCommitFiles.hasNext
      override def next(): DeltaSingleCommit =
        DeltaV2SingleCommit(kernelCommitFiles.next(), logStore, hadoopConf)
      override def close(): Unit = kernelCommitFiles.close()
    }
  }

  override def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot =
    cachedSnapshotManager.loadLatestSnapshot(queryContext)

  override def loadSnapshotAt(version: Long, queryContext: DeltaV2QueryContext): Snapshot =
    cachedSnapshotManager.loadSnapshotAt(version, queryContext)

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean,
      queryContext: DeltaV2QueryContext): KernelDeltaHistoryManager.Commit =
    cachedSnapshotManager.getActiveCommitAtTime(
      timestampMillis,
      canReturnLastCommit,
      mustBeRecreatable,
      canReturnEarliestCommit,
      queryContext)

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean,
      queryContext: DeltaV2QueryContext): Unit =
    cachedSnapshotManager.checkVersionExists(
      version, mustBeRecreatable, allowOutOfRange, queryContext)

  override def getTableChanges(
      kernelEngine: KernelEngine,
      params: GetTableChangesParam,
      queryContext: DeltaV2QueryContext): KernelCommitRange =
    cachedSnapshotManager.getTableChanges(kernelEngine, params, queryContext)

  override def retire(): Unit = cachedSnapshotManager.retire()
}
