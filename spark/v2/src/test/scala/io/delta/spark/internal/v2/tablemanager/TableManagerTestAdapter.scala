/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.spark.internal.v2.tablemanager

import java.util.Optional

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.v2.interop.{DeltaV2QueryContext, DeltaV2SnapshotManager}
import io.delta.spark.internal.v2.kernel.KernelContext
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{DeltaHistoryManager => KernelDeltaHistoryManager}

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/** Preserves raw snapshot-manager fixtures while testing callers of the table-manager API. */
private[v2] object TableManagerTestAdapter {
  def apply(delegate: DeltaV2SnapshotManager): DeltaV2TableManager = new DeltaV2TableManager {
    override private[v2] def kernelContext: KernelContext =
      throw new UnsupportedOperationException("Test adapter does not own a KernelContext")

    override private[v2] def logStore: LogStore =
      throw new UnsupportedOperationException("Test adapter does not own a LogStore")

    override private[v2] def snapshotManager(
        catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager = delegate

    override def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot =
      delegate.loadLatestSnapshot(queryContext)

    override def loadSnapshotAt(version: Long, queryContext: DeltaV2QueryContext): Snapshot =
      delegate.loadSnapshotAt(version, queryContext)

    override def getActiveCommitAtTime(
        timestampMillis: Long,
        canReturnLastCommit: Boolean,
        mustBeRecreatable: Boolean,
        canReturnEarliestCommit: Boolean,
        queryContext: DeltaV2QueryContext): KernelDeltaHistoryManager.Commit =
      delegate.getActiveCommitAtTime(
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
      delegate.checkVersionExists(version, mustBeRecreatable, allowOutOfRange, queryContext)

    override def getTableChanges(
        kernelEngine: KernelEngine,
        startVersion: Long,
        endVersion: Optional[java.lang.Long],
        queryContext: DeltaV2QueryContext): KernelCommitRange =
      delegate.getTableChanges(kernelEngine, startVersion, endVersion, queryContext)
  }
}
