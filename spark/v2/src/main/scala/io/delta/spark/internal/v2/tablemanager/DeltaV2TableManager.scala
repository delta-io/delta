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

import java.util.Optional

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.storage.LogStore
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.{DeltaV2QueryContext, DeltaV2SnapshotManager}
import io.delta.spark.internal.v2.kernel.KernelContext
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{DeltaHistoryManager => KernelDeltaHistoryManager}

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Contract for a Delta table manager used by the DSv2 connector.
 */
private[v2] trait DeltaV2TableManager {

  /** Returns the table-scoped Kernel context. */
  private[v2] def kernelContext: KernelContext

  /** Returns the table-scoped log store. */
  private[v2] def logStore: LogStore

  /** Returns a snapshot manager using the caller's current catalog metadata. */
  private[v2] def snapshotManager(catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager

  /** Loads the latest snapshot using inputs scoped to the current query. */
  def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot

  /** Loads a versioned snapshot using inputs scoped to the current query. */
  def loadSnapshotAt(version: Long, queryContext: DeltaV2QueryContext): Snapshot

  /** Finds the active commit using inputs scoped to the current query. */
  def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean,
      queryContext: DeltaV2QueryContext): KernelDeltaHistoryManager.Commit

  /** Checks version availability using inputs scoped to the current query. */
  @throws[VersionNotFoundException]
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean,
      queryContext: DeltaV2QueryContext): Unit

  /** Gets table changes using inputs scoped to the current query. */
  def getTableChanges(
      kernelEngine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long],
      queryContext: DeltaV2QueryContext): KernelCommitRange

  /** Retires this manager and releases any resources it owns. */
  def retire(): Unit = {}
}
