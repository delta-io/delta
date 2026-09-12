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
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.DeltaUnsupportedOperationException
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.DeltaV2Logging
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.spark.internal.v2.kernel.KernelContext
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory
import org.apache.hadoop.fs.Path
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{
  DeltaHistoryManager => KernelDeltaHistoryManager
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Table-scoped snapshot manager that caches the [[DeltaV2Snapshot]]
 * and serves it to every operation on the same table.
 *
 * [[currentSnapshot]] remains `null` until the first successful load. The first installed snapshot
 * captures [[tableId]]. A subsequent table identity replaces the cached snapshot and is logged,
 * matching DeltaLog's drop-and-recreate behavior. Stale entries refresh through an uncached
 * snapshot manager; dependent modules may layer incremental refresh strategies on this base.
 */
private[tablemanager] class CachedSnapshotManager(
    tablePath: Path,
    kernelContext: KernelContext)
    extends DeltaV2SnapshotManager
    with DeltaV2Logging {

  private case class CachedSnapshot(snapshot: Snapshot, validatedAtMs: Long)

  @volatile private var currentSnapshot: CachedSnapshot = _
  @volatile private var tableId: String = _
  private val _unsafeVolatileCatalogTable = new AtomicReference[CatalogTable]()

  /**
   * The catalog table most recently associated with this manager.
   *
   * This is best-effort state: concurrent callers may replace it at any time, so a caller must
   * capture the returned value once rather than expect successive reads to agree.
   */
  private[tablemanager] def unsafeVolatileCatalogTable: Option[CatalogTable] =
    Option(_unsafeVolatileCatalogTable.get())

  private[tablemanager] def setUnsafeVolatileCatalogTable(table: CatalogTable): Unit =
    _unsafeVolatileCatalogTable.set(table)

  def this(
      tablePath: Path,
      catalogTableOpt: Option[CatalogTable],
      kernelContext: KernelContext) = {
    this(tablePath, kernelContext)
    catalogTableOpt.foreach(setUnsafeVolatileCatalogTable)
  }

  // === DeltaV2SnapshotManager implementation ================================

  override def loadLatestSnapshot(): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.loadLatestSnapshot") {
      acquireLatest()
    }
  }

  override def loadSnapshotAt(version: Long): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.loadSnapshotAt") {
      acquireSnapshotAt(version)
    }
  }

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit = {
    throw new DeltaUnsupportedOperationException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Array("Cached manager does not support getActiveCommitAtTime"))
  }

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    throw new DeltaUnsupportedOperationException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Array("Cached manager does not support checkVersionExists"))
  }

  override def getTableChanges(
      kernelEngine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): KernelCommitRange = {
    throw new DeltaUnsupportedOperationException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Array("Cached manager does not support getTableChanges"))
  }

  // === Snapshot lifecycle ===================================================

  // Eviction drops persisted StateCache data without invalidating escaped snapshots or managers.
  private[tablemanager] def retire(): Unit = synchronized {
    Option(currentSnapshot).foreach(_.snapshot.uncache())
  }

  // === Acquisition ==========================================================

  private def acquireLatest(): Snapshot = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val requiredFreshAfter =
      if (stalenessLimit > 0) math.max(0, now - stalenessLimit) else now
    recordFrameProfile("cachedSnapshotManager.acquireLatest") {
      val existing = currentSnapshot
      if (existing != null && existing.validatedAtMs >= requiredFreshAfter) {
        return existing.snapshot
      }
      rebuildAndInstall()
    }
  }

  private def rebuildAndInstall(): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val tableIdAtLoadStart = tableId
      val refreshed = withUncachedSnapshotManager(_.loadLatestSnapshot())
      val (snapshotToReturn, expiredSnapshotOpt) = synchronized {
        val existing = currentSnapshot
        val refreshedTableId = refreshed.metadata.id
        val stalePreviousLineage = existing != null &&
          tableIdAtLoadStart != null &&
          existing.snapshot.metadata.id != tableIdAtLoadStart &&
          refreshedTableId == tableIdAtLoadStart
        val tableIdentityChanged = !stalePreviousLineage && updateTableIdentity(refreshed)
        if (existing != null &&
            (stalePreviousLineage ||
              (!tableIdentityChanged && existing.snapshot.version >= refreshed.version))) {
          val validatedAt = math.max(validationStartedAt, existing.validatedAtMs)
          currentSnapshot = CachedSnapshot(existing.snapshot, validatedAt)
          val expired = if (refreshed ne existing.snapshot) Some(refreshed) else None
          existing.snapshot -> expired
        } else {
          currentSnapshot = CachedSnapshot(refreshed, validationStartedAt)
          val expired = Option(existing).map(_.snapshot).filterNot(_ eq refreshed)
          refreshed -> expired
        }
      }
      expiredSnapshotOpt.foreach(_.uncache())
      snapshotToReturn
    }
  }

  private def acquireSnapshotAt(version: Long): Snapshot = {
    val existing = currentSnapshot
    // Exact-version time travel reuses the cached facade; latest-table freshness is irrelevant.
    if (existing != null && version == existing.snapshot.version) {
      return existing.snapshot
    }
    // A request beyond the cache must refresh latest to establish a trustworthy upper bound.
    // An older request only refreshes latest when the configured staleness window requires it.
    val upperBound = if (existing == null || version > existing.snapshot.version) {
      rebuildAndInstall()
    } else {
      acquireLatest()
    }
    // The refreshed latest snapshot is retained even when the requested version does not exist.
    if (version > upperBound.version) {
      throw new VersionNotFoundException(version, 0, upperBound.version)
    }
    if (version == upperBound.version) {
      return upperBound
    }
    // Historical snapshots are returned to the caller but never replace the cached latest snapshot.
    val historicalSnapshot = withUncachedSnapshotManager(_.loadSnapshotAt(version))
    updateTableIdentity(historicalSnapshot)
    historicalSnapshot
  }

  // === Uncached loading =====================================================

  private def withUncachedSnapshotManager[T](f: DeltaV2SnapshotManager => T): T =
    f(SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      unsafeVolatileCatalogTable.toJava))

  private def updateTableIdentity(snapshot: Snapshot): Boolean = synchronized {
    val snapshotTableId = snapshot.metadata.id
    val changed = tableId != null && tableId != snapshotTableId
    if (tableId == null) {
      tableId = snapshotTableId
    } else if (changed) {
      logWarning(
        s"Table identity changed while refreshing snapshot: previous=$tableId, " +
          s"current=$snapshotTableId")
      tableId = snapshotTableId
    }
    changed
  }

}
