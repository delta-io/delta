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
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.DeltaV2Logging
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
    kernelContext: KernelContext,
    latestCatalogTable: AtomicReference[CatalogTable])
    extends DeltaV2SnapshotManager
    with DeltaV2Logging {

  private case class CachedSnapshot(snapshot: Snapshot, validatedAtMs: Long)

  private val snapshotLock = new ReentrantLock()
  @volatile private var currentSnapshot: CachedSnapshot = _

  // === DeltaV2SnapshotManager implementation ================================

  override def loadLatestSnapshot(): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.loadLatestSnapshot") {
      loadLatestSnapshotInternal()
    }
  }

  override def loadSnapshotAt(version: Long): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.loadSnapshotAt") {
      loadSnapshotAtInternal(version)
    }
  }

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit =
    withUncachedSnapshotManager(latestCatalogTable.get())(
      _.getActiveCommitAtTime(
        timestampMillis,
        canReturnLastCommit,
        mustBeRecreatable,
        canReturnEarliestCommit))

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit =
    withUncachedSnapshotManager(latestCatalogTable.get())(
      _.checkVersionExists(version, mustBeRecreatable, allowOutOfRange))

  override def getTableChanges(
      kernelEngine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): KernelCommitRange =
    withUncachedSnapshotManager(latestCatalogTable.get())(
      _.getTableChanges(kernelEngine, startVersion, endVersion))

  // === Snapshot lifecycle ===================================================

  // Eviction drops persisted StateCache data without invalidating escaped snapshots or managers.
  private[tablemanager] def retire(): Unit = withSnapshotLockInterruptibly {
    retireSnapshotInternal(currentSnapshot)
  }

  // === Acquisition ==========================================================

  private def loadLatestSnapshotInternal(): Snapshot = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val requiredFreshAfter =
      if (stalenessLimit > 0) math.max(0, now - stalenessLimit) else now
    recordFrameProfile("cachedSnapshotManager.loadLatestSnapshotInternal") {
      val existing = currentSnapshot
      if (isFresh(existing, requiredFreshAfter)) {
        return existing.snapshot
      }
      withSnapshotLockInterruptibly {
        val current = currentSnapshot
        if (isFresh(current, requiredFreshAfter)) {
          current.snapshot
        } else {
          rebuildAndInstallInternal()
        }
      }
    }
  }

  private def rebuildAndInstallInternal(): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val existing = currentSnapshot
      val refreshed = withUncachedSnapshotManager(latestCatalogTable.get())(
        _.loadLatestSnapshot())
      val sameTable = existing != null && existing.snapshot.metadata.id == refreshed.metadata.id
      if (sameTable && existing.snapshot.version >= refreshed.version) {
        val validatedAt = math.max(validationStartedAt, existing.validatedAtMs)
        currentSnapshot = CachedSnapshot(existing.snapshot, validatedAt)
        retireSnapshotInternal(refreshed, existing.snapshot)
        existing.snapshot
      } else {
        if (existing != null && !sameTable) {
          logWarning(
            s"Table identity changed while refreshing snapshot: " +
              s"previous=${existing.snapshot.metadata.id}, current=${refreshed.metadata.id}")
        }
        currentSnapshot = CachedSnapshot(refreshed, validationStartedAt)
        retireSnapshotInternal(existing)
        refreshed
      }
    }
  }

  private def loadSnapshotAtInternal(version: Long): Snapshot = {
    val existing = currentSnapshot
    // Exact-version time travel reuses the cached facade; latest-table freshness is irrelevant.
    if (existing != null && version == existing.snapshot.version) {
      return existing.snapshot
    }
    // A request beyond the cache must refresh latest to establish a trustworthy upper bound.
    // An older request only refreshes latest when the configured staleness window requires it.
    val upperBound = if (existing == null || version > existing.snapshot.version) {
      withSnapshotLockInterruptibly {
        val current = currentSnapshot
        if (current != null && current.snapshot.version >= version) {
          current.snapshot
        } else {
          rebuildAndInstallInternal()
        }
      }
    } else {
      loadLatestSnapshotInternal()
    }
    // If latest still trails the requested version, attempt the exact load before rejecting it.
    if (version > upperBound.version) {
      return loadAndInstallSnapshotAtInternal(version)
    }
    if (version == upperBound.version) {
      return upperBound
    }
    // Historical snapshots are returned to the caller but never replace the cached latest snapshot.
    val historicalSnapshot = withUncachedSnapshotManager(latestCatalogTable.get())(
      _.loadSnapshotAt(version))
    historicalSnapshot
  }

  private def loadAndInstallSnapshotAtInternal(version: Long): Snapshot =
    withSnapshotLockInterruptibly {
      val existing = currentSnapshot
      if (existing != null && existing.snapshot.version == version) {
        return existing.snapshot
      }
      val loaded = withUncachedSnapshotManager(latestCatalogTable.get()) { manager =>
        try {
          manager.loadSnapshotAt(version)
        } catch {
          case NonFatal(loadFailure) =>
            manager.checkVersionExists(
              version,
              /* mustBeRecreatable= */ true,
              /* allowOutOfRange= */ false)
            throw loadFailure
        }
      }
      val sameTable = existing != null && existing.snapshot.metadata.id == loaded.metadata.id
      if (sameTable && existing.snapshot.version > loaded.version) {
        loaded
      } else {
        if (existing != null && !sameTable) {
          logWarning(
            s"Table identity changed while loading snapshot at version $version: " +
              s"previous=${existing.snapshot.metadata.id}, current=${loaded.metadata.id}")
        }
        // An exact-version load proves this snapshot exists, but not that it is the latest.
        currentSnapshot = CachedSnapshot(loaded, validatedAtMs = -1L)
        retireSnapshotInternal(existing)
        loaded
      }
    }

  // === Uncached loading =====================================================

  private def withUncachedSnapshotManager[T](
      catalogTable: CatalogTable)(
      f: DeltaV2SnapshotManager => T): T = {
    f(SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      Option(catalogTable).toJava))
  }

  private def isFresh(snapshot: CachedSnapshot, requiredFreshAfter: Long): Boolean =
    snapshot != null && snapshot.validatedAtMs >= requiredFreshAfter

  private def withSnapshotLockInterruptibly[T](body: => T): T = {
    snapshotLock.lockInterruptibly()
    try {
      body
    } finally {
      snapshotLock.unlock()
    }
  }

  private def retireSnapshotInternal(
      snapshotToRetire: Snapshot,
      retainedSnapshot: Snapshot = null): Unit = {
    if (snapshotToRetire != null && (snapshotToRetire ne retainedSnapshot)) {
      snapshotToRetire.uncache()
    }
  }

  private def retireSnapshotInternal(cachedSnapshot: CachedSnapshot): Unit = {
    if (cachedSnapshot != null) {
      retireSnapshotInternal(cachedSnapshot.snapshot)
    }
  }

}
