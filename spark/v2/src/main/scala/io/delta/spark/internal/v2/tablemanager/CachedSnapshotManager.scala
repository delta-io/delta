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
 * records its table identity. A subsequent table identity replaces the cached snapshot and is
 * logged, matching DeltaLog's drop-and-recreate behavior. Stale entries refresh through an uncached
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
    val requiredFreshAfter = latestSnapshotFreshnessThreshold()
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
      val refreshed = CachedSnapshot(
        withUncachedSnapshotManager(latestCatalogTable.get())(_.loadLatestSnapshot()),
        validationStartedAt)
      val sameTable =
        existing != null && existing.snapshot.metadata.id == refreshed.snapshot.metadata.id
      if (sameTable && existing.snapshot.version >= refreshed.snapshot.version) {
        val validatedAt = math.max(validationStartedAt, existing.validatedAtMs)
        currentSnapshot = CachedSnapshot(existing.snapshot, validatedAt)
        retireSnapshotInternal(refreshed)
        existing.snapshot
      } else {
        if (existing != null && !sameTable) {
          logWarning(
            s"Table identity changed while refreshing snapshot: " +
              s"previous=${existing.snapshot.metadata.id}, " +
              s"current=${refreshed.snapshot.metadata.id}")
        }
        currentSnapshot = refreshed
        retireSnapshotInternal(existing)
        refreshed.snapshot
      }
    }
  }

  private def loadSnapshotAtInternal(version: Long): Snapshot = {
    val existing = currentSnapshot
    // Exact-version time travel reuses the cached facade; latest-table freshness is irrelevant.
    if (existing != null && version == existing.snapshot.version) {
      return existing.snapshot
    }
    // Linearize upper-bound discovery and any exact fallback under one lock acquisition.
    val upperBound = withSnapshotLockInterruptibly {
      val current = currentSnapshot
      if (current != null && version == current.snapshot.version) {
        return current.snapshot
      }
      val refreshed =
        if (isFresh(current, latestSnapshotFreshnessThreshold(), Some(version))) current.snapshot
        else rebuildAndInstallInternal()
      // If latest still trails the requested version, attempt the exact load before rejecting it.
      if (version > refreshed.version) {
        val loaded = CachedSnapshot(
          withUncachedSnapshotManager(latestCatalogTable.get())(_.loadSnapshotAt(version)),
          validatedAtMs = -1L)
        val previous = currentSnapshot
        if (previous != null && previous.snapshot.metadata.id != loaded.snapshot.metadata.id) {
          logWarning(
            s"Table identity changed while loading snapshot at version $version: " +
              s"previous=${previous.snapshot.metadata.id}, current=${loaded.snapshot.metadata.id}")
        }
        // An exact-version load proves this snapshot exists, but not that it is the latest.
        currentSnapshot = loaded
        retireSnapshotInternal(previous)
        loaded.snapshot
      } else {
        refreshed
      }
    }
    if (version == upperBound.version) {
      return upperBound
    }
    // Historical snapshots are returned to the caller but never replace the cached latest snapshot.
    val historicalSnapshot = withUncachedSnapshotManager(latestCatalogTable.get())(
      _.loadSnapshotAt(version))
    historicalSnapshot
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

  private def latestSnapshotFreshnessThreshold(): Long = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    if (stalenessLimit > 0) math.max(0, now - stalenessLimit) else now
  }

  private def isFresh(
      snapshot: CachedSnapshot,
      requiredFreshAfter: Long,
      requiredVersion: Option[Long] = None): Boolean =
    snapshot != null &&
      snapshot.validatedAtMs >= requiredFreshAfter &&
      requiredVersion.forall(snapshot.snapshot.version >= _)

  private def withSnapshotLockInterruptibly[T](body: => T): T = {
    snapshotLock.lockInterruptibly()
    try {
      body
    } finally {
      snapshotLock.unlock()
    }
  }

  private def retireSnapshotInternal(cachedSnapshot: CachedSnapshot): Unit = {
    if (cachedSnapshot != null) {
      cachedSnapshot.snapshot.uncache()
    }
  }

}
