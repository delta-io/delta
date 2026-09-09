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
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

// format: off
// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off import.ordering.wrongOrderInGroup
import io.delta.kernel.{
  CommitRange => KernelCommitRange,
  TableManager => KernelTableManager
}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{
  DeltaHistoryManager => KernelDeltaHistoryManager,
  SnapshotImpl => KernelSnapshot
}
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory

import org.apache.spark.sql.delta.DeltaIllegalStateException
import org.apache.spark.sql.delta.DeltaUnsupportedOperationException
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.DeltaV2Logging
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.spark.internal.v2.kernel.KernelContext

import org.apache.hadoop.fs.Path

// scalastyle:on import.ordering.noEmptyLine
// scalastyle:on import.ordering.wrongOrderInGroup
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
// format: on

/**
 * Table-scoped snapshot manager that caches and refreshes the [[DeltaV2Snapshot]] served to
 * operations on the same table.
 *
 * [[currentSnapshot]] remains `null` until the first successful load. The first installed snapshot
 * captures [[tableId]], and every subsequent installation validates the same identity or throws
 * [[DeltaIllegalStateException]]. Stale refreshes serialize on [[snapshotLock]] and recheck
 * freshness after acquiring it. Backends without incremental replay support fall back to a full
 * load.
 */
private[tablemanager] class CachedSnapshotManager(
    val tablePath: Path,
    catalogTableOpt: Option[CatalogTable],
    kernelContext: KernelContext)
    extends DeltaV2SnapshotManager
    with DeltaV2Logging {

  private case class CachedSnapshot(snapshot: Snapshot, validatedAtMs: Long)

  private val snapshotLock = new ReentrantLock()
  @volatile private var currentSnapshot: CachedSnapshot = _
  @volatile private var tableId: String = _

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

  // Eviction only drops the process cache's reference. Escaped managers remain fully functional.
  def retire(): Unit = ()

  // === Acquisition ==========================================================

  private def acquireLatest(minimumVersion: Option[Long] = None): Snapshot = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val requiredFreshAfter =
      if (minimumVersion.nonEmpty || stalenessLimit <= 0) now
      else math.max(0, now - stalenessLimit)
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.acquireLatest") {
      val existing = currentSnapshot
      if (isFreshEnough(existing, requiredFreshAfter, minimumVersion)) {
        existing.snapshot
      } else {
        withSnapshotLockInterruptibly {
          val lockedExisting = currentSnapshot
          if (isFreshEnough(lockedExisting, requiredFreshAfter, minimumVersion)) {
            lockedExisting.snapshot
          } else {
            rebuildAndInstall()
          }
        }
      }
    }
  }

  private def isFreshEnough(
      cached: CachedSnapshot,
      requiredFreshAfter: Long,
      minimumVersion: Option[Long]): Boolean = {
    cached != null && {
      val satisfiesMinimumVersion = minimumVersion.exists(cached.snapshot.version >= _)
      cached.validatedAtMs >= requiredFreshAfter || satisfiesMinimumVersion
    }
  }

  private def withSnapshotLockInterruptibly[T](body: => T): T = {
    snapshotLock.lockInterruptibly()
    try {
      body
    } finally {
      snapshotLock.unlock()
    }
  }

  private def rebuildAndInstall(): Snapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val existing = currentSnapshot
      val refreshed = if (existing == null) {
        withUncachedSnapshotManager(_.loadLatestSnapshot())
      } else {
        loadLatestFrom(existing.snapshot)
      }
      synchronized {
        try {
          validateTableIdentity(refreshed)
        } catch {
          case NonFatal(error) =>
            throw error
        }
        val current = currentSnapshot
        if (current != null && current.snapshot.version >= refreshed.version) {
          currentSnapshot = CachedSnapshot(current.snapshot, validationStartedAt)
          current.snapshot
        } else {
          currentSnapshot = CachedSnapshot(refreshed, validationStartedAt)
          refreshed
        }
      }
    }
  }

  private def loadLatestFrom(existing: Snapshot): Snapshot =
    withUncachedSnapshotManager(_.loadLatestSnapshotFrom(existing))

  private def acquireSnapshotAt(version: Long): Snapshot = {
    val existing = currentSnapshot
    // Exact-version time travel reuses the cached facade; latest-table freshness is irrelevant.
    if (existing != null && version == existing.snapshot.version) {
      return existing.snapshot
    }
    // A request beyond the cache must refresh latest to establish a trustworthy upper bound.
    // An older request only refreshes latest when the configured staleness window requires it.
    val upperBound = if (existing == null || version > existing.snapshot.version) {
      acquireLatest(Some(version))
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
    val historicalSnapshot =
      withUncachedSnapshotManager(_.loadSnapshotAt(version))
    validateTableIdentity(historicalSnapshot)
    historicalSnapshot
  }

  // === Uncached loading =====================================================

  private def withUncachedSnapshotManager[T](f: DeltaV2SnapshotManager => T): T =
    f(SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      catalogTableOpt.toJava))

  private def validateTableIdentity(snapshot: Snapshot): Unit = synchronized {
    val snapshotTableId = snapshot.metadata.id
    if (tableId == null) {
      tableId = snapshotTableId
    } else if (tableId != snapshotTableId) {
      throw new DeltaIllegalStateException(
        errorClass = "INTERNAL_ERROR",
        messageParameters = Array(
          s"Table identity mismatch: expected $tableId but got $snapshotTableId"))
    }
  }

}
