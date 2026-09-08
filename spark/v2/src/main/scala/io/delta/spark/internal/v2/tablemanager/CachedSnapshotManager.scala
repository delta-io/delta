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
import io.delta.kernel.CommitRange
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{DeltaHistoryManager, SnapshotImpl => KernelSnapshot}
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory

import org.apache.spark.sql.delta.DeltaIllegalStateException
import org.apache.spark.sql.delta.DeltaUnsupportedOperationException
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager

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
 * State invariants:
 *  - [[currentSnapshot]] is `null` until the first successful load.
 *  - [[tableId]] is captured on first load and validated on every
 *    subsequent install; a mismatch throws [[IllegalStateException]].
 *  - Stale refreshes are serialized and recheck freshness after acquiring [[snapshotLock]].
 *  - Backends without incremental replay support fall back to a full load.
 */
private[tablemanager] class CachedSnapshotManager(
    val tablePath: Path,
    catalogTableOpt: Option[CatalogTable],
    sessionInvariantFsOptions: Map[String, String])
    extends DeltaV2SnapshotManager
    with DeltaLogging {

  private case class CachedSnapshot(snapshot: Snapshot, validatedAtMs: Long)

  private val snapshotLock = new ReentrantLock()
  @volatile private var currentSnapshot: CachedSnapshot = _
  @volatile private var tableId: String = _

  // === DeltaV2SnapshotManager implementation ================================

  override def loadLatestSnapshot(): Snapshot = {
    recordFrameProfile("Delta", "CachedSnapshotManager.loadLatestSnapshot") {
      acquireLatestWithConfiguredStaleness()
    }
  }

  override def loadSnapshotAt(version: Long): Snapshot = {
    recordFrameProfile("Delta", "CachedSnapshotManager.loadSnapshotAt") {
      acquireSnapshotAt(version)
    }
  }

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit = {
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
      engine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): CommitRange = {
    throw new DeltaUnsupportedOperationException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Array("Cached manager does not support getTableChanges"))
  }

  // === Snapshot lifecycle ===================================================

  // Eviction only drops the process cache's reference. Escaped managers remain fully functional.
  def retire(): Unit = ()

  // === Acquisition ==========================================================

  private def acquireLatestWithConfiguredStaleness(): Snapshot = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val freshAfter = if (stalenessLimit > 0) math.max(0, now - stalenessLimit) else now
    acquireLatest(freshAfter)
  }

  private def acquireLatest(requiredFreshAfter: Long): Snapshot =
    acquireLatest(requiredFreshAfter, minimumVersion = None)

  private def acquireLatest(
      requiredFreshAfter: Long,
      minimumVersion: Option[Long]): Snapshot = {
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
            rebuild()
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

  private def rebuild(): Snapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val existing = currentSnapshot
      val refreshed = if (existing == null) {
        loadLatestUncached()
      } else {
        loadLatestFrom(existing.snapshot)
      }
      install(refreshed, validationStartedAt)
    }
  }

  private def loadLatestFrom(existing: Snapshot): KernelSnapshot = {
    loadLatestUncached()
  }

  private def acquireSnapshotAt(version: Long): Snapshot = {
    val existing = currentSnapshot
    if (existing != null && version == existing.snapshot.version) {
      return existing.snapshot
    }
    val upperBound = if (existing == null || version > existing.snapshot.version) {
      acquireLatest(System.currentTimeMillis(), Some(version))
    } else {
      acquireLatestWithConfiguredStaleness()
    }
    if (version > upperBound.version) {
      throw new VersionNotFoundException(version, 0, upperBound.version)
    }
    if (version == upperBound.version) {
      return upperBound
    }
    val kernelSnapshot = loadSnapshotAtUncached(version)
    validateTableIdentity(kernelSnapshot)
    wrapSnapshot(kernelSnapshot)
  }

  // === Uncached loading =====================================================

  private def wrapSnapshot(kernelSnapshot: KernelSnapshot): Snapshot = {
    DeltaV2SnapshotManager.wrapKernelSnapshot(kernelSnapshot, tablePath.toString)
  }

  private def loadLatestUncached(): KernelSnapshot = {
    withUncachedManager { manager =>
      DeltaV2Snapshot.getKernelSnapshot(manager.loadLatestSnapshot())
    }
  }

  private def loadSnapshotAtUncached(version: Long): KernelSnapshot = {
    withUncachedManager { manager =>
      DeltaV2Snapshot.getKernelSnapshot(manager.loadSnapshotAt(version))
    }
  }

  private def createKernelEngine(): KernelEngine = {
    // scalastyle:off deltahadoopconfiguration
    val conf =
      SparkSession.active.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)
    // scalastyle:on deltahadoopconfiguration
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.createEngine") {
      KernelEngineFactory.createDefaultEngine(conf)
    }
  }

  private def withEngine[T](f: KernelEngine => T): T = f(createKernelEngine())

  private def createUncachedManager(kernelEngine: KernelEngine): DeltaV2SnapshotManager =
    SnapshotManagerFactory.create(tablePath.toString, kernelEngine, catalogTableOpt.toJava)

  private def withUncachedManager[T](f: DeltaV2SnapshotManager => T): T = {
    withEngine { kernelEngine =>
      f(createUncachedManager(kernelEngine))
    }
  }

  // === Snapshot installation =================================================

  private[tablemanager] def install(kernelSnapshot: KernelSnapshot, validatedAt: Long): Snapshot =
    synchronized {
      try {
        validateTableIdentity(kernelSnapshot)
      } catch {
        case NonFatal(error) =>
          throw error
      }
      val existing = currentSnapshot
      if (existing != null && existing.snapshot.version >= kernelSnapshot.getVersion) {
        currentSnapshot = CachedSnapshot(existing.snapshot, validatedAt)
        existing.snapshot
      } else {
        val refreshedSnapshot = wrapSnapshot(kernelSnapshot)
        currentSnapshot = CachedSnapshot(refreshedSnapshot, validatedAt)
        refreshedSnapshot
      }
    }

  private def validateTableIdentity(snapshot: KernelSnapshot): Unit = synchronized {
    val snapshotTableId = snapshot.getMetadata.getId
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
