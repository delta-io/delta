/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.util.Optional

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.v2.interop.{
  DeltaV2Snapshot,
  DeltaV2SnapshotManager
}
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory
// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off import.ordering.wrongOrderInGroup
import io.delta.kernel.{
  CommitRange,
  TableManager => KernelTableManager
}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{
  DeltaHistoryManager,
  SnapshotImpl => KernelSnapshot,
  Snapshots
}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// scalastyle:on import.ordering.noEmptyLine
// scalastyle:on import.ordering.wrongOrderInGroup
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Per-request snapshot manager that caches and incrementally updates the
 * kernel [[KernelSnapshot]] shared across requests for the same table.
 *
 * State invariants:
 *  - [[currentSnapshot]] is `null` until the first successful load.
 *  - [[tableId]] is captured on first load and validated on every
 *    subsequent install; a mismatch throws [[IllegalStateException]].
 *  - [[retire]] is idempotent and prevents further snapshot acquisition.
 *  - Incremental builds via [[KernelTableManager.builderFrom]] are
 *    attempted for path-based tables when a current snapshot exists.
 *    UC-managed tables always do a full reload through the uncached
 *    manager because builderFrom cannot discover unbackfilled commits.
 */
private[tablemanager]
class CachedSnapshotManager(
    val tablePath: Path,
    catalogTableOpt: Option[CatalogTable],
    sessionInvariantFsOptions: Map[String, String])
    extends DeltaV2SnapshotManager
    with DeltaLogging {

  @volatile private var currentSnapshot: KernelSnapshot = _
  @volatile private var tableId: String = _
  @volatile private var lastValidatedAtMs: Long = -1L
  @volatile private var retired: Boolean = false

  // === DeltaV2SnapshotManager implementation ================================

  override def loadLatestSnapshot(): Snapshot = {
    recordFrameProfile("Delta", "CachedSnapshotManager.loadLatestSnapshot") {
      val now = System.currentTimeMillis()
      val stalenessLimit = SparkSession.active.sessionState.conf
        .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
      val freshAfter = if (stalenessLimit > 0) {
        math.max(0, now - stalenessLimit)
      } else {
        now
      }
      new DeltaV2Snapshot(acquireLatest(freshAfter), catalogTableOpt)
    }
  }

  override def loadSnapshotAt(version: Long): Snapshot = {
    recordFrameProfile("Delta", "CachedSnapshotManager.loadSnapshotAt") {
      new DeltaV2Snapshot(acquireSnapshotAt(version), catalogTableOpt)
    }
  }

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean
  ): DeltaHistoryManager.Commit = {
    recordFrameProfile("Delta", "CachedSnapshotManager.getActiveCommitAtTime") {
      withUncachedManager(_.getActiveCommitAtTime(timestampMillis, canReturnLastCommit,
        mustBeRecreatable, canReturnEarliestCommit))
    }
  }

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    recordFrameProfile("Delta", "CachedSnapshotManager.checkVersionExists") {
      withUncachedManager(_.checkVersionExists(version, mustBeRecreatable, allowOutOfRange))
    }
  }

  override def getTableChanges(
      engine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): CommitRange = {
    recordFrameProfile("Delta", "CachedSnapshotManager.getTableChanges") {
      withUncachedManager(_.getTableChanges(engine, startVersion, endVersion))
    }
  }

  // === Snapshot lifecycle ===================================================

  def retire(): Unit = synchronized {
    if (!retired) {
      retired = true
      val snap = currentSnapshot
      if (snap != null) {
        currentSnapshot = null
        snap.close()
      }
    }
  }

  def isRetired: Boolean = retired

  // === Acquisition ==========================================================

  private[tablemanager] def acquireLatest(requiredFreshAfter: Long): KernelSnapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.acquireLatest") {
      if (retired) {
        return loadLatestUncached()
      }
      val existing = currentSnapshot
      if (existing != null && lastValidatedAtMs >= requiredFreshAfter) {
        return existing
      }
      rebuild()
    }
  }

  private def rebuild(): KernelSnapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val existing = currentSnapshot
      val refreshed = if (existing != null && catalogTableOpt.isEmpty) {
          incrementalBuild(existing)
        } else {
          loadLatestUncached()
        }
      installSnapshot(refreshed, validationStartedAt)
    }
  }

  private def incrementalBuild(existing: KernelSnapshot): KernelSnapshot = {
    withEngine { kernelEngine =>
      val builder = KernelTableManager.builderFrom(kernelEngine, existing)
      try {
        Snapshots.wrap(builder.build()).asInstanceOf[KernelSnapshot]
      } finally {
        builder.close()
      }
    }
  }

  private def acquireSnapshotAt(version: Long): KernelSnapshot = {
    withUncachedManager { manager =>
      val kernelSnapshot = DeltaV2Snapshot.getKernelSnapshot(manager.loadSnapshotAt(version))
      validateTableIdentity(kernelSnapshot)
      kernelSnapshot
    }
  }

  // === Uncached loading =====================================================

  private[tablemanager] def loadLatestUncached(): KernelSnapshot = {
    withUncachedManager { manager =>
      DeltaV2Snapshot.getKernelSnapshot(manager.loadLatestSnapshot())
    }
  }

  private def withEngine[T](f: KernelEngine => T): T = {
    // scalastyle:off deltahadoopconfiguration
    val conf =
      SparkSession.active.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)
    // scalastyle:on deltahadoopconfiguration
    val kernelEngine = recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.createEngine") {
      KernelEngineFactory.createDefaultEngine(conf)
    }
    try {
      f(kernelEngine)
    } finally {
      kernelEngine.close()
    }
  }

  private def withUncachedManager[T](f: DeltaV2SnapshotManager => T): T = {
    withEngine { engine =>
      f(SnapshotManagerFactory.create(tablePath.toString, engine, catalogTableOpt.toJava))
    }
  }

  // === Snapshot installation =================================================

  private[tablemanager] def installSnapshot(refreshed: KernelSnapshot,
      validationStartedAt: Long): KernelSnapshot = synchronized {
    if (retired) {
      refreshed.close()
      return refreshed
    }
    validateTableIdentity(refreshed)
    val existing = currentSnapshot
    if (existing != null && existing.getVersion >= refreshed.getVersion) {
      lastValidatedAtMs = validationStartedAt
      refreshed.close()
      existing
    } else {
      currentSnapshot = refreshed
      lastValidatedAtMs = validationStartedAt
      if (existing != null) {
        existing.close()
      }
      refreshed
    }
  }

  private[tablemanager] def validateTableIdentity(snapshot: KernelSnapshot): Unit = synchronized {
    val snapshotTableId = snapshot.getMetadata.getId
    if (tableId == null) {
      tableId = snapshotTableId
    } else if (tableId != snapshotTableId) {
      throw new IllegalStateException(
        s"Table identity mismatch: expected $tableId but got $snapshotTableId")
    }
  }

  // === Test-only accessors ==================================================

  private[tablemanager] def currentSnapshotForTesting: KernelSnapshot =
    currentSnapshot

  private[tablemanager] def lastValidatedAtMsForTesting: Long =
    lastValidatedAtMs

  private[tablemanager] def tableIdForTesting: String = tableId
}
