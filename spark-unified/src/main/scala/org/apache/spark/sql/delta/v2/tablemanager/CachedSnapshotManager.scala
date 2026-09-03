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

// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off import.ordering.wrongOrderInGroup
import io.delta.kernel.CommitRange
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{DeltaHistoryManager, SnapshotImpl => KernelSnapshot}
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory

import org.apache.spark.sql.delta.{DeltaIllegalStateException, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.v2.interop.{DeltaV2Snapshot, DeltaV2SnapshotManager}

import org.apache.hadoop.fs.Path
// scalastyle:on import.ordering.noEmptyLine
// scalastyle:on import.ordering.wrongOrderInGroup
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Table-scoped snapshot manager that caches the [[DeltaV2Snapshot]]
 * and serves it to every operation on the same table.
 *
 * State invariants:
 *  - [[currentSnapshot]] is `null` until the first successful load.
 *  - [[tableId]] is captured on first load and validated on every
 *    subsequent install; a mismatch throws [[IllegalStateException]].
 *  - Stale entries are refreshed through the uncached snapshot manager.
 *  - Incremental refresh strategies are layered by dependent modules.
 */
private[tablemanager] class CachedSnapshotManager(
    val tablePath: Path,
    catalogTableOpt: Option[CatalogTable],
    sessionInvariantFsOptions: Map[String, String])
    extends DeltaV2SnapshotManager
    with DeltaLogging {

  @volatile private var currentSnapshot: DeltaV2Snapshot = _
  @volatile private var tableId: String = _
  @volatile private var lastValidatedAtMs: Long = -1L

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
    recordFrameProfile("Delta", "CachedSnapshotManager.getActiveCommitAtTime") {
      val kernelSnapshot =
        DeltaV2Snapshot.getKernelSnapshot(acquireLatestWithConfiguredStaleness())
      withEngine { kernelEngine =>
        DeltaHistoryManager.getActiveCommitAtTimestamp(
          kernelEngine,
          kernelSnapshot,
          kernelSnapshot.getLogPath,
          timestampMillis,
          mustBeRecreatable,
          canReturnLastCommit,
          canReturnEarliestCommit,
          kernelSnapshot.getLogSegment.getAllCatalogCommits)
      }
    }
  }

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    recordFrameProfile("Delta", "CachedSnapshotManager.checkVersionExists") {
      val snapshot = acquireLatestWithConfiguredStaleness()
      val kernelSnapshot = DeltaV2Snapshot.getKernelSnapshot(snapshot)
      if (version > snapshot.version && !allowOutOfRange) {
        throw new VersionNotFoundException(version, 0, snapshot.version)
      }
      withEngine { kernelEngine =>
        val earliestCatalogVersion = getEarliestCatalogVersion(kernelSnapshot)
        val earliestVersion = if (mustBeRecreatable) {
          DeltaHistoryManager.getEarliestRecreatableCommit(
            kernelEngine, kernelSnapshot.getLogPath, earliestCatalogVersion)
        } else {
          DeltaHistoryManager.getEarliestDeltaFile(
            kernelEngine, kernelSnapshot.getLogPath, earliestCatalogVersion)
        }
        if (version < earliestVersion) {
          throw new VersionNotFoundException(version, earliestVersion, snapshot.version)
        }
      }
    }
  }

  override def getTableChanges(
      engine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): CommitRange = {
    recordFrameProfile("Delta", "CachedSnapshotManager.getTableChanges") {
      val effectiveEndVersion = if (endVersion.isPresent) {
        endVersion
      } else {
        Optional.of(Long.box(acquireLatestWithConfiguredStaleness().version))
      }
      withUncachedManager(_.getTableChanges(engine, startVersion, effectiveEndVersion))
    }
  }

  // === Snapshot lifecycle ===================================================

  // Eviction only drops the process cache's reference. Escaped managers remain fully functional.
  def retire(): Unit = ()

  // === Acquisition ==========================================================

  private def acquireLatestWithConfiguredStaleness(): DeltaV2Snapshot = {
    val now = System.currentTimeMillis()
    val stalenessLimit = SparkSession.active.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    val freshAfter = if (stalenessLimit > 0) math.max(0, now - stalenessLimit) else now
    acquireLatest(freshAfter)
  }

  private[tablemanager] def acquireLatest(requiredFreshAfter: Long): DeltaV2Snapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.acquireLatest") {
      val existing = currentSnapshot
      if (existing != null && lastValidatedAtMs >= requiredFreshAfter) {
        return existing
      }
      rebuild()
    }
  }

  private def rebuild(): DeltaV2Snapshot = {
    recordFrameProfile("Delta", "DeltaV2.cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val refreshed = loadLatestUncached()
      installSnapshot(refreshed, validationStartedAt)
    }
  }

  private def acquireSnapshotAt(version: Long): DeltaV2Snapshot = {
    val existing = currentSnapshot
    if (existing != null && version == existing.version) {
      return existing
    }
    val kernelSnapshot = loadSnapshotAtUncached(version)
    validateTableIdentity(kernelSnapshot)
    wrapSnapshot(kernelSnapshot)
  }

  // === Uncached loading =====================================================

  private def wrapSnapshot(kernelSnapshot: KernelSnapshot): DeltaV2Snapshot = {
    new DeltaV2Snapshot(kernelSnapshot, catalogTableOpt)
  }

  private[tablemanager] def loadLatestUncached(): KernelSnapshot = {
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

  private def withUncachedManager[T](f: DeltaV2SnapshotManager => T): T = {
    withEngine { kernelEngine =>
      f(SnapshotManagerFactory.create(tablePath.toString, kernelEngine, catalogTableOpt.toJava))
    }
  }

  // === Snapshot installation =================================================

  private[tablemanager] def installSnapshot(
      refreshed: KernelSnapshot,
      validationStartedAt: Long): DeltaV2Snapshot = synchronized {
    validateTableIdentity(refreshed)
    val existing = currentSnapshot
    if (existing != null && existing.version >= refreshed.getVersion) {
      lastValidatedAtMs = validationStartedAt
      existing
    } else {
      val refreshedSnapshot = wrapSnapshot(refreshed)
      currentSnapshot = refreshedSnapshot
      lastValidatedAtMs = validationStartedAt
      refreshedSnapshot
    }
  }

  private[tablemanager] def validateTableIdentity(snapshot: KernelSnapshot): Unit = synchronized {
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

  private def getEarliestCatalogVersion(
      kernelSnapshot: KernelSnapshot): Optional[java.lang.Long] = {
    val commits = kernelSnapshot.getLogSegment.getAllCatalogCommits.iterator()
    var earliestVersion: java.lang.Long = null
    while (commits.hasNext) {
      val version = commits.next().getVersion
      if (earliestVersion == null || version < earliestVersion) {
        earliestVersion = version
      }
    }
    Optional.ofNullable(earliestVersion)
  }

}
