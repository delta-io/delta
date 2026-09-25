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

import java.util.{Objects, Optional}
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.{DeltaUnsupportedOperationException, Snapshot}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.DeltaV2Logging
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext
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
    kernelContext: KernelContext)
    extends DeltaV2SnapshotManager
    with DeltaV2Logging {

  private case class CachedSnapshot(
      snapshot: Snapshot,
      validatedAtMs: Long,
      requestAuthority: SnapshotRequestAuthority,
      catalogAuthorityHighWatermark: Option[SnapshotRequestAuthority])

  private case class SnapshotRequestAuthority(
      hasCatalogTable: Boolean,
      isCatalogManaged: Boolean,
      catalogTableIdOpt: Option[String],
      catalogGenerationOpt: Option[Long]) {

    // Catalog generations are comparable only within one table identity. A different identity
    // always revalidates; it may legitimately have restarted its generation after recreation.

    def sameRoute(other: SnapshotRequestAuthority): Boolean =
      hasCatalogTable == other.hasCatalogTable &&
        isCatalogManaged == other.isCatalogManaged &&
        catalogTableIdOpt == other.catalogTableIdOpt

    def hasNewerCatalogGenerationThan(other: SnapshotRequestAuthority): Boolean =
      hasCatalogTable == other.hasCatalogTable &&
        catalogTableIdOpt == other.catalogTableIdOpt &&
        ((catalogGenerationOpt, other.catalogGenerationOpt) match {
          case (Some(cached), Some(incoming)) => cached > incoming
          case (Some(_), None) => true
          case _ => false
        })

    def accepts(requested: SnapshotRequestAuthority): Boolean = {
      if (hasCatalogTable != requested.hasCatalogTable ||
          catalogTableIdOpt != requested.catalogTableIdOpt) {
        false
      } else {
        (catalogGenerationOpt, requested.catalogGenerationOpt) match {
          case (None, Some(_)) => false
          case (Some(cached), Some(incoming)) if incoming < cached => true
          case (Some(cached), Some(incoming)) if incoming > cached => false
          case _ => isCatalogManaged == requested.isCatalogManaged
        }
      }
    }

    def advanceTo(refreshed: SnapshotRequestAuthority): SnapshotRequestAuthority = {
      if (hasCatalogTable && !refreshed.hasCatalogTable) {
        return this
      }
      if (!hasCatalogTable || !refreshed.hasCatalogTable ||
          catalogTableIdOpt != refreshed.catalogTableIdOpt) {
        return refreshed
      }
      (catalogGenerationOpt, refreshed.catalogGenerationOpt) match {
        case (Some(cached), Some(incoming)) if incoming < cached => this
        case (Some(_), None) => this
        case _ => refreshed
      }
    }

    def advanceRouteTo(refreshed: SnapshotRequestAuthority): SnapshotRequestAuthority = {
      if (!sameRoute(refreshed)) {
        refreshed
      } else {
        advanceTo(refreshed)
      }
    }
  }

  private val snapshotLock = new ReentrantLock()
  @volatile private var currentSnapshot: CachedSnapshot = _

  // === DeltaV2SnapshotManager implementation ================================

  override def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    recordFrameProfile("cachedSnapshotManager.loadLatestSnapshot") {
      loadLatestSnapshotInternal(queryContext)
    }
  }

  override def loadSnapshotAt(
      version: Long,
      queryContext: DeltaV2QueryContext): Snapshot = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    recordFrameProfile("cachedSnapshotManager.loadSnapshotAt") {
      loadSnapshotAtInternal(version, queryContext)
    }
  }

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit =
    unsupportedWithoutQueryContext()

  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean,
      queryContext: DeltaV2QueryContext): KernelDeltaHistoryManager.Commit = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    withUncachedSnapshotManager(queryContext)(
      _.getActiveCommitAtTime(
        timestampMillis,
        canReturnLastCommit,
        mustBeRecreatable,
        canReturnEarliestCommit,
        queryContext))
  }

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit =
    unsupportedWithoutQueryContext()

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean,
      queryContext: DeltaV2QueryContext): Unit = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    withUncachedSnapshotManager(queryContext)(
      _.checkVersionExists(version, mustBeRecreatable, allowOutOfRange, queryContext))
  }

  override def getTableChanges(
      kernelEngine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): KernelCommitRange =
    unsupportedWithoutQueryContext()

  override def getTableChanges(
      kernelEngine: KernelEngine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long],
      queryContext: DeltaV2QueryContext): KernelCommitRange = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    withUncachedSnapshotManager(queryContext)(
      _.getTableChanges(kernelEngine, startVersion, endVersion, queryContext))
  }

  private def unsupportedWithoutQueryContext(): Nothing = {
    throw new DeltaUnsupportedOperationException(
      "DELTA_OPERATION_NOT_ALLOWED",
      Array("cached snapshot manager operations without a DeltaV2QueryContext"))
  }

  // === Snapshot lifecycle ===================================================

  // Eviction drops persisted StateCache data without invalidating escaped snapshots or managers.
  private[tablemanager] def retire(): Unit = withSnapshotLockInterruptibly {
    retireSnapshotInternal(currentSnapshot)
  }

  // === Acquisition ==========================================================

  private def loadLatestSnapshotInternal(queryContext: DeltaV2QueryContext): Snapshot = {
    val requiredFreshAfter = latestSnapshotFreshnessThreshold()
    recordFrameProfile("cachedSnapshotManager.loadLatestSnapshotInternal") {
      val existing = currentSnapshot
      if (isFresh(existing, requiredFreshAfter, queryContext)) {
        return existing.snapshot
      }
      withSnapshotLockInterruptibly {
        val current = currentSnapshot
        if (isFresh(current, requiredFreshAfter, queryContext)) {
          current.snapshot
        } else {
          val requestedAuthority = requestAuthority(queryContext)
          if (staleRequestWouldChangeRoute(current, requestedAuthority)) {
            // staleRequestWouldChangeRoute implies a recorded catalog authority.
            val observedAuthority = current.catalogAuthorityHighWatermark.get
            logWarning(
              "Rejecting stale catalog authority for an expired cached snapshot: " +
                s"observed=${describeAuthority(observedAuthority)}, " +
                s"requested=${describeAuthority(requestedAuthority)}")
            throw new DeltaUnsupportedOperationException(
              "DELTA_OPERATION_NOT_ALLOWED",
              Array(
                "refreshing an expired cached snapshot requires current catalog metadata"))
          }
          rebuildAndInstallInternal(queryContext)
        }
      }
    }
  }

  private def rebuildAndInstallInternal(queryContext: DeltaV2QueryContext): Snapshot = {
    recordFrameProfile("cachedSnapshotManager.rebuild") {
      val validationStartedAt = System.currentTimeMillis()
      val existing = currentSnapshot
      val requestedAuthority = requestAuthority(queryContext)
      val refreshed = CachedSnapshot(
        withUncachedSnapshotManager(queryContext)(
            _.loadLatestSnapshot(queryContext)),
        validationStartedAt,
        requestedAuthority,
        advanceCatalogAuthorityHighWatermark(existing, requestedAuthority))
      val sameTable =
        existing != null && existing.snapshot.metadata.id == refreshed.snapshot.metadata.id
      val sameRoute =
        existing != null && existing.requestAuthority.sameRoute(refreshed.requestAuthority)
      val publicationAuthority =
        if (existing != null) {
          existing.requestAuthority.advanceRouteTo(refreshed.requestAuthority)
        } else {
          refreshed.requestAuthority
        }
      if (sameTable && !sameRoute && existing.snapshot.version > refreshed.snapshot.version) {
        retireSnapshotInternal(refreshed)
        throw new DeltaUnsupportedOperationException(
          "DELTA_OPERATION_NOT_ALLOWED",
          Array("replacing cached request resources with an older snapshot"))
      } else if (
          sameTable && sameRoute && existing.snapshot.version >= refreshed.snapshot.version) {
        val validatedAt = math.max(validationStartedAt, existing.validatedAtMs)
        currentSnapshot = CachedSnapshot(
          existing.snapshot,
          validatedAt,
          publicationAuthority,
          refreshed.catalogAuthorityHighWatermark)
        retireSnapshotInternal(refreshed)
        existing.snapshot
      } else {
        if (existing != null && !sameTable) {
          logWarning(
            s"Table identity changed while refreshing snapshot: " +
              s"previous=${existing.snapshot.metadata.id}, " +
              s"current=${refreshed.snapshot.metadata.id}")
        }
        currentSnapshot = refreshed.copy(requestAuthority = publicationAuthority)
        retireSnapshotInternal(existing)
        refreshed.snapshot
      }
    }
  }

  private def loadSnapshotAtInternal(
      version: Long,
      queryContext: DeltaV2QueryContext): Snapshot = {
    // Catalog metadata is request-scoped authority. An exact-version cache hit cannot prove that
    // the request still addresses the same table lineage after a same-path table replacement.
    if (queryContext.catalogTableOpt.nonEmpty) {
      return withSnapshotLockInterruptibly {
        withUncachedSnapshotManager(queryContext)(
          _.loadSnapshotAt(version, queryContext))
      }
    }
    val existing = currentSnapshot
    // Exact-version time travel reuses the cached facade; latest-table freshness is irrelevant.
    if (existing != null &&
        version == existing.snapshot.version &&
        routingMatches(existing, queryContext)) {
      return existing.snapshot
    }
    // Linearize upper-bound discovery and any exact fallback under one lock acquisition.
    val upperBound = withSnapshotLockInterruptibly {
      val current = currentSnapshot
      if (current != null &&
          version == current.snapshot.version &&
          routingMatches(current, queryContext)) {
        return current.snapshot
      }
      val refreshed =
        if (isFresh(
            current,
            latestSnapshotFreshnessThreshold(),
            queryContext,
            Some(version))) {
          current.snapshot
        } else {
          rebuildAndInstallInternal(queryContext)
        }
      // If latest still trails the requested version, attempt the exact load before rejecting it.
      if (version > refreshed.version) {
        val previous = currentSnapshot
        val requestedAuthority = requestAuthority(queryContext)
        val loaded = CachedSnapshot(
          withUncachedSnapshotManager(queryContext)(
            _.loadSnapshotAt(version, queryContext)),
          validatedAtMs = -1L,
          requestAuthority = requestedAuthority,
          catalogAuthorityHighWatermark = advanceCatalogAuthorityHighWatermark(
            previous,
            requestedAuthority))
        val tableIdentityChanged =
          previous != null && previous.snapshot.metadata.id != loaded.snapshot.metadata.id
        if (tableIdentityChanged) {
          logWarning(
            s"Table identity changed while loading snapshot at version $version: " +
              s"previous=${previous.snapshot.metadata.id}, current=${loaded.snapshot.metadata.id}")
          // The exact snapshot belongs to this request, but must not publish a different table
          // lineage into the shared latest-snapshot cache.
          loaded.snapshot
        } else {
          // An exact-version load proves this snapshot exists, but not that it is the latest.
          currentSnapshot = loaded
          retireSnapshotInternal(previous)
          loaded.snapshot
        }
      } else {
        refreshed
      }
    }
    if (version == upperBound.version) {
      return upperBound
    }
    // Historical snapshots are returned to the caller but never replace the cached latest snapshot.
    val historicalSnapshot = withUncachedSnapshotManager(queryContext)(
      _.loadSnapshotAt(version, queryContext))
    historicalSnapshot
  }

  // === Uncached loading =====================================================

  private def withUncachedSnapshotManager[T](
      queryContext: DeltaV2QueryContext)(
      f: DeltaV2SnapshotManager => T): T = {
    f(createUncachedSnapshotManager(queryContext))
  }

  private[tablemanager] def createUncachedSnapshotManager(
      queryContext: DeltaV2QueryContext): DeltaV2SnapshotManager = {
    SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      queryContext.catalogTableOpt.toJava)
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
      queryContext: DeltaV2QueryContext,
      requiredVersion: Option[Long] = None): Boolean = {
    if (snapshot == null || requiredVersion.exists(snapshot.snapshot.version < _)) {
      return false
    }
    val requestedAuthority = requestAuthority(queryContext)
    snapshot.validatedAtMs >= requiredFreshAfter &&
      (staleRequestWouldChangeRoute(snapshot, requestedAuthority) ||
        snapshot.requestAuthority.accepts(requestedAuthority))
  }

  private def staleRequestWouldChangeRoute(
      snapshot: CachedSnapshot,
      requestedAuthority: SnapshotRequestAuthority): Boolean = {
    snapshot != null &&
      snapshot.catalogAuthorityHighWatermark.exists(
        _.hasNewerCatalogGenerationThan(requestedAuthority)) &&
      !snapshot.requestAuthority.sameRoute(requestedAuthority)
  }

  /** Renders a request authority for internal diagnostics. */
  private def describeAuthority(authority: SnapshotRequestAuthority): String = {
    val identity = authority.catalogTableIdOpt.getOrElse("an unidentified table")
    authority.catalogGenerationOpt match {
      case Some(generation) => s"$identity at catalog generation $generation"
      case None => s"$identity at an unknown catalog generation"
    }
  }

  /** Returns the factory route and the strongest catalog identity/high watermark available. */
  private def requestAuthority(
      queryContext: DeltaV2QueryContext): SnapshotRequestAuthority = {
    queryContext.catalogTableOpt match {
      case Some(catalogTable) =>
        val isCatalogManaged =
          false
        val catalogTableIdOpt =
          Some(s"catalog:${catalogTable.identifier.unquotedString}")
        val catalogGenerationOpt =
          None
        SnapshotRequestAuthority(
          hasCatalogTable = true,
          isCatalogManaged,
          catalogTableIdOpt,
          catalogGenerationOpt)
      case None =>
        SnapshotRequestAuthority(
          hasCatalogTable = false,
          isCatalogManaged = false,
          catalogTableIdOpt = None,
          catalogGenerationOpt = None)
    }
  }

  private def routingMatches(
      snapshot: CachedSnapshot,
      queryContext: DeltaV2QueryContext): Boolean =
    snapshot.requestAuthority.accepts(requestAuthority(queryContext))

  private def advanceCatalogAuthorityHighWatermark(
      existing: CachedSnapshot,
      requestedAuthority: SnapshotRequestAuthority): Option[SnapshotRequestAuthority] = {
    val existingHighWatermark = Option(existing).flatMap(_.catalogAuthorityHighWatermark)
    if (!requestedAuthority.hasCatalogTable) {
      existingHighWatermark
    } else {
      Some(existingHighWatermark
        .map(_.advanceTo(requestedAuthority))
        .getOrElse(requestedAuthority))
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

  private def retireSnapshotInternal(cachedSnapshot: CachedSnapshot): Unit = {
    if (cachedSnapshot != null) {
      cachedSnapshot.snapshot.uncache()
    }
  }

}
