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

package org.apache.spark.sql.delta.v2.interop

import java.lang.{Long => JLong}
import java.util.{List => JList, Objects, Optional}

import scala.util.control.NonFatal

import io.delta.kernel.CommitRange
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.DeltaHistoryManager
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.files.ParsedCatalogCommitData
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam
import org.apache.spark.sql.delta.v2.kernel.KernelContext

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.v2.DeltaV2Logging
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental

/**
 * Contract for managing Delta table snapshots in the DSv2 connector.
 *
 * This connector exposes loaded state through the V1 snapshot facade so callers use the same
 * metadata, protocol, schema, timestamp, column-mapping, and file-access surface as V1. Kernel
 * execution details remain confined to the connector's execution seams.
 */
@Experimental
trait DeltaV2SnapshotManager {

  /** Loads and returns the latest snapshot of the Delta table. */
  @deprecated("Use loadLatestSnapshot with a query context instead")
  def loadLatestSnapshot(): Snapshot

  /** Loads the latest snapshot using inputs scoped to the current query. */
  def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    loadLatestSnapshot()
  }

  /**
   * Loads and returns a snapshot at a specific version.
   *
   * @param version the version number to load (must be >= 0)
   */
  @deprecated("Use loadSnapshotAt with a query context instead")
  def loadSnapshotAt(version: Long): Snapshot

  /** Loads a versioned snapshot using inputs scoped to the current query. */
  def loadSnapshotAt(
      version: Long,
      queryContext: DeltaV2QueryContext): Snapshot = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    loadSnapshotAt(version)
  }

  /**
   * Finds the commit that was active at a specific timestamp.
   *
   * @param timestampMillis timestamp in milliseconds since epoch (UTC)
   * @param canReturnLastCommit if true, returns the last commit when
   *   the timestamp is after all commits
   * @param mustBeRecreatable if true, only considers commits that can
   *   be fully recreated from available log files
   * @param canReturnEarliestCommit if true, returns the earliest commit
   *   when the timestamp is before all commits
   */
  @deprecated("Use getActiveCommitAtTime with a query context instead")
  def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit

  /** Finds the active commit using inputs scoped to the current query. */
  def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean,
      queryContext: DeltaV2QueryContext): DeltaHistoryManager.Commit = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    getActiveCommitAtTime(
      timestampMillis,
      canReturnLastCommit,
      mustBeRecreatable,
      canReturnEarliestCommit)
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be
   *   fully recreated from available log files
   * @param allowOutOfRange if true, allows versions greater than the
   *   latest version without throwing
   * @throws VersionNotFoundException if the version is not available
   */
  @throws[VersionNotFoundException]
  @deprecated("Use checkVersionExists with a query context instead")
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit

  /** Checks version availability using inputs scoped to the current query. */
  @throws[VersionNotFoundException]
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean,
      queryContext: DeltaV2QueryContext): Unit = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    checkVersionExists(version, mustBeRecreatable, allowOutOfRange)
  }

  /**
   * Gets a range of table changes between start and end versions.
   *
   * @param engine the engine for executing operations
   * @param params the inclusive version range and validation behavior
   */
  @deprecated("Use getTableChanges with a query context instead")
  def getTableChanges(
      engine: Engine,
      params: GetTableChangesParam): CommitRange

  /** Gets table changes using inputs scoped to the current query. */
  def getTableChanges(
      kernelEngine: Engine,
      params: GetTableChangesParam,
      queryContext: DeltaV2QueryContext): CommitRange = {
    Objects.requireNonNull(queryContext, "queryContext is null")
    getTableChanges(kernelEngine, params)
  }

  // ---------------------------------------------------------------------------------------------
  // Shared time-travel implementation for managers backed by a loaded Kernel snapshot
  // ---------------------------------------------------------------------------------------------

  /**
   * Resolves the commit active at a timestamp using a loaded Kernel snapshot and any catalog
   * commits. Catalog commits participate in history resolution alongside commits available from
   * the log. Implementation helper for `getActiveCommitAtTime`.
   *
   * @param kernelEngine engine used to read the transaction log
   * @param snapshot loaded snapshot whose log and version bound the lookup
   * @param catalogCommits catalog-managed commits visible to the loaded snapshot; empty for a
   *   path-based table
   * @param timestampMillis timestamp to resolve, in milliseconds since the epoch
   * @param canReturnLastCommit whether a timestamp after the latest commit may return that commit
   * @param mustBeRecreatable whether the returned commit must be recreatable from available files
   * @param canReturnEarliestCommit whether a timestamp before history may return the earliest
   *   commit
   * @return the commit active at `timestampMillis`
   */
  protected def getActiveCommitAtTimeInSnapshot(
      kernelEngine: Engine,
      snapshot: SnapshotImpl,
      catalogCommits: JList[ParsedCatalogCommitData],
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit = {
    DeltaHistoryManager.getActiveCommitAtTimestamp(
      kernelEngine,
      snapshot,
      snapshot.getLogPath,
      timestampMillis,
      mustBeRecreatable,
      canReturnLastCommit,
      canReturnEarliestCommit,
      catalogCommits)
  }

  /**
   * Validates a version against a loaded Kernel snapshot and the earliest available history. The
   * minimum catalog-commit version bounds the filesystem search for the earliest version; an empty
   * list performs the search without a catalog bound. Implementation helper for
   * `checkVersionExists`.
   *
   * When `checkUpperBoundFirst` is true, an invalid upper bound is rejected before the filesystem
   * lookup and reports zero as the earliest version. When false, the filesystem lookup happens
   * first and an upper-bound error reports the actual earliest available version.
   *
   * @param kernelEngine engine used to read the transaction log
   * @param snapshot loaded snapshot whose log and latest version bound the validation
   * @param catalogCommits catalog-managed commits visible to the loaded snapshot; empty for a
   *   path-based table
   * @param version version to validate
   * @param mustBeRecreatable whether the version must be recreatable from available files
   * @param allowOutOfRange whether versions above the loaded snapshot are allowed
   * @param checkUpperBoundFirst whether to reject an invalid upper bound before filesystem access
   * @throws VersionNotFoundException if the requested version is unavailable
   */
  @throws[VersionNotFoundException]
  protected def checkVersionExistsInSnapshot(
      kernelEngine: Engine,
      snapshot: SnapshotImpl,
      catalogCommits: JList[ParsedCatalogCommitData],
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean,
      checkUpperBoundFirst: Boolean): Unit = {
    val latest = snapshot.getVersion
    if (checkUpperBoundFirst && version > latest && !allowOutOfRange) {
      // The fast path skips the filesystem lookup, so it reports zero as the earliest version.
      throw new VersionNotFoundException(version, 0L, latest)
    }

    val earliestCatalogCommitVersion: Optional[JLong] = {
      val minVersion = catalogCommits.stream().mapToLong(_.getVersion).min()
      if (minVersion.isPresent) Optional.of[JLong](minVersion.getAsLong) else Optional.empty()
    }
    val earliest = if (mustBeRecreatable) {
      DeltaHistoryManager.getEarliestRecreatableCommit(
        kernelEngine, snapshot.getLogPath, earliestCatalogCommitVersion)
    } else {
      DeltaHistoryManager.getEarliestDeltaFile(
        kernelEngine, snapshot.getLogPath, earliestCatalogCommitVersion)
    }

    if (version < earliest || (!checkUpperBoundFirst && version > latest && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest)
    }
  }
}

object DeltaV2SnapshotManager extends DeltaV2Logging {

  /**
   * Inputs for loading an inclusive range of table changes.
   *
   * @param startVersion the first version to include in the range
   * @param endVersion the last version to include in the range, or empty to read through the
   *   latest available commit.
   * @param commitRangeValidation whether to eagerly validate data loss during commit range
   *   build time (default true).
   * @param failOnDataLoss whether lazily validate data loss during commit range iteration
   *   (default true).
   */
  case class GetTableChangesParam(
      startVersion: Long,
      endVersion: Option[Long],
      commitRangeValidation: Boolean = true,
      failOnDataLoss: Boolean = true)

  object GetTableChangesParam {
    def apply(startVersion: Long, endVersion: Option[Long]): GetTableChangesParam =
      new GetTableChangesParam(startVersion, endVersion)
  }

  /**
   * Wraps a Kernel snapshot in the V1 [[Snapshot]] facade returned by manager load APIs.
   *
   * @param kernelSnapshot the Kernel snapshot to wrap
   * @param kernelContext the context used to load and access the snapshot
   * @param tablePath table path used in construction-error messages
   * @return the V1 snapshot facade
   */
  def wrapKernelSnapshot(
      kernelSnapshot: SnapshotImpl,
      kernelContext: KernelContext,
      tablePath: String): Snapshot = {
    Objects.requireNonNull(kernelSnapshot, "kernelSnapshot is null")
    Objects.requireNonNull(kernelContext, "kernelContext is null")
    Objects.requireNonNull(tablePath, "tablePath is null")
    try {
      new DeltaV2Snapshot(kernelSnapshot, kernelContext)
    } catch {
      case NonFatal(e) =>
        recordDeltaEventForTablePath(
          tablePath = new Path(tablePath),
          opType = "snapshot.wrap.failure",
          data = Map("version" -> kernelSnapshot.getVersion))
        throw e
    }
  }
}
