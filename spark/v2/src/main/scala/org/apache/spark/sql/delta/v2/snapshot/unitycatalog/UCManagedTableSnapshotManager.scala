/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2.snapshot.unitycatalog

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.Objects.requireNonNull
import java.util.Optional

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam
import org.apache.spark.sql.delta.v2.kernel.KernelContext
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{
  DeltaHistoryManager => KernelDeltaHistoryManager,
  SnapshotImpl => KernelSnapshot
}
import io.delta.kernel.unitycatalog.{
  UCCatalogManagedClient => KernelUCCatalogManagedClient,
  UCLoadCommitRangeOptions => KernelUCLoadCommitRangeOptions
}

/**
 * Snapshot manager for Unity Catalog managed tables.
 *
 * Used for tables with the catalog-managed commit feature enabled. Unity Catalog serves as the
 * source of truth for the table's commit history.
 *
 * Creates a new UCManagedTableSnapshotManager.
 *
 * @param ucCatalogManagedClient the UC client for catalog-managed operations
 * @param tableInfo the UC table information (tableId, tablePath, etc.)
 * @param kernelContext the configured context for table operations
 */
private[v2]
class UCManagedTableSnapshotManager(
    ucCatalogManagedClient: KernelUCCatalogManagedClient,
    tableInfo: UCTableInfo,
    kernelContext: KernelContext)
    extends DeltaV2SnapshotManager {

  requireNonNull(ucCatalogManagedClient, "ucCatalogManagedClient is null")
  requireNonNull(tableInfo, "tableInfo is null")
  private val tableId = tableInfo.getTableId
  private val tablePath = tableInfo.getTablePath
  private val tableIdentifier = tableInfo.getTableIdentifier
  requireNonNull(kernelContext, "kernelContext is null")

  /**
   * Loads and returns the latest snapshot of the UC-managed Delta table.
   *
   * @return the latest snapshot of the table
   */
  override def loadLatestSnapshot(): Snapshot = {
    DeltaV2SnapshotManager.wrapKernelSnapshot(loadKernelSnapshot(None), kernelContext, tablePath)
  }

  override def loadSnapshotAt(version: Long): Snapshot = {
    DeltaV2SnapshotManager.wrapKernelSnapshot(
      loadKernelSnapshot(Some(version)),
      kernelContext,
      tablePath
    )
  }

  private def loadKernelSnapshot(versionOpt: Option[Long]): KernelSnapshot = {
    ucCatalogManagedClient
      .loadSnapshot(
        kernelContext.getDefaultEngine,
        tableId,
        tablePath,
        tableIdentifier,
        versionOpt match {
          case Some(version) => Optional.of[JLong](version)
          case None => Optional.empty[JLong]
        },
        Optional.empty[JLong] /* timestampOpt */
      )
      .asInstanceOf[KernelSnapshot]
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * For UC-managed tables, this loads the latest snapshot and uses
   * `DeltaHistoryManager.getActiveCommitAtTimestamp` to resolve the timestamp to a commit.
   *
   * @param timestampMillis the timestamp to find the version for in milliseconds since the unix
   *   epoch
   * @param canReturnLastCommit whether we can return the latest version of the table if the
   *   provided timestamp is after the latest commit
   * @param mustBeRecreatable whether the state at the returned commit should be recreatable
   * @param canReturnEarliestCommit whether we can return the earliest version of the table if the
   *   provided timestamp is before the earliest commit
   * @return the commit that was active at the specified timestamp
   */
  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit = {
    val snapshot = loadKernelSnapshot(None)
    val catalogCommits = snapshot.getLogSegment.getAllCatalogCommits
    KernelDeltaHistoryManager.getActiveCommitAtTimestamp(
      kernelContext.getDefaultEngine,
      snapshot,
      snapshot.getLogPath,
      timestampMillis,
      mustBeRecreatable,
      canReturnLastCommit,
      canReturnEarliestCommit,
      catalogCommits
    )
  }

  /**
   * Checks if a specific version exists and is accessible.
   *
   * For UC-managed tables with catalogManaged, log files may be cleaned up, so we need to use
   * DeltaHistoryManager to find the earliest available version based on filesystem state.
   *
   * @param version the version to check
   * @param mustBeRecreatable whether the state at this version should be recreatable
   * @param allowOutOfRange whether versions greater than the latest version are allowed without
   *   throwing an exception
   * @throws VersionNotFoundException if the version is not available or does not meet the specified
   *   criteria
   */
  @throws[VersionNotFoundException]
  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    // Load latest to get the current version bounds
    val snapshot = loadKernelSnapshot(None)
    // Latest version visible in this UC-managed snapshot.
    val latestSnapshotVersion = snapshot.getVersion

    // Fast path: check upper bound before expensive filesystem operations
    if (version > latestSnapshotVersion && !allowOutOfRange) {
      throw new VersionNotFoundException(
        version,
        /* earliest */
        0,
        latestSnapshotVersion)
    }

    // Get the earliest version among catalog commits. This bounds the Kernel's filesystem search
    // for the earliest available version (e.g., if catalog has v0, no filesystem search is needed).
    val catalogCommits = snapshot.getLogSegment.getAllCatalogCommits
    val commitIterator = catalogCommits.iterator
    val earliestCatalogCommitVersion = if (commitIterator.hasNext) {
      var earliest = commitIterator.next().getVersion
      while (commitIterator.hasNext) {
        earliest = math.min(earliest, commitIterator.next().getVersion)
      }
      Some(earliest)
    } else {
      None
    }
    val kernelEarliestCatalogCommitVersion = earliestCatalogCommitVersion match {
      case Some(version) => Optional.of[JLong](version)
      case None => Optional.empty[JLong]
    }

    val earliestVersion = if (mustBeRecreatable) {
      KernelDeltaHistoryManager.getEarliestRecreatableCommit(
        kernelContext.getDefaultEngine,
        snapshot.getLogPath,
        kernelEarliestCatalogCommitVersion
      )
    } else {
      KernelDeltaHistoryManager.getEarliestDeltaFile(
        kernelContext.getDefaultEngine,
        snapshot.getLogPath,
        kernelEarliestCatalogCommitVersion
      )
    }

    if (version < earliestVersion) {
      throw new VersionNotFoundException(version, earliestVersion, latestSnapshotVersion)
    }
  }

  /**
   * Gets a range of table changes (commits) between start and end versions.
   *
   * @param engine the engine implementation for executing operations
   * @param params the inclusive version range and validation behavior
   * @return a CommitRange representing the specified range of commits
   */
  override def getTableChanges(
      engine: KernelEngine,
      params: GetTableChangesParam): KernelCommitRange = {
    ucCatalogManagedClient.loadCommitRange(
      engine,
      tableId,
      tablePath,
      tableIdentifier,
      new KernelUCLoadCommitRangeOptions(
        /* startVersionOpt */ Optional.of[JLong](params.startVersion),
        /* startTimestampOpt */ Optional.empty[JLong],
        /* endVersionOpt */ {
          val endVersion = params.endVersion
          if (endVersion.isDefined) Optional.of[JLong](endVersion.get) else Optional.empty[JLong]
        },
        /* endTimestampOpt */ Optional.empty[JLong],
        Optional.of[JBoolean](params.commitRangeValidation),
        Optional.of[JBoolean](params.failOnDataLoss)
      )
    )
  }

}
