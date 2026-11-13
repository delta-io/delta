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
package org.apache.spark.sql.delta.v2.snapshot

import java.util.Objects.requireNonNull

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam
import org.apache.spark.sql.delta.v2.kernel.KernelContext
import io.delta.kernel.{CommitRange => KernelCommitRange, CommitRangeBuilder => KernelCommitRangeBuilder, TableManager => KernelTableManager}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{DeltaHistoryManager => KernelDeltaHistoryManager, SnapshotImpl => KernelSnapshot}
import io.delta.kernel.internal.files.{ParsedCatalogCommitData => KernelParsedCatalogCommitData}

import org.apache.spark.annotation.Experimental

/** Implementation of DeltaV2SnapshotManager for managing Delta snapshots for Path-based Table. */
@Experimental
class PathBasedSnapshotManager(tablePath: String, kernelContext: KernelContext)
    extends DeltaV2SnapshotManager {

  requireNonNull(tablePath, "tablePath is null")
  requireNonNull(kernelContext, "kernelContext is null")

  /**
   * Loads the latest snapshot of the Delta table.
   *
   * @return the newly loaded snapshot
   */
  override def loadLatestSnapshot(): Snapshot = {
    DeltaV2SnapshotManager.wrapKernelSnapshot(
      loadLatestKernelSnapshot(), kernelContext, tablePath)
  }

  /**
   * Loads a specific version of the Delta table.
   *
   * @param version the version to load
   * @return the snapshot at the specified version
   */
  override def loadSnapshotAt(version: Long): Snapshot = {
    DeltaV2SnapshotManager.wrapKernelSnapshot(
      loadKernelSnapshotAt(version), kernelContext, tablePath)
  }

  private def loadLatestKernelSnapshot(): KernelSnapshot = {
    KernelTableManager.loadSnapshot(tablePath)
      .build(kernelContext.getDefaultEngine).asInstanceOf[KernelSnapshot]
  }

  private def loadKernelSnapshotAt(version: Long): KernelSnapshot = {
    KernelTableManager.loadSnapshot(tablePath)
      .atVersion(version)
      .build(kernelContext.getDefaultEngine).asInstanceOf[KernelSnapshot]
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * This method searches the Delta table's commit history to find the commit that was active at
   * the specified timestamp.
   *
   * @param timestampMillis the timestamp in milliseconds since epoch (UTC)
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *   commits
   * @param mustBeRecreatable if true, only considers commits that can be recreated (i.e., all
   *   necessary log files are available)
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *   all commits
   * @return the commit that was active at the specified timestamp
   */
  override def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit = {
    val kernelSnapshot = loadLatestKernelSnapshot()
    SnapshotTimeTravelUtils.getActiveCommitAtTime(
      kernelContext.getDefaultEngine,
      kernelSnapshot,
      List.empty[KernelParsedCatalogCommitData].asJava,
      timestampMillis,
      canReturnLastCommit,
      mustBeRecreatable,
      canReturnEarliestCommit)
  }

  /**
   * Checks if a specific version of the Delta table exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *   available log files
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *   throwing an exception
   * @throws VersionNotFoundException if the version is not available
   */
  @throws[VersionNotFoundException]
  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    val kernelSnapshot = loadLatestKernelSnapshot()
    SnapshotTimeTravelUtils.checkVersionExists(
      kernelContext.getDefaultEngine,
      kernelSnapshot,
      List.empty[KernelParsedCatalogCommitData].asJava,
      version,
      mustBeRecreatable,
      allowOutOfRange,
      false /* checkUpperBoundFirst */)
  }

  override def getTableChanges(
      kernelEngine: KernelEngine,
      params: GetTableChangesParam): KernelCommitRange = {
    var kernelBuilder = KernelTableManager.loadCommitRange(
      tablePath, KernelCommitRangeBuilder.CommitBoundary.atVersion(params.startVersion))
    if (params.endVersion.isDefined) {
      kernelBuilder = kernelBuilder.withEndBoundary(
        KernelCommitRangeBuilder.CommitBoundary.atVersion(params.endVersion.get))
    }
    kernelBuilder
      .withCommitRangeValidation(params.commitRangeValidation)
      .withFailOnDataLoss(params.failOnDataLoss)
      .build(kernelEngine)
  }

}
