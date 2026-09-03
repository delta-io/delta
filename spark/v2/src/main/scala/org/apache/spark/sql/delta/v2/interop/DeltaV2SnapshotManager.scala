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

import java.util.{Objects, Optional}

import io.delta.kernel.CommitRange
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.DeltaHistoryManager
import io.delta.kernel.internal.SnapshotImpl
import io.delta.spark.internal.v2.exception.VersionNotFoundException

import org.apache.spark.sql.delta.Snapshot

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
  def loadLatestSnapshot(): Snapshot

  /**
   * Loads and returns a snapshot at a specific version.
   *
   * @param version the version number to load (must be >= 0)
   */
  def loadSnapshotAt(version: Long): Snapshot

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
  def getActiveCommitAtTime(
      timestampMillis: Long,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit

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
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit

  /**
   * Gets a range of table changes between start and end versions.
   *
   * @param engine the engine for executing operations
   * @param startVersion starting version (inclusive)
   * @param endVersion optional ending version (inclusive)
   */
  def getTableChanges(
      engine: Engine,
      startVersion: Long,
      endVersion: Optional[java.lang.Long]): CommitRange
}

object DeltaV2SnapshotManager {

  /**
   * Wraps a Kernel snapshot in the V1 [[Snapshot]] facade returned by manager load APIs.
   *
   * @param kernelSnapshot the Kernel snapshot to wrap
   * @param tablePath table path used in construction-error messages
   * @return the V1 snapshot facade
   */
  def wrapKernelSnapshot(kernelSnapshot: SnapshotImpl, tablePath: String): Snapshot = {
    Objects.requireNonNull(kernelSnapshot, "kernelSnapshot is null")
    Objects.requireNonNull(tablePath, "tablePath is null")
    new DeltaV2Snapshot(kernelSnapshot)
  }
}
