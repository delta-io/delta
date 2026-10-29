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

package org.apache.spark.sql.delta.sources

import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaHistoryManager, DeltaSingleCommit, Snapshot}


/**
 * The Delta log operations that the Delta streaming sources use to discover offsets. Source logic
 * that reads the table through this interface runs on either backend: [[DeltaLogStreamingBackend]]
 * (V1, `DeltaLog`) or `DeltaV2StreamingBackend` (V2, Kernel). Snapshots are always V1
 * [[Snapshot]]s.
 */
trait DeltaStreamingBackend {

  /**
   * Refreshes the table state and returns the latest snapshot. The Kernel backend can return a
   * cached snapshot when `spark.databricks.delta.stalenessLimit` is set.
   */
  def update(): Snapshot

  /**
   * The latest snapshot that the last refreshing call loaded ([[update]] or
   * [[getActiveCommitAtTime]]). Does not refresh the table state after the first refreshing call.
   * Before it, a backend can load the latest snapshot.
   */
  def unsafeVolatileSnapshot: Snapshot

  /** Returns the snapshot at `version`. */
  def getSnapshotAt(version: Long): Snapshot

  /**
   * Throws a version-not-found error if `version` is not in the available history.
   *
   * @param mustBeRecreatable if true, the snapshot at `version` must be recreatable.
   * @param allowOutOfRange if true, versions after the latest version do not cause an error.
   */
  def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit

  /**
   * Returns the commit that was active at `timestamp`. Also refreshes the table state, so
   * [[unsafeVolatileSnapshot]] is at least as new as the returned commit. The Kernel backend does
   * not keep this when [[update]] returns a cached snapshot.
   */
  def getActiveCommitAtTime(
      timestamp: Timestamp,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit

  /**
   * Returns the snapshot at `version`, to validate its protocol. Throws a
   * [[org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException]] if this client
   * cannot read a table feature at `version`.
   */
  def getSnapshotToValidateProtocol(version: Long): Snapshot

  /**
   * Returns the commits from `startVersion` to the latest version, in version order.
   *
   * @param failOnDataLoss if true, throws if a commit in the range is missing.
   */
  def getChangesIterator(startVersion: Long, failOnDataLoss: Boolean): Iterator[DeltaSingleCommit]
}
