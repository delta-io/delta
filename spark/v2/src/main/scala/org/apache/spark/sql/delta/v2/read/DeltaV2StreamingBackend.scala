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

package org.apache.spark.sql.delta.v2.read

import java.sql.Timestamp

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.{DeltaErrors, DeltaHistoryManager, Snapshot, VersionNotFoundException}
import org.apache.spark.sql.delta.sources.DeltaStreamingBackend
import org.apache.spark.sql.delta.v2.exception.{VersionNotFoundException => V2VersionNotFoundException}
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.kernel.exceptions.UnsupportedTableFeatureException


/**
 * The [[DeltaStreamingBackend]] backend that reads the table through Kernel. `checkVersionExists`
 * and `getSnapshotToValidateProtocol` throw the same Delta errors as
 * [[org.apache.spark.sql.delta.sources.DeltaLogStreamingBackend]]. The other methods can
 * throw Kernel errors.
 */
private[read] class DeltaV2StreamingBackend(snapshotManager: DeltaV2SnapshotManager)
  extends DeltaStreamingBackend {

  @volatile private var latestSnapshot: Snapshot = _

  override def update(): Snapshot = {
    latestSnapshot = snapshotManager.loadLatestSnapshot()
    latestSnapshot
  }

  // Loads the latest snapshot if no refreshing call has run yet.
  override def unsafeVolatileSnapshot: Snapshot =
    Option(latestSnapshot).getOrElse(update())

  override def getSnapshotAt(version: Long): Snapshot = snapshotManager.loadSnapshotAt(version)

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    try {
      snapshotManager.checkVersionExists(version, mustBeRecreatable, allowOutOfRange)
    } catch {
      case e: V2VersionNotFoundException =>
        throw VersionNotFoundException(e.getUserVersion, e.getEarliest, e.getLatest)
    }
  }

  override def getActiveCommitAtTime(
      timestamp: Timestamp,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit = {
    val commit = snapshotManager.getActiveCommitAtTime(
      timestamp.getTime, canReturnLastCommit, mustBeRecreatable, canReturnEarliestCommit)
    // Refresh after the lookup, so the latest snapshot is at least as new as the commit, unless
    // `update` returns a cached snapshot.
    // TODO(#5999): optimize duplicate loadLatestSnapshot calls
    update()
    DeltaHistoryManager.Commit(commit.getVersion, commit.getTimestamp)
  }

  override def getSnapshotToValidateProtocol(version: Long): Snapshot = {
    try {
      getSnapshotAt(version)
    } catch {
      case e: UnsupportedTableFeatureException =>
        throw DeltaErrors.unsupportedReaderTableFeaturesInTableException(
          e.getTablePath, e.getUnsupportedFeatures.asScala)
    }
  }
}
