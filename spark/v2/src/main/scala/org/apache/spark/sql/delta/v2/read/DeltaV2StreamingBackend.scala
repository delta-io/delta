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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaHistoryManager, DeltaSingleCommit, Snapshot, VersionNotFoundException}
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaStreamingBackend}
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator.ClosableWrapper
import org.apache.spark.sql.delta.v2.exception.{VersionNotFoundException => V2VersionNotFoundException}
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager
import io.delta.kernel.exceptions.UnsupportedTableFeatureException


/**
 * The [[DeltaStreamingBackend]] backend that reads the table through Kernel. `checkVersionExists`
 * and `getSnapshotToValidateProtocol` throw the same Delta errors as
 * [[org.apache.spark.sql.delta.sources.DeltaLogStreamingBackend]]. The other methods can
 * throw Kernel errors.
 *
 * @param tableManager provides snapshot, history, and commit access for the source table.
 * @param queryContext request-scoped inputs for every snapshot load. The stream reuses it across
 *                     triggers.
 * @param snapshotVersionAtSourceInit table snapshot version captured at source initialization.
 */
private[read] class DeltaV2StreamingBackend(
    tableManager: DeltaV2TableManager,
    queryContext: DeltaV2QueryContext,
    snapshotVersionAtSourceInit: Long)
  extends DeltaStreamingBackend {

  @volatile private var latestSnapshot: Snapshot = _

  override def update(): Snapshot = {
    latestSnapshot = tableManager.loadLatestSnapshot(queryContext)
    latestSnapshot
  }

  // Loads the latest snapshot if no refreshing call has run yet.
  override def unsafeVolatileSnapshot: Snapshot =
    Option(latestSnapshot).getOrElse(update())

  override def getSnapshotAt(version: Long): Snapshot =
    tableManager.loadSnapshotAt(version, queryContext)

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    try {
      tableManager.checkVersionExists(
        version, mustBeRecreatable, allowOutOfRange, queryContext)
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
    val commit = tableManager.getActiveCommitAtTime(
      timestamp.getTime,
      canReturnLastCommit,
      mustBeRecreatable,
      canReturnEarliestCommit,
      queryContext)
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

  override def getSingleCommitIterator(
      startVersion: Long,
      failOnDataLoss: Boolean,
      endOffset: Option[DeltaSourceOffset]): ClosableIterator[DeltaSingleCommit] = {
    var endVersionOpt = endOffset.map(_.reservoirVersion)
    if (endVersionOpt.isDefined) {
      // Cap endVersion to the latest available version. The Kernel's getTableChanges requires
      // endVersion to be an actual existing version or empty.
      val latestVersion: Long = snapshotVersionAtSourceInit
      if (endVersionOpt.get > latestVersion) {
        // This could happen because:
        // 1. data could be added after the source's initial snapshot was captured.
        // 2. buildOffsetFromIndexedFile bumps the version up by one when we hit the END_INDEX.
        // TODO(#5318): consider caching the latest version to avoid loading a new snapshot.
        // TODO(#5318): kernel should ideally relax this constraint.
        endVersionOpt = Some(update().version)
      }

      // After capping, check if startVersion is beyond the endVersion.
      // This can happen when all files in the batch come from the initial snapshot
      // (e.g., offset was bumped to next version due to END_INDEX, but no new commits exist).
      if (startVersion > endVersionOpt.get) {
        return new ClosableWrapper[DeltaSingleCommit](Iterator.empty).toClosable
      }
    } else {
      // When endOffset is empty (offset discovery), check if startVersion exceeds the current
      // latest version. The snapshot version captured at source initialization may be outdated.
      val currentLatestVersion: Long = update().version
      if (startVersion > currentLatestVersion) {
        return new ClosableWrapper[DeltaSingleCommit](Iterator.empty).toClosable
      }
    }

    tableManager.getSingleCommitIterator(
      startVersion, endVersionOpt, queryContext, failOnDataLoss)
  }
}
