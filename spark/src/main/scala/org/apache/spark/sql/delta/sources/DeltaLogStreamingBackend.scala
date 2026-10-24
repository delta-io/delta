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

import org.apache.spark.sql.delta.{DeltaHistoryManager, DeltaLog, Snapshot}

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * The [[DeltaStreamingBackend]] backend that reads the table through a [[DeltaLog]].
 *
 * @param catalogTableOpt returns the catalog table to use for each read. A function, because a
 *                        streaming source can refresh its catalog table while it runs.
 */
private[delta] class DeltaLogStreamingBackend(
    deltaLog: DeltaLog,
    catalogTableOpt: () => Option[CatalogTable]) extends DeltaStreamingBackend {

  override def update(): Snapshot = deltaLog.update(catalogTableOpt = catalogTableOpt())

  override def unsafeVolatileSnapshot: Snapshot = deltaLog.unsafeVolatileSnapshot

  override def getSnapshotAt(version: Long): Snapshot =
    deltaLog.getSnapshotAt(version, catalogTableOpt = catalogTableOpt())

  override def checkVersionExists(
      version: Long,
      mustBeRecreatable: Boolean,
      allowOutOfRange: Boolean): Unit = {
    // The streaming source has always checked the version without the catalog table. Keep that,
    // because the catalog table changes the latest version that the check compares against.
    deltaLog.history.checkVersionExists(
      version, catalogTableOpt = None, mustBeRecreatable, allowOutOfRange)
  }

  // `DeltaHistoryManager.getActiveCommitAtTime` updates the Delta log.
  override def getActiveCommitAtTime(
      timestamp: Timestamp,
      canReturnLastCommit: Boolean,
      mustBeRecreatable: Boolean,
      canReturnEarliestCommit: Boolean): DeltaHistoryManager.Commit =
    deltaLog.history.getActiveCommitAtTime(
      timestamp,
      catalogTableOpt = catalogTableOpt(),
      canReturnLastCommit = canReturnLastCommit,
      mustBeRecreatable = mustBeRecreatable,
      canReturnEarliestCommit = canReturnEarliestCommit)

  override def getSnapshotToValidateProtocol(version: Long): Snapshot = getSnapshotAt(version)
}
