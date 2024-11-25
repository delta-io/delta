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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Action, CommitInfo, Metadata}
import org.apache.spark.sql.util.ScalaExtensions._

object InCommitTimestampUtils {

  /** Returns true if the current transaction implicitly/explicitly enables ICT. */
  def didCurrentTransactionEnableICT(
      currentTransactionMetadata: Metadata,
      readSnapshot: Snapshot): Boolean = {
    // If ICT is currently enabled, and the read snapshot did not have ICT enabled,
    // then the current transaction must have enabled it.
    // In case of a conflict, any winning transaction that enabled it after
    // our read snapshot would have caused a metadata conflict abort
    // (see [[ConflictChecker.checkNoMetadataUpdates]]), so we know that
    // all winning transactions' ICT enablement status must match the read snapshot.
    //
    // WARNING: The Metadata() of InitialSnapshot can enable ICT by default. To ensure that
    // this function returns true if ICT is enabled during the first commit, we explicitly handle
    // the case where the readSnapshot.version is -1.
    val isICTCurrentlyEnabled =
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(currentTransactionMetadata)
    val wasICTEnabledInReadSnapshot = readSnapshot.version != -1 &&
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(readSnapshot.metadata)
    isICTCurrentlyEnabled && !wasICTEnabledInReadSnapshot
  }

  /**
   * Returns the updated [[Metadata]] with inCommitTimestamp enablement related info
   * (version and timestamp) correctly set.
   * This is done only
   * 1. If this transaction enables inCommitTimestamp.
   * 2. If the commit version is not 0. This is because we only need to persist
   *  the enablement info if there are non-ICT commits in the Delta log.
   * Note: This function must only be called after transaction conflicts have been resolved.
   */
  def getUpdatedMetadataWithICTEnablementInfo(
      inCommitTimestamp: Long,
      readSnapshot: Snapshot,
      metadata: Metadata,
      commitVersion: Long): Option[Metadata] = {
    Option.when(didCurrentTransactionEnableICT(metadata, readSnapshot) && commitVersion != 0) {
      val enablementTrackingProperties = Map(
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key -> commitVersion.toString,
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key -> inCommitTimestamp.toString)
      metadata.copy(configuration = metadata.configuration ++ enablementTrackingProperties)
    }
  }

  def getValidatedICTEnablementInfo(metadata: Metadata): Option[DeltaHistoryManager.Commit] = {
    val enablementTimestampOpt =
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(metadata)
    val enablementVersionOpt =
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(metadata)
    (enablementTimestampOpt, enablementVersionOpt) match {
      case (Some(enablementTimestamp), Some(enablementVersion)) =>
        Some(DeltaHistoryManager.Commit(enablementVersion, enablementTimestamp))
      case (None, None) =>
        None
      case _ =>
        throw new IllegalStateException(
          "Both enablement version and timestamp should be present or absent together.")
    }
  }
}
