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

import org.apache.spark.sql.delta.DeltaCommitTag.PreservedRowTrackingTag
import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.actions.CommitInfo

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

/**
 * Utility functions for Row Tracking that are shared between Row IDs and Row Commit Versions.
 */
object RowTracking {
  /**
   * Returns whether the protocol version supports the Row Tracking table feature. Whenever Row
   * Tracking is support, fresh Row IDs and Row Commit Versions must be assigned to all newly
   * committed files, even when Row IDs are disabled in the current table version.
   */
  def isSupported(protocol: Protocol): Boolean = protocol.isFeatureSupported(RowTrackingFeature)

  /**
   * Returns whether Row Tracking is enabled on this table version. Checks that Row Tracking is
   * supported, which is a pre-requisite for enabling Row Tracking, throws an error if not.
   */
  def isEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)
    if (isEnabled && !isSupported(protocol)) {
      throw new IllegalStateException(
        s"Table property '${DeltaConfigs.ROW_TRACKING_ENABLED.key}' is " +
          s"set on the table but this table version doesn't support table feature " +
          s"'${TableFeatureProtocolUtils.propertyKey(RowTrackingFeature)}'.")
    }
    isEnabled
  }

  /**
   * Checks whether CONVERT TO DELTA collects statistics if row tracking is supported. If it does
   * not collect statistics, we cannot assign fresh row IDs, hence we throw an error to either rerun
   * the command without enabling the row tracking table feature, or to enable the necessary
   * flags to collect statistics.
   */
  private[delta] def checkStatsCollectedIfRowTrackingSupported(
      protocol: Protocol,
      convertToDeltaShouldCollectStats: Boolean,
      statsCollectionEnabled: Boolean): Unit = {
    if (!isSupported(protocol)) return
    if (!convertToDeltaShouldCollectStats || !statsCollectionEnabled) {
      throw DeltaErrors.convertToDeltaRowTrackingEnabledWithoutStatsCollection
    }
  }

  /**
   * @return the Row Tracking metadata fields for the file's _metadata
   *         when Row Tracking is enabled.
   */
  def createMetadataStructFields(
      protocol: Protocol,
      metadata: Metadata,
      nullableConstantFields: Boolean,
      nullableGeneratedFields: Boolean): Iterable[StructField] = {
    RowId.createRowIdField(protocol, metadata, nullableGeneratedFields) ++
      RowId.createBaseRowIdField(protocol, metadata, nullableConstantFields) ++
      DefaultRowCommitVersion.createDefaultRowCommitVersionField(
        protocol, metadata, nullableConstantFields) ++
      RowCommitVersion.createMetadataStructField(protocol, metadata, nullableGeneratedFields)
  }

  /**
   * @param preserved The value of [[DeltaCommitTag.PreservedRowTrackingTag.key]] tag
   * @return A copy of ``tagsMap`` with the [[DeltaCommitTag.PreservedRowTrackingTag.key]] tag added
   *         or replaced with the new value.
   */
  private def addPreservedRowTrackingTag(
      tagsMap: Map[String, String],
      preserved: Boolean = true): Map[String, String] = {
    tagsMap + (DeltaCommitTag.PreservedRowTrackingTag.key -> preserved.toString)
  }

  /**
   * Sets the [[DeltaCommitTag.PreservedRowTrackingTag.key]] tag to true if not set. We add the tag
   * to every operation because we assume all operations preserve row tracking by default. The
   * absence of the tag means that row tracking is not preserved.
   * Operations can set the tag to mark row tracking as preserved/not preserved.
   */
  private[delta] def addPreservedRowTrackingTagIfNotSet(
      snapshot: SnapshotDescriptor,
      tagsMap: Map[String, String] = Map.empty): Map[String, String] = {
    if (!isEnabled(snapshot.protocol, snapshot.metadata) ||
      tagsMap.contains(PreservedRowTrackingTag.key)) {
      return tagsMap
    }
    addPreservedRowTrackingTag(tagsMap)
  }

  /**
   * Returns a copy of the CommitInfo passed in with the PreservedRowTrackingTag tag set to false.
   */
  private def addRowTrackingNotPreservedTag(commitInfo: CommitInfo): CommitInfo = {
    val tagsMap = commitInfo.tags.getOrElse(Map.empty[String, String])
    val newCommitInfoTags = addPreservedRowTrackingTag(tagsMap, preserved = false)
    commitInfo.copy(tags = Some(newCommitInfoTags))
  }

  /**
   * Checks whether the CommitInfo has the RowTrackingEnablementOnly tag set to true.
   * If omitted, we assume it is false.
   */
  private def isRowTrackingEnablementOnlyCommit(commitInfo: Option[CommitInfo]): Boolean = {
    DeltaCommitTag
        .getTagValueFromCommitInfo(commitInfo, DeltaCommitTag.RowTrackingEnablementOnlyTag.key)
        .exists(_.toBoolean)
  }

  /**
   * Returns a Boolean indicating whether it is safe the resolve the metadata update conflict
   * between the current and winning transaction conflict, from the perspective of row tracking
   * enablement.
   */
  def canResolveMetadataUpdateConflict(
      currentTransactionInfo: CurrentTransactionInfo,
      winningCommitSummary: WinningCommitSummary): Boolean = {
    if (!isSupported(currentTransactionInfo.protocol)) return false

    RowTracking.isRowTrackingEnablementOnlyCommit(winningCommitSummary.commitInfo) &&
      !currentTransactionInfo.metadataChanged
  }

  /**
   * Update the currentTransactionInfo properly to resolve a metadata update conflict when the
   * winning commit is tagged as RowTrackingEnablementOnly. It is only safe to call this function if
   * [[RowTracking.canResolveMetadataUpdateConflict]] returns true.
   *
   * See [[ConflictCheckerEdge.checkNoMetadataUpdates()]] for more details.
   */
  def resolveRowTrackingEnablementOnlyMetadataUpdateConflict(
      currentTransactionInfo: CurrentTransactionInfo,
      winningCommitSummary: WinningCommitSummary): CurrentTransactionInfo = {
    require(canResolveMetadataUpdateConflict(currentTransactionInfo, winningCommitSummary))
    // If the CommitInfo is None, do nothing because the absence of the
    // [[DeltaCommitTag.PreservedRowTrackingTag]] means it is false.
    val newCommitInfo =
        currentTransactionInfo.commitInfo.map(RowTracking.addRowTrackingNotPreservedTag)

    val newMetadata = winningCommitSummary.metadataUpdates.head

    // OptimisticTransactions sets the metadata seen by the current txn
    // (currentTransactionInfo.metadata) to the updated metadata. To be consistent, let's do this
    // even if the current txn does not update metadata.
    currentTransactionInfo.copy(
      metadata = newMetadata,
      commitInfo = newCommitInfo
    )
  }

  def preserveRowTrackingColumns(
      dfWithoutRowTrackingColumns: DataFrame,
      snapshot: SnapshotDescriptor): DataFrame = {
    val dfWithRowIds = RowId.preserveRowIds(dfWithoutRowTrackingColumns, snapshot)
    RowCommitVersion.preserveRowCommitVersions(dfWithRowIds, snapshot)
  }
}
