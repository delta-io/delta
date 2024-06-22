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
      nullable: Boolean): Iterable[StructField] = {
    RowId.createRowIdField(protocol, metadata, nullable) ++
      RowId.createBaseRowIdField(protocol, metadata) ++
      DefaultRowCommitVersion.createDefaultRowCommitVersionField(protocol, metadata) ++
      RowCommitVersion.createMetadataStructField(protocol, metadata, nullable)
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

  def preserveRowTrackingColumns(
      dfWithoutRowTrackingColumns: DataFrame,
      snapshot: SnapshotDescriptor): DataFrame = {
    val dfWithRowIds = RowId.preserveRowIds(dfWithoutRowTrackingColumns, snapshot)
    RowCommitVersion.preserveRowCommitVersions(dfWithRowIds, snapshot)
  }
}
