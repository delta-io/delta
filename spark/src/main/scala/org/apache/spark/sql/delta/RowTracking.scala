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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol, TableFeatureProtocolUtils}


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
   * Returns the sourceMetadata with the row tracking property coming from the targetMetadata.
   */
  private[delta] def takeRowTrackingPropertyFromTarget(
      targetMetadata: Metadata,
      sourceMetadata: Metadata): Metadata = {
    var newConfig = sourceMetadata.configuration - DeltaConfigs.ROW_TRACKING_ENABLED.key
    targetMetadata.configuration.get(DeltaConfigs.ROW_TRACKING_ENABLED.key).foreach { v =>
      newConfig += DeltaConfigs.ROW_TRACKING_ENABLED.key -> v
    }
    sourceMetadata.copy(configuration = newConfig)
  }

  /**
   * Removes the row tracking property from the metadata.
   */
  private[delta] def removeRowTrackingProperty(metadata: Metadata): Metadata = {
    metadata.copy(configuration = metadata.configuration - DeltaConfigs.ROW_TRACKING_ENABLED.key)
  }

  /**
   * Removes the row tracking table feature from the protocol.
   */
  private[delta] def removeRowTrackingTableFeature(protocol: Protocol): Protocol = {
    val writerFeaturesWithoutRowTracking = protocol.writerFeatures.map(_ - RowTrackingFeature.name)
    protocol.copy(writerFeatures = writerFeaturesWithoutRowTracking)
  }
}
