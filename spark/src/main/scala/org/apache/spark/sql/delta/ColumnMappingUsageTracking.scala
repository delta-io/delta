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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

object ColumnMappingUsageTracking {
  /**
   * Updates the table metadata when the column mapping usage tracking feature is first enabled
   * to record whether a column may have been dropped or renamed in the table.
   */
  def updateMetadata(
      protocol: Protocol,
      metadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {
    val configurationIsSet =
      metadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.key)
    val isSupportedInProtocol =
      protocol.isFeatureSupported(ColumnMappingUsageTrackingTableFeature)
    if (configurationIsSet || !isSupportedInProtocol) {
      return metadata
    }

    // We cannot be certain if a column has been dropped or renamed already if the feature is
    // enabled after the table is created. Therefore, we have to assume that a column was already
    // dropped or renamed.
    val mayHaveDroppedOrRenamedColumnBefore =
      metadata.columnMappingMode != NoMapping && !isCreatingNewTable
    metadata.copy(
      configuration = metadata.configuration +
        (DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.key ->
          mayHaveDroppedOrRenamedColumnBefore.toString)
    )
  }

  /**
   * Records that a column has been dropped or renamed in the table properties.
   */
  def trackColumnDropOrRename(protocol: Protocol): Map[String, String] = {
    if (!protocol.isFeatureSupported(ColumnMappingUsageTrackingTableFeature)) {
      Map.empty
    } else {
      Map(DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.key -> "true")
    }
  }

  /**
   * Returns true if the table has dropped or renamed columns before.
   */
  def hasDroppedOrRenamedColumnBefore(metadata: Metadata): Boolean = {
    DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.fromMetaData(metadata)
  }

  /**
   * Filters out the column mapping usage tracking property from the given properties.
   */
  def filterProperties(properties: Map[String, String]): Map[String, String] = {
    properties - DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.key
  }
}
