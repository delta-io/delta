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
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/**
 * Collection of helpers to handle Row IDs.
 */
object RowId {

  /**
   * Returns whether Row IDs can be written to Delta tables and read from Delta tables. This acts as
   * a feature flag during development: every Row ID code path should be hidden behind this flag and
   * behave as if Row IDs didn't exist when this returns false to avoid leaking an incomplete
   * implementation.
   */
  def rowIdsAllowed(spark: SparkSession): Boolean = {
    spark.conf.get(DeltaSQLConf.ROW_IDS_ALLOWED)
  }

  /**
   * Returns whether the protocol version supports the Row ID table feature. Whenever Row IDs are
   * supported, fresh Row IDs must be assigned to all newly committed files, even when Row IDs are
   * disabled in the current table version.
   */
  def rowIdsSupported(protocol: Protocol): Boolean = {
    protocol.isFeatureSupported(RowIdFeature)
  }

  /**
   * Returns whether Row IDs are enabled on this table version. Checks that Row IDs are supported,
   * which is a pre-requisite for enabling Row IDs, throws an error if not.
   */
  def rowIdsEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ROW_IDS_ENABLED.fromMetaData(metadata)
    if (isEnabled && !rowIdsSupported(protocol)) {
      throw new IllegalStateException(s"Table property '${DeltaConfigs.ROW_IDS_ENABLED.key}' is" +
        s"set on the table but this table version doesn't support table feature " +
        s"'${propertyKey(RowIdFeature)}'.")
    }
    isEnabled
  }

  /**
   * Marks row IDs as readable if the row ID writer feature is enabled on a new table and
   * verifies that row IDs are only set as readable when a new table is created.
   */
  private[delta] def verifyAndUpdateMetadata(
      spark: SparkSession,
      protocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {
    if (!rowIdsAllowed(spark)) return newMetadata
    val latestMetadata = if (isCreatingNewTable && rowIdsSupported(protocol)) {
      val newConfig = newMetadata.configuration + (DeltaConfigs.ROW_IDS_ENABLED.key -> "true")
      newMetadata.copy(configuration = newConfig)
    } else {
      newMetadata
    }

    val rowIdsEnabledBefore = rowIdsEnabled(protocol, oldMetadata)
    val rowIdsEnabledAfter = rowIdsEnabled(protocol, latestMetadata)

    if (rowIdsEnabledAfter && !rowIdsEnabledBefore && !isCreatingNewTable) {
      throw new UnsupportedOperationException(
        "Cannot enable Row IDs on an existing table.")
    }
    latestMetadata
  }
}
