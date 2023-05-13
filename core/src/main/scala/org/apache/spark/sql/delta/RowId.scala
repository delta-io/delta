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

import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol, RowIdHighWaterMark}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/**
 * Collection of helpers to handle Row IDs.
 */
object RowId {

  val MISSING_HIGH_WATER_MARK: RowIdHighWaterMark = RowIdHighWaterMark(highWaterMark = -1L)

  /**
   * Returns whether Row IDs can be written to Delta tables and read from Delta tables. This acts as
   * a feature flag during development: every Row ID code path should be hidden behind this flag and
   * behave as if Row IDs didn't exist when this returns false to avoid leaking an incomplete
   * implementation.
   */
  def isAllowed(spark: SparkSession): Boolean = spark.conf.get(DeltaSQLConf.ROW_IDS_ALLOWED)

  /**
   * Returns whether the protocol version supports the Row ID table feature. Whenever Row IDs are
   * supported, fresh Row IDs must be assigned to all newly committed files, even when Row IDs are
   * disabled in the current table version.
   */
  def isSupported(protocol: Protocol): Boolean = RowTracking.isSupported(protocol)

  /**
   * Returns whether Row IDs are enabled on this table version. Checks that Row IDs are supported,
   * which is a pre-requisite for enabling Row IDs, throws an error if not.
   */
  def isEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)
    if (isEnabled && !isSupported(protocol)) {
      throw new IllegalStateException(
        s"Table property '${DeltaConfigs.ROW_TRACKING_ENABLED.key}' is " +
        s"set on the table but this table version doesn't support table feature " +
        s"'${propertyKey(RowTrackingFeature)}'.")
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
    if (!isAllowed(spark)) return newMetadata
    val latestMetadata = if (isCreatingNewTable && isSupported(protocol)) {
      val newConfig = newMetadata.configuration + (DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true")
      newMetadata.copy(configuration = newConfig)
    } else {
      newMetadata
    }

    val rowIdsEnabledBefore = isEnabled(protocol, oldMetadata)
    val rowIdsEnabledAfter = isEnabled(protocol, latestMetadata)

    if (rowIdsEnabledAfter && !rowIdsEnabledBefore && !isCreatingNewTable) {
      throw new UnsupportedOperationException(
        "Cannot enable Row IDs on an existing table.")
    }
    latestMetadata
  }

  /**
   * Assigns fresh row IDs to all AddFiles inside `actions` that do not have row IDs yet and emits
   * a [[RowIdHighWaterMark]] action with the new high-water mark.
   */
  private[delta] def assignFreshRowIds(
      spark: SparkSession,
      protocol: Protocol,
      snapshot: Snapshot,
      actions: Iterator[Action]): Iterator[Action] = {
    if (!isAllowed(spark) || !isSupported(protocol)) return actions

    val oldHighWatermark = extractHighWatermark(spark, snapshot)
        .getOrElse(MISSING_HIGH_WATER_MARK)
        .highWaterMark
    var newHighWatermark = oldHighWatermark

    val actionsWithFreshRowIds = actions.map {
      case a: AddFile if a.baseRowId.isEmpty =>
        val baseRowId = newHighWatermark + 1L
        newHighWatermark += a.numPhysicalRecords.getOrElse {
          throw new UnsupportedOperationException(
            "Cannot assign row IDs without row count statistics.")
        }
        a.copy(baseRowId = Some(baseRowId))
      case _: RowIdHighWaterMark =>
        throw new IllegalStateException(
          "Manually setting the Row ID high water mark is not allowed")
      case other => other
    }

    val newHighWatermarkAction: Iterator[Action] = new Iterator[Action] {
      // Iterators are lazy, so the first call to `hasNext` won't happen until after we
      // exhaust the remapped actions iterator. At that point, the watermark (changed or not)
      // decides whether the iterator is empty or infinite; take(1) below to bound it.
      override def hasNext(): Boolean = newHighWatermark != oldHighWatermark
      override def next(): Action = RowIdHighWaterMark(newHighWatermark)
    }
    actionsWithFreshRowIds ++ newHighWatermarkAction.take(1)
  }

  /**
   * Extracts the high watermark of row IDs from a snapshot.
   */
  private[delta] def extractHighWatermark(
      spark: SparkSession, snapshot: Snapshot): Option[RowIdHighWaterMark] = {
    if (isAllowed(spark)) {
      snapshot.rowIdHighWaterMarkOpt
    } else {
      None
    }
  }

  private[delta] def extractHighWatermark(
      spark: SparkSession, actions: Seq[Action]): Option[RowIdHighWaterMark] = {
    if (isAllowed(spark)) {
      actions.collectFirst { case r: RowIdHighWaterMark => r }
    } else {
      None
    }
  }

  /**
   * Checks whether CONVERT TO DELTA collects statistics if row tracking is supported. If it does
   * not collect statistics, we cannot assign fresh row IDs, hence we throw an error to either rerun
   * the command without enabling the row tracking table feature, or to enable the necessary
   * flags to collect statistics.
   */
  private[delta] def checkStatsCollectedIfRowTrackingSupported(
      spark: SparkSession,
      protocol: Protocol,
      convertToDeltaShouldCollectStats: Boolean,
      statsCollectionEnabled: Boolean): Unit = {
    if (!isAllowed(spark) || !isSupported(protocol)) return
    if (!convertToDeltaShouldCollectStats || !statsCollectionEnabled) {
      throw DeltaErrors.convertToDeltaRowTrackingEnabledWithoutStatsCollection
    }
  }
}
