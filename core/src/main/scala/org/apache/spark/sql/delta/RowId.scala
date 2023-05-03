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

  val MISSING_HIGH_WATER_MARK: RowIdHighWaterMark =
    RowIdHighWaterMark(highWaterMark = -1L, preservedRowIds = false)

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
      throw new IllegalStateException(s"Table property '${DeltaConfigs.ROW_IDS_ENABLED.key}' is " +
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

  /**
   * Iterator to assign fresh row IDs to AddFiles in an iterator of actions.
   */
  private class RowIdAssigningIterator(
      actions: Iterator[Action],
      highWatermark: Option[Long]) extends Iterator[Action] {

    val oldHighWatermark: Long = highWatermark.getOrElse(-1L)
    var newHighWatermark: Long = oldHighWatermark

    // Keep iterating until we processed all actions and we emitted the new high-water mark if it
    // changed.
    override def hasNext: Boolean = actions.hasNext || (newHighWatermark != oldHighWatermark)

    override def next(): Action = {
      if (actions.hasNext) {
        actions.next() match {
          case a: AddFile if a.baseRowId.isEmpty =>
            val baseRowId = newHighWatermark + 1L
            if (a.numPhysicalRecords.isEmpty) {
              throw new UnsupportedOperationException(
                "Cannot assign row IDs without row count statistics.")
            }
            newHighWatermark += a.numPhysicalRecords.get
            a.copy(baseRowId = Some(baseRowId))
          case a => a
        }
      } else {
        val watermarkToCommit = newHighWatermark
        newHighWatermark = oldHighWatermark // reset high watermark to make hasNext return false
        RowIdHighWaterMark(watermarkToCommit, preservedRowIds = false)
      }
    }
  }

  /**
   * Assigns fresh row IDs to all AddFiles inside `actions` that do not have row IDs yet.
   */
  private[delta] def assignFreshRowIds(
      spark: SparkSession,
      protocol: Protocol,
      snapshot: Snapshot,
      actions: Iterator[Action]): Iterator[Action] = {
    if (rowIdsAllowed(spark) && rowIdsSupported(protocol)) {
      val highWatermark = extractHighWatermark(spark, snapshot).map(_.highWaterMark)
      new RowIdAssigningIterator(actions, highWatermark)
    } else {
      actions
    }
  }

  /**
   * Reassigns newly assigned Row IDs (i.e. Row IDs above `readHighWaterMark`) of all AddFile
   * actions in `actions` to be above `winningHighWaterMarkOpt` (if it is defined), and
   * updates the high watermark in `actions`.
   */
  private[delta] def reassignOverlappingRowIds(
      readHighWaterMark: Long,
      winningHighWaterMark: Long,
      actions: Seq[Action]): (Seq[Action], RowIdHighWaterMark) = {
    assert(winningHighWaterMark >= readHighWaterMark)
    val watermarkDiff = winningHighWaterMark - readHighWaterMark
    val newActions = actions.map {
      case a: AddFile =>
        // We should only update the row IDs that were assigned by this transaction, and not the
        // row IDs that were assigned by an earlier transaction and merely copied over to a new
        // AddFile as part of this transaction. I.e., we should only update the base row IDs that
        // are larger than the read high watermark.
        a.baseRowId match {
          case Some(baseRowId) if baseRowId > readHighWaterMark =>
            val newBaseRowId = baseRowId + watermarkDiff
            a.copy(baseRowId = Some(newBaseRowId))
          case _ =>
            a
        }

      case RowIdHighWaterMark(v, preservedRowIds) =>
        RowIdHighWaterMark(v + watermarkDiff, preservedRowIds)

      case a => a
    }
    val newHighWaterMark = actions
      .collectFirst { case r: RowIdHighWaterMark => r }
      .getOrElse(RowId.MISSING_HIGH_WATER_MARK)
    newActions -> newHighWaterMark
  }

  /**
   * Extracts the high watermark of row IDs from a snapshot.
   */
  private[delta] def extractHighWatermark(
      spark: SparkSession, snapshot: Snapshot): Option[RowIdHighWaterMark] = {
    if (rowIdsAllowed(spark)) {
      snapshot.rowIdHighWaterMarkOpt
    } else {
      None
    }
  }

  private[delta] def extractHighWatermark(
      spark: SparkSession, actions: Seq[Action]): Option[RowIdHighWaterMark] = {
    if (rowIdsAllowed(spark)) {
      actions.collectFirst { case r: RowIdHighWaterMark => r }
    } else {
      None
    }
  }

  private[delta] def verifyRowIdHighWaterMarkNotSet(actions: Seq[Action]): Unit = {
    if (actions.exists(_.isInstanceOf[RowIdHighWaterMark])) {
      throw new IllegalStateException("Manually setting the Row ID high water mark is not allowed")
    }
  }
}
