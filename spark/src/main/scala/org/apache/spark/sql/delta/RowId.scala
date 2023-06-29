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

import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey

/**
 * Collection of helpers to handle Row IDs.
 */
object RowId {
  /**
   * Metadata domain for the high water mark stored using a [[DomainMetadata]] action.
   */
  case class RowTrackingMetadataDomain(rowIdHighWaterMark: Long)
      extends JsonMetadataDomain[RowTrackingMetadataDomain] {
    override val domainName: String = RowTrackingMetadataDomain.domainName
  }

  object RowTrackingMetadataDomain extends JsonMetadataDomainUtils[RowTrackingMetadataDomain] {
    override protected val domainName = "delta.rowTracking"

    def unapply(action: Action): Option[RowTrackingMetadataDomain] = action match {
      case d: DomainMetadata if d.domain == domainName => Some(fromJsonConfiguration(d))
      case _ => None
    }

    def isRowTrackingDomain(d: DomainMetadata): Boolean = d.domain == domainName
  }

  val MISSING_HIGH_WATER_MARK: Long = -1L

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
   * Verifies that row IDs are only set as readable when a new table is created.
   */
  private[delta] def verifyMetadata(
      oldProtocol: Protocol,
      newProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Unit = {

    val rowIdsEnabledBefore = isEnabled(oldProtocol, oldMetadata)
    val rowIdsEnabledAfter = isEnabled(newProtocol, newMetadata)

    if (rowIdsEnabledAfter && !rowIdsEnabledBefore && !isCreatingNewTable) {
      throw new UnsupportedOperationException(
        "Cannot enable Row IDs on an existing table.")
    }
  }

  /**
   * Assigns fresh row IDs to all AddFiles inside `actions` that do not have row IDs yet and emits
   * a [[RowIdHighWaterMark]] action with the new high-water mark.
   */
  private[delta] def assignFreshRowIds(
      protocol: Protocol,
      snapshot: Snapshot,
      actions: Iterator[Action]): Iterator[Action] = {
    if (!isSupported(protocol)) return actions

    val oldHighWatermark = extractHighWatermark(snapshot).getOrElse(MISSING_HIGH_WATER_MARK)

    var newHighWatermark = oldHighWatermark

    val actionsWithFreshRowIds = actions.map {
      case a: AddFile if a.baseRowId.isEmpty =>
        val baseRowId = newHighWatermark + 1L
        newHighWatermark += a.numPhysicalRecords.getOrElse {
          throw DeltaErrors.rowIdAssignmentWithoutStats
        }
        a.copy(baseRowId = Some(baseRowId))
      case d: DomainMetadata if RowTrackingMetadataDomain.isRowTrackingDomain(d) =>
        throw new IllegalStateException(
          "Manually setting the Row ID high water mark is not allowed")
      case other => other
    }

    val newHighWatermarkAction: Iterator[Action] = new Iterator[Action] {
      // Iterators are lazy, so the first call to `hasNext` won't happen until after we
      // exhaust the remapped actions iterator. At that point, the watermark (changed or not)
      // decides whether the iterator is empty or infinite; take(1) below to bound it.
      override def hasNext(): Boolean = newHighWatermark != oldHighWatermark
      override def next(): Action = RowTrackingMetadataDomain(newHighWatermark).toDomainMetadata
    }
    actionsWithFreshRowIds ++ newHighWatermarkAction.take(1)
  }

  /**
   * Extracts the high watermark of row IDs from a snapshot.
   */
  private[delta] def extractHighWatermark(snapshot: Snapshot): Option[Long] =
    RowTrackingMetadataDomain.fromSnapshot(snapshot).map(_.rowIdHighWaterMark)

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
}
