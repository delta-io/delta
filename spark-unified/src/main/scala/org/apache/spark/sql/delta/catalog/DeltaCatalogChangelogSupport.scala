/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import java.sql.Timestamp
import java.util.Optional

import org.apache.hadoop.fs.Path

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.rowtracking.RowTracking
import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.read.changelog.DeltaChangelog
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory
import io.delta.spark.internal.v2.utils.{SchemaUtils => V2SchemaUtils}

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo, Identifier}
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Adds Auto-CDF [[loadChangelog]] support to [[AbstractDeltaCatalog]].
 *
 * Lives in the spark-unified module because the implementation references sparkV2
 * classes (SparkTable, DeltaChangelog, SnapshotManagerFactory, V2SchemaUtils) which
 * the sparkV1 module that hosts [[AbstractDeltaCatalog]] cannot depend on.
 */
abstract class DeltaCatalogChangelogSupport extends AbstractDeltaCatalog {

  override def loadChangelog(ident: Identifier, changelogInfo: ChangelogInfo): Changelog = {
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED)) {
      // Feature gated off: fall back to the parent's default, which surfaces
      // UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE to the user.
      return super.loadChangelog(ident, changelogInfo)
    }
    changelogInfo.range() match {
      case vr: VersionRange =>
        val catalogTable = resolveDeltaCatalogTable(ident)
        val resolvedEndingVersion: Option[Long] =
          if (vr.endingVersion().isPresent()) Some(vr.endingVersion().get.toLong) else None

        buildChangelog(
          ident,
          catalogTable,
          vr.startingVersion().toLong,
          resolvedEndingVersion,
          vr.startingBoundInclusive(),
          vr.endingBoundInclusive())
      case tr: TimestampRange =>
        val catalogTable = resolveDeltaCatalogTable(ident)
        val deltaLog = DeltaLog.forTable(spark, catalogTable)
        // TimestampRange carries Catalyst micros; java.sql.Timestamp expects millis.
        val startTs = new Timestamp(tr.startingTimestamp / 1000)
        val startVersion = deltaLog.history
          .getActiveCommitAtTime(startTs, Some(catalogTable), false, true)
          .version
        val endVersion: Option[Long] = if (tr.endingTimestamp.isPresent()) {
          val endTs = new Timestamp(tr.endingTimestamp.get / 1000)
          Some(deltaLog.history
            .getActiveCommitAtTime(endTs, Some(catalogTable), true, true)
            .version)
        } else {
          None
        }

        buildChangelog(
          ident,
          catalogTable,
          startVersion,
          endVersion,
          tr.startingBoundInclusive(),
          tr.endingBoundInclusive())
      case _: UnboundedRange =>
        throw DeltaErrors.changelogUnboundedRange()
    }
  }

  private def buildChangelog(
      ident: Identifier,
      catalogTable: CatalogTable,
      startVersion: Long,
      endingVersion: Option[Long],
      startingBoundInclusive: Boolean,
      endingBoundInclusive: Boolean): DeltaChangelog = {
    val tablePath = new Path(catalogTable.location).toString()
    val deltaLog = DeltaLog.forTable(spark, catalogTable)
    val engine = DefaultEngine.create(deltaLog.newDeltaHadoopConf())
    val snapshotManager =
      SnapshotManagerFactory.create(tablePath, engine, Optional.of(catalogTable))

    // The latest snapshot is needed to default the ending version when the user did not
    // specify one, and to surface a clear "start > latest" error before issuing a snapshot
    // load that would otherwise fail with a low-level kernel error.
    val latestVersion = snapshotManager.loadLatestSnapshot().getVersion()
    val nonOptEndingVersion = endingVersion.getOrElse(latestVersion)
    val (start, end) = validateAndAdjustVersionRange(
      startVersion,
      nonOptEndingVersion,
      latestVersion,
      startingBoundInclusive,
      endingBoundInclusive)

    // Validate row tracking on the START snapshot (the leftmost commit the reader consumes).
    // Validating on the latest snapshot would pass when row tracking was enabled mid-life and
    // then misread a range whose start predates that change.
    val startSnapshot = snapshotManager.loadSnapshotAt(start)
    val startSnapshotImpl = startSnapshot.asInstanceOf[SnapshotImpl]
    if (!RowTracking.isEnabled(startSnapshotImpl.getProtocol, startSnapshotImpl.getMetadata)) {
      throw DeltaErrors.changelogRequiresRowTracking(ident.name())
    }

    val schema = V2SchemaUtils.convertKernelSchemaToSparkSchema(startSnapshot.getSchema())
    new DeltaChangelog(ident.name(), schema, snapshotManager, startSnapshot, start, end)
  }

  /** Resolve a Delta-backed CatalogTable for the given identifier, or fail with a clear error. */
  private def resolveDeltaCatalogTable(ident: Identifier): CatalogTable = {
    loadTable(ident) match {
      case st: SparkTable => st.getCatalogTable().orElseThrow()
      case dt: DeltaTableV2 =>
        dt.catalogTable.getOrElse(throw new IllegalStateException(
          s"Delta table `${ident.name()}` has no catalogTable; loadChangelog cannot proceed."))
      case other =>
        throw new IllegalStateException(
          "loadChangelog only supports SparkTable and DeltaTableV2; got " +
            other.getClass.getName)
    }
  }

  /**
   * Apply the bounds-inclusivity adjustments and range validations for a VersionRange.
   * Returns the (start, end) commit versions to read.
   */
  private def validateAndAdjustVersionRange(
      start: Long,
      end: Long,
      latest: Long,
      startingBoundInclusive: Boolean,
      endingBoundInclusive: Boolean): (Long, Long) = {
    val adjustedStart = if (!startingBoundInclusive) start + 1 else start
    val adjustedEnd = if (!endingBoundInclusive) end - 1 else end

    if (adjustedStart > adjustedEnd) {
      throw DeltaErrors.endBeforeStartVersionInCDC(adjustedStart, adjustedEnd)
    }
    if (adjustedStart > latest) {
      throw DeltaErrors.startVersionAfterLatestVersion(adjustedStart, latest)
    }
    (adjustedStart, adjustedEnd)
  }
}
