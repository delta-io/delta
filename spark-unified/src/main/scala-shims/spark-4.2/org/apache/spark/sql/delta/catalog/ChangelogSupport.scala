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

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.read.changelog.DeltaChangelog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Mixed into a [[TableCatalog]] implementation to add Auto-CDF support. Provides the
 * catalog-driven `TableCatalog.loadChangelog` entrypoint introduced by SPARK-56685.
 *
 * <p>This trait extends [[TableCatalog]] as a dependency marker: every concrete catalog that
 * mixes this trait in must already be a `TableCatalog`. The trait itself does not provide a
 * `TableCatalog` implementation.
 *
 * <p>The trait is intentionally thin. `loadChangelog` resolves the table via the catalog's own
 * `loadTable`, validates that the result is a V2 [[SparkTable]] (read-time CDF only flows
 * through the V2 connector. V1 tables go through the legacy Delta CDF path), resolves the
 * requested [[ChangelogRange]] against the table's snapshot manager, and wraps everything into
 * a [[DeltaChangelog]]. All connector-level work (loading snapshots, validating row tracking,
 * inspecting metadata actions) is deferred to the read path inside [[DeltaChangelog]].
 *
 * <p>The whole entry point is gated by [[DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED]] (default
 * `false`). When the flag is off the trait delegates to the parent `loadChangelog` default,
 * which surfaces `UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE`.
 */
trait ChangelogSupport extends TableCatalog {

  override def loadChangelog(ident: Identifier, changelogInfo: ChangelogInfo): Changelog = {
    val spark = SparkSession.active
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED)) {
      // Feature gated off: fall back to the parent's default, which surfaces
      // UNSUPPORTED_FEATURE.CHANGE_DATA_CAPTURE to the user.
      return super.loadChangelog(ident, changelogInfo)
    }
    val sparkTable = loadTable(ident) match {
      case st: SparkTable => st
      case other =>
        // Auto-CDF only supports the V2 connector. V1 Delta tables (DeltaTableV2 under the
        // hood) keep going through the legacy CDF path that DeltaCatalog already exposes.
        DeltaErrors.throwChangelogRequiresV2Table(ident.toString, other.getClass.getName)
    }
    val (startVersion, endVersion) = resolveRange(sparkTable, changelogInfo.range())
    new DeltaChangelog(ident.name(), sparkTable, startVersion, endVersion)
  }

  /**
   * Resolves a [[ChangelogRange]] against the snapshot manager owned by the resolved table.
   *
   * <p>Returned bounds have inclusivity already applied (exclusive start adds 1, exclusive end
   * subtracts 1) and are validated. `UnboundedRange` is rejected on batch reads.
   */
  private def resolveRange(
      sparkTable: SparkTable,
      range: org.apache.spark.sql.connector.catalog.ChangelogRange): (Long, Long) = {
    val snapshotManager = sparkTable.getSnapshotManager
    val latestVersion = snapshotManager.loadLatestSnapshot().getVersion
    range match {
      case vr: VersionRange =>
        val rawStart = vr.startingVersion().toLong
        val rawEnd: Long =
          if (vr.endingVersion().isPresent) vr.endingVersion().get.toLong else latestVersion
        adjustBounds(
          rawStart, rawEnd, vr.startingBoundInclusive(), vr.endingBoundInclusive(), latestVersion)
      case tr: TimestampRange =>
        // TimestampRange carries Catalyst micros. The kernel API takes millis.
        val rawStart = snapshotManager
          .getActiveCommitAtTime(
            tr.startingTimestamp / 1000,
            /* canReturnLastCommit */ false,
            /* mustBeRecreatable */ true,
            /* canReturnEarliestCommit */ false)
          .getVersion
        val rawEnd: Long = if (tr.endingTimestamp.isPresent) {
          snapshotManager
            .getActiveCommitAtTime(
              tr.endingTimestamp.get / 1000,
              /* canReturnLastCommit */ true,
              /* mustBeRecreatable */ true,
              /* canReturnEarliestCommit */ false)
            .getVersion
        } else {
          latestVersion
        }
        adjustBounds(
          rawStart, rawEnd, tr.startingBoundInclusive(), tr.endingBoundInclusive(), latestVersion)
      case _: UnboundedRange =>
        DeltaErrors.throwChangelogUnboundedRange()
    }
  }

  /**
   * Apply per-bound inclusivity (`+1` / `-1`) and verify the resulting range is non-empty and
   * within the table's commit history.
   */
  private def adjustBounds(
      start: Long,
      end: Long,
      startInclusive: Boolean,
      endInclusive: Boolean,
      latest: Long): (Long, Long) = {
    val adjustedStart = if (startInclusive) start else start + 1
    val adjustedEnd = if (endInclusive) end else end - 1
    if (adjustedStart > adjustedEnd) {
      throw DeltaErrors.endBeforeStartVersionInCDC(adjustedStart, adjustedEnd)
    }
    if (adjustedStart > latest) {
      throw DeltaErrors.startVersionAfterLatestVersion(adjustedStart, latest)
    }
    (adjustedStart, adjustedEnd)
  }
}
