/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

object ConcurrentIdentityColumnWriteReservation {

  /**
   * Returns a populated [[IdentityColumnReservation]] if the CIC reservation path
   * should run for this write, else `None`.
   *
   * Gating is table-state-driven:
   *   - Table protocol supports [[ConcurrentIdentityColumnsTableFeature]].
   *   - The schema declares at least one identity column the input data does not
   *     already supply (those are the ones that need new values).
   * An unstamped identity column on a feature table is unconverted legacy state; the
   * reservation's pre-scan fails it loud with the conversion remedy.
   */
  def maybeReserveForWrite(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalog: Option[CatalogTable],
      metadata: Metadata,
      protocol: Protocol,
      data: DataFrame): Option[IdentityColumnReservation] = {
    // Kill switch: when CIC is disabled, a write to a table that still carries CIC sequence
    // pointers must fail loud rather than fall through to the legacy high-water-mark generator
    if (!spark.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED) &&
        metadata.schema.exists(ConcurrentIdentityColumnSchema.hasConcurrentSequenceMetadata)) {
      throw ConcurrentIdentityColumnErrors.concurrentIdentityColumnsDisabled(
        operation = "write to", tableId = metadata.id)
    }
    // Feature gate: the selective routing check, so a non-CIC write (the overwhelming common
    // case) returns here without the per-column work below.
    if (!protocol.isFeatureSupported(ConcurrentIdentityColumnsTableFeature)) return None

    val identityColumns =
      metadata.schema.filter(ColumnWithDefaultExprUtils.isIdentityColumn)
    if (identityColumns.isEmpty) return None

    val columnsByName = CaseInsensitiveMap(data.schema.map(f => f.name -> f).toMap)
    val needsGeneration = identityColumns.filterNot(f => columnsByName.contains(f.name))
    if (needsGeneration.isEmpty) return None

    // Cheap sizing hint only: a planning-time row-count estimate (present for sources that carry
    // stats, e.g. a Delta scan), so CIC INSERT never forces an extra full scan of the input.
    // data.count() was considered but has additional downsides on a non-deterministic /
    // side-effecting source.
    //   Some(0) => provably empty input, nothing to reserve.
    //   Some(n) => non-empty input of ~n rows (a sizing hint; values are reserved on demand).
    //   None    => size unknown without a scan, so reserve rate-based on demand, like MERGE.
    val rowCountEstimate = data.queryExecution.optimizedPlan.stats.rowCount.map(_.toLong)
    if (rowCountEstimate.contains(0L)) return None
    val sizeHint = rowCountEstimate.filter(_ > 0L)

    // Only >0 vs 0 matters now (the empty-source gate); the driver's controller sizes the actual
    // reserve, so any positive fallback (1) is fine when the row count is unknown.
    val seedSize = sizeHint.getOrElse(1L)
    val reservation = new IdentityColumnReservation(deltaLog, catalog, spark)
    reservation.reserveValuesForIdentityColumns(seedSize)
    Some(reservation)
  }
}
