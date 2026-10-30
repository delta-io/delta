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
    val hasSequencePointer =
      ConcurrentIdentityColumnSchema.hasConcurrentSequenceMetadata(metadata.schema)
    val featureSupported = protocol.isFeatureSupported(ConcurrentIdentityColumnsTableFeature)

    if (!featureSupported && !hasSequencePointer) return None

    // Kill switch: when CIC is disabled, a write to a table that still carries CIC sequence
    // pointers must fail loud rather than fall through to the legacy high-water-mark generator
    if (!spark.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED) &&
        hasSequencePointer) {
      throw ConcurrentIdentityColumnErrors.concurrentIdentityColumnsDisabled(
        operation = "write to", tableId = metadata.id)
    }

    val identityColumns =
      metadata.schema.filter(ColumnWithDefaultExprUtils.isIdentityColumn)
    val columnsByName = CaseInsensitiveMap(data.schema.map(f => f.name -> f).toMap)
    val needsGeneration = identityColumns.filterNot(f => columnsByName.contains(f.name))

    // The feature was removed but a per-column sequence pointer remains.
    if (ConcurrentIdentityColumnSchema.isOrphaned(protocol, metadata.schema)) {
      if (needsGeneration.nonEmpty) {
        throw ConcurrentIdentityColumnErrors.orphanedSequencePointerRequiresSync(metadata.id)
      }
      return None
    }

    // The feature is supported. Reserve only when the write needs new identity values.
    if (needsGeneration.isEmpty) return None

    // Cheap sizing hint only: a planning-time row-count estimate (present for sources that carry
    // stats, e.g. a Delta scan), so CIC INSERT never forces an extra full scan of the input.
    // data.count() was considered but has additional downsides on a non-deterministic /
    // side-effecting source.
    //   Some(n) => ~n input rows, passed on as a sizing hint (values are reserved on demand).
    //   None    => size unknown without a scan, so reserve rate-based on demand, like MERGE.
    // An estimate of 0 is not proof of an empty input (stale stats, CBO estimates), so it is
    // treated as unknown (None) and still sets up the full reservation.
    val sizeHint = data.queryExecution.optimizedPlan.stats.rowCount.map(_.toLong).filter(_ > 0L)

    val reservation = new IdentityColumnReservation(deltaLog, catalog, spark)
    reservation.reserveValuesForIdentityColumns(sizeHint)
    Some(reservation)
  }
}
