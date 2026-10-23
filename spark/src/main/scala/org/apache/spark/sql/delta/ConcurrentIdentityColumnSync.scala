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

import org.apache.spark.sql.delta.cic.IdentitySequenceServices
import org.apache.spark.sql.delta.cic.{CreateSequenceRequest, ReserveIdsRequest}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.StructField

/**
 * `ALTER TABLE ... ALTER COLUMN c SYNC IDENTITY` for Concurrent Identity Columns (CIC).
 *
 * Like legacy SYNC ([[IdentityColumn.syncIdentity]]), this reconciles the effective high-water
 * mark with the data. The difference is WHERE that mark lives: legacy writes the schema
 * `delta.identity.highWaterMark`, but for a service-backed CIC column generation reads the
 * external sequence, so [[syncIdentity]] rescans the data, mints a FRESH sequence under the
 * column's original start/step, advances its counter past the data extreme by reserving the
 * already-covered range, and re-stamps `delta.identity.concurrent.sequenceId`.
 *
 * Reconciling the service sequence against the ACTUAL DATA lives here, not in conversion:
 * conversion ([[ConcurrentIdentityColumnConversion]]) trusts the schema HWM and never scans,
 * SYNC is the single place that reads the data extreme to repair drift. SYNC only ever moves
 * the service sequence forward (it reseeds past the data); it never lowers it.
 *
 * SYNC does NOT convert between identity backends. Joining or leaving the service backend is
 * a table-level conversion expressed as DDL (opting into the CIC table feature via
 * ALTER TABLE ... SET TBLPROPERTIES, or leaving it via ALTER TABLE ... DROP FEATURE); a
 * column without a sequence pointer is unconverted state and is refused here.
 *
 * The re-stamp is a *normal* (conflict-causing) metadata change; the caller must NOT mark the
 * commit `setSyncIdentity()` (that would make concurrent writers tolerate it) but must
 * `readWholeTable()` so a racing write conflicts. The service `createSequence` happens here,
 * before the commit, so a failed commit only leaves a harmless orphaned sequence.
 */
object ConcurrentIdentityColumnSync {

  /** True iff SYNC on this table should use the CIC path (the table opts into the feature). */
  def isCicTable(snapshot: Snapshot): Boolean =
    snapshot.protocol.isFeatureSupported(ConcurrentIdentityColumnsTableFeature)

  /**
   * Repair one CIC identity column: scan the data extreme, mint a fresh sequence under the
   * original start/step whose counter is advanced strictly past that extreme, and return the
   * re-stamped field.
   *
   * @param snapshot the table snapshot SYNC is reading.
   * @param field the identity column to sync; must carry a sequence pointer.
   * @param df the table data (used to scan the actual max/min).
   */
  def syncIdentity(snapshot: Snapshot, field: StructField, df: DataFrame): StructField = {
    assert(ColumnWithDefaultExprUtils.isIdentityColumn(field))
    // SYNC needs an existing service-backed column; it does not convert one.
    if (ConcurrentIdentityColumnSchema.getSequenceId(field).isEmpty) {
      throw ConcurrentIdentityColumnErrors.conversionIncomplete(field.name, snapshot.metadata.id)
    }
    val info = IdentityColumn.getIdentityInfo(field)
    // The extreme the next auto value must clear: max for an ascending column, min for a
    // descending one. `None` for an empty table.
    val extreme: Option[Long] = {
      val expr = if (info.step > 0) max(field.name) else min(field.name)
      val row = df.select(expr).collect().head
      if (row.isNullAt(0)) None else Some(row.getLong(0))
    }
    // First lattice value strictly past the extreme (`start` for an empty table), and the number
    // of already-covered values the fresh counter must advance by: (seed - start) / step.
    val (seed, usedCount) = try {
      val seed = extreme
        .map(e => Math.addExact(IdentityColumn.roundToNext(info.start, info.step, e), info.step))
        .getOrElse(info.start)
      (seed, Math.subtractExact(seed, info.start) / info.step)
    } catch {
      case e: ArithmeticException =>
        IdentityOverflowLogger.logOverflow()
        throw e
    }
    // The driver generates the fresh sequenceId up front and passes it to the
    // service, then stamps the same value; the service stores it rather than minting one.
    val sequenceId = java.util.UUID.randomUUID().toString
    val service = IdentitySequenceServices.resolve(df.sparkSession)
    val tableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(snapshot.metadata)
    // Register the sequence with the column's original start/step and set it to the correct value.
    service.createSequence(
      CreateSequenceRequest(
        sequenceId = sequenceId,
        tableId = tableId,
        start = info.start,
        step = info.step,
        columnInformation = Some(DeltaColumnMapping.getPhysicalName(field))))
    if (usedCount > 0L) {
      service.reserveIds(ReserveIdsRequest(
        sequenceId = sequenceId,
        tableId = tableId,
        count = usedCount,
        step = info.step))
    }
    ConcurrentIdentityColumnSchema.withConcurrentSequenceMetadata(field, sequenceId)
  }
}
