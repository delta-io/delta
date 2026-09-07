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
import org.apache.spark.sql.delta.cic.CreateSequenceRequest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.StructField

/**
 * `ALTER TABLE ... ALTER COLUMN c SYNC IDENTITY` for Concurrent Identity Columns (CIC).
 *
 * Like legacy SYNC ([[IdentityColumn.syncIdentity]]), this reconciles the effective high-water
 * mark with the data. The difference is WHERE that mark lives: legacy writes the schema
 * `delta.identity.highWaterMark`, but for a service-backed CIC column generation reads the
 * external sequence, so [[syncIdentity]] rescans the data and reseeds the service just past it,
 * minting a FRESH sequence and re-stamping `delta.identity.concurrent.sequenceId`. Reseeding past
 * the data advances the effective high-water mark exactly as legacy SYNC does. As with legacy
 * SYNC it doubles as repair: it covers both a normal reconcile (explicit inserts the service never
 * saw) and a deleted/restarted-service repair.
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
   * Repair one CIC identity column: scan the data extreme, mint a fresh sequence seeded
   * strictly past it, and return the re-stamped field.
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
    // First lattice value strictly past the extreme; `start` for an empty table.
    val seed = extreme
      .map(e => Math.addExact(IdentityColumn.roundToNext(info.start, info.step, e), info.step))
      .getOrElse(info.start)
    // The driver generates the fresh sequenceId up front and passes it to the
    // service, then stamps the same value; the service stores it rather than minting one.
    val sequenceId = java.util.UUID.randomUUID().toString
    IdentitySequenceServices.resolve(df.sparkSession).createSequence(
      CreateSequenceRequest(
        sequenceId = sequenceId,
        tableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(snapshot.metadata),
        start = seed,
        step = info.step,
        columnInformation = Some(DeltaColumnMapping.getPhysicalName(field))))
    ConcurrentIdentityColumnSchema.withConcurrentSequenceMetadata(field, sequenceId)
  }
}
