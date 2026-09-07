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

import org.apache.spark.sql.delta.actions.{Action, Metadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.cic.IdentitySequenceServices
import org.apache.spark.sql.delta.cic.CreateSequenceRequest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Table-level conversion of identity columns to the service backend, triggered by opting the
 * table into [[ConcurrentIdentityColumnsTableFeature]] via
 * `ALTER TABLE ... SET TBLPROPERTIES ('delta.feature.concurrentIdentityColumns_preview' =
 * 'supported')`.
 *
 * For every identity column without a service-sequence pointer, [[convertOnFeatureOptIn]]
 * mints a sequence seeded from the stock schema high-water mark alone and stamps
 * `delta.identity.concurrent.sequenceId`. The hook is idempotent: already-stamped columns are
 * untouched, so re-running the property set on a table that already supports the feature
 * converts an unstamped column in place.
 *
 * Conversion is metadata-only: it trusts the schema HWM (a correctly-maintained stock identity
 * column keeps its HWM at or past the max value it emitted) and does NOT scan the data to guard
 * against a HWM that lags it. Reconciling the sequence against the actual data is
 * [[ConcurrentIdentityColumnSync]]'s job (SYNC IDENTITY = repair only). This keeps conversion
 * cheap: a stock identity table with a multi-petabyte history converts without a full scan.
 *
 * This is idempotent, NOT a replace: it only stamps columns that lack a pointer and leaves an
 * already-stamped column (and its live sequence) untouched. Minting a fresh sequence for an
 * identity column that already has one, dropping the superseded sequence, is REPLACE TABLE's job
 * ([[ConcurrentIdentityColumnCreateTableHook.dropReplacedSequences]], since REPLACE re-stamps under
 * a new table id); reconciling an existing sequence against the data is
 * [[ConcurrentIdentityColumnSync]]'s; leaving the service backend is the DROP FEATURE
 * pre-downgrade.
 *
 * The service `createSequence` happens before the ALTER commit, so a failed commit only leaves
 * harmless orphaned sequences; the caller's transaction is marked `readWholeTable` so a racing
 * identity write conflicts with the conversion.
 */
object ConcurrentIdentityColumnConversion {

  /**
   * True iff this property map marks the CIC feature as supported (the table-feature opt-in).
   * Delegates to the shared [[TableFeatureProtocolUtils.isFeatureSupportedInTableConfigs]], which
   * does the case-insensitive `delta.feature.*` key comparison against `supported` -- the canonical
   * opt-in status a `SET TBLPROPERTIES` ALTER produces.
   */
  def containsCicFeatureEnablement(configuration: Map[String, String]): Boolean =
    TableFeatureProtocolUtils.isFeatureSupportedInTableConfigs(
      configuration, ConcurrentIdentityColumnsTableFeature)

  /**
   * Convert the unstamped identity columns of `newMetadata` to the service backend.
   *
   * @return the (possibly re-stamped) metadata; `(newMetadata, Nil)` when there is nothing
   *         to convert. The action sequence is always empty (kept for the commit-site shape).
   */
  def convertOnFeatureOptIn(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      newMetadata: Metadata): (Metadata, Seq[Action]) = {
    val identityColumns = newMetadata.schema.filter(ColumnWithDefaultExprUtils.isIdentityColumn)
    if (identityColumns.isEmpty) return (newMetadata, Nil)
    ConcurrentIdentityColumnSchema.checkColumnLimit(
      sparkSession, newMetadata, identityColumns.length)
    val unstamped =
      identityColumns.filter(f => ConcurrentIdentityColumnSchema.getSequenceId(f).isEmpty)
    if (unstamped.isEmpty) {
      // Already fully converted: keep the ALTER a plain no-op property set.
      return (newMetadata, Nil)
    }

    val unstampedNames = unstamped.map(_.name).toSet
    val converted = newMetadata.schema.map { field =>
      if (unstampedNames.contains(field.name)) {
        stampFromSchemaHighWaterMark(sparkSession, txn.snapshot, field)
      } else {
        field
      }
    }
    // A racing identity write that commits before the conversion must conflict it (same
    // contract as SYNC repair: the re-stamp is a normal conflict-causing metadata change).
    txn.readWholeTable()
    (newMetadata.copy(schemaString = StructType(converted).json), Nil)
  }

  // Mint a sequence for one unstamped identity column, seeded from the stock schema high-water
  // mark alone, and stamp the returned sequence id. No data scan: conversion trusts the HWM and
  // leaves data reconciliation to SYNC (see the object doc).
  private def stampFromSchemaHighWaterMark(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      field: StructField): StructField = {
    val info = IdentityColumn.getIdentityInfo(field)
    // First lattice value strictly past the schema HWM; `start` when the column has never
    // emitted (no HWM). The HWM already follows start/step, so a single step clears it.
    val seed = info.highWaterMark
      .map(h => Math.addExact(h, info.step))
      .getOrElse(info.start)
    // The driver generates the fresh sequenceId up front and passes it to the service, then stamps
    // the same value; the service stores it rather than minting one.
    val sequenceId = java.util.UUID.randomUUID().toString
    IdentitySequenceServices.resolve(sparkSession).createSequence(
      CreateSequenceRequest(
        sequenceId = sequenceId,
        tableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(snapshot.metadata),
        start = seed,
        step = info.step,
        columnInformation = Some(DeltaColumnMapping.getPhysicalName(field))))
    ConcurrentIdentityColumnSchema.withConcurrentSequenceMetadata(field, sequenceId)
  }
}
