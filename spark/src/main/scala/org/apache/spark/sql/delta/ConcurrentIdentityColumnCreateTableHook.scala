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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{Metadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.spark.sql.delta.cic.{
  CreateSequenceRequest,
  DropSequenceRequest,
  IdentitySequenceService
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

/**
 * CREATE TABLE-time helpers for Concurrent Identity Columns (CIC) that split the work into
 * two halves:
 *
 *   1. [[maybeStampSequenceMetadata]] (pre-commit): per identity column, generate a unique
 *      `sequenceId` and stamp it into the column's StructField metadata under
 *      `delta.identity.concurrent.sequenceId` (see [[ConcurrentIdentityColumnSchema]]). This
 *      runs during metadata preparation, before the CREATE TABLE Delta log is written, so the
 *      stamped id is part of the committed schema. No service call happens here.
 *   2. [[createSequencesForCommittedSchema]] (post-commit): once CREATE TABLE has committed and
 *      `table_id` (the Delta `metadata.id`) is known, register each stamped column's `sequenceId`
 *      with the sequence service via `createSequence(CreateOrGet)`. start/step come from the
 *      standard `delta.identity.*` keys and are not duplicated.
 *
 * Splitting the service call out of the stamping hook keeps the stamp and the registration
 * separate: the driver stamps the id pre-commit and only calls the service after the commit,
 * when `table_id` resolves in `mc_tables`.
 *
 * Stamping is gated on two conditions, if either is false, the metadata is
 * returned unchanged:
 *   - The new table's properties opt into
 *     [[ConcurrentIdentityColumnsTableFeature]] (the conversion decision is
 *     table-level, no session conf is consulted).
 *   - At least one column is an identity column.
 *
 * The `tableId` passed to the service is resolved via
 * [[ConcurrentIdentityColumnSchema.sequenceServiceTableId]]: the UC table id for
 * catalog-owned tables, the Delta `metadata.id` only as a TEST-ONLY fallback.
 *
 * Idempotency: stamping skips columns that already carry concurrent sequence metadata, and
 * `createSequence` is CreateOrGet, so a post-commit retry registering the same id is safe.
 */
object ConcurrentIdentityColumnCreateTableHook extends DeltaLogging {

  /**
   * Returns a possibly-updated copy of `metadata` with concurrent sequence
   * metadata stamped onto identity columns. No-op if any gating condition is
   * not met. Does NOT call the service; registration happens post-commit via
   * [[createSequencesForCommittedSchema]].
   */
  def maybeStampSequenceMetadata(
      sparkSession: SparkSession,
      metadata: Metadata): Metadata = {
    if (!shouldStamp(sparkSession, metadata)) {
      metadata
    } else if (!sparkSession.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED)) {
      // Kill switch: refuse rather than skip
      throw ConcurrentIdentityColumnErrors.concurrentIdentityColumnsDisabled(
        operation = "create", tableId = metadata.id)
    } else {
      val schema = metadata.schema
      ConcurrentIdentityColumnSchema.checkColumnLimit(
        sparkSession, metadata, schema.count(ColumnWithDefaultExprUtils.isIdentityColumn))
      val stampedFields = schema.map(stampColumnIfNeeded)
      // Check if nothing changed to skip the expensive copy.
      val anyStamped = stampedFields.zip(schema).exists { case (out, in) => out ne in }
      if (!anyStamped) {
        metadata
      } else {
        // Rebuild via `schema.copy(fields = ...)` rather than `StructType(...)`
        // so any future schema-level metadata (`StructType` carries none today
        // but may evolve) is preserved across the stamp.
        metadata.copy(schemaString = schema.copy(fields = stampedFields.toArray).json)
      }
    }
  }

  private def stampColumnIfNeeded(field: StructField): StructField = {
    if (!ColumnWithDefaultExprUtils.isIdentityColumn(field) ||
        ConcurrentIdentityColumnSchema.hasConcurrentSequenceMetadata(field)) {
      field
    } else {
      // Double-check isIdentityColumn: make sure all required keys exist.
      val md = field.metadata
      require(md.contains(DeltaSourceUtils.IDENTITY_INFO_START) &&
          md.contains(DeltaSourceUtils.IDENTITY_INFO_STEP) &&
          md.contains(DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT),
        s"Identity column ${field.name} is missing one of the required " +
          s"legacy metadata keys (${DeltaSourceUtils.IDENTITY_INFO_START}, " +
          s"${DeltaSourceUtils.IDENTITY_INFO_STEP}, " +
          s"${DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT}). " +
          "isIdentityColumn returned true but the trio is incomplete.")
      // The driver generates the sequenceId up front and stamps it. Generating it here before the
      // commit means the same value is both committed into the schema and later registered with
      // the service post-commit.
      val sequenceId = java.util.UUID.randomUUID().toString
      ConcurrentIdentityColumnSchema.withConcurrentSequenceMetadata(field, sequenceId)
    }
  }

  /**
   * Post-commit registration of the sequences stamped into the committed schema. For every
   * identity column carrying a `delta.identity.concurrent.sequenceId` pointer, calls the
   * service's idempotent `createSequence(CreateOrGet)` using the resolved service table id
   * ([[ConcurrentIdentityColumnSchema.sequenceServiceTableId]]: the UC table id for
   * catalog-owned tables, the `metadata.id` only as a TEST-ONLY fallback for
   * path-based tables and the local stub) and the column's start/step from the standard
   * `delta.identity.*` keys.
   */
  def createSequencesForCommittedSchema(
      postCommitSnapshot: Snapshot,
      service: IdentitySequenceService): Unit = {
    val committedMetadata = postCommitSnapshot.metadata
    val tableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(committedMetadata)
    for {
      field <- committedMetadata.schema
      sequenceId <- ConcurrentIdentityColumnSchema.getSequenceId(field)
    } {
      val info = IdentityColumn.getIdentityInfo(field)
      service.createSequence(CreateSequenceRequest(
        sequenceId = sequenceId,
        tableId = tableId,
        start = info.start,
        step = info.step,
        columnInformation = Some(DeltaColumnMapping.getPhysicalName(field))))
      ConcurrentIdentityColumnObservability.recordCreateSequence(
        provider = postCommitSnapshot,
        tableId = tableId,
        sequenceId = sequenceId,
        start = info.start,
        step = info.step,
        columnName = field.name)
    }
  }

  /**
   * REPLACE retires the replaced table's identity sequences. REPLACE strips the pre-replace stamps
   * and mints fresh sequence ids (see [[commands.CreateDeltaTableCommand]]). Drops each one no
   * longer present in the committed schema, under the pre-replace table scope.
   */
  def dropReplacedSequences(
      preReplaceSnapshot: Snapshot,
      postCommitSnapshot: Snapshot,
      service: IdentitySequenceService): Unit = {
    val oldTableId =
      ConcurrentIdentityColumnSchema.sequenceServiceTableId(preReplaceSnapshot.metadata)
    val liveSequenceIds = postCommitSnapshot.metadata.schema
      .flatMap(ConcurrentIdentityColumnSchema.getSequenceId).toSet
    for {
      field <- preReplaceSnapshot.metadata.schema
      sequenceId <- ConcurrentIdentityColumnSchema.getSequenceId(field)
      if !liveSequenceIds.contains(sequenceId)
    } {
      try {
        service.dropSequence(DropSequenceRequest(sequenceId, oldTableId))
        ConcurrentIdentityColumnObservability.recordDropSequence(
          provider = preReplaceSnapshot,
          tableId = oldTableId,
          sequenceId = sequenceId,
          reason = ConcurrentIdentityColumnObservability.DropReason.Replace)
      } catch {
        case NonFatal(e) =>
          logWarning(s"REPLACE TABLE could not retire identity sequence $sequenceId for table " +
            s"$oldTableId; it stays orphaned in the service.", e)
          ConcurrentIdentityColumnObservability.recordDropSequenceFailed(
            provider = preReplaceSnapshot,
            tableId = oldTableId,
            sequenceId = sequenceId,
            reason = ConcurrentIdentityColumnObservability.DropReason.Replace)
      }
    }
  }

  /**
   * DROP TABLE also retires the dropped table's identity sequences. The caller captures the
   * snapshot before the drop, while the schema still carries the `sequenceId` pointers, and
   * invokes this only after the drop succeeds.
   */
  def dropSequencesForDroppedTable(
      snapshot: Snapshot,
      service: IdentitySequenceService): Unit = {
    val tableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(snapshot.metadata)
    for {
      field <- snapshot.metadata.schema
      sequenceId <- ConcurrentIdentityColumnSchema.getSequenceId(field)
    } {
      try {
        service.dropSequence(DropSequenceRequest(sequenceId, tableId))
        ConcurrentIdentityColumnObservability.recordDropSequence(
          provider = snapshot,
          tableId = tableId,
          sequenceId = sequenceId,
          reason = ConcurrentIdentityColumnObservability.DropReason.DropTable)
      } catch {
        case NonFatal(e) =>
          logWarning(s"DROP TABLE could not retire identity sequence $sequenceId for table " +
            s"$tableId; it stays orphaned in the service.", e)
          ConcurrentIdentityColumnObservability.recordDropSequenceFailed(
            provider = snapshot,
            tableId = tableId,
            sequenceId = sequenceId,
            reason = ConcurrentIdentityColumnObservability.DropReason.DropTable)
      }
    }
  }

  /**
   * Checks the gating conditions in isolation so callers (and tests) can reason about
   * them without invoking the service. Stamping is table-state-driven: opting into the
   * CIC feature at CREATE TABLE is the conversion decision, no session conf is consulted.
   */
  def shouldStamp(sparkSession: SparkSession, metadata: Metadata): Boolean =
    hasCicFeature(metadata) &&
      ColumnWithDefaultExprUtils.hasIdentityColumn(metadata.schema)

  private def hasCicFeature(metadata: Metadata): Boolean =
    TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(metadata.configuration)
      .contains(ConcurrentIdentityColumnsTableFeature)
}
