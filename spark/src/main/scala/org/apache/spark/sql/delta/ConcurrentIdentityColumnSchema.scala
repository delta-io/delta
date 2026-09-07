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

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

/**
 * Schema-metadata for Concurrent Identity Columns (CIC), persisted on identity column
 * StructFields.
 *
 * A CIC column carries a single extra key, the sequence ID
 * `delta.identity.concurrent.sequenceId`, alongside the standard
 * `delta.identity.{start, step, allowExplicitInsert}` keys. Those standard keys remain the
 * source of truth for start/step (read via [[IdentityColumn.getIdentityInfo]]); we do not
 * duplicate them here. The key name is the cross-system contract with Unity Catalog: UC's
 * registration hook extracts this exact key from the schema it processes, so it must match
 * the catalog side verbatim.
 *
 * Presence of [[SEQUENCE_ID]] is the canonical signal that a column is backed by a
 * service-allocated sequence. The reservation path reads it from here.
 */
object ConcurrentIdentityColumnSchema {
  val SEQUENCE_ID = "delta.identity.concurrent.sequenceId"

  /**
   * The table id under which this table's sequences live in the sequence service. For a
   * catalog-owned (UC coordinated-commits) table this is the UC table id, stamped into the
   * table configuration at creation ([[UCCommitCoordinatorClient.UC_TABLE_ID_KEY]]).
   * The `metadata.id` fallback below is test-only, as CIC requires a table to be managed.
   */
  def sequenceServiceTableId(metadata: Metadata): String =
    metadata.configuration.getOrElse(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, metadata.id)

  /**
   * Rejects a CIC table with more identity columns than
   * [[DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_MAX_COLUMNS_PER_TABLE]]. Called by both producers of
   * service-backed columns (the CREATE/REPLACE stamping hook and the feature opt-in conversion)
   * before any sequence is minted, so the bound holds regardless of which path adds the columns.
   */
  def checkColumnLimit(
      sparkSession: SparkSession,
      metadata: Metadata,
      identityColumnCount: Int): Unit = {
    val maxCicColumnsPerTable =
      sparkSession.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_MAX_COLUMNS_PER_TABLE)
    if (identityColumnCount > maxCicColumnsPerTable) {
      throw ConcurrentIdentityColumnErrors.tooManyColumns(
        metadata.id, identityColumnCount, maxCicColumnsPerTable)
    }
  }

  /** True iff this column carries the sequence ID. */
  def hasConcurrentSequenceMetadata(field: StructField): Boolean =
    field.metadata.contains(SEQUENCE_ID)

  /** True iff any column in `schema` carries the sequence ID. */
  def hasConcurrentSequenceMetadata(schema: StructType): Boolean =
    schema.exists(hasConcurrentSequenceMetadata)

  /** The driver-generated sequence id for this column, if present. */
  def getSequenceId(field: StructField): Option[String] =
    if (hasConcurrentSequenceMetadata(field)) Some(field.metadata.getString(SEQUENCE_ID)) else None

  /**
   * Names of the columns in `schema` that carry the sequence ID. Non-empty only
   * on a table stamped with sequence IDs; empty on a stock-identity table. Used by
   * the reservation downgrade guard to refuse a flag-off write that would otherwise pick the
   * wrong backend.
   */
  def stampedSequenceColumnNames(schema: StructType): Seq[String] =
    schema.filter(hasConcurrentSequenceMetadata).map(_.name)

  /**
   * Returns a copy of `metadata` with the sequence ID stripped from every
   * identity column. Use this before invoking the create-table stamping hook on a path
   * where the input schema may carry over a stale sequence ID (e.g. REPLACE TABLE), so the
   * hook's idempotency short-circuit cannot reuse a sequenceId minted for a different
   * table id. SYNC's downgrade path also uses it to drop a dangling sequence ID.
   */
  def stripConcurrentSequenceMetadata(metadata: Metadata): Metadata = {
    val schema = metadata.schema
    val stripped = schema.map { field =>
      if (!hasConcurrentSequenceMetadata(field)) {
        field
      } else {
        withoutConcurrentSequenceMetadata(field)
      }
    }
    val anyStripped = stripped.zip(schema).exists { case (out, in) => out ne in }
    if (!anyStripped) {
      metadata
    } else {
      metadata.copy(schemaString = schema.copy(fields = stripped.toArray).json)
    }
  }

  /** Returns a copy of `field` with the sequence ID removed. */
  def withoutConcurrentSequenceMetadata(field: StructField): StructField =
    field.copy(metadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .remove(SEQUENCE_ID)
      .build())

  /**
   * Returns a copy of `field` with the sequence ID attached. Existing
   * metadata is preserved, including the standard `delta.identity.*` keys.
   */
  def withConcurrentSequenceMetadata(field: StructField, sequenceId: String): StructField =
    field.copy(metadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putString(SEQUENCE_ID, sequenceId)
      .build())
}
