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

/**
 * Thrown by the Concurrent Identity Column (CIC) service-backed reservation guards when a
 * write would emit identity values outside a reserved range. It is a [[DeltaThrowable]] carrying
 * one of the `DELTA_CONCURRENT_IDENTITY_COLUMN_*` error classes, so driver call sites and tests
 * can match on the type or the error class instead of message prose.
 *
 * The executor-side generator guard throws this across the Spark task boundary, so it reaches the
 * driver wrapped in a `SparkException`; tests exercising that path match the error-class token on
 * the cause chain rather than the type.
 */
class ConcurrentIdentityColumnReservationException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends IllegalStateException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass

  override def getMessageParameters: java.util.Map[String, String] =
    DeltaThrowableHelper.getMessageParameters(errorClass, errorSubClass = null, messageParameters)
}

/**
 * Builders for the CIC reservation-guard errors. The message text lives in the Delta
 * error-classes JSON under the `DELTA_CONCURRENT_IDENTITY_COLUMN_*` classes; each
 * builder only names its error class and supplies the parameters, in the order the placeholders
 * appear in the template.
 */
object ConcurrentIdentityColumnErrors {
  // Builders are ordered to match the alphabetical class order in the Delta error-classes JSON.

  /** The column is missing its sequence service pointer; re-run the feature opt-in conversion. */
  def conversionIncomplete(
      columnName: String,
      tableId: String): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_CONVERSION_INCOMPLETE",
      Array(columnName, tableId))

  /**
   * The CIC feature is disabled, blocking the attempted operation. Covers identity-generating
   * writes to service-backed tables, CREATE TABLE with CIC, the feature opt-in conversion and
   * SYNC IDENTITY repair.
   */
  def concurrentIdentityColumnsDisabled(
      operation: String,
      tableId: String): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_DISABLED",
      Array(operation, tableId))

  /**
   * A CIC pull granted an empty range. The driver should never return zero values; this guards
   * the executor generator against a no-progress retry spin.
   */
  def emptyReserveRange(
      sequenceId: String,
      tableId: String): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_EMPTY_RESERVE_RANGE",
      Array(sequenceId, tableId))

  /**
   * Service-returned metadata disagrees with the schema-declared metadata. Generalized from the
   * old STEP_MISMATCH so the same class covers start/other metadata drift later: the `property`
   * param names which one drifted (value "step" today).
   */
  def metadataMismatch(
      grantedStep: Long,
      sequenceId: String,
      columnStep: Long): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_METADATA_MISMATCH",
      Array("step", grantedStep.toString, sequenceId, columnStep.toString))

  /**
   * A schema-stamped sequenceId is gone from the service. Not recoverable on the write path:
   * reseeding a fresh counter could hand out values colliding with already-written rows, so
   * the table needs deliberate repair.
   */
  def sequenceNotFound(
      sequenceId: String,
      columnName: String,
      tableId: String): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_SEQUENCE_NOT_FOUND",
      Array(columnName, tableId, sequenceId))

  /**
   * A single call to the UC identity-sequence service exceeded the bounded wait
   * (`spark.databricks.delta.identityColumn.concurrent.uc.timeoutMs`) before it returned. The
   * service is likely temporarily unavailable (e.g. disabled), which the managed-catalog client
   * would otherwise retry for minutes; we fail fast instead so the caller is not blocked.
   */
  def serviceTimeout(
      tableId: String,
      timeoutMs: Long): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_SERVICE_TIMEOUT",
      Array(tableId, timeoutMs.toString))

  /**
   * A CIC table would carry more service-backed identity columns than the configured maximum
   * (`spark.databricks.delta.identityColumn.concurrent.maxColumnsPerTable`). Thrown by the
   * CREATE/REPLACE stamping hook and the feature opt-in conversion before any sequence is minted.
   */
  def tooManyColumns(
      tableId: String,
      numColumns: Int,
      maxColumns: Int): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_TOO_MANY_COLUMNS",
      Array(tableId, numColumns.toString, maxColumns.toString))

  /**
   * Defends against an unexpected write path that reaches the legacy high-water-mark
   * generator on a service-backed (sequence-stamped) table. Every wired write path
   * (INSERT, all three MERGE variants) reserves from the service before writing, so this
   * is a backstop: if a future or otherwise-unhandled path skipped the reservation, its
   * legacy generator would silently emit duplicate identity values, so we abort before
   * any data is written rather than corrupt the table.
   */
  def usingWrongGenerator(
      columnNames: Seq[String],
      tableId: String): ConcurrentIdentityColumnReservationException =
    new ConcurrentIdentityColumnReservationException(
      "DELTA_CONCURRENT_IDENTITY_COLUMN_USING_WRONG_GENERATOR",
      Array(columnNames.mkString(", "), tableId))
}
