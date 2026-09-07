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

import scala.concurrent.duration._

import com.databricks.spark.util.MetricDefinitions
import com.databricks.spark.util.TagDefinitions.TAG_OP_TYPE
import org.apache.spark.sql.delta.metering.{DeltaLogging, DeltaLoggingProvider}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.delta.metering.LogThrottler

/**
 * Usage-event instrumentation for the Concurrent Identity Columns (CIC) sequence service.
 *
 * Every opType shares the `delta.identityColumn.concurrent.` prefix (one `startswith` selects the
 * feature), and every event reports `tableId` as the service table id
 * ([[ConcurrentIdentityColumnSchema.sequenceServiceTableId]]), never the Delta `metadata.id`, so
 * ids join across events.
 *
 * Throttled opTypes carry the suppressed count in `numSkippedEvents`, which recovers volume but
 * not payloads: averages over them (range size, park rounds, total time) sample quiet periods.
 */
object ConcurrentIdentityColumnObservability extends DeltaLogging {

  val opTypeCreateSequence = "delta.identityColumn.concurrent.createSequence"
  val opTypeReserve = "delta.identityColumn.concurrent.reserve"
  val opTypeServedRequest = "delta.identityColumn.concurrent.servedRequest"
  val opTypeSequenceSummary = "delta.identityColumn.concurrent.sequenceSummary"
  val opTypeServiceTimeout = "delta.identityColumn.concurrent.serviceTimeout"
  val opTypeDropSequence = "delta.identityColumn.concurrent.dropSequence"
  val opTypeDropSequenceFailed = "delta.identityColumn.concurrent.dropSequenceFailed"
  val opTypeDowngradeUnstampedSkipped =
    "delta.identityColumn.concurrent.downgrade.unstampedSkipped"
  val opTypeRefillFailed = "delta.identityColumn.concurrent.refillFailed"
  val opTypeParkedOnEviction = "delta.identityColumn.concurrent.parkedOnEviction"
  val opTypeEndpointRegistrationFailed =
    "delta.identityColumn.concurrent.endpointRegistrationFailed"

  case class SequenceSummaryIdentity(tableId: String, sequenceId: String)


  case class SequenceSummaryMetrics(
      servedRequestCount: Long,
      totalIdsHandedOut: Long,
      parkEnqueueCount: Long,
      refillKickedCount: Long,
      refillIgnoredCount: Long,
      refillFailedCount: Long,
      refillsLanded: Long,
      lifetime: FiniteDuration)

  /** Which path retired a sequence; one value per retirement site. */
  object DropReason {
    val Replace = "replace"
    val DropColumn = "dropColumn"
    val DropFeature = "dropFeature"
    val DropTable = "dropTable"
    val SyncReseed = "syncReseed"
  }

  // reserve + servedRequest are the same reserve seen from either end, so one bucket caps both.
  private val volumeThrottler = new LogThrottler(bucketSize = 5, tokenRecoveryInterval = 1.second)

  // Timeouts get their own bucket, else a busy sequence spends every token on reserves and starves
  // the rarest, most diagnostic event out of the log just as the service degrades.
  private val timeoutThrottler = new LogThrottler(bucketSize = 5, tokenRecoveryInterval = 1.second)

  /**
   * Post-commit: a sequence was registered with the service at CREATE/REPLACE TABLE. Driver-side,
   * so it carries the table's common tags via `provider` (the post-commit snapshot).
   */
  def recordCreateSequence(
      provider: DeltaLoggingProvider,
      tableId: String,
      sequenceId: String,
      start: Long,
      step: Long,
      columnName: String): Unit =
    recordDelta(provider, opTypeCreateSequence, Map(
      "tableId" -> tableId,
      "sequenceId" -> sequenceId,
      "start" -> start.toString,
      "step" -> step.toString,
      "columnName" -> columnName))

  /**
   * Post-commit: a sequence was retired. Mirror of [[recordCreateSequence]], so a create with no
   * matching drop is an orphan. Unthrottled: bounded by identity columns per DDL, never per row.
   */
  def recordDropSequence(
      provider: DeltaLoggingProvider,
      tableId: String,
      sequenceId: String,
      reason: String): Unit =
    recordDelta(provider, opTypeDropSequence, Map(
      "tableId" -> tableId,
      "sequenceId" -> sequenceId,
      "reason" -> reason))

  /**
   * Post-commit: retirement failed, orphaning the sequence. Every drop path is best-effort, so this
   * event is the only trace the orphan leaves.
   */
  def recordDropSequenceFailed(
      provider: DeltaLoggingProvider,
      tableId: String,
      sequenceId: String,
      reason: String): Unit =
    recordDelta(provider, opTypeDropSequenceFailed, Map(
      "tableId" -> tableId,
      "sequenceId" -> sequenceId,
      "reason" -> reason))

  /**
   * Executor: a task overshot its reserved range and reserved more mid-write. Stays on the plain
   * `recordEvent` path: executors have no DeltaLog, so there is no [[DeltaLoggingProvider]] to
   * attach the table's common tags. Throttled: capped per JVM, suppressed count in the payload.
   */
  def recordReserve(
      tableId: String,
      sequenceId: String,
      count: Long,
      rangeStart: Long,
      rangeEnd: Long,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long): Unit =
    volumeThrottler.throttled { numSkipped =>
      record(opTypeReserve, Map(
        "tableId" -> tableId,
        "sequenceId" -> sequenceId,
        "count" -> count.toString,
        "rangeStart" -> rangeStart.toString,
        "rangeEnd" -> rangeEnd.toString,
        "stageId" -> stageId.toString,
        "partitionId" -> partitionId.toString,
        "taskAttemptId" -> taskAttemptId.toString,
        "numSkippedEvents" -> numSkipped.toString))
    }

  /**
   * A bounded-wait call to the identity-sequence service timed out. Emitted on the plain
   * `recordEvent` path (the service layer has no DeltaLog). Throttled: a disabled/unavailable
   * service times out every reserve across a wide job, so an unthrottled event would flood; the
   * suppressed count rides along. `operation` is create / reserve / drop.
   */
  def recordServiceTimeout(
      operation: String,
      tableId: String,
      timeoutMs: Long): Unit =
    timeoutThrottler.throttled { numSkipped =>
      record(opTypeServiceTimeout, Map(
        "operation" -> operation,
        "tableId" -> tableId,
        "timeoutMs" -> timeoutMs.toString,
        "numSkippedEvents" -> numSkipped.toString))
    }

  /**
   * Driver: a background refill failed. Also tallied in `sequenceSummary.refillFailedCount`, but
   * that only lands at eviction, so this is the only per-occurrence signal. Throttled: a broken
   * service fails every refill.
   */
  def recordRefillFailed(tableId: String, sequenceId: String, error: Throwable): Unit =
    timeoutThrottler.throttled { numSkipped =>
      record(opTypeRefillFailed, Map(
        "tableId" -> tableId,
        "sequenceId" -> sequenceId,
        "errorClass" -> error.getClass.getName,
        "numSkippedEvents" -> numSkipped.toString))
    }

  /**
   * Driver: a buffer was evicted with reserves still parked, which the coordinator treats as
   * unreachable (refill completion clears parking well inside the TTL). Assertion-grade, so
   * unthrottled -- one per evicted buffer at most.
   */
  def recordParkedOnEviction(tableId: String, sequenceId: String, parkedCount: Int): Unit =
    record(opTypeParkedOnEviction, Map(
      "tableId" -> tableId,
      "sequenceId" -> sequenceId,
      "parkedCount" -> parkedCount.toString))

  /**
   * Driver: the coordinator endpoint could not be registered, so no reservation can succeed on this
   * driver. Once per driver at most.
   */
  def recordEndpointRegistrationFailed(error: Throwable): Unit =
    record(opTypeEndpointRegistrationFailed, Map(
      "errorClass" -> error.getClass.getName))

  /**
   * Driver: one executor reserve request was fully served (buffer hit, or after parking + refill).
   * `parkRounds` is how many distribute passes granted this caller 0 before it was served (0 on a
   * hit); `totalTime` is receipt to reply. Throttled; suppressed count in the payload.
   */
  def recordServedRequest(
      tableId: String,
      sequenceId: String,
      desiredCount: Long,
      actualCount: Long,
      parkRounds: Long,
      totalTime: FiniteDuration,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long): Unit =
    volumeThrottler.throttled { numSkipped =>
      record(opTypeServedRequest, Map(
        "tableId" -> tableId,
        "sequenceId" -> sequenceId,
        "desiredCount" -> desiredCount.toString,
        "actualCount" -> actualCount.toString,
        "parkRounds" -> parkRounds.toString,
        "totalTimeMs" -> totalTime.toMillis.toString,
        "stageId" -> stageId.toString,
        "partitionId" -> partitionId.toString,
        "taskAttemptId" -> taskAttemptId.toString,
        "numSkippedEvents" -> numSkipped.toString))
    }

  /** Driver: a `(tableId, sequenceId)` buffer was evicted. */
  def recordSequenceSummary(
      identity: SequenceSummaryIdentity,
      metrics: SequenceSummaryMetrics): Unit =
    record(opTypeSequenceSummary, Map(
      "tableId" -> identity.tableId,
      "sequenceId" -> identity.sequenceId,
      "servedRequestCount" -> metrics.servedRequestCount.toString,
      "totalIdsHandedOut" -> metrics.totalIdsHandedOut.toString,
      "parkEnqueueCount" -> metrics.parkEnqueueCount.toString,
      "refillKickedCount" -> metrics.refillKickedCount.toString,
      "refillIgnoredCount" -> metrics.refillIgnoredCount.toString,
      "refillFailedCount" -> metrics.refillFailedCount.toString,
      "refillsLanded" -> metrics.refillsLanded.toString,
      "lifetimeMs" -> metrics.lifetime.toMillis.toString))

  /**
   * Driver emit: routes through `recordDeltaEvent` so the event carries the table's common tags
   * from `provider`. `recordDeltaEvent` serializes `data` to JSON internally.
   */
  private def recordDelta(
      provider: DeltaLoggingProvider,
      opType: String,
      data: Map[String, String]): Unit =
    recordDeltaEvent(provider, opType, data = data)

  /** Executor emit: no DeltaLog available, so use the plain `recordEvent` path. */
  private def record(opType: String, data: Map[String, String]): Unit =
    recordEvent(
      MetricDefinitions.EVENT_TAHOE,
      Map(TAG_OP_TYPE -> opType),
      JsonUtils.toJson(data))

}
