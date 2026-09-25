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

// Lives in an `org.apache.spark.*` package because the RpcEnv / RpcEndpoint types it uses are
// `private[spark]`.
package org.apache.spark.sql.delta.cic

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.ConcurrentIdentityColumnObservability
import org.apache.spark.sql.delta.cic.{ReserveBuffer, ReserveConfig}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.cic.{IdentitySequenceService, ReserveIdsRequest}
import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.cic.IdentitySequenceCoordinatorMessage._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * A reserved, non-overlapping range of identity values: `rangeStart + i * step` for `i` in
 * `[0, count)`. Public so the executor-side generator can consume it without referencing the
 * `private[spark]` RPC types.
 */
case class IdRange(rangeStart: Long, rangeEnd: Long, step: Long) {
  def numValues: Long = (rangeEnd - rangeStart) / step + 1L
}

/**
 * Executor -> driver reserve request. Serializable so it rides the wire over `RpcEnv` directly
 * (Spark serializes RPC messages itself). Public so the same-package executor client can build it.
 */
case class RequestIds(
    tableId: String,
    sequenceId: String,
    lastCount: Long,
    elapsedMs: Long,
    step: Long,
    stageId: Int,
    partitionId: Int,
    taskAttemptId: Long)

// DRIVER SIDE: this object registers the RPC endpoint (class below the WIRE section); the
// reservation math is the pure [[org.apache.spark.sql.delta.cic.ReserveBuffer]].

/**
 * Driver-side owner of the reservation RPC endpoint; executors reach it through
 * [[IdentitySequenceClient]]. Two hops: executor -> driver over `RpcEnv`,
 * then driver -> service via [[IdentitySequenceService.reserveIds]]. No up-front
 * seed: the first request on a key misses, parks the executor, and triggers a refill.
 */
object IdentitySequenceCoordinator extends Logging {

  val ENDPOINT_NAME = "IdentitySequenceCoordinator"

  /** Register the endpoint (idempotent; first wins) so executors can find it. */
  def ensureDriverEndpoint(service: IdentitySequenceService): Unit = synchronized {
    val env = SparkEnv.get
    try {
      env.rpcEnv.setupEndpoint(
        ENDPOINT_NAME, new IdentitySequenceCoordinatorEndpoint(env.rpcEnv, service))
      logInfo(log"Registered IdentitySequenceCoordinator endpoint")
    } catch {
      // "already an RpcEndpoint" is the expected already-registered case; anything else is real.
      case e: IllegalArgumentException
          if Option(e.getMessage).exists(_.contains("already an RpcEndpoint")) =>
        logDebug(log"IdentitySequenceCoordinator endpoint already registered; reusing it")
      case e: IllegalArgumentException =>
        logWarning(log"Failed to register the IdentitySequenceCoordinator endpoint", e)
        ConcurrentIdentityColumnObservability.recordEndpointRegistrationFailed(e)
        throw e
    }
  }
}

// ===========================================================================================
// WIRE: messages exchanged between the executor client and the driver endpoint.
// ===========================================================================================

/**
 * The `(tableId, sequenceId)` pair identifying one identity sequence's buffer.
 */
private[spark] case class SequenceKey(tableId: String, sequenceId: String)

// Driver-internal only: these travel via `self.send` (same JVM), which Spark delivers by reference
// without serializing, so the trait is not `Serializable`. The executor -> driver reserve request
// and its reply cross a machine boundary as the serializable [[RequestIds]] / [[IdRange]] messages.
private[spark] sealed trait IdentitySequenceCoordinatorMessage

/** Driver-internal refill messages posted to the [[IdentitySequenceCoordinatorEndpoint]]. */
private[spark] object IdentitySequenceCoordinatorMessage {

  /** Async refill result, handled on the endpoint thread (keeps the buffer single-threaded). */
  case class RefillCompleted(
      key: SequenceKey,
      rangeStart: Long,
      count: Long,
      step: Long) extends IdentitySequenceCoordinatorMessage

  /**
   * Async refill failure: clears the in-flight flag and fails any parked callers, else they block
   * until the RPC ask timeout.
   */
  case class RefillFailed(key: SequenceKey, error: Throwable)
    extends IdentitySequenceCoordinatorMessage
}

/**
 * The reservation endpoint: reserves from the service in rate-sized chunks and hands out sub-ranges
 * locally, collapsing many writers' reserves to a few round-trips. As an
 * [[IsolatedThreadSafeRpcEndpoint]] a single dispatch thread serializes all buffer access (no
 * locks) and never blocks on the service: a hit replies immediately; an empty buffer parks the
 * caller and a background refill replies via [[distribute]] on the [[RefillCompleted]] message.
 * The imperative shell around the pure [[ReserveBuffer]]: transport, cache, threadpool, parking.
 */
private[spark] class IdentitySequenceCoordinatorEndpoint(
    override val rpcEnv: RpcEnv,
    service: IdentitySequenceService,
    clock: Clock = new SystemClock())
  extends IsolatedThreadSafeRpcEndpoint with Logging {

  // Idle buffers are dropped after this long; the live-key count is capped (LRU eviction).
  private val bufferTtl = 60.minute
  private val maxLiveKeys = 1L << 20 // 1Mi

  private def now(): FiniteDuration = FiniteDuration(clock.getTimeMillis(), MILLISECONDS)

  /**
   * Sizing knobs, read fresh per buffer creation from the driver's DEFAULT `SparkSession` (this
   * session-less dispatch thread cannot see the thread-local active one), so a runtime conf change
   * is picked up by the next cold / post-eviction buffer. Falls back to entry defaults if no
   * session exists yet (a reserve request implies a running driver, so this should not happen).
   */
  private def reserveConfig: ReserveConfig = {
    val conf = SparkSession.getDefaultSession.map(_.sessionState.conf)
    def get[T](entry: ConfigEntry[T]): T =
      conf.map(_.getConf(entry)).getOrElse(entry.defaultValue.get)


    ReserveConfig(
      maxBatch = get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_RESERVE_MAX_BATCH_SIZE),
      refillWindowMs = get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_RESERVE_WINDOW_MS),
      driverInitialSize = get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_RESERVE_DRIVER_INITIAL_SIZE),
      minBatchSize = get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_RESERVE_MIN_BATCH_SIZE))
  }

  // An executor reserve parked on an empty buffer. `desiredCount` splits the refill by demand;
  // `receiveTime` + task coords feed the per-request summary; `parkRounds` counts distribute passes
  // that granted this caller 0 (the "parked without an allocation" signal). Replied by distribute.
  private case class ParkedExecutorReserve(
      context: RpcCallContext,
      desiredCount: Long,
      receiveTime: FiniteDuration,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long,
      var parkRounds: Long = 0L)

  // State for one [[SequenceKey]]: IS the buffer (extends it) plus its parked queue, so the
  // two evict atomically as one cache value. The counters are per-sequence lifetime totals for the
  // eviction summary; single dispatch thread, so plain vars need no synchronization.
  private class SequenceState(step: Long, cfg: ReserveConfig) extends ReserveBuffer(step, cfg) {
    val parked = new mutable.Queue[ParkedExecutorReserve]
    val createdAt: FiniteDuration = now()
    var servedRequestCount: Long = 0L
    var totalIdsHandedOut: Long = 0L
    var parkEnqueueCount: Long = 0L
    var refillKickedCount: Long = 0L
    var refillIgnoredCount: Long = 0L
    var refillFailedCount: Long = 0L
  }

  // Eviction sweeps buffer + parked queue together; a key evicted with reserves still parked fails
  // them (clean task abort) rather than stranding them until the RPC ask timeout.
  private val sequenceCache: Cache[SequenceKey, SequenceState] =
    CacheBuilder.newBuilder()
      .maximumSize(maxLiveKeys)
      .expireAfterAccess(bufferTtl.toMinutes, TimeUnit.MINUTES)
      .removalListener { (n: RemovalNotification[SequenceKey, SequenceState]) =>
        val key = n.getKey
        val state = n.getValue
        if (state.parked.nonEmpty) {
          // Should be unreachable: RefillCompleted/RefillFailed clear parking well under bufferTtl
          // and the executor's askSync times out first. Reaching here means a refill hung the whole
          // TTL -- an internal error, so log loud and still fail the callers so they abort cleanly.
          val err = new IllegalStateException(
            s"Identity-sequence buffer (${key.tableId}, ${key.sequenceId}) evicted with " +
              s"${state.parked.size} executor reserve(s) still parked after $bufferTtl")
          logError(err.getMessage)
          ConcurrentIdentityColumnObservability.recordParkedOnEviction(
            key.tableId, key.sequenceId, state.parked.size)
          state.parked.foreach(_.context.sendFailure(err))
          state.parked.clear()
        }
        logInfo(log"Removed identity-sequence buffer " +
          log"(${MDC(DeltaLogKeys.TABLE_ID, key.tableId)}, " +
          log"${MDC(DeltaLogKeys.SEQUENCE_ID, key.sequenceId)}) " +
          log"(${MDC(DeltaLogKeys.REMOVAL_CAUSE, n.getCause)}) after " +
          log"${MDC(DeltaLogKeys.NUM_RESERVATIONS, state.sequenceReservationCount)} " +
          log"reservations: " +
          log"served=${MDC(DeltaLogKeys.NUM_SERVED_REQUESTS, state.servedRequestCount)} " +
          log"ids=${MDC(DeltaLogKeys.NUM_IDS, state.totalIdsHandedOut)} " +
          log"parked=${MDC(DeltaLogKeys.NUM_PARKED, state.parkEnqueueCount)} " +
          log"refillsKicked=${MDC(DeltaLogKeys.NUM_REFILLS_KICKED, state.refillKickedCount)} " +
          log"refillsIgnored=${MDC(DeltaLogKeys.NUM_REFILLS_IGNORED, state.refillIgnoredCount)} " +
          log"refillsFailed=${MDC(DeltaLogKeys.NUM_REFILLS_FAILED, state.refillFailedCount)}")
        // One event per buffer at end of life, so it needs no throttle.
        ConcurrentIdentityColumnObservability.recordSequenceSummary(
          identity = ConcurrentIdentityColumnObservability.SequenceSummaryIdentity(
            key.tableId, key.sequenceId),
          metrics = ConcurrentIdentityColumnObservability.SequenceSummaryMetrics(
            servedRequestCount = state.servedRequestCount,
            totalIdsHandedOut = state.totalIdsHandedOut,
            parkEnqueueCount = state.parkEnqueueCount,
            refillKickedCount = state.refillKickedCount,
            refillIgnoredCount = state.refillIgnoredCount,
            refillFailedCount = state.refillFailedCount,
            refillsLanded = state.sequenceReservationCount,
            lifetime = now() - state.createdAt))
      }
      .build()

  // Background refills: each task does only the blocking service reserve, then posts the result as
  // a self-message (never touches `sequenceCache`). Bounded pool so independent sequences reserve
  // in parallel; `refillInFlight` caps each sequence at one in-flight reserve, so the pool only
  // adds cross-sequence concurrency, never a same-sequence race.
  private lazy val refillExecutor =
    ThreadUtils.newDaemonCachedThreadPool("cic-seq-refill", maxThreadNumber = 8)

  /** Find the sequence's state, creating a cold buffer from [[reserveConfig]] if absent. */
  private def getOrCreateSequenceState(key: SequenceKey, step: Long): SequenceState = {
    Option(sequenceCache.getIfPresent(key)) match {
      case Some(existing) if existing.step != step =>
        throw new IllegalStateException(s"Identity sequence step mismatch " +
          s"(table=${key.tableId}, sequence=${key.sequenceId}): stored step=${existing.step} " +
          s"but requested step=$step")
      case Some(existing) => existing
      case None =>
        val fresh = new SequenceState(step, reserveConfig)
        sequenceCache.put(key, fresh)
        fresh
    }
  }

  // The one executor -> driver ask carries a [[RequestIds]] message. This endpoint has exactly one
  // ask message, so matching it is unambiguous; the driver-internal refill messages arrive on
  // `receive` instead. Add a discriminator here before introducing a second ask type.
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req: RequestIds =>
      serveOrDefer(context, SequenceKey(req.tableId, req.sequenceId), req.lastCount,
        req.elapsedMs, req.step, req.stageId, req.partitionId, req.taskAttemptId)
  }

  // Async refill results, handled on the endpoint thread so the buffer stays single-threaded.
  override def receive: PartialFunction[Any, Unit] = {
    case RefillCompleted(key, rangeStart, count, _) =>
      // If the key was evicted mid-refill, its parked executor reserves were already failed by the
      // removal listener, so the arriving range has no one to serve -- drop it (its values age out
      // as a gap, which identity columns permit).
      Option(sequenceCache.getIfPresent(key)).foreach { state =>
        state.onRefillArrived(rangeStart, count, now())
        distribute(key, state)
      }

    case RefillFailed(key, error) =>
      Option(sequenceCache.getIfPresent(key)).foreach { state =>
        state.onRefillFailed()
        state.refillFailedCount += 1L
        // Fail any parked executor reserves, else they block until the RPC ask timeout.
        while (state.parked.nonEmpty) state.parked.dequeue().context.sendFailure(error)
      }
  }

  /** Reply to an executor reserve with the granted range (the serializable [[IdRange]]). */
  private def replyRange(context: RpcCallContext, range: IdRange): Unit =
    context.reply(range)

  /**
   * Serve an executor reserve from the buffer, or park it when empty. The sizing strategy sizes the
   * grant from the reported `(lastCount, elapsedMs)`; a hit replies with up to that (fewer if short
   * - the generator loops) and tops up in the background if low. An empty buffer parks the caller
   * and refills instead of blocking the endpoint thread; [[distribute]] replies when it lands.
   */
  private def serveOrDefer(
      context: RpcCallContext,
      key: SequenceKey,
      lastCount: Long,
      elapsedMs: Long,
      step: Long,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long): Unit = {
    val state = getOrCreateSequenceState(key, step)
    // Executor-grant scope: the executor window (half the driver's) and per-task opening size.
    val elapsedTime = FiniteDuration(elapsedMs, MILLISECONDS)
    val desiredCount = state.executorGrantSize(lastCount, elapsedTime)
    val receiveTime = now()
    state.draw(desiredCount) match {
      case Some(range) =>
        logDebug(log"Served ${MDC(DeltaLogKeys.NUM_IDS, range.numValues)} identity values for " +
          log"sequence ${MDC(DeltaLogKeys.SEQUENCE_ID, key.sequenceId)} " +
          log"(table=${MDC(DeltaLogKeys.TABLE_ID, key.tableId)} " +
          log"stage=${MDC(DeltaLogKeys.STAGE_ID, stageId)} " +
          log"partition=${MDC(DeltaLogKeys.PARTITION_ID, partitionId)} " +
          log"attempt=${MDC(DeltaLogKeys.TASK_ATTEMPT_ID, taskAttemptId)}): " +
          log"[${MDC(DeltaLogKeys.RANGE_START, range.rangeStart)}, " +
          log"${MDC(DeltaLogKeys.RANGE_END, range.rangeEnd)}] " +
          log"step=${MDC(DeltaLogKeys.STEP, step)}")
        replyRange(context, range)
        // Served straight from the buffer: no parking, and the round-trip time is negligible.
        recordServed(state, key, desiredCount, range.numValues, parkRounds = 0L,
          receiveTime, stageId, partitionId, taskAttemptId)
        maybeKickRefill(key, state, desiredCount, step)
      case None =>
        logDebug(log"Identity sequence ${MDC(DeltaLogKeys.SEQUENCE_ID, key.sequenceId)} " +
          log"(table ${MDC(DeltaLogKeys.TABLE_ID, key.tableId)}) buffer empty; parking the " +
          log"executor reserve and refilling")
        state.parked.enqueue(ParkedExecutorReserve(
          context, desiredCount, receiveTime, stageId, partitionId, taskAttemptId))
        state.parkEnqueueCount += 1L
        maybeKickRefill(key, state, desiredCount, step)
    }
  }

  /** Tally a fully-served request and emit its (throttled) per-request summary. */
  private def recordServed(
      state: SequenceState,
      key: SequenceKey,
      desiredCount: Long,
      actualCount: Long,
      parkRounds: Long,
      receiveTime: FiniteDuration,
      stageId: Int,
      partitionId: Int,
      taskAttemptId: Long): Unit = {
    state.servedRequestCount += 1L
    state.totalIdsHandedOut += actualCount
    ConcurrentIdentityColumnObservability.recordServedRequest(
      tableId = key.tableId,
      sequenceId = key.sequenceId,
      desiredCount = desiredCount,
      actualCount = actualCount,
      parkRounds = parkRounds,
      totalTime = now() - receiveTime,
      stageId = stageId,
      partitionId = partitionId,
      taskAttemptId = taskAttemptId)
  }

  /**
   * Reply to callers parked on a just-refilled buffer, split by desired size (see
   * [[ReserveBuffer.planDistribution]]). A caller granted 0 stays parked; if any remain, kick
   * another refill for them.
   */
  private def distribute(key: SequenceKey, state: SequenceState): Unit = {
    if (state.parked.isEmpty) return
    val grants =
      ReserveBuffer.planDistribution(state.parked.map(_.desiredCount).toSeq, state.available,
        state.cfg)
    val stillParked = new mutable.Queue[ParkedExecutorReserve]
    for ((parked, grant) <- state.parked.zip(grants)) {
      if (grant > 0L) {
        val range = state.draw(grant).getOrElse(throw new IllegalStateException(
          s"Positive grant $grant undrawable for sequence ${key.sequenceId} " +
            s"(table ${key.tableId})"))
        logDebug(log"Served ${MDC(DeltaLogKeys.NUM_IDS, range.numValues)} parked identity values " +
          log"for sequence ${MDC(DeltaLogKeys.SEQUENCE_ID, key.sequenceId)} " +
          log"(table=${MDC(DeltaLogKeys.TABLE_ID, key.tableId)}): " +
          log"[${MDC(DeltaLogKeys.RANGE_START, range.rangeStart)}, " +
          log"${MDC(DeltaLogKeys.RANGE_END, range.rangeEnd)}] " +
          log"step=${MDC(DeltaLogKeys.STEP, range.step)}")
        replyRange(parked.context, range)
        recordServed(state, key, parked.desiredCount, range.numValues,
          parked.parkRounds, parked.receiveTime, parked.stageId, parked.partitionId,
          parked.taskAttemptId)
      } else {
        // Granted nothing this round: count the miss and keep it parked for the next refill.
        parked.parkRounds += 1L
        stillParked.enqueue(parked)
      }
    }
    state.parked.clear()
    state.parked ++= stillParked
    if (state.parked.nonEmpty) {
      val desiredCount = state.parked.map(_.desiredCount).sum
      maybeKickRefill(key, state, desiredCount, state.step)
    }
  }

  /**
   * Kick a background refill when the buffer cannot cover `desiredCount` and none is in flight.
   * The UC-pull size is set by the driver-scope sizing strategy
   * ([[ReserveBuffer.nextUcReserveSize]]), floored at the parked count times the minimum batch
   * size so one refill can serve every waiting task.
   */
  private def maybeKickRefill(
      key: SequenceKey,
      state: SequenceState,
      desiredCount: Long,
      step: Long): Unit = {
    if (state.refillInFlight || state.has(desiredCount)) {
      // Already refilling, or the buffer already covers demand: no new UC pull needed. Counted so
      // the eviction summary shows how often the buffer absorbed demand without a round-trip.
      state.refillIgnoredCount += 1L
      return
    }
    val refillCount =
      math.max(state.nextUcReserveSize(now()), state.parked.size * state.cfg.minBatchSize)
    state.refillKickedCount += 1L
    state.refillInFlight = true
    refillExecutor.submit(new Runnable {
      override def run(): Unit = {
        try {
          val resp = service.reserveIds(
            ReserveIdsRequest(key.sequenceId, key.tableId, refillCount, step))
          val reserved = IdRange(resp.rangeStart, resp.rangeEnd, resp.step)
          self.send(RefillCompleted(key, reserved.rangeStart, refillCount, step))
        } catch {
          case NonFatal(e) =>
            logWarning(log"Async identity-sequence refill failed; will retry on demand", e)
            ConcurrentIdentityColumnObservability.recordRefillFailed(
              key.tableId, key.sequenceId, e)
            self.send(RefillFailed(key, e))
        }
      }
    })
  }

}
