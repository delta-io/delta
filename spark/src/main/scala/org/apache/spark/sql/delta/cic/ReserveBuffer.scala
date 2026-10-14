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

// DRIVER SIDE (pure): the single-threaded reservation math, no Spark/RPC/threads/clock, so the
// whole reserve/draw/refill/distribute machine is unit-testable by calling methods. The endpoint
// (in the `org.apache.spark...cic` package) owns transport, cache, threadpool, and parked callers.
package org.apache.spark.sql.delta.cic

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import org.apache.spark.sql.delta.cic.IdRange

/**
 * A single contiguous run of reserved-but-unhanded identity values: `nextStart + i * step` for
 * `i` in `[0, remaining)`. Mutated in place as values are drawn.
 */
private[cic] class Segment(var nextStart: Long, var remaining: Long, val step: Long) {

  def isEmpty: Boolean = remaining == 0L

  /**
   * Draw up to `n` values (fewer when the segment is short) from the front and advance. Never draws
   * from an empty segment (callers guard with [[isEmpty]]); `n` must be positive.
   */
  def draw(n: Long): IdRange = {
    val k = math.min(n, remaining)
    val range = IdRange(nextStart, nextStart + (k - 1L) * step, step)
    nextStart += k * step
    remaining -= k
    range
  }

  /** Overwrite this segment with a freshly reserved range (reused instead of allocating). */
  def set(rangeStart: Long, count: Long): Unit = {
    nextStart = rangeStart
    remaining = count
  }
}

/**
 * Sizing knobs shared by the driver's UC pull and one executor's grant.
 *
 * @param maxBatch          ceiling on a single reserve (bounds an abandoned tail's gap).
 * @param refillWindowMs    driver's refill cadence in ms; the executor window is half this.
 * @param driverInitialSize cold-sequence first-pull size (`>= 1`); the executor's is half.
 * @param minBatchSize      floor on any reserve / grant.
 */
// Public (not private[cic]) because the constructing endpoint is in the sibling
// `org.apache.spark...cic` package, so private[cic] would hide it, same as [[IdRange]].
case class ReserveConfig(
    maxBatch: Long,
    refillWindowMs: Long,
    driverInitialSize: Long,
    minBatchSize: Long) {

  def driverRefillWindow: FiniteDuration = FiniteDuration(refillWindowMs, MILLISECONDS)

  def executorRefillWindow: FiniteDuration = driverRefillWindow / 2

  def executorInitialSize: Long = math.max(1L, driverInitialSize / 2L)

  /** Clamp a raw target into `[minBatchSize, maxBatch]`, rounding to the nearest id. */
  private[cic] def clamp(target: Double): Long =
    math.min(maxBatch, math.max(minBatchSize, math.round(target)))

}

/**
 * Per-`(tableId, sequenceId)` reservation buffer: two contiguous [[Segment]]s (`current` handed
 * out, `prefetched` a spare promoted when `current` drains) plus refill bookkeeping. `step` is a
 * constructor param (immutable per sequence). Mutated only on the endpoint's dispatch thread.
 */
// Public for the same cross-package reason as [[ReserveConfig]].
class ReserveBuffer(val step: Long, val cfg: ReserveConfig) {

  private val current = new Segment(0L, 0L, step)
  private val prefetched = new Segment(0L, 0L, step) // remaining == 0 means "no spare"

  // Count (not a boolean) so it can be logged and the "beyond first fill" threshold tuned.
  var sequenceReservationCount: Long = 0L
  var refillInFlight: Boolean = false

  // Sizing history for THIS sequence: last UC reservation size + when it landed, so the next
  // pull is sized by the gap between refills. 0 / None until the first refill -> driverInitialSize.
  var lastUcReservationSize: Long = 0L
  var lastUcRefillTime: Option[FiniteDuration] = None


  // The index of the reservation the driver already processed plus the size it produced.
  // This ensures that each reservation is sized exactly once, even when [[nextUcReserveSize]]
  // is called multiple times when a previous attempt failed and a retry was necessary.
  private var sizedForReservation: Long = -1L
  private var lastSizedReserve: Long = 0L

  /** Double after a fast drain, halve after a slow one, and otherwise keep the same size. */
  private[cic] def desiredSize(
      lastCount: Long,
      elapsedTime: FiniteDuration,
      window: FiniteDuration,
      initialSize: Long): Long = {
    val target =
      if (lastCount <= 0L) initialSize.toDouble
      else if (elapsedTime < window / 2) lastCount * 2.0
      else if (elapsedTime > window * 4) (lastCount / 2L).toDouble
      else lastCount.toDouble
    cfg.clamp(target)
  }

  /**
   * Size the next UC pull from this sequence's refill history at the DRIVER scope.
   * Note that this method may be called multiple times for the same reservation attempt, when this
   * attempt failed and a retry is necessary. Therefore, we reuse the already computed size for the
   * same reservation attempt.
   */
  def nextUcReserveSize(now: FiniteDuration): Long = {
    if (sizedForReservation == sequenceReservationCount) return lastSizedReserve
    val elapsedTime: FiniteDuration =
      lastUcRefillTime.map(now - _).getOrElse(FiniteDuration(0L, MILLISECONDS))
    lastSizedReserve = desiredSize(
      lastUcReservationSize, elapsedTime, cfg.driverRefillWindow, cfg.driverInitialSize)
    sizedForReservation = sequenceReservationCount
    lastSizedReserve
  }

  /** Size an executor's grant. The buffer keeps no executor-scope sizing state. */
  def executorGrantSize(lastCount: Long, elapsedTime: FiniteDuration): Long = {
    desiredSize(lastCount, elapsedTime, cfg.executorRefillWindow, cfg.executorInitialSize)
  }

  def remaining: Long = current.remaining
  def isEmpty: Boolean = current.isEmpty && prefetched.isEmpty

  /** Total values available to hand out right now (current + spare). */
  def available: Long = current.remaining + prefetched.remaining

  /** Promote the spare into `current` once `current` is exhausted. */
  private def promote(): Unit =
    if (current.isEmpty && !prefetched.isEmpty) {
      current.set(prefetched.nextStart, prefetched.remaining)
      prefetched.remaining = 0L
    }

  /**
   * Draw `n` (or fewer) contiguous values from `current`, promoting the spare first if needed. A
   * draw never spans the current/prefetched boundary (single contiguous reply; executor loops on a
   * short one). None when the buffer is dry.
   */
  def draw(n: Long): Option[IdRange] = {
    promote()
    if (current.isEmpty) None else Some(current.draw(n))
  }

  /** True when the buffer holds at least `n` values (across current + spare). */
  def has(n: Long): Boolean = available >= n

  /**
   * Record a landed refill: fill `current` if empty, else overwrite the spare (its old tail ages
   * out as a permitted gap). Clears the in-flight flag, counts the reserve, and stamps the
   * sizing history so the next pull is sized by the gap to this refill.
   */
  def onRefillArrived(rangeStart: Long, count: Long, now: FiniteDuration): Unit = {
    if (current.isEmpty) current.set(rangeStart, count) else prefetched.set(rangeStart, count)
    refillInFlight = false
    sequenceReservationCount += 1L
    lastUcReservationSize = count
    lastUcRefillTime = Some(now)
  }

  def onRefillFailed(): Unit = refillInFlight = false
}

object ReserveBuffer {

  /**
   * Split a just-refilled buffer across parked callers in arrival order, returning the per-caller
   * grant. Floor per caller is [[ReserveConfig.minBatchSize]]. Three regimes on what fits:
   *   - flush   (`available >= Sum desired`): everyone gets their full desired size.
   *   - partial (`available >= Sum floor`): floor + a leftover share proportional to desired.
   *   - scarce  (else): serve floors in arrival order, stop at the first that does not fit; the
   *               rest get 0 and stay parked.
   */
  def planDistribution(desired: Seq[Long], available: Long, cfg: ReserveConfig): Seq[Long] = {
    val floors = desired.map(_ => cfg.minBatchSize)
    val fulls = desired.map(d => math.max(cfg.minBatchSize, d))

    if (available >= fulls.sum) {
      fulls
    } else if (available >= floors.sum) {
      // Floor + leftover share. Long division rounds down and sum(want) == desiredSum, so
      // sum(grants) <= floors.sum + leftover == available: never over-allocates. Remainder stays.
      val leftover = available - floors.sum
      val desiredSum = fulls.sum
      fulls.zip(floors).map { case (want, floor) =>
        if (desiredSum == 0L) floor else floor + leftover * want / desiredSum
      }
    } else {
      // Scarce: serve floors in arrival order, STOP at the first that does not fit so a later
      // smaller caller never jumps an earlier one (anti-starvation). The rest get 0, stay parked.
      var left = available
      var stopped = false
      floors.map { floor =>
        if (!stopped && floor <= left) {
          left -= floor
          floor
        } else {
          stopped = true
          0L
        }
      }
    }
  }
}
