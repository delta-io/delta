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

package org.apache.spark.sql.delta.cic

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

/**
 * Test-only in-process implementation of [[IdentitySequenceService]]. Not durable, not
 * distributed, only for testing purposes. Always use the UC-backed service. Test suites inject
 * this class by name via `identityColumn.concurrent.serviceClassName`.
 *
 * Per-instance state (a `ConcurrentHashMap` keyed by the caller-supplied `(tableId, sequenceId)`
 * pair -> [[SequenceState]], plus invocation counters): there is no process-wide singleton, so
 * each instance is isolated. A suite that needs every per-resolve instance to share state (so it
 * can assert invocation counts across the actual UC resolve path) injects a wrapper that forwards
 * to one shared instance.
 *
 * The pair key mirrors the UC contract: sequences live in a table scope, so a reserve carrying the
 * wrong `tableId` is a not-found (the same symptom a mis-keyed UC call produces), not a silent
 * hit. `createSequence` is CreateOrGet (idempotent on matching start/step, error on mismatch);
 * `reserveIds` advances `current` by `step * count` using checked arithmetic and returns the
 * inclusive range.
 *
 * Test isolation: call [[reset]] from `beforeEach`/`afterEach` in suites that depend on a clean
 * sequence set.
 */
class LocalIdentitySequenceService extends IdentitySequenceService {
  import LocalIdentitySequenceService._

  // Keyed by (tableId, sequenceId): the pair names the counter (see the class doc).
  private val sequences = new ConcurrentHashMap[(String, String), SequenceState]

  // Test-only invocation counters. Tests that need to verify the service-backed path
  // actually executed during a DML check that these increment as expected. Not exposed on the
  // trait; backend-specific.
  private val createSequenceInvocations = new AtomicLong(0L)
  private val reserveIdsInvocations = new AtomicLong(0L)
  private val dropSequenceInvocations = new AtomicLong(0L)

  // Test-only: the `count` of each reserveIds call, in order, so buffer tests can assert the
  // adaptive refill-size ramp. Its own monitor (reserveIds runs on many executor threads).
  private val reserveRequestSizesBuf = mutable.ArrayBuffer.empty[Long]

  // Test-only fault injection: the 1-based ordinal at or after which every `reserveIds` throws
  // (Long.MaxValue, the default, never fails). Set it to N so the first N-1 reserves succeed and
  // the Nth and every later reserve fail; N > 1 models a mid-write RPC failure (the write reserved
  // at least once, then a refill fails). It is a floor, not an exact match, so once armed the fault
  // survives Spark task retries: every retry's reserve also fails, so the write cannot slip past.
  private val failReserveIdsAtOrAfter = new AtomicLong(Long.MaxValue)

  override def createSequence(req: CreateSequenceRequest): Unit = {
    require(req.step != 0L, "step must be non-zero")
    require(req.sequenceId.nonEmpty, "sequenceId must be non-empty")
    require(req.tableId.nonEmpty, "tableId must be non-empty")
    createSequenceInvocations.incrementAndGet()
    // CreateOrGet: register the caller's (tableId, sequenceId) pair if absent. A repeat
    // call with matching start/step is idempotent (recovery path); a mismatch is a hard
    // error rather than silently resetting a live counter.
    val existing = sequences.putIfAbsent(
      (req.tableId, req.sequenceId),
      new SequenceState(req.start, new AtomicLong(req.start), req.step)
    )
    if (existing != null && (existing.start != req.start || existing.step != req.step)) {
      throw new IllegalStateException(
        s"Sequence ${req.sequenceId} on table ${req.tableId} already exists with " +
        s"start=${existing.start}, step=${existing.step}; cannot recreate with " +
        s"start=${req.start}, step=${req.step}."
      )
    }
  }

  override def reserveIds(req: ReserveIdsRequest): ReserveIdsResponse = {
    // Checks before an RPC is issued: a non-positive count or a zero step is a caller bug,
    // rejected with IllegalArgumentException.
    require(req.count > 0L, s"reserveIds requires count > 0, got ${req.count}")
    require(req.step != 0L, "reserveIds requires a non-zero step")
    val invocation = reserveIdsInvocations.incrementAndGet()
    reserveRequestSizesBuf.synchronized { reserveRequestSizesBuf += req.count }
    // Test-only fault injection: throw before advancing the counter so the failing reserve hands
    // out no range (mirrors a UC RPC that fails after the request is sent but before it commits).
    // Counted above, so the injected failure still increments reserveIdsCount.
    if (invocation >= failReserveIdsAtOrAfter.get()) {
      throw new RuntimeException(
        s"Injected reserveIds failure on invocation $invocation for sequence ${req.sequenceId} " +
        s"(table ${req.tableId})")
    }
    val state = sequences.get((req.tableId, req.sequenceId))
    if (state == null) {
      // Also the wrong-tableId symptom: the pair names the counter, so a reserve under a
      // different table scope than the create is a not-found. The reservation path catches the
      // thrown NoSuchElementException and raises SEQUENCE_NOT_FOUND, so throw the same type here.
      throw new NoSuchElementException(
        s"Sequence not found: ${req.sequenceId} (table ${req.tableId})")
    }
    // The caller's schema-declared step must match the registered step. The client detects this
    // as step drift in the client (the range-step echo check) and throws IllegalStateException;
    // the stub raises the same type for the same drift.
    if (req.step != state.step) {
      throw new IllegalStateException(
        s"Requested step ${req.step} does not match the registered step ${state.step} " +
        s"for sequence ${req.sequenceId} (table ${req.tableId}); service state drifted.")
    }
    // Checked arithmetic: pre-validate both the post-update `current` and the
    // last-emitted value inside the update fn, so any overflow rolls back the
    // CAS instead of advancing `current` past a partially-emitted range.
    val stride = Math.multiplyExact(state.step, req.count)
    val endOffset = Math.multiplyExact(state.step, req.count - 1L)
    val rangeStart = state.current.getAndUpdate { c =>
      Math.addExact(c, endOffset)
      Math.addExact(c, stride)
    }
    val rangeEnd = rangeStart + endOffset
    ReserveIdsResponse(
      sequenceId = req.sequenceId,
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      step = state.step
    )
  }

  override def dropSequence(req: DropSequenceRequest): Unit = {
    require(req.sequenceId.nonEmpty, "sequenceId must be non-empty")
    require(req.tableId.nonEmpty, "tableId must be non-empty")
    dropSequenceInvocations.incrementAndGet()
    // Idempotent: removing an unknown (tableId, sequenceId) is a no-op, mirroring the UC handler
    // (drop of an unknown/already-retired sequence succeeds).
    sequences.remove((req.tableId, req.sequenceId))
  }

  /**
   * Drops all sequences and resets the invocation counters. Test-only: the maps
   * clear non-atomically, so this is not safe to call concurrently with
   * reservations.
   */
  def reset(): Unit = {
    sequences.clear()
    createSequenceInvocations.set(0L)
    reserveIdsInvocations.set(0L)
    dropSequenceInvocations.set(0L)
    reserveRequestSizesBuf.synchronized { reserveRequestSizesBuf.clear() }
    failReserveIdsAtOrAfter.set(Long.MaxValue)
  }

  /**
   * Test-only fault injection: arm every [[reserveIds]] call whose 1-based ordinal is `>= ordinal`
   * to throw. Pass N > 1 to let the first N-1 reserves succeed and fail the Nth onward, modelling a
   * mid-write RPC failure. Cleared by [[reset]].
   */
  def failReserveIdsStartingAt(ordinal: Long): Unit = failReserveIdsAtOrAfter.set(ordinal)

  /** True iff a sequence is registered under exactly this (tableId, sequenceId) scope. */
  def hasSequence(tableId: String, sequenceId: String): Boolean =
    sequences.containsKey((tableId, sequenceId))

  /** Number of times [[createSequence]] has been called since last [[reset]]. */
  def createSequenceCount: Long = createSequenceInvocations.get()

  /** Number of times [[reserveIds]] has been called since last [[reset]]. */
  def reserveIdsCount: Long = reserveIdsInvocations.get()

  /** Number of times [[dropSequence]] has been called since last [[reset]]. */
  def dropSequenceCount: Long = dropSequenceInvocations.get()

  /** The `count` of each [[reserveIds]] call since last [[reset]], in call order. */
  def reserveRequestSizes: Seq[Long] =
    reserveRequestSizesBuf.synchronized { reserveRequestSizesBuf.toSeq }
}

object LocalIdentitySequenceService {

  /**
   * Internal mutable per-sequence state. `current` is the value the next
   * reservation will return; `start` and `step` are immutable for the lifetime of
   * the sequence and are retained so CreateOrGet can verify a repeat create matches.
   */
  private final class SequenceState(val start: Long, val current: AtomicLong, val step: Long)
}
