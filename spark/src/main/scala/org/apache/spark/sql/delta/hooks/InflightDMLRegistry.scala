/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.hooks

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * JVM-wide registry of in-flight long-running DML operations
 * (MERGE / DELETE / UPDATE / OVERWRITE).
 *
 * Async Auto Compaction consults this registry to decide whether to start a new OPTIMIZE
 * batch, and registers a per-table listener so it can be told to cancel mid-flight when a
 * new DML acquires while AC is running. This eliminates the failure mode where a long
 * MERGE loses to a concurrently-finishing OPTIMIZE and is forced to throw
 * `ConcurrentModificationException` -- losing minutes of Spark work -- after the AC commit
 * lands first.
 *
 * Intra-JVM only. A cross-JVM lease (via a `DomainMetadata` entry in the Delta log) is a
 * deferred follow-up; in practice async AC runs as a post-commit hook on the writer's own
 * driver, so the same-driver topology is dominant.
 *
 * Listener slot is per-table and exclusive (overwrites on re-registration) because the
 * async Auto Compaction service's dedup guarantees at most one async AC task is running per
 * table at a time.
 */
private[delta] object InflightDMLRegistry {

  /**
   * Listener invoked when a DML `acquire` happens for the registered tableId. AC registers
   * one for the duration of its task; on invocation it sets its internal `cancelled` flag
   * and calls `SparkContext.cancelJobGroup` so its in-flight Spark stages are interrupted.
   */
  trait AcquireListener {
    def onDMLAcquired(): Unit
  }

  /**
   * Per-table active count. Incremented at DML start, decremented at end. The async AC
   * task's `isActive` check uses `count > 0` to decide whether to yield.
   */
  private val activeOps = new ConcurrentHashMap[String, AtomicInteger]()

  /**
   * Per-table listener slot. Only one listener per table -- async AC's dedup ensures at
   * most one task per table at a time. Re-registration overwrites (a new task superseding
   * a finished one).
   */
  private val listeners = new ConcurrentHashMap[String, AcquireListener]()

  /**
   * Acquire a slot for a long-running DML on `tableId`. Increments the active count and,
   * if any AC listener is registered, fires it so AC can begin cancellation.
   *
   * Idempotent only in the sense that nested DML calls (rare) are reference-counted; each
   * `acquire` MUST be paired with exactly one `release` (use try/finally).
   */
  def acquire(tableId: String): Unit = {
    activeOps.computeIfAbsent(tableId, _ => new AtomicInteger(0)).incrementAndGet()
    val l = listeners.get(tableId)
    if (l != null) {
      try l.onDMLAcquired() catch { case _: Throwable => () }
    }
  }

  /**
   * Release a slot acquired via `acquire`. Removes the per-table counter when it drops to
   * zero to avoid unbounded growth across many tables in long-lived JVMs.
   */
  def release(tableId: String): Unit = {
    val ctr = activeOps.get(tableId)
    if (ctr != null) {
      if (ctr.decrementAndGet() <= 0) {
        // Race-tolerant removal: only remove if the value is still <= 0. A concurrent
        // acquire that snuck a +1 between our decrement and check will leave the counter
        // positive and we won't remove it.
        activeOps.remove(tableId, ctr)
      }
    }
  }

  /** True iff there is at least one in-flight DML on `tableId`. */
  def isActive(tableId: String): Boolean = {
    val ctr = activeOps.get(tableId)
    ctr != null && ctr.get() > 0
  }

  /**
   * Register a listener that will be invoked when a DML `acquire` for `tableId` happens
   * while this listener is registered. Overwrites any prior listener for the same table.
   *
   * Pair with `unregisterAcquireListener` in a `finally` so a crashed AC task doesn't
   * leave a stale listener behind.
   */
  def registerAcquireListener(tableId: String, listener: AcquireListener): Unit = {
    listeners.put(tableId, listener)
  }

  /**
   * Unregister the listener for `tableId`. Idempotent.
   */
  def unregisterAcquireListener(tableId: String): Unit = {
    listeners.remove(tableId)
  }

  /** Test-only. Clears all state. */
  private[delta] def resetForTesting(): Unit = {
    activeOps.clear()
    listeners.clear()
  }

  /** Test-only. Returns the current active count for `tableId`. */
  private[delta] def activeCountForTesting(tableId: String): Int = {
    val ctr = activeOps.get(tableId)
    if (ctr == null) 0 else ctr.get()
  }

  /** Test-only. True iff a listener is currently registered for `tableId`. */
  private[delta] def hasListenerForTesting(tableId: String): Boolean = {
    listeners.get(tableId) != null
  }
}
