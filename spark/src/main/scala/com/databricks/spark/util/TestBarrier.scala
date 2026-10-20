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

package com.databricks.spark.util

import java.util.concurrent.{Semaphore, TimeoutException, TimeUnit}

import scala.collection.mutable


/** This class is used to wait for threads blocked on `label` and unblock these threads. */
class TestBarrier private[util] (
    val label: String,
    private val semaphore: Semaphore,
    private var hitCount: Int = 1) {
  /** The threads blocked by this TestBarrier object. */
  private val blockingThreads = new mutable.HashSet[Long]()
  /**
   * True if this barrier is enabled. If it is disabled, any thread that enters this barrier
   * wouldn't be blocked.
   */
  private var isEnabled = true
  private var blockingNewThreads = true
  /** The interval a thread need to sleep before checking the current status of barrier. */
  private final val DEFAULT_TIMEOUT_DURATION_MS = 100
  private final val DEFAULT_WAIT_TIMEOUT_MS = 120000

  def getNumBlockedThreads: Int = synchronized { blockingThreads.size }

  private[util] def isBlockingNewThreads: Boolean = synchronized { blockingNewThreads }

  /** Wait until at least `numThreads` threads are blocked. */
  def waitUntilBlocked(
      numThreads: Int = 1,
      checkIntervalMs: Long = DEFAULT_TIMEOUT_DURATION_MS,
      timeout: Long = DEFAULT_WAIT_TIMEOUT_MS): Unit = {
    val startTimeMs = System.currentTimeMillis()
    var keepSpinning = true
    while (keepSpinning) {
      val numBlocked = getNumBlockedThreads
      keepSpinning = numBlocked < numThreads
      if (keepSpinning) {
        if (System.currentTimeMillis() - startTimeMs > timeout) {
          throw new TimeoutException(
            s"Only $getNumBlockedThreads(expect $numThreads) threads are blocked " +
              s"after $timeout milliseconds.")
        }
        Thread.sleep(checkIntervalMs)
      }
    }
  }

  /** Release all threads that are blocked by this TestBarrier. */
  def releaseAll(): Unit = synchronized {
    semaphore.release(getNumBlockedThreads)
    blockingThreads.clear()
    isEnabled = false
  }

  /** Release one thread that are blocked by this TestBarrier. */
  def releaseOne(): Unit = synchronized {
    // Keep the barrier enabled, only wake a single waiter.
    semaphore.release(1)
    // The awakened thread will remove itself from `blockingThreads` in `enter`'s `finally`.
  }

  /**
   * Disable the barrier so that any thread that enters this barrier wouldn't be blocked, but the
   * threads that are already blocked by this barrier still need to be released.
   */
  private[util] def disableWithoutReleasing(): Unit = synchronized { blockingNewThreads = false }

  def enable(): Unit = synchronized {
    isEnabled = true
    // Drain any leftover permits from previous barrier use to prevent threads from
    // acquiring spurious permits when the barrier is reused.
    semaphore.drainPermits()
  }

  /** Register current thread to this TestBarrier and block on it. */
  private[util] def enter(): Unit = {
    val threadId = Thread.currentThread().getId
    // To avoid this method racing with `releaseAll()`, this method unblocks itself if isEnabled is
    // false.
    synchronized {
      if (isEnabled) {
        blockingThreads.add(threadId)
      } else {
        // Barrier is disabled, return directly.
        return
      }
    }
    try {
      var done = false
      while (!done) {
        val stillEnabled = synchronized { isEnabled }
        if (!stillEnabled) {
          // Barrier disabled globally, every blocked thread can exit.
          done = true
        } else if (semaphore.tryAcquire(DEFAULT_TIMEOUT_DURATION_MS, TimeUnit.MILLISECONDS)) {
          // This thread got a permit to exit.
          done = true
        }
        // else, no permit yet, loop again
      }
    } finally {
      synchronized {
        // This is safe with releaseAll() because removing non-existing threadId from the set
        // is no-op.
        blockingThreads.remove(threadId)
      }
    }
  }

  /**
   * Hit the barrier for the given times. Will return true if the barrier hitCount is zero or less,
   * which means the barrier can be destroyed.
   */
  def hit(times: Int = 1): Boolean = synchronized {
    hitCount -= times
    return hitCount <= 0
  }
}

/** Registry for labeled barriers shared by test-specific entry points. */
object TestBarrierRegistry {
  @volatile private var enabled = false
  private val barriersByLabel = new mutable.HashMap[String, TestBarrier]()

  def createBarrier(label: String, hitCount: Int, isTesting: Boolean): TestBarrier = synchronized {
    assert(isTesting)
    enabled = true
    val barrier = new TestBarrier(label, new Semaphore(0), hitCount)
    barriersByLabel.put(label, barrier)
    barrier
  }

  def waitIfEnabled(label: String, isTesting: Boolean): Unit = {
    if (!enabled || !isTesting) return
    val barrier = synchronized { barriersByLabel.get(label) }
    barrier.filter(_.isBlockingNewThreads).foreach(_.enter())
  }

  def disableBarrierWithoutReleasing(label: String): Unit = synchronized {
    barriersByLabel.get(label).foreach(_.disableWithoutReleasing())
  }

  def hitBarrier(label: String, times: Int = 1): Unit = synchronized {
    barriersByLabel.get(label).foreach { barrier =>
      if (barrier.hit(times)) {
        destroyBarrier(label)
      }
    }
  }

  def enableBarrier(label: String): Unit = synchronized {
    barriersByLabel.get(label).foreach(_.enable())
  }

  def destroyBarrier(label: String): Unit = synchronized {
    barriersByLabel.remove(label).foreach(_.releaseAll())
    enabled = barriersByLabel.nonEmpty
  }

  def isBarrierEnabled(label: String): Boolean = synchronized {
    barriersByLabel.contains(label)
  }
}

