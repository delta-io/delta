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

package org.apache.spark.sql.delta.concurrency

import scala.concurrent.duration._

import org.apache.spark.sql.delta.BusyWait
import org.apache.spark.sql.delta.fuzzer.AtomicBarrier

import org.apache.spark.SparkFunSuite

trait PhaseLockingTestMixin { self: SparkFunSuite =>
  /** Keep checking if `barrier` in `state` until it's the case or `waitTime` expires. */
  def busyWaitForState(
      barrier: AtomicBarrier,
      state: AtomicBarrier.State,
      waitTime: FiniteDuration): Unit =
    busyWaitFor(
      barrier.load() == state,
      waitTime,
      s"Exceeded deadline waiting for $barrier to transition to state $state")

  /**
   * Keep checking if `check` return `true` until it's the case or `waitTime` expires.
   *
   * Optionally provide a custom error `message`.
   */
  def busyWaitFor(
      check: => Boolean,
      timeout: FiniteDuration,
      // lazy evaluate so closed over states are evaluated at time of failure not invocation
      message: => String = "Exceeded deadline waiting for check to become true."): Unit = {
    if (!BusyWait.until(check, timeout)) {
      fail(message)
    }
  }
}
