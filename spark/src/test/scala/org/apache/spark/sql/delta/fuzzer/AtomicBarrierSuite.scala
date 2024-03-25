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

package org.apache.spark.sql.delta.fuzzer

import scala.concurrent.duration._

import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin

import org.apache.spark.SparkFunSuite

class AtomicBarrierSuite extends SparkFunSuite
  with PhaseLockingTestMixin {

  val timeout: FiniteDuration = 5000.millis

  test("Atomic Barrier - wait before unblock") {
    val barrier = new AtomicBarrier
    assert(AtomicBarrier.State.Blocked === barrier.load())
    val thread = new Thread(() => {
      barrier.waitToPass()
    })
    assert(AtomicBarrier.State.Blocked === barrier.load())
    thread.start()
    busyWaitForState(barrier, AtomicBarrier.State.Requested, timeout)
    assert(thread.isAlive) // should be stuck waiting for unblock
    barrier.unblock()
    busyWaitForState(barrier, AtomicBarrier.State.Passed, timeout)
    thread.join(timeout.toMillis) // shouldn't take long
    assert(!thread.isAlive) // should have passed the barrier and completed
  }

  test("Atomic Barrier - unblock before wait") {
    val barrier = new AtomicBarrier
    assert(AtomicBarrier.State.Blocked === barrier.load())
    val thread = new Thread(() => {
      barrier.waitToPass()
    })
    assert(AtomicBarrier.State.Blocked === barrier.load())
    barrier.unblock()
    assert(AtomicBarrier.State.Unblocked === barrier.load())
    thread.start()
    busyWaitForState(barrier, AtomicBarrier.State.Passed, timeout)
    thread.join(timeout.toMillis) // shouldn't take long
    assert(!thread.isAlive) // should have passed the barrier and completed
  }
}
