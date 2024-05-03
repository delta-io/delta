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

package org.apache.spark.sql.delta

import java.util.concurrent.Semaphore

import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.SparkFunSuite

class FileMetadataMaterializationTrackerSuite extends SparkFunSuite with TimeLimits {

  test("tracker - unit test") {

    def acquireForTask(tracker: FileMetadataMaterializationTracker, numPermits: Int): Unit = {
      val taskLevelPermitAllocator = tracker.createTaskLevelPermitAllocator()
      for (i <- 1 to numPermits) {
        taskLevelPermitAllocator.acquirePermit()
      }
    }

    // Initialize the semaphore for tests
    val semaphore = new Semaphore(10)
    FileMetadataMaterializationTracker.initializeSemaphoreForTests(semaphore)
    val tracker = new FileMetadataMaterializationTracker()

    // test that acquiring a permit should work and decrement the available permits.
    acquireForTask(tracker, 1)
    assert(semaphore.availablePermits() === 9)

    // releasing the permit should increment the semaphore's count
    tracker.releasePermits(1)
    assert(semaphore.availablePermits() === 10)

    // test overallocation
    acquireForTask(tracker, 11) // allowed to over allocate
    assert(semaphore.availablePermits() === 0)
    assert(semaphore.availablePermits() === 0)
    tracker.releasePermits(11)
    assert(semaphore.availablePermits() === 10) // make sure we don't overflow

    // test - wait for other task to release overallocation lock
    acquireForTask(tracker, 11)

    val acquireThread = new Thread() {
      override def run(): Unit = {
        val taskLevelPermitAllocator = tracker.createTaskLevelPermitAllocator()
        taskLevelPermitAllocator.acquirePermit()
      }
    }
    // we acquire in a separate thread so that we can make sure the acquiring is blocked
    // until another thread(main thread here) releases a permit.
    acquireThread.start()
    Thread.sleep(2000) // Sleep for 2 seconds to make sure the acquireThread is blocked
    assert(acquireThread.isAlive) // acquire thread is actually blocked
    tracker.releasePermits(11)
    failAfter(Span(2, Seconds)) {
      acquireThread.join() // acquire thread should get unblocked
    }

    // test releaseAllPermits
    assert(semaphore.availablePermits() === 9)
    tracker.releaseAllPermits()
    assert(semaphore.availablePermits() === 10)
  }
}
