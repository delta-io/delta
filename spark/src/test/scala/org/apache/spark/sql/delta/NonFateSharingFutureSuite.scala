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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.util.control.ControlThrowable

import org.apache.spark.sql.delta.util.DeltaThreadPool

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession

class NonFateSharingFutureSuite extends SparkFunSuite with SharedSparkSession {
  test("function only runs once on success") {
    val count = new AtomicInteger
    val future = DeltaThreadPool("test", 1).submitNonFateSharing { _ => count.incrementAndGet }
    assert(future.get(10.seconds) === 1)
    assert(future.get(10.seconds) === 1)
    spark.cloneSession().withActive {
      assert(future.get(10.seconds) === 1)
    }
  }

  test("non-fatal exception in future is ignored") {
    val count = new AtomicInteger
    val future = DeltaThreadPool("test", 1).submitNonFateSharing { _ =>
      count.incrementAndGet match {
        case 1 => throw new Exception
        case i => i
      }
    }

    // Make sure the future already failed before waiting on it. This should happen ~immediately
    // unless the test runner is horribly overloaded/slow/etc, and stabilizes the assertions below.
    eventually(timeout(100.seconds)) {
      assert(count.get == 1)
    }

    spark.cloneSession().withActive {
      assert(future.get(1.seconds) === 2)
    }
    assert(future.get(1.seconds) === 3)

    spark.cloneSession().withActive {
      assert(future.get(1.seconds) === 4)
    }
    assert(future.get(1.seconds) === 5)
  }

  test("fatal exception in future only propagates once, and only to owning session") {
    val count = new AtomicInteger
    val future = DeltaThreadPool("test", 1).submitNonFateSharing { _ =>
      count.incrementAndGet match {
        case 1 => throw new InternalError
        case i => i
      }
    }

    // Make sure the future already failed before waiting on it. This should happen ~immediately
    // unless the test runner is horribly overloaded/slow/etc, and stabilizes the assertions below.
    eventually(timeout(100.seconds)) {
      assert(count.get == 1)
    }

    spark.cloneSession().withActive {
      assert(future.get(1.seconds) === 2)
    }
    intercept[InternalError] {
      future.get(1.seconds)
    }
    spark.cloneSession().withActive {
      assert(future.get(1.seconds) === 3)
    }
    assert(future.get(1.seconds) === 4)
  }
}
