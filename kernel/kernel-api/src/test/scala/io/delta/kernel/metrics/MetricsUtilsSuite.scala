/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.metrics

import java.util.concurrent.Callable
import java.util.function.Supplier

import io.delta.kernel.internal.metrics.Timer
import org.scalatest.funsuite.AnyFunSuite

class MetricsUtilsSuite extends AnyFunSuite {

  def millisToNanos(millis: Long): Long = {
    millis*1000000
  }

  /**
   * @param incrementFx Function given (duration, timer) increments the timer by approximately
   *                    duration ms
   */
  def testTimer(incrementFx: (Long, Timer) => Unit): Unit = {
    val timer = new Timer()
    assert(timer.count == 0)
    assert(timer.totalDuration == 0)

    val incrementAmt1 = 0
    val paddedEndTimeOp1 = incrementAmt1 + 5 // We pad each operation by 5ms
    incrementFx(incrementAmt1, timer)
    assert(timer.count == 1)
    assert(timer.totalDuration >= millisToNanos(incrementAmt1) &&
      timer.totalDuration < millisToNanos(paddedEndTimeOp1))

    val incrementAmt2 = 20
    val paddedEndTimeOp2 = paddedEndTimeOp1 + incrementAmt2 + 5 // 30
    incrementFx(incrementAmt2, timer)
    assert(timer.count == 2)
    assert(timer.totalDuration >= millisToNanos(incrementAmt1 + incrementAmt2) &&
      timer.totalDuration < millisToNanos(paddedEndTimeOp2))

    val incrementAmt3 = 50
    val paddedEndTimeOp3 = paddedEndTimeOp2 + incrementAmt3 + 5 // 85
    incrementFx(incrementAmt3, timer)
    assert(timer.count == 3)
    assert(timer.totalDuration >= millisToNanos(incrementAmt1 + incrementAmt2 + incrementAmt3) &&
      timer.totalDuration < millisToNanos(paddedEndTimeOp3))
  }

  test("Timer class") {
    // Using Timer.record()
    testTimer((amount, timer) => timer.record(millisToNanos(amount)))

    // Using Timer.start()
    testTimer((amount, timer) => {
      val timed = timer.start()
      Thread.sleep(amount)
      timed.stop()
    })

    // Using Timer.time(supplier)
    def supplier(amount: Long): Supplier[Long] = {
      () => {
        Thread.sleep(amount)
        amount
      }
    }
    testTimer((amount, timer) => {
      timer.time(supplier(amount))
    })

    // Using Timer.timeCallable
    def callable(amount: Long): Callable[Long] = {
      () => {
        Thread.sleep(amount)
        amount
      }
    }
    testTimer((amount, timer) => {
      timer.timeCallable(callable(amount))
    })

    // Using Timer.time(runnable)
    def runnable(amount: Long): Runnable = {
      () => Thread.sleep(amount)
    }
    testTimer((amount, timer) => {
      timer.time(runnable(amount))
    })
  }

  test("Timer class with exceptions") {
    // We catch the exception outside of the functional interfaces
    def catchException(fx: () => Any): Unit = {
      try {
        fx.apply()
      } catch {
        case _: Exception =>
      }
    }

    // Using Timer.time(supplier)
    def supplier(amount: Long): Supplier[Long] = {
      () => {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.time(supplier(amount)))
    })

    // Using Timer.timeCallable
    def callable(amount: Long): Callable[Long] = {
      () => {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.timeCallable(callable(amount)))
    })

    // Using Timer.time(runnable)
    def runnable(amount: Long): Runnable = {
      () => {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.time(runnable(amount)))
    })
  }
}
