package io.delta.kernel.internal.metrics

import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.Callable
import java.util.function.Supplier


class TimerSuite extends AnyFunSuite {

  val NANOSECONDS_PER_MILLISECOND = 1000000

  def millisToNanos(millis: Long): Long = {
    millis * NANOSECONDS_PER_MILLISECOND
  }

  /**
   * @param incrementFx Function given (duration, timer) increments the timer by approximately
   *                    duration ms
   */
  def testTimer(incrementFx: (Long, Timer) => Unit): Unit = {
    val timer = new Timer()
    // Verify initial values
    assert(timer.count == 0)
    assert(timer.totalDurationNs == 0)

    def incrementAndCheck(amtMillis: Long): Unit = {
      val initialCount = timer.count()
      val initialDuration = timer.totalDurationNs() // in nanoseconds

      val startTime = System.currentTimeMillis()
      incrementFx(amtMillis, timer)
      // upperLimitDuration is in milliseconds; we take the max of time elapsed vs the incrementAmt
      val upperLimitDuration = Math.max(
        // we pad by 1 due to rounding of nanoseconds to milliseconds for system time
        System.currentTimeMillis() - startTime + 1,
        amtMillis)

      // check count
      assert(timer.count == initialCount + 1)
      // check lowerbound
      assert(timer.totalDurationNs >= initialDuration + millisToNanos(amtMillis))
      // check upperbound
      assert(timer.totalDurationNs <= initialDuration + millisToNanos(upperLimitDuration))
    }

    incrementAndCheck(0)
    incrementAndCheck(20)
    incrementAndCheck(50)
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
      () =>
      {
        Thread.sleep(amount)
        amount
      }
    }
    testTimer((amount, timer) => {
      timer.time(supplier(amount))
    })

    // Using Timer.timeCallable
    def callable(amount: Long): Callable[Long] = {
      () =>
      {
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
      () =>
      {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.time(supplier(amount)))
    })

    // Using Timer.timeCallable
    def callable(amount: Long): Callable[Long] = {
      () =>
      {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.timeCallable(callable(amount)))
    })

    // Using Timer.time(runnable)
    def runnable(amount: Long): Runnable = {
      () =>
      {
        Thread.sleep(amount)
        throw new RuntimeException()
      }
    }
    testTimer((amount, timer) => {
      catchException(() => timer.time(runnable(amount)))
    })
  }
}
