package io.delta.kernel.internal.metrics

import org.scalatest.funsuite.AnyFunSuite

class CounterSuite extends AnyFunSuite {


  test("Counter class") {
    val counter = new Counter()
    assert(counter.value == 0)
    counter.increment(0)
    assert(counter.value == 0)
    counter.increment()
    assert(counter.value == 1)
    counter.increment()
    assert(counter.value == 2)
    counter.increment(10)
    assert(counter.value == 12)
    counter.reset()
    assert(counter.value == 0)
    counter.increment()
    assert(counter.value == 1)
  }
}
