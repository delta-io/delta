/*
 * Copyright (2025) The Delta Lake Project Authors.
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

  test("Counter toString representation") {
    val counter = new Counter()
    counter.increment(42)

    val stringRepresentation = counter.toString()
    assert(stringRepresentation === "Counter(42)")
  }
}
