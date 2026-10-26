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
package io.delta.kernel.internal.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CounterTest {

  @Test
  void startsAtZero() {
    assertEquals(0L, new Counter().value());
  }

  @Test
  void incrementsByOne() {
    Counter counter = new Counter();
    counter.increment();
    assertEquals(1L, counter.value());
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, 1L, 10L})
  void incrementsBySpecifiedAmount(long amount) {
    Counter counter = new Counter();
    counter.increment(amount);
    assertEquals(amount, counter.value());
  }

  @Test
  void accumulatesAndResets() {
    Counter counter = new Counter();
    counter.increment();
    counter.increment();
    counter.increment(10);
    assertEquals(12L, counter.value());

    counter.reset();
    assertEquals(0L, counter.value());

    counter.increment();
    assertEquals(1L, counter.value());
  }

  @Test
  void counterToStringRepresentation() {
    Counter counter = new Counter();
    counter.increment(42);

    assertEquals("Counter(42)", counter.toString());
  }
}
