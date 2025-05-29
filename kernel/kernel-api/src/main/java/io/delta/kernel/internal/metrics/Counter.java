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
package io.delta.kernel.internal.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** A long counter that uses {@link AtomicLong} to count events. */
public class Counter {

  private final LongAdder counter = new LongAdder();

  /** Increment the counter by 1. */
  public void increment() {
    increment(1L);
  }

  /**
   * Increment the counter by the provided amount.
   *
   * @param amount to be incremented.
   */
  public void increment(long amount) {
    counter.add(amount);
  }

  /**
   * Reports the current count.
   *
   * @return The current count.
   */
  public long value() {
    return counter.longValue();
  }

  /** Resets the current count to 0. */
  public void reset() {
    counter.reset();
  }

  @Override
  public String toString() {
    return String.format("Counter(%s)", counter.longValue());
  }
}
