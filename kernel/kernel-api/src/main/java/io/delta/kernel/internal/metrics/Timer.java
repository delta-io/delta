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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/** A timer class for measuring the duration of operations in nanoseconds */
public class Timer {
  private final LongAdder count = new LongAdder();
  private final LongAdder totalTime = new LongAdder();

  /** @return the number of times this timer was used to record a duration. */
  public long count() {
    return count.longValue();
  }

  /** @return the total duration that was recorded in nanoseconds */
  public long totalDuration() {
    return totalTime.longValue();
  }

  /**
   * Starts the timer and returns a {@link Timed} instance. Call {@link Timed#stop()} to complete
   * the timing.
   *
   * @return A {@link Timed} instance with the start time recorded.
   */
  public Timed start() {
    return new DefaultTimed(this);
  }

  /**
   * Records a custom amount.
   *
   * @param amount The amount to record in nanoseconds
   */
  public void record(long amount) {
    checkArgument(amount >= 0, "Cannot record %s %s: must be >= 0", amount);
    this.totalTime.add(amount);
    this.count.increment();
  }

  public <T> T time(Supplier<T> supplier) {
    try (Timed ignore = start()) {
      return supplier.get();
    }
  }

  public <T> T timeCallable(Callable<T> callable) throws Exception {
    try (Timed ignore = start()) {
      return callable.call();
    }
  }

  public void time(Runnable runnable) {
    try (Timed ignore = start()) {
      runnable.run();
    }
  }

  /**
   * A timing sample that carries internal state about the Timer's start position. The timing can be
   * completed by calling {@link Timed#stop()}.
   */
  public interface Timed extends AutoCloseable {
    /** Stops the timer and records the total duration up until {@link Timer#start()} was called. */
    void stop();

    @Override
    default void close() {
      stop();
    }

    Timed NOOP = () -> {};
  }

  private static class DefaultTimed implements Timed {
    private final Timer timer;
    private final long startTime;
    private boolean closed;

    private DefaultTimed(Timer timer) {
      this.timer = timer;
      this.startTime = System.nanoTime();
    }

    @Override
    public void stop() {
      if (closed) {
        throw new IllegalStateException("called stop() multiple times");
      }
      timer.record(System.nanoTime() - startTime);
      closed = true;
    }
  }
}
