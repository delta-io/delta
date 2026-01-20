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
package io.delta.kernel.internal.util;

import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A CloseableIterator that retries exactly once before any data is returned.
 *
 * <p>Intended for retrying lazy file reads where the first attempt may fail due to transient
 * conditions (e.g., staged file cleanup or thread interrupts).
 */
public final class RetryingCloseableIterator<T> implements CloseableIterator<T> {
  @FunctionalInterface
  public interface IteratorSupplier<T> {
    CloseableIterator<T> get() throws IOException;
  }

  public static <T> CloseableIterator<T> create(
      IteratorSupplier<T> primary,
      IteratorSupplier<T> retry,
      Predicate<Throwable> isRetryable,
      Consumer<Throwable> onRetry) {
    return new RetryingCloseableIterator<>(primary, retry, isRetryable, onRetry);
  }

  private final IteratorSupplier<T> primary;
  private final IteratorSupplier<T> retry;
  private final Predicate<Throwable> isRetryable;
  private final Consumer<Throwable> onRetry;
  private CloseableIterator<T> currentIter;
  private int attempt = 0;
  private boolean returnedAny = false;

  private RetryingCloseableIterator(
      IteratorSupplier<T> primary,
      IteratorSupplier<T> retry,
      Predicate<Throwable> isRetryable,
      Consumer<Throwable> onRetry) {
    this.primary = Objects.requireNonNull(primary, "primary is null");
    this.retry = Objects.requireNonNull(retry, "retry is null");
    this.isRetryable = Objects.requireNonNull(isRetryable, "isRetryable is null");
    this.onRetry = Objects.requireNonNull(onRetry, "onRetry is null");
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (currentIter == null && !openNext()) {
        return false;
      }
      try {
        return currentIter.hasNext();
      } catch (RuntimeException e) {
        if (!canRetry(e)) {
          throw e;
        }
        handleRetry(e);
      }
    }
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No next element");
    }
    try {
      T next = currentIter.next();
      returnedAny = true;
      return next;
    } catch (RuntimeException e) {
      if (!canRetry(e)) {
        throw e;
      }
      handleRetry(e);
      return next();
    }
  }

  @Override
  public void close() throws IOException {
    if (currentIter != null) {
      currentIter.close();
      currentIter = null;
    }
  }

  private boolean openNext() {
    while (attempt < 2) {
      IteratorSupplier<T> supplier = attempt == 0 ? primary : retry;
      attempt += 1;
      try {
        currentIter = supplier.get();
        return true;
      } catch (RuntimeException e) {
        if (!canRetry(e)) {
          throw e;
        }
        handleRetry(e);
      } catch (IOException e) {
        UncheckedIOException wrapped = new UncheckedIOException(e);
        if (!canRetry(wrapped)) {
          throw wrapped;
        }
        handleRetry(wrapped);
      }
    }
    return false;
  }

  private boolean canRetry(Throwable throwable) {
    return !returnedAny && isRetryable.test(throwable) && attempt < 2;
  }

  private void handleRetry(Throwable throwable) {
    onRetry.accept(throwable);
    Utils.closeCloseablesSilently(currentIter);
    currentIter = null;
  }
}
