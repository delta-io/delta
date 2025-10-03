/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.utils;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.util.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Closeable extension of {@link Iterator}
 *
 * @param <T> the type of elements returned by this iterator
 * @since 3.0.0
 */
@Evolving
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

  /**
   * Represents the result of applying the filter condition in the {@link
   * #breakableFilter(Function)} method of a {@link CloseableIterator}. This enum determines how
   * each element in the iterator should be handled.
   */
  enum BreakableFilterResult {
    /**
     * Indicates that the current element should be included in the resulting iterator produced by
     * {@link #breakableFilter(Function)}.
     */
    INCLUDE,

    /**
     * Indicates that the current element should be excluded from the resulting iterator produced by
     * {@link #breakableFilter(Function)}.
     */
    EXCLUDE,

    /**
     * Indicates that the iteration should stop immediately and that no further elements should be
     * processed by {@link #breakableFilter(Function)}.
     */
    BREAK
  }

  /**
   * Returns true if the iteration has more elements. (In other words, returns true if next would
   * return an element rather than throwing an exception.)
   *
   * @return true if the iteration has more elements
   * @throws KernelEngineException For any underlying exception occurs in {@link Engine} while
   *     trying to execute the operation. The original exception is (if any) wrapped in this
   *     exception as cause. E.g. {@link IOException} thrown while trying to read from a Delta log
   *     file. It will be wrapped in this exception as cause.
   * @throws KernelException When encountered an operation or state that is invalid or unsupported.
   */
  @Override
  boolean hasNext();

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   * @throws KernelEngineException For any underlying exception occurs in {@link Engine} while
   *     trying to execute the operation. The original exception is (if any) wrapped in this
   *     exception as cause. E.g. {@link IOException} thrown while trying to read from a Delta log
   *     file. It will be wrapped in this exception as cause.
   * @throws KernelException When encountered an operation or state that is invalid or unsupported
   *     in Kernel. For example, trying to read from a Delta table that has advanced features which
   *     are not yet supported by Kernel.
   */
  @Override
  T next();

  default <U> CloseableIterator<U> map(Function<T, U> mapper) {
    CloseableIterator<T> delegate = this;
    return new CloseableIterator<U>() {
      @Override
      public void remove() {
        delegate.remove();
      }

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public U next() {
        return mapper.apply(delegate.next());
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  /**
   * Returns a new {@link CloseableIterator} that includes only the elements of this iterator for
   * which the given {@code mapper} function returns {@code true}.
   *
   * @param mapper A function that determines whether an element should be included in the resulting
   *     iterator.
   * @return A {@link CloseableIterator} that includes only the filtered the elements of this
   *     iterator.
   */
  default CloseableIterator<T> filter(Function<T, Boolean> mapper) {
    return breakableFilter(
        t -> {
          if (mapper.apply(t)) {
            return BreakableFilterResult.INCLUDE;
          } else {
            return BreakableFilterResult.EXCLUDE;
          }
        });
  }

  /**
   * Returns a new {@link CloseableIterator} that includes elements from this iterator as long as
   * the given {@code mapper} function returns {@code true}. Once the mapper function returns {@code
   * false}, the iteration is terminated.
   *
   * @param mapper A function that determines whether to include an element in the resulting
   *     iterator.
   * @return A {@link CloseableIterator} that stops iteration when the condition is not met.
   */
  default CloseableIterator<T> takeWhile(Function<T, Boolean> mapper) {
    return breakableFilter(
        t -> {
          if (mapper.apply(t)) {
            return BreakableFilterResult.INCLUDE;
          } else {
            return BreakableFilterResult.BREAK;
          }
        });
  }

  /**
   * Returns a new {@link CloseableIterator} that applies a {@link BreakableFilterResult}-based
   * filtering function to determine whether elements of this iterator should be included or
   * excluded, or whether the iteration should terminate.
   *
   * @param mapper A function that determines the filtering action for each element: include,
   *     exclude, or break.
   * @return A {@link CloseableIterator} that applies the specified {@link
   *     BreakableFilterResult}-based logic.
   */
  default CloseableIterator<T> breakableFilter(Function<T, BreakableFilterResult> mapper) {
    CloseableIterator<T> delegate = this;
    return new CloseableIterator<T>() {
      T next;
      boolean hasLoadedNext;
      boolean shouldBreak = false;

      @Override
      public boolean hasNext() {
        if (shouldBreak) {
          return false;
        }
        if (hasLoadedNext) {
          return true;
        }
        while (delegate.hasNext()) {
          final T potentialNext = delegate.next();
          final BreakableFilterResult result = mapper.apply(potentialNext);
          if (result == BreakableFilterResult.INCLUDE) {
            next = potentialNext;
            hasLoadedNext = true;
            return true;
          } else if (result == BreakableFilterResult.BREAK) {
            shouldBreak = true;
            return false;
          }
        }
        return false;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        hasLoadedNext = false;
        return next;
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  /**
   * Combine the current iterator with another iterator. The resulting iterator will return all
   * elements from the current iterator followed by all elements from the other iterator.
   *
   * @param other the other iterator to combine with
   * @return a new iterator that combines the current iterator with the other iterator
   */
  default CloseableIterator<T> combine(CloseableIterator<T> other) {

    CloseableIterator<T> delegate = this;
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return delegate.hasNext() || other.hasNext();
      }

      @Override
      public T next() {
        if (delegate.hasNext()) {
          return delegate.next();
        } else {
          return other.next();
        }
      }

      @Override
      public void close() throws IOException {
        Utils.closeCloseables(delegate, other);
      }
    };
  }

  /**
   * Collects all elements from this {@link CloseableIterator} into a {@link List}.
   *
   * <p>This method iterates through all elements of the iterator, storing them in an in-memory
   * list. Once iteration is complete, the iterator is automatically closed to release any
   * underlying resources.
   *
   * @return A {@link List} containing all elements from this iterator.
   * @throws UncheckedIOException If an {@link IOException} occurs while closing the iterator.
   */
  default List<T> toInMemoryList() {
    final List<T> result = new ArrayList<>();
    try (CloseableIterator<T> iterator = this) {
      while (iterator.hasNext()) {
        result.add(iterator.next());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close the CloseableIterator", e);
    }
    return result;
  }
}
