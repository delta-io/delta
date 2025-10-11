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
package io.delta.kernel.spark.utils;

import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A wrapper around {@link CloseableIterator} that allows peeking at the next element without
 * consuming it.
 *
 * <p>This is useful for detecting boundaries in a stream (e.g., when processing multiple versions
 * in a single iterator and needing to insert sentinel values at version boundaries).
 *
 * <p>Implementation note: This iterator maintains a one-element buffer. The buffer is lazily filled
 * on the first call to {@code peek()} or {@code hasNext()}.
 *
 * @param <T> the type of elements in this iterator
 */
public class PeekableCloseableIterator<T> implements CloseableIterator<T> {
  private final CloseableIterator<T> delegate;
  private T peeked;

  /**
   * Creates a peekable iterator wrapping the given iterator.
   *
   * @param iterator the iterator to wrap
   */
  public PeekableCloseableIterator(CloseableIterator<T> iterator) {
    this.delegate = iterator;
    this.peeked = null;
  }

  /**
   * Returns the next element without consuming it, or empty if no more elements.
   *
   * <p>Multiple calls to peek() without an intervening next() will return the same element.
   *
   * @return Optional containing the next element, or empty if no more elements
   */
  public Optional<T> peek() {
    if (peeked == null && delegate.hasNext()) {
      peeked = delegate.next();
    }
    return Optional.ofNullable(peeked);
  }

  @Override
  public boolean hasNext() {
    return peeked != null || delegate.hasNext();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (peeked != null) {
      T result = peeked;
      peeked = null;
      return result;
    }
    return delegate.next();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
