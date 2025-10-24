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

package io.delta.kernel.utils;

import io.delta.kernel.internal.util.Utils;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Iterator that supports peeking at the next item without advancing position.
 *
 * @param <T> the type of elements returned by this iterator
 */
public class PeekableIterator<T> implements CloseableIterator<T> {

  private final CloseableIterator<T> iterator;
  private T peekedItem = null;
  private boolean hasPeeked = false;

  /**
   * Creates a PeekableIterator wrapping the given iterator.
   *
   * @param iterator the underlying iterator
   */
  public PeekableIterator(CloseableIterator<T> iterator) {
    this.iterator = iterator;
  }

  /**
   * Get the next item in the iterator without advancing position.
   *
   * @return Optional containing the next item, or empty if no more items
   */
  public Optional<T> peek() {
    if (!hasPeeked) {
      hasPeeked = true;
      if (iterator.hasNext()) {
        peekedItem = iterator.next();
      } else {
        peekedItem = null;
      }
    }
    return Optional.ofNullable(peekedItem);
  }

  @Override
  public boolean hasNext() {
    if (hasPeeked) {
      return peekedItem != null;
    }
    return iterator.hasNext();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (hasPeeked) {
      T item = peekedItem;
      peekedItem = null;
      hasPeeked = false;
      return item;
    }

    return iterator.next();
  }

  @Override
  public void close() throws IOException {
    Utils.closeCloseables(iterator);
  }
}
