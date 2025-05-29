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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that allows peeking at the next element without consuming it. This iterator wraps
 * another iterator and provides the ability to look ahead at the next element while maintaining the
 * standard Iterator contract.
 *
 * <p>The peek operation does not advance the iterator, so multiple calls to peek() will return the
 * same element until next() is called.
 *
 * @param <T> the type of elements returned by this iterator
 */
public class PeekableIterator<T> implements Iterator<T> {
  private final Iterator<T> iterator;
  private T peekedItem = null;
  private boolean hasPeeked = false;

  public PeekableIterator(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  public T peek() {
    if (!hasPeeked && iterator.hasNext()) {
      peekedItem = iterator.next();
      hasPeeked = true;
    }
    if (!hasPeeked) {
      throw new NoSuchElementException("No element to peek");
    }
    return peekedItem;
  }

  @Override
  public boolean hasNext() {
    return hasPeeked || iterator.hasNext();
  }

  @Override
  public T next() {
    T result = peek();
    peekedItem = null;
    hasPeeked = false;
    return result;
  }
}
