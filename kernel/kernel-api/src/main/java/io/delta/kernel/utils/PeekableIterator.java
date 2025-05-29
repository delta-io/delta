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
import java.util.Optional;

public class PeekableIterator<T> implements Iterator<T> {
  private final Iterator<T> iterator;
  private Optional<T> peekedItem = Optional.empty();

  public PeekableIterator(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  public T peek() {
    if (!peekedItem.isPresent() && iterator.hasNext()) {
      peekedItem = Optional.of(iterator.next());
    }
    if (!peekedItem.isPresent()) {
      throw new NoSuchElementException("No element to peek");
    }
    return peekedItem.get();
  }

  @Override
  public boolean hasNext() {
    return peekedItem.isPresent() || iterator.hasNext();
  }

  @Override
  public T next() {
    if (peekedItem.isPresent()) {
      T result = peekedItem.get();
      peekedItem = Optional.empty();
      return result;
    }
    return iterator.next();
  }
}
