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

import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Utils;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A simple utility that extracts the first item from an iterator and provides access to both the
 * head item and a full iterator (head + rest).*
 */
public class IteratorWithHead<T> implements AutoCloseable {

  private final Optional<T> head;
  private final CloseableIterator<T> restIterator;
  private final Boolean consumed;

  /** Creates an IteratorWithHead from the given iterator. */
  public IteratorWithHead(CloseableIterator<T> iterator) {
    Objects.requireNonNull(iterator, "iterator should not be null");
    this.head = iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    this.restIterator = iterator;
    this.consumed = false;
  }

  /**
   * Returns the first element from the original iterator, if present.
   *
   * @return Optional containing the first element, or empty if the iterator was empty
   */
  public Optional<T> getHead() {
    return head;
  }

  /**
   * Returns a full iterator containing all elements (head + rest), can only called once.
   *
   * @return CloseableIterator over all elements
   */
  public CloseableIterator<T> getFullIterator() {
    Preconditions.checkState(!consumed, "getFullIterator could only be called once");
    return head.map(head -> Utils.singletonCloseableIterator(head).combine(restIterator))
        .orElse(restIterator);
  }

  @Override
  public void close() throws IOException {
    if (!consumed) {
      restIterator.close();
    }
  }
}
