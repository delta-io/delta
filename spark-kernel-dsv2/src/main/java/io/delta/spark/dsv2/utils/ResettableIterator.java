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
package io.delta.spark.dsv2.utils;

import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A CloseableIterator wrapper that supports reset without pre-materializing all elements.
 *
 * <p>Behavior and trade-offs:
 *
 * <ul>
 *   <li><b>Streaming first pass</b>: Elements are read lazily from the underlying {@code source}
 *       and appended to an in-memory cache as they are consumed.
 *   <li><b>Reset semantics</b>: {@link #reset()} rewinds the read position to the beginning of the
 *       <i>already cached</i> prefix. When replay reaches the end of the cache and the source is
 *       not yet exhausted, this iterator continues reading from the source and appends to the
 *       cache.
 *   <li><b>Resource management</b>: The underlying source is closed when this iterator is closed.
 *   <li><b>Not thread-safe</b>: This iterator is not safe for concurrent use.
 * </ul>
 */
public final class ResettableIterator<T> implements CloseableIterator<T> {

  // Underlying source iterator; consumed lazily and only closed when this iterator is closed.
  private final CloseableIterator<T> source;
  // In-memory cache for elements yielded so far. Enables replay after reset().
  private final List<T> cache;
  // Current read cursor into the cache. reset() sets this to 0.
  private int curReadPosition;
  // True when the underlying source has been fully consumed.
  private boolean allSourceConsumed;
  // True after close() has been called.
  private boolean closed;

  public ResettableIterator(CloseableIterator<T> source) {
    this.source = Objects.requireNonNull(source, "source is null");
    this.cache = new ArrayList<>();
    this.curReadPosition = 0;
    this.allSourceConsumed = false;
    this.closed = false;
  }

  public void reset() {
    ensureOpen();
    // Reset only rewinds the read cursor over the cached prefix; it does not drain the source.
    this.curReadPosition = 0;
  }

  @Override
  public boolean hasNext() {
    ensureOpen();
    if (curReadPosition < cache.size()) {
      return true;
    }
    if (allSourceConsumed) {
      return false;
    }

    boolean has = source.hasNext();
    if (!has) {
      allSourceConsumed = true;
    }
    return has;
  }

  @Override
  public T next() {
    ensureOpen();
    if (curReadPosition < cache.size()) {
      return cache.get(curReadPosition++);
    }
    if (allSourceConsumed) {
      throw new NoSuchElementException();
    }

    T value = source.next();
    cache.add(value);
    curReadPosition++;

    if (!source.hasNext()) {
      allSourceConsumed = true;
    }
    return value;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    source.close();
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Iterator is closed");
    }
  }
}
