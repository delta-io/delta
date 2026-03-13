/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

public class CloseableIteratorTest {

  @Test
  public void testFilterMapAndClose() throws IOException {
    AtomicBoolean closed = new AtomicBoolean(false);
    Iterator<Integer> base = new CloseableTestIterator<>(Arrays.asList(1, 2, 3, 4, 5, 6), closed);

    CloseableIterator<String> iter =
        CloseableIterator.wrap(base).filterCloseable(x -> x % 2 == 0).mapCloseable(x -> "v" + x);

    List<String> result = new ArrayList<>();
    while (iter.hasNext()) {
      result.add(iter.next());
    }

    assertEquals(List.of("v2", "v4", "v6"), result);
    assertFalse(closed.get());
    iter.close();
    assertTrue(closed.get());
  }

  private static class CloseableTestIterator<T> implements Iterator<T>, Closeable {
    private final Iterator<T> delegate;
    private final AtomicBoolean closed;

    CloseableTestIterator(List<T> elements, AtomicBoolean closed) {
      this.delegate = JavaConverters.asScalaIterator(elements.iterator());
      this.closed = closed;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public T next() {
      return delegate.next();
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }
}
