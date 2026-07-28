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

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

/**
 * A Scala iterator that implements {@link Closeable}. Unlike standard Scala iterators, the {@link
 * #filterCloseable} and {@link #mapCloseable} methods return a {@link CloseableIterator} that
 * properly delegates {@link #close()} to the underlying iterator.
 *
 * <p>This is important for iterators that hold resources (e.g., file handles from Parquet readers)
 * that must be released when iteration completes or is interrupted.
 *
 * <p>Inspired by Spark's {@code org.apache.spark.sql.util.CloseableIterator}, which is
 * package-private ({@code private[sql]}) and cannot be used directly.
 *
 * @param <T> the type of elements returned by this iterator
 */
public abstract class CloseableIterator<T> extends AbstractIterator<T> implements Closeable {

  /**
   * Wraps a Scala iterator as a {@link CloseableIterator}. If the iterator already implements
   * {@link Closeable}, {@link #close()} will delegate to it; otherwise close is a no-op.
   */
  public static <T> CloseableIterator<T> wrap(Iterator<T> iterator) {
    if (iterator instanceof CloseableIterator) {
      return (CloseableIterator<T>) iterator;
    }
    return new WrappedIterator<>(iterator);
  }

  /**
   * Returns a new {@link CloseableIterator} that applies the given function to each element.
   * Closing the returned iterator will close this iterator.
   */
  public <U> CloseableIterator<U> mapCloseable(Function<T, U> mapper) {
    CloseableIterator<T> self = this;
    return new CloseableIterator<U>() {
      @Override
      public boolean hasNext() {
        return self.hasNext();
      }

      @Override
      public U next() {
        return mapper.apply(self.next());
      }

      @Override
      public void close() throws IOException {
        self.close();
      }
    };
  }

  /**
   * Returns a new {@link CloseableIterator} that includes only elements matching the predicate.
   * Closing the returned iterator will close this iterator.
   */
  public CloseableIterator<T> filterCloseable(Predicate<T> predicate) {
    CloseableIterator<T> self = this;
    return new CloseableIterator<T>() {
      private T nextElement;
      private boolean hasNextElement;

      @Override
      public boolean hasNext() {
        if (hasNextElement) {
          return true;
        }
        while (self.hasNext()) {
          T element = self.next();
          if (predicate.test(element)) {
            nextElement = element;
            hasNextElement = true;
            return true;
          }
        }
        return false;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        hasNextElement = false;
        return nextElement;
      }

      @Override
      public void close() throws IOException {
        self.close();
      }
    };
  }

  /** A wrapper that makes any Scala iterator closeable. */
  private static class WrappedIterator<T> extends CloseableIterator<T> {
    private final Iterator<T> delegate;

    WrappedIterator(Iterator<T> delegate) {
      this.delegate = delegate;
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
    public void close() throws IOException {
      if (delegate instanceof Closeable) {
        ((Closeable) delegate).close();
      }
    }
  }
}
