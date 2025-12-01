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

package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Various utility methods to help the connectors work with data objects returned by Kernel
 *
 * @since 3.0.0
 */
@Evolving
public class Utils {
  /**
   * Utility method to create a singleton {@link CloseableIterator}.
   *
   * @param elem Element to create iterator with.
   * @param <T> Element type.
   * @return A {@link CloseableIterator} with just one element.
   */
  public static <T> CloseableIterator<T> singletonCloseableIterator(T elem) {
    return new CloseableIterator<T>() {
      private boolean accessed;

      @Override
      public void close() throws IOException {
        // nothing to close
      }

      @Override
      public boolean hasNext() {
        return !accessed;
      }

      @Override
      public T next() {
        accessed = true;
        return elem;
      }
    };
  }

  /**
   * Convert a {@link Iterator} to {@link CloseableIterator}. Useful when passing normal iterators
   * for arguments that require {@link CloseableIterator} type.
   *
   * @param iter {@link Iterator} instance
   * @param <T> Element type
   * @return A {@link CloseableIterator} wrapping the given {@link Iterator}
   */
  public static <T> CloseableIterator<T> toCloseableIterator(Iterator<T> iter) {
    return new CloseableIterator<T>() {
      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        return iter.next();
      }
    };
  }

  /**
   * Close the given one or more {@link AutoCloseable}s. {@link AutoCloseable#close()} will be
   * called on all given non-null closeables. Will throw unchecked {@link RuntimeException} if an
   * error occurs while closing. If multiple closeables causes exceptions in closing, the exceptions
   * will be added as suppressed to the main exception that is thrown.
   *
   * @param closeables
   */
  public static void closeCloseables(AutoCloseable... closeables) {
    RuntimeException exception = null;
    for (AutoCloseable closeable : closeables) {
      if (closeable == null) {
        continue;
      }
      try {
        closeable.close();
      } catch (Exception ex) {
        if (exception == null) {
          exception = new RuntimeException(ex);
        } else {
          exception.addSuppressed(ex);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * Close the given list of {@link AutoCloseable} objects. Any exception thrown is silently
   * ignored.
   *
   * @param closeables
   */
  public static void closeCloseablesSilently(AutoCloseable... closeables) {
    try {
      closeCloseables(closeables);
    } catch (Throwable throwable) {
      // ignore
    }
  }

  // Utility class to support `intoRows` below
  private static class FilteredBatchToRowIter implements CloseableIterator<Row> {
    private final CloseableIterator<FilteredColumnarBatch> sourceBatches;
    private CloseableIterator<Row> current;
    private boolean isClosed = false;

    FilteredBatchToRowIter(CloseableIterator<FilteredColumnarBatch> sourceBatches) {
      this.sourceBatches = sourceBatches;
    }

    @Override
    public boolean hasNext() {
      if (isClosed) {
        return false;
      }
      while ((current == null || !current.hasNext()) && sourceBatches.hasNext()) {
        closeCloseables(current);
        FilteredColumnarBatch next = sourceBatches.next();
        current = next.getRows();
      }
      return current != null && current.hasNext();
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException("No more rows available");
      }
      return current.next();
    }

    @Override
    public void close() throws IOException {
      isClosed = true;
      closeCloseables(current, sourceBatches);
    }
  }

  /** Convert a ClosableIterator of FilteredColumnarBatch into a CloseableIterator of Row */
  public static CloseableIterator<Row> intoRows(
      CloseableIterator<FilteredColumnarBatch> sourceBatches) {
    return new FilteredBatchToRowIter(sourceBatches);
  }

  /**
   * Flattens a nested {@link CloseableIterator} structure into a single flat iterator. This method
   * takes an iterator of iterators (nested structure) and flattens it into a single iterator that
   * yields all elements from all inner iterators in sequence.
   *
   * <p><b>Important:</b> Callers must call {@link CloseableIterator#close()} on the returned
   * iterator even if it is not fully consumed, to ensure all inner iterators are properly closed
   * and resources are released.
   *
   * @param nestedIterator An iterator of iterators to flatten
   * @param <T> The type of elements in the inner iterators
   * @return A new {@link CloseableIterator} that flattens all nested iterators
   */
  public static <T> CloseableIterator<T> flatten(
      CloseableIterator<CloseableIterator<T>> nestedIterator) {
    return new CloseableIterator<>() {
      private CloseableIterator<T> currentInnerIterator = null;

      @Override
      public boolean hasNext() {
        while (true) {
          if (currentInnerIterator != null && currentInnerIterator.hasNext()) {
            return true;
          }

          if (currentInnerIterator != null) {
            closeCloseables(currentInnerIterator);
            currentInnerIterator = null;
          }

          if (!nestedIterator.hasNext()) {
            return false;
          }

          try {
            currentInnerIterator = nestedIterator.next();
          } catch (Exception e) {
            // Ensure cleanup on exception
            closeCloseables(nestedIterator);
            throw e;
          }
        }
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return currentInnerIterator.next();
      }

      @Override
      public void close() {
        // Close both the current inner iterator and the outer iterator
        // closeCloseables works with null closeable.
        closeCloseables(currentInnerIterator, nestedIterator);
      }
    };
  }

  public static String resolvePath(Engine engine, String path) {
    try {
      return wrapEngineExceptionThrowsIO(
          () -> engine.getFileSystemClient().resolvePath(path), "Resolving path %s", path);
    } catch (IOException io) {
      throw new UncheckedIOException(io);
    }
  }
}
