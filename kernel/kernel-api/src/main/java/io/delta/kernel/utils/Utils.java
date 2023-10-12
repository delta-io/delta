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

import java.io.IOException;
import java.util.Iterator;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.Row;

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
     * @param <T>  Element type.
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
     * @param <T>  Element type
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
     * Close the given one or more {@link AutoCloseable}s. {@link AutoCloseable#close()}
     * will be called on all given non-null closeables. Will throw unchecked
     * {@link RuntimeException} if an error occurs while closing. If multiple closeables causes
     * exceptions in closing, the exceptions will be added as suppressed to the main exception
     * that is thrown.
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
     * Close the given list of {@link AutoCloseable} objects. Any exception thrown is
     * silently ignored.
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

    public static Row requireNonNull(Row row, int ordinal, String columnName) {
        if (row.isNullAt(ordinal)) {
            throw new IllegalArgumentException(
                "Expected a non-null value for column: " + columnName);
        }
        return row;
    }
}
