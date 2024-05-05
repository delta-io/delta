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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import io.delta.kernel.exceptions.KernelException;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

/**
 * Extend the Java {@link Iterable} interface to provide a way to close the iterator.
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface CloseableIterable<T> extends Iterable<T>, Closeable {

    /**
     * Overrides the default iterator method to return a {@link CloseableIterator}.
     *
     * @return a {@link CloseableIterator} instance.
     */
    @Override
    CloseableIterator<T> iterator();

    @Override
    default void forEach(Consumer<? super T> action) {
        try (CloseableIterator<T> iterator = iterator()) {
            iterator.forEachRemaining(action);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    default Spliterator<T> spliterator() {
        // We need a way to close the iterator and is not used in Kernel, so for now
        // make the default implementation throw an exception.
        throw new UnsupportedOperationException("spliterator is not supported");
    }

    /**
     * Return an {@link CloseableIterable} object that is backed by an in-memory collection of given
     * {@link CloseableIterator}. Users should aware that the returned {@link CloseableIterable}
     * will hold the data in memory.
     *
     * @param iterator the iterator to be converted to a {@link CloseableIterable}. It will be
     *                 closed by this callee.
     * @param <T>      the type of elements returned by the iterator
     * @return a {@link CloseableIterable} instance.
     */
    static <T> CloseableIterable<T> inMemoryIterable(CloseableIterator<T> iterator) {
        final ArrayList<T> elements = new ArrayList<>();
        try (CloseableIterator<T> iter = iterator) {
            while (iter.hasNext()) {
                elements.add(iter.next());
            }
        } catch (Exception e) {
            // TODO: we may need utility methods to throw the KernelException as is
            // without wrapping in RuntimeException.
            if (e instanceof KernelException) {
                throw (KernelException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
        return new CloseableIterable<T>() {
            @Override
            public void close() throws IOException {
                // nothing to close
            }

            @Override
            public CloseableIterator<T> iterator() {
                return toCloseableIterator(elements.iterator());
            }
        };
    }

    /**
     * Return an {@link CloseableIterable} object for an empty collection.
     * @return a {@link CloseableIterable} instance.
     * @param <T> the type of elements returned by the iterator
     */
    static <T> CloseableIterable<T> emptyIterable() {
        final CloseableIterator<T> EMPTY_ITERATOR =
                toCloseableIterator(Collections.<T>emptyList().iterator());
        return new CloseableIterable<T>() {
            @Override
            public void close() throws IOException {
                // nothing to close
            }

            @Override
            public CloseableIterator<T> iterator() {
                return EMPTY_ITERATOR;
            }
        };
    }
}
