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
import java.util.Spliterator;

/**
 * Extend the Java {@link Iterable} interface to provide a way to close the iterator.
 *
 * @param <T>
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
    default Spliterator<T> spliterator() {
        // We need a way to close the iterator, so we don't support spliterator for now.
        throw new UnsupportedOperationException("spliterator is not supported");
    }
}
