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
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

public interface CloseableIterator<T> extends Iterator<T>, Closeable
{
    default <U> CloseableIterator<U> map(Function<T, U> mapper)
    {
        CloseableIterator<T> delegate = this;
        return new CloseableIterator<U>()
        {
            @Override
            public void remove()
            {
                delegate.remove();
            }

            @Override
            public void forEachRemaining(Consumer<? super U> action)
            {
                this.forEachRemaining(action);
            }

            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public U next()
            {
                return mapper.apply(delegate.next());
            }

            @Override
            public void close()
                throws IOException
            {
                delegate.close();
            }
        };
    }
}
