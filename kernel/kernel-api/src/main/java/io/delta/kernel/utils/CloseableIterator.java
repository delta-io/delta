package io.delta.kernel.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {
    default <U> CloseableIterator<U> map(Function<T, U> mapper) {
        CloseableIterator<T> delegate = this;
        return new CloseableIterator<U>() {
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
