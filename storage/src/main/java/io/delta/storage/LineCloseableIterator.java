/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

/**
 * Turn a {@link Reader} to {@link CloseableIterator} which can be read on demand. Each element is
 * a trimmed line.
 */
public class LineCloseableIterator implements CloseableIterator<String> {
    private final BufferedReader reader;

    // Whether `nextValue` is valid. If it's invalid, we should try to read the next line.
    private boolean gotNext = false;

    // The next value to return when `next` is called. This is valid only if `getNext` is true.
    private String nextValue = null;

    // Whether the reader is closed.
    private boolean closed = false;

    // Whether we have consumed all data in the reader.
    private boolean finished = false;

    public LineCloseableIterator(Reader reader) {
        this.reader =
            reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    }

    @Override
    public boolean hasNext() {
        try {
            if (!finished) {
                // Check whether we have closed the reader before reading. Even if `nextValue` is
                // valid, we still don't return `nextValue` after a reader is closed. Otherwise, it
                // would be confusing.
                if (closed) {
                    throw new IllegalStateException("Iterator is closed");
                }
                if (!gotNext) {
                    String nextLine = reader.readLine();
                    if (nextLine == null) {
                        finished = true;
                        close();
                    } else {
                        nextValue = nextLine.trim();
                    }
                    gotNext = true;
                }
            }
            return !finished;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException("End of stream");
        }
        gotNext = false;
        return nextValue;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            reader.close();
        }
    }
}
