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
package io.delta.kernel.defaults.engine.io;

import java.io.IOException;
import java.io.InputStream;

public abstract class SeekableInputStream extends InputStream
{
    /**
     * Get the current position in the stream.
     *
     * @return the current position in bytes from the start of the stream
     * @throws IOException if the underlying stream throws an IOException
     */
    public abstract long getPos() throws IOException;

    /**
     * Seek to a new position in the stream.
     *
     * @param newPos the new position to seek to
     * @throws IOException if the underlying stream throws an IOException
     */
    public abstract void seek(long newPos) throws IOException;
}
