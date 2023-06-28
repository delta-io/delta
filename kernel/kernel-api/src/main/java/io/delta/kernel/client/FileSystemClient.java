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

package io.delta.kernel.client;

import java.io.FileNotFoundException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

/**
 * Provides file system related functionalities to Delta Kernel. Delta Kernel uses this client
 * whenever it needs to access the underlying file system where the Delta table is present.
 * Connector implementation of this interface can hide filesystem specific details from Delta
 * Kernel.
 */
public interface FileSystemClient
{
    /**
     * List the paths in the same directory that are lexicographically greater or equal to
     * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
     *
     * @param filePath Fully qualified path to a file
     * @return Closeable iterator of files. It is the responsibility of the caller to close the
     *         iterator.
     * @throws FileNotFoundException if the file at the given path is not found
     */
    CloseableIterator<FileStatus> listFrom(String filePath)
            throws FileNotFoundException;

    // TODO: solidify input type; need some combination of path, offset, size
    /**
     * Read data specified by the start and end offset from the file. It is the responsibility
     * of the caller close each returned stream.
     *
     * @param iter Iterator for tuples (file path, range (start offset, end offset)
     * @return Data for each range requested as one {@link ByteArrayInputStream}.
     * @throws IOException
     */
    CloseableIterator<ByteArrayInputStream> readFiles(
            CloseableIterator<Tuple2<String, Tuple2<Integer, Integer>>> iter)
            throws IOException;
}
