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

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides file handling functionality to Delta Kernel. Connectors can implement this client to
 * provide Delta Kernel their own custom implementation of file splitting, additional predicate
 * pushdown or any other connector-specific capabilities.
 */
public interface FileHandler
{
    /**
     * Associates a connector specific {@link FileReadContext} for each scan file represented by a
     * {@link Row} in {@code fileIter}. Delta Kernel will supply the returned
     * {@link FileReadContext}s back to the connector when reading the file (for example, in
     * {@link ParquetHandler#readParquetFiles}). Delta Kernel does not interpret
     * {@link FileReadContext}.
     *
     * For example, a connector can attach split information in its own implementation
     * of {@link FileReadContext} or attach any predicates.
     *
     * @param fileIter iterator of scan files where each {@link Row} contains {@link FileStatus}
     *                 information
     * @param predicate Predicate to prune data. This is optional for the connector to use for
     *                  further optimization. Filtering by this predicate is not required.
     * @return Iterator of {@link FileReadContext} to read data from.
     */
    CloseableIterator<FileReadContext> contextualizeFileReads(
            CloseableIterator<Row> fileIter,
            Expression predicate);
}
