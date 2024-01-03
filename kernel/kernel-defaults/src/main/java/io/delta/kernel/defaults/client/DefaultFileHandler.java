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
package io.delta.kernel.defaults.client;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.client.FileHandler;
import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Default client implementation of {@link FileHandler}. It splits file as one split.
 */
public class DefaultFileHandler implements FileHandler {
    @Override
    public CloseableIterator<FileReadContext> contextualizeFileReads(
        CloseableIterator<Row> fileIter, Predicate filter) {
        requireNonNull(fileIter, "fileIter is null");
        requireNonNull(filter, "filter is null");
        // Note: we are not currently using the filter but it may be used later
        return fileIter.map(scanFileRow -> new DefaultFileReadContext(scanFileRow));
    }
}
