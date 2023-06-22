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

package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;

import io.delta.kernel.internal.actions.Action;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.lang.CloseableIterable;
import io.delta.kernel.internal.util.InternalUtils;

public class ReverseFilesToActionsIterable
    implements CloseableIterable<Tuple2<Action, Boolean>>
{
    private final TableClient tableClient;
    private final List<FileStatus> reverseSortedFiles;

    public ReverseFilesToActionsIterable(
        TableClient tableClient,
        Stream<FileStatus> filesUnsorted)
    {
        this.tableClient = tableClient;
        this.reverseSortedFiles = filesUnsorted
            .sorted(Comparator.comparing(
                (FileStatus a) -> new Path(a.getPath()).getName()).reversed())
            .collect(Collectors.toList());
    }

    @Override
    public CloseableIterator<Tuple2<Action, Boolean>> iterator()
    {
        return new CloseableIterator<Tuple2<Action, Boolean>>()
        {
            private final Iterator<FileStatus> filesIter = reverseSortedFiles.iterator();
            private Optional<CloseableIterator<Tuple2<Action, Boolean>>> actionsIter =
                Optional.empty();

            @Override
            public boolean hasNext()
            {
                tryEnsureNextActionsIterIsReady();

                // By definition of tryEnsureNextActionsIterIsReady, we know that if actionsIter
                // is non-empty then it has a next element

                return actionsIter.isPresent();
            }

            @Override
            public Tuple2<Action, Boolean> next()
            {
                return actionsIter.get().next();
            }

            @Override
            public void close()
                throws IOException
            {
                if (actionsIter.isPresent()) {
                    actionsIter.get().close();
                    actionsIter = Optional.empty();
                }
            }

            /**
             * If the current `actionsIter` has no more elements, this function finds the next
             * non-empty file in `filesIter` and uses it to set `actionsIter`.
             */
            private void tryEnsureNextActionsIterIsReady()
            {
                if (actionsIter.isPresent()) {
                    // This iterator already has a next element, so we can exit early;
                    if (actionsIter.get().hasNext()) {
                        return;
                    }

                    // Clean up resources
                    try {
                        actionsIter.get().close();
                    }
                    catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }

                    // Set this to empty since we don't know if there's a next file yet
                    actionsIter = Optional.empty();
                }

                // Search for the next non-empty file and use that iter
                while (filesIter.hasNext()) {
                    actionsIter = Optional.of(getNextActionsIter());

                    if (actionsIter.get().hasNext()) {
                        return;
                    }

                    // It was an empty file.// Clean up resources
                    try {
                        actionsIter.get().close();
                    }
                    catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }

                    // Set this to empty since we don't know if there's a next file yet
                    actionsIter = Optional.empty();
                }
            }

            /**
             * Requires that `filesIter.hasNext` is true
             */
            private CloseableIterator<Tuple2<Action, Boolean>> getNextActionsIter()
            {
                final FileStatus nextFile = filesIter.next();
                final Path nextFilePath = new Path(nextFile.getPath());
                final JsonHandler jsonHandler = tableClient.getJsonHandler();
                CloseableIterator<FileReadContext> fileReadContextIter =
                    jsonHandler.contextualizeFileReads(
                        Utils.singletonCloseableIterator(
                            InternalUtils.getScanFileRow(nextFile)),
                        Literal.TRUE
                    );
                try {
                    if (nextFilePath.getName().endsWith(".json")) {
                        return new ColumnarBatchToActionIterator(
                            tableClient.getJsonHandler().readJsonFiles(
                                fileReadContextIter,
                                SingleAction.READ_SCHEMA),
                            false // isFromCheckpoint
                        );
                    }
                    else if (nextFilePath.getName().endsWith(".parquet")) {
                        ParquetHandler parquetHandler = tableClient.getParquetHandler();
                        CloseableIterator<FileReadContext> fileWithContext =
                            parquetHandler.contextualizeFileReads(
                                Utils.singletonCloseableIterator(
                                    InternalUtils.getScanFileRow(nextFile)),
                                Literal.TRUE);

                        return new ColumnarBatchToActionIterator(
                            tableClient.getParquetHandler().readParquetFiles(
                                fileWithContext,
                                SingleAction.READ_SCHEMA),
                            true // isFromCheckpoint
                        );
                    }
                    else {
                        throw new IllegalStateException(
                            String.format("Unexpected log file path: %s", nextFilePath)
                        );
                    }
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    private class ColumnarBatchToActionIterator
        implements CloseableIterator<Tuple2<Action, Boolean>>
    {
        private final CloseableIterator<FileDataReadResult> batchIterator;
        private final boolean isFromCheckpoint;

        private CloseableIterator<Row> currentBatchIterator;

        /**
         * Requires that Row represents a SingleAction.
         */
        ColumnarBatchToActionIterator(
            CloseableIterator<FileDataReadResult> batchIterator,
            boolean isFromCheckpoint)
        {
            this.batchIterator = batchIterator;
            this.isFromCheckpoint = isFromCheckpoint;
        }

        @Override
        public boolean hasNext()
        {
            if (
                (currentBatchIterator == null || !currentBatchIterator.hasNext()) &&
                    batchIterator.hasNext()) {
                currentBatchIterator = batchIterator.next().getData().getRows();
            }
            return currentBatchIterator != null && currentBatchIterator.hasNext();
        }

        @Override
        public Tuple2<Action, Boolean> next()
        {
            return new Tuple2<>(
                SingleAction.fromRow(currentBatchIterator.next(), tableClient).unwrap(),
                isFromCheckpoint
            );
        }

        @Override
        public void close()
            throws IOException
        {
            // TODO: Use a safe close of the both Closeables
            if (currentBatchIterator != null) {
                currentBatchIterator.close();
            }
            if (batchIterator != null) {
                batchIterator.close();
            }
        }
    }
}
