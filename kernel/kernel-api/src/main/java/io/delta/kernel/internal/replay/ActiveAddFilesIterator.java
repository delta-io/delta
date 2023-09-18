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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import static io.delta.kernel.utils.Utils.requireNonNull;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;

/**
 * This class takes an iterator of ({@link FileDataReadResult}, isFromCheckpoint), where the
 * columnar data inside the FileDataReadResult represents {@link LogReplay#ADD_REMOVE_READ_SCHEMA},
 * and produces an iterator of {@link FilteredColumnarBatch} with schema
 * {@link LogReplay#ADD_ONLY_DATA_SCHEMA}, and with a selection vector indicating which AddFiles are
 * still active in the table (have not been tombstoned).
 */
class ActiveAddFilesIterator implements CloseableIterator<FilteredColumnarBatch> {
    private static class UniqueFileActionTuple extends Tuple2<URI, Optional<String>> {
        UniqueFileActionTuple(URI fileURI, Optional<String> deletionVectorId) {
            super(fileURI, deletionVectorId);
        }
    }

    private final TableClient tableClient;
    private final CloseableIterator<Tuple2<FileDataReadResult, Boolean>> iter;
    private final Set<UniqueFileActionTuple> tombstonesFromJson;
    private final Set<UniqueFileActionTuple> addFilesFromJson;

    private Optional<FilteredColumnarBatch> next;
    private boolean[] selectionVectorBuffer;
    private boolean closed;

    ActiveAddFilesIterator(
        TableClient tableClient,
        CloseableIterator<Tuple2<FileDataReadResult, Boolean>> iter) {
        this.tableClient = tableClient;
        this.iter = iter;
        this.tombstonesFromJson = new HashSet<>();
        this.addFilesFromJson = new HashSet<>();
        this.next = Optional.empty();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
        }
        if (!next.isPresent()) {
            prepareNext();
        }
        return next.isPresent();
    }

    @Override
    public FilteredColumnarBatch next() {
        if (closed) {
            throw new IllegalStateException("Can't call `next` on a closed iterator.");
        }
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // By the definition of `hasNext`, we know that `next` is non-empty

        final FilteredColumnarBatch ret = next.get();
        next = Optional.empty();
        return ret;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            iter.close();
            closed = true;
        }
    }

    /**
     * Grabs the next FileDataReadResult from `iter` and updates the value of `next`.
     *
     * Internally, implements the following algorithm:
     * 1. read all the RemoveFiles in the next ColumnarBatch to update the `tombstonesFromJson` set
     * 2. read all the AddFiles in that same ColumnarBatch, unselecting ones that have already
     *    been removed or returned by updating a selection vector
     * 3. produces a DataReadResult by dropping that RemoveFile column from the ColumnarBatch and
     *    using that selection vector
     *
     * Note that, according to the Delta protocol, "a valid [Delta] version is restricted to contain
     * at most one file action of the same type (i.e. add/remove) for any one combination of path
     * and dvId". This means that step 2 could actually come before 1 - there's no temporal
     * dependency between them.
     *
     * Ensures that
     * - `next` is non-empty if there is a next result
     * - `next` is empty if there is no next result
     */
    private void prepareNext() {
        if (next.isPresent()) return; // already have a next result
        if (!iter.hasNext()) return; // no next result, and no batches to read

        final Tuple2<FileDataReadResult, Boolean> _next = iter.next();
        final FileDataReadResult fileDataReadResult = _next._1;
        final boolean isFromCheckpoint = _next._2;
        final ColumnarBatch addRemoveColumnarBatch = fileDataReadResult.getData();

        assert(addRemoveColumnarBatch.getSchema().equals(LogReplay.ADD_REMOVE_READ_SCHEMA));

        // Step 1: Update `tombstonesFromJson` with all the RemoveFiles in this columnar batch, if
        //         and only if this batch is not from a checkpoint.
        //
        //         There's no reason to put a RemoveFile from a checkpoint into `tombstonesFromJson`
        //         since, when we generate a checkpoint, any corresponding AddFile would have
        //         been excluded already
        if (!isFromCheckpoint) {
            final ColumnVector removesVector = addRemoveColumnarBatch.getColumnVector(1);
            for (int rowId = 0; rowId < removesVector.getSize(); rowId++) {
                if (removesVector.isNullAt(rowId)) continue;

                // Note: this row doesn't represent the complete RemoveFile schema. It only contains
                //       the fields we need for this replay.

                final Row row = removesVector.getStruct(rowId);
                final String path = requireNonNull(row, 0, "path").getString(0);
                final URI pathAsUri = pathToUri(path);
                final Optional<String> dvId = Optional.ofNullable(
                    DeletionVectorDescriptor.fromRow(row.getStruct(1))
                ).map(DeletionVectorDescriptor::getUniqueId);
                final UniqueFileActionTuple key = new UniqueFileActionTuple(pathAsUri, dvId);
                tombstonesFromJson.add(key);
            }
        }

        // Step 2: Iterate over all the AddFiles in this columnar batch in order to build up the
        //         selection vector. We unselect an AddFile when it was removed by a RemoveFile
        final ColumnVector addsVector = addRemoveColumnarBatch.getColumnVector(0);
        prepareSelectionVectorBuffer(addsVector.getSize());
        boolean atLeastOneUnselected = false;

        for (int rowId = 0; rowId < addsVector.getSize(); rowId++) {
            if (addsVector.isNullAt(rowId)) {
                atLeastOneUnselected = true;
                continue; // selectionVector will be `false` at rowId by default
            }

            final Row row = addsVector.getStruct(rowId);
            final String path = AddFile.getPathFromRow(row);
            final URI pathAsUri = pathToUri(path);
            final Optional<String> dvId = Optional.ofNullable(
                AddFile.getDeletionVectorDescriptorFromRow(row)
            ).map(DeletionVectorDescriptor::getUniqueId);
            final UniqueFileActionTuple key = new UniqueFileActionTuple(pathAsUri, dvId);
            final boolean alreadyDeleted = tombstonesFromJson.contains(key);
            final boolean alreadyReturned = addFilesFromJson.contains(key);

            boolean doSelect = false;

            if (!alreadyReturned) {
                // Note: No AddFile will appear twice in a checkpoint, so we only need
                //       non-checkpoint AddFiles in the set
                if (!isFromCheckpoint) {
                    addFilesFromJson.add(key);
                }

                if (!alreadyDeleted) {
                    doSelect = true;
                    selectionVectorBuffer[rowId] = true;
                }
            }

            if (!doSelect) {
                atLeastOneUnselected = true;
            }
        }

        // Step 3: Drop the RemoveFile column and use the selection vector to build a new
        //         DataReadResult
        Optional<ColumnVector> selectionColumnVector = atLeastOneUnselected ?
            Optional.of(tableClient.getExpressionHandler()
                .createSelectionVector(selectionVectorBuffer, 0, addsVector.getSize())) :
            Optional.empty();
        next = Optional.of(
            new FilteredColumnarBatch(
                addRemoveColumnarBatch.withDeletedColumnAt(1),
                selectionColumnVector));
    }

    private void prepareSelectionVectorBuffer(int size) {
        if (selectionVectorBuffer == null || selectionVectorBuffer.length < size) {
            selectionVectorBuffer = new boolean[size];
        } else {
            // reset the array - if we are reusing the same buffer.
            Arrays.fill(selectionVectorBuffer, false);
        }
    }

    private URI pathToUri(String path) {
        try {
            return new URI(path);
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }
}
