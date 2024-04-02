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
import java.io.UncheckedIOException;
import java.util.*;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.fs.Path.getName;
import static io.delta.kernel.internal.util.FileNames.*;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

/**
 * This class takes as input a list of delta files (.json, .checkpoint.parquet) and produces an
 * iterator of (ColumnarBatch, isFromCheckpoint) tuples, where the schema of the
 * ColumnarBatch semantically represents actions (or, a subset of action fields) parsed from
 * the Delta Log.
 * <p>
 * Users must pass in a `readSchema` to select which actions and sub-fields they want to consume.
 */
class ActionsIterator implements CloseableIterator<ActionWrapper> {
    private final TableClient tableClient;

    /**
     * Linked list of iterator files (commit files and/or checkpoint files)
     * {@link LinkedList} to allow removing the head of the list and also to peek at the head
     * of the list. The {@link Iterator} doesn't provide a way to peek.
     * <p>
     * Each of these files return an iterator of {@link ColumnarBatch} containing the actions
     */
    private final LinkedList<FileStatus> filesList;

    private final StructType readSchema;

    /**
     * The current (ColumnarBatch, isFromCheckpoint) tuple. Whenever this iterator
     * is exhausted, we will try and fetch the next one from the `filesList`.
     * <p>
     * If it is ever empty, that means there are no more batches to produce.
     */
    private Optional<CloseableIterator<ActionWrapper>> actionsIter;

    private boolean closed;

    ActionsIterator(
            TableClient tableClient,
            List<FileStatus> files,
            StructType readSchema) {
        this.tableClient = tableClient;
        this.filesList = new LinkedList<>();
        this.filesList.addAll(files);
        this.readSchema = readSchema;
        this.actionsIter = Optional.empty();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
        }

        tryEnsureNextActionsIterIsReady();

        // By definition of tryEnsureNextActionsIterIsReady, we know that if actionsIter
        // is non-empty then it has a next element

        return actionsIter.isPresent();
    }

    /**
     * @return a tuple of (ColumnarBatch, isFromCheckpoint), where ColumnarBatch conforms
     * to the instance {@link #readSchema}.
     */
    @Override
    public ActionWrapper next() {
        if (closed) {
            throw new IllegalStateException("Can't call `next` on a closed iterator.");
        }

        if (!hasNext()) {
            throw new NoSuchElementException("No next element");
        }

        return actionsIter.get().next();
    }

    @Override
    public void close() throws IOException {
        if (!closed && actionsIter.isPresent()) {
            actionsIter.get().close();
            actionsIter = Optional.empty();
            closed = true;
        }
    }

    /**
     * If the current `actionsIter` has no more elements, this function finds the next
     * non-empty file in `filesList` and uses it to set `actionsIter`.
     */
    private void tryEnsureNextActionsIterIsReady() {
        if (actionsIter.isPresent()) {
            // This iterator already has a next element, so we can exit early;
            if (actionsIter.get().hasNext()) {
                return;
            }

            // Clean up resources
            Utils.closeCloseables(actionsIter.get());

            // Set this to empty since we don't know if there's a next file yet
            actionsIter = Optional.empty();
        }

        // Search for the next non-empty file and use that iter
        while (!filesList.isEmpty()) {
            actionsIter = Optional.of(getNextActionsIter());

            if (actionsIter.get().hasNext()) {
                // It is ready, we are done
                return;
            }

            // It was an empty file. Clean up resources
            Utils.closeCloseables(actionsIter.get());

            // Set this to empty since we don't know if there's a next file yet
            actionsIter = Optional.empty();
        }
    }

    /**
     * Get the next file from `filesList` (.json or .checkpoint.parquet)
     * read it + inject the `isFromCheckpoint` information.
     * <p>
     * Requires that `filesList.isEmpty` is false.
     */
    private CloseableIterator<ActionWrapper> getNextActionsIter() {
        final FileStatus nextFile = filesList.pop();
        final Path nextFilePath = new Path(nextFile.getPath());
        try {
            if (isCommitFile(nextFilePath.getName())) {
                final long fileVersion = FileNames.deltaVersion(nextFilePath);

                // We can not read multiple JSON files in parallel (like the checkpoint files),
                // because each one has a different version, and we need to associate the version
                // with actions read from the JSON file for further optimizations later on (faster
                // metadata & protocol loading in subsequent runs by remembering the version of
                // the last version where the metadata and protocol are found).

                final CloseableIterator<ColumnarBatch> dataIter =
                    tableClient.getJsonHandler().readJsonFiles(
                        singletonCloseableIterator(nextFile),
                        readSchema,
                        Optional.empty());

                return combine(dataIter, false /* isFromCheckpoint */, fileVersion);
            } else if (isCheckpointFile(nextFilePath.getName())) {
                CheckpointInstance checkpointInstance =
                        new CheckpointInstance(nextFilePath.getName());
                final long fileVersion = checkpointInstance.version;

                // Try to retrieve the remaining checkpoint files (if there are any) and issue
                // read request for all in one go, so that the {@link ParquetHandler} can do
                // optimizations like reading multiple files in parallel.
                List<FileStatus> checkpointFiles =
                        retrieveRemainingCheckpointFiles(fileVersion);

                // If the checkpoint file is a UUID-named checkpoint, we may need to read the JSON
                // actions separately. Otherwise, we can add the checkpoint manifest to the list of
                // checkpoint files to be read in parallel.
                CloseableIterator<ColumnarBatch> checkpointJsonActions;
                if (checkpointInstance.format.equals(CheckpointInstance.CheckpointFormat.V2) &&
                    nextFilePath.getName().endsWith(".json")) {
                    checkpointJsonActions = tableClient.getJsonHandler().readJsonFiles(
                            toCloseableIterator(
                                    Collections.singletonList(nextFile).iterator()),
                            readSchema,
                            Optional.empty());
                } else {
                    checkpointFiles.add(nextFile);
                    checkpointJsonActions = toCloseableIterator(
                            new ArrayList<ColumnarBatch>().iterator());
                }

                // Read any potential sidecars for a checkpoint format supporting sidecars and if
                // the current read is for Add/Remove Files (otherwise, no reason to read sidecars).
                if (checkpointInstance.format.usesSidecars() &&
                        LogReplay.isAddRemoveReadSchema(readSchema)) {
                    // If the checkpoint format may contain sidecars, read the sidecar actions from
                    // the checkpoint.
                    final CloseableIterator<ColumnarBatch> manifestIter;
                    if (nextFilePath.getName().endsWith(".parquet")) {
                        manifestIter = tableClient.getParquetHandler().readParquetFiles(
                                toCloseableIterator(
                                        Collections.singletonList(nextFile).iterator()),
                                LogReplay.getSidecarFileSchema(),
                                Optional.empty());
                    } else if (nextFilePath.getName().endsWith(".json")) {
                        manifestIter = tableClient.getJsonHandler().readJsonFiles(
                                toCloseableIterator(
                                        Collections.singletonList(nextFile).iterator()),
                                LogReplay.getSidecarFileSchema(),
                                Optional.empty());
                    } else {
                        throw new IOException("Unrecognized file format");
                    }

                    while (manifestIter.hasNext()) {
                        ColumnarBatch currentBatch = manifestIter.next();
                        CloseableIterator<SidecarFile> sidecars =
                                currentBatch.getRows().map(SidecarFile::fromRow);
                        while (sidecars.hasNext()) {
                            SidecarFile f = sidecars.next();
                            if (f != null) {
                                checkpointFiles.add(
                                        FileStatus.of(
                                                FileNames.sidecarFile(nextFilePath.getParent(),
                                                        f.path),
                                                f.sizeInBytes, f.modificationTime));
                            }
                        }
                    }
                }

                // For a classic checkpoint, this will read all files (single part or multipart).
                // For a V2 checkpoint, this will read the manifest file (if Parquet), and all
                // sidecar files. If the manifest is a JSON file, append the manifest's batches
                // to the result of this read.
                final CloseableIterator<ColumnarBatch> dataIter =
                    tableClient.getParquetHandler().readParquetFiles(
                        toCloseableIterator(checkpointFiles.iterator()),
                        readSchema,
                        Optional.empty()).append(checkpointJsonActions);

                return combine(dataIter, true /* isFromCheckpoint */, fileVersion);
            } else {
                throw new IllegalStateException(
                    String.format("Unexpected log file path: %s", nextFile.getPath())
                );
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Take input (iterator<T>, boolean) and produce an iterator<T, boolean>.
     */
    private CloseableIterator<ActionWrapper> combine(
            CloseableIterator<ColumnarBatch> fileReadDataIter,
            boolean isFromCheckpoint,
            long version) {
        return new CloseableIterator<ActionWrapper>() {
            @Override
            public boolean hasNext() {
                return fileReadDataIter.hasNext();
            }

            @Override
            public ActionWrapper next() {
                return new ActionWrapper(fileReadDataIter.next(), isFromCheckpoint, version);
            }

            @Override
            public void close() throws IOException {
                fileReadDataIter.close();
            }
        };
    }

    /**
     * Given a checkpoint file, retrieve all the files that are part of the same checkpoint.
     * <p>
     * This is done by looking at the file name and finding all the files that have the same
     * version number.
     */
    private List<FileStatus> retrieveRemainingCheckpointFiles(long version) {

        // Find the contiguous parquet files that are part of the same checkpoint
        final List<FileStatus> checkpointFiles = new ArrayList<>();

        // Do not add the already retrieved checkpoint file to the list - it may be a JSON
        // checkpoint.
        FileStatus peek = filesList.peek();
        while (peek != null &&
                isCheckpointFile(getName(peek.getPath())) &&
                checkpointVersion(peek.getPath()) == version) {
            checkpointFiles.add(filesList.pop());
            peek = filesList.peek();
        }

        return checkpointFiles;
    }
}
