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
import java.util.stream.Collectors;

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
    private final LinkedList<FileWrapper> filesList;

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
        this.filesList.addAll(
                files.stream().map(file -> new FileWrapper(file, null))
                        .collect(Collectors.toList()));
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

        ActionWrapper next = actionsIter.get().next();

        // If the ActionWrapper schema does not match the read schema, then we have injected sidecar
        // files into the schema. Append any sidecar files found to the end of the file list and
        // remove the sidecars from the schema.
        ColumnarBatch nextBatch = next.getColumnarBatch();
        if (!next.getColumnarBatch().getSchema().equals(readSchema) && next.isFromCheckpoint()) {
            return appendSidecarsFromCurrentBatch(next, nextBatch);
        }

        return next;
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
     * Reads SidecarFile actions from ColumnarBatch, appending the SidecarFile actions to the end
     * of the file list and removing sidecar actions from the ColumnarBatch.
     */
    private ActionWrapper appendSidecarsFromCurrentBatch(
            ActionWrapper action,
            ColumnarBatch batch) {
        // If the source checkpoint for this action does not use sidecars, sidecars will not exist
        // in schema.
        CheckpointInstance sourceCheckpoint =
                new CheckpointInstance(action.getCheckpointPath().toString());
        if (!sourceCheckpoint.format.usesSidecars()) {
            return action;
        }

        // Sidecars will exist in schema. Extract sidecar files, then remove sidecar files from
        // batch output.
        int sidecarIndex = batch.getSchema().fieldNames().indexOf(LogReplay.SIDECAR_FIELD_NAME);
        CloseableIterator<SidecarFile> sidecars =
                batch.getRows().map(r -> SidecarFile.fromRow(r, sidecarIndex));
        while (sidecars.hasNext()) {
            SidecarFile f = sidecars.next();
            if (f != null) {
                filesList.add(new FileWrapper(FileStatus.of(
                        FileNames.sidecarFile(action.getCheckpointPath().getParent(),
                                f.path), f.sizeInBytes, f.modificationTime), sourceCheckpoint));
            }
        }
        return new ActionWrapper(batch.withDeletedColumnAt(sidecarIndex),
                action.getCheckpointPath(), action.getVersion());
    }

    /**
     * Get an iterator of actions from a checkpoint manifest that may contain sidecar files. If the
     * current read schema includes Add/Remove files, then inject the sidecar column into this
     * schema to read the sidecar files from the checkpoint manifest. When the returned
     * ColumnarBatches are processed, these sidecars will be appended to the end of the file list
     * and read as part of a subsequent batch (avoiding reading the checkpoint manifest more than
     * once).
     */
    private CloseableIterator<ColumnarBatch> getActionsIterFromSidecarCheckpoint(
            FileStatus file,
            String fileName) throws IOException {
        // If the sidecars may contain the current action, read sidecars from the checkpoint
        // manifest (to be read later).
        StructType modifiedReadSchema = readSchema;
        if (LogReplay.isAddRemoveReadSchema(readSchema)) {
            modifiedReadSchema = LogReplay.withSidecarFileSchema(readSchema);
        }

        final CloseableIterator<ColumnarBatch> manifestIter;
        if (fileName.endsWith(".parquet")) {
            manifestIter = tableClient.getParquetHandler().readParquetFiles(
                    Utils.singletonCloseableIterator(file),
                    modifiedReadSchema,
                    Optional.empty());
        } else if (fileName.endsWith(".json")) {
            manifestIter = tableClient.getJsonHandler().readJsonFiles(
                    Utils.singletonCloseableIterator(file),
                    modifiedReadSchema,
                    Optional.empty());
        } else {
            throw new IOException(String.format("Unrecognized file format for file: %s",
                    fileName));
        }
        return manifestIter;
    }

    /**
     * Get the next file from `filesList` (.json or .checkpoint.parquet)
     * read it + inject the `isFromCheckpoint` information.
     * <p>
     * Requires that `filesList.isEmpty` is false.
     */
    private CloseableIterator<ActionWrapper> getNextActionsIter() {
        final FileWrapper nextFileWrapper = filesList.pop();
        final FileStatus nextFile = nextFileWrapper.getFile();
        final Path nextFilePath = new Path(nextFile.getPath());
        final String fileName = nextFilePath.getName();
        try {
            if (isCommitFile(fileName)) {
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

                return combine(dataIter, null /* isFromCheckpoint */, fileVersion);
            } else if (nextFileWrapper.isSidecar()) {
                // The current file is a sidecar file. Because all sidecar files are appended at the
                // end of the file list, read all sidecar files together here.
                List<FileStatus> sidecars = Collections.singletonList(nextFile);
                final long fileVersion = nextFileWrapper.getSidecarManifest().version;
                while(!filesList.isEmpty() && filesList.peek().isSidecar() &&
                        filesList.peek().getSidecarManifest().version == fileVersion) {
                    sidecars.add(filesList.pop().getFile());
                }
                CloseableIterator<ColumnarBatch> dataIter =
                        tableClient.getParquetHandler().readParquetFiles(
                                toCloseableIterator(sidecars.iterator()),
                                readSchema, Optional.empty());

                return combine(dataIter,
                        nextFileWrapper.getSidecarManifest().filePath.get(), fileVersion);
            } else if (isCheckpointFile(fileName)) {
                CheckpointInstance checkpointInstance = new CheckpointInstance(fileName);
                final long fileVersion = checkpointInstance.version;

                // If the checkpoint file is a UUID or classic checkpoint, read the checkpoint
                // manifest and any potential sidecars. Otherwise, look for any other parts of the
                // current multipart checkpoint.
                CloseableIterator<ColumnarBatch> dataIter;
                if (checkpointInstance.format.usesSidecars()) {
                    dataIter = getActionsIterFromSidecarCheckpoint(
                            nextFile, fileName);
                } else {
                    // Try to retrieve the remaining checkpoint files (if there are any) and issue
                    // read request for all in one go, so that the {@link ParquetHandler} can do
                    // optimizations like reading multiple files in parallel.
                    List<FileStatus> checkpointFiles =
                            retrieveRemainingCheckpointFiles(nextFile, fileVersion);
                    dataIter = tableClient.getParquetHandler().readParquetFiles(
                            toCloseableIterator(checkpointFiles.iterator()),
                            readSchema, Optional.empty());
                }

                return combine(dataIter, nextFilePath, checkpointVersion(nextFilePath));
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
            Path checkpointPath,
            long version) {
        return new CloseableIterator<ActionWrapper>() {
            @Override
            public boolean hasNext() {
                return fileReadDataIter.hasNext();
            }

            @Override
            public ActionWrapper next() {
                return new ActionWrapper(fileReadDataIter.next(), checkpointPath, version);
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
    private List<FileStatus> retrieveRemainingCheckpointFiles(
            FileStatus checkpointFile,
            long version) {

        // Find the contiguous parquet files that are part of the same checkpoint
        final List<FileStatus> checkpointFiles = new ArrayList<>();

        // Add the already retrieved checkpoint file to the list.
        checkpointFiles.add(checkpointFile);

        FileStatus peek = filesList.peek().getFile();
        while (peek != null &&
                isCheckpointFile(getName(peek.getPath())) &&
                checkpointVersion(peek.getPath()) == version) {
            checkpointFiles.add(filesList.pop().getFile());
            peek = filesList.peek().getFile();
        }

        return checkpointFiles;
    }
}
