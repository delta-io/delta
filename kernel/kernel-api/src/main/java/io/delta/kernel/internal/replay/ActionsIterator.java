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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.replay.DeltaLogFile.LogType.MULTIPART_CHECKPOINT;
import static io.delta.kernel.internal.replay.DeltaLogFile.LogType.SIDECAR;
import static io.delta.kernel.internal.util.FileNames.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class takes as input a list of delta files (.json, .checkpoint.parquet) and produces an
 * iterator of (ColumnarBatch, isFromCheckpoint) tuples, where the schema of the ColumnarBatch
 * semantically represents actions (or, a subset of action fields) parsed from the Delta Log.
 *
 * <p>Users must pass in a `readSchema` to select which actions and sub-fields they want to consume.
 */
public class ActionsIterator implements CloseableIterator<ActionWrapper> {
  private final Engine engine;

  private final Optional<Predicate> checkpointPredicate;

  /**
   * Linked list of iterator files (commit files and/or checkpoint files) {@link LinkedList} to
   * allow removing the head of the list and also to peek at the head of the list. The {@link
   * Iterator} doesn't provide a way to peek.
   *
   * <p>Each of these files return an iterator of {@link ColumnarBatch} containing the actions
   */
  private final LinkedList<DeltaLogFile> filesList;

  private final StructType readSchema;

  private final boolean schemaContainsAddOrRemoveFiles;

  /**
   * The current (ColumnarBatch, isFromCheckpoint) tuple. Whenever this iterator is exhausted, we
   * will try and fetch the next one from the `filesList`.
   *
   * <p>If it is ever empty, that means there are no more batches to produce.
   */
  private Optional<CloseableIterator<ActionWrapper>> actionsIter;

  private boolean closed;

  public ActionsIterator(
      Engine engine,
      List<FileStatus> files,
      StructType readSchema,
      Optional<Predicate> checkpointPredicate) {
    this.engine = engine;
    this.checkpointPredicate = checkpointPredicate;
    this.filesList = new LinkedList<>();
    this.filesList.addAll(
        files.stream()
            .map(file -> DeltaLogFile.forCommitOrCheckpoint(file))
            .collect(Collectors.toList()));
    this.readSchema = readSchema;
    this.actionsIter = Optional.empty();
    this.schemaContainsAddOrRemoveFiles = LogReplay.containsAddOrRemoveFileActions(readSchema);
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
   * @return a tuple of (ColumnarBatch, isFromCheckpoint), where ColumnarBatch conforms to the
   *     instance {@link #readSchema}.
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
   * If the current `actionsIter` has no more elements, this function finds the next non-empty file
   * in `filesList` and uses it to set `actionsIter`.
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
   * Get an iterator of actions from the v2 checkpoint file that may contain sidecar files. If the
   * current read schema includes Add/Remove files, then inject the sidecar column into this schema
   * to read the sidecar files from the top-level v2 checkpoint file. When the returned
   * ColumnarBatches are processed, these sidecars will be appended to the end of the file list and
   * read as part of a subsequent batch (avoiding reading the top-level v2 checkpoint files more
   * than once).
   */
  private CloseableIterator<ColumnarBatch> getActionsIterFromSinglePartOrV2Checkpoint(
      FileStatus file, String fileName) throws IOException {
    // If the sidecars may contain the current action, read sidecars from the top-level v2
    // checkpoint file(to be read later).
    StructType modifiedReadSchema = readSchema;
    if (schemaContainsAddOrRemoveFiles) {
      modifiedReadSchema = LogReplay.withSidecarFileSchema(readSchema);
    }

    long checkpointVersion = checkpointVersion(file.getPath());

    // If the read schema contains Add/Remove files, we should always read the sidecar file
    // actions from the checkpoint manifest regardless of the checkpoint predicate.
    Optional<Predicate> checkpointPredicateIncludingSidecars;
    if (schemaContainsAddOrRemoveFiles) {
      Predicate containsSidecarPredicate =
          new Predicate("IS_NOT_NULL", new Column(LogReplay.SIDECAR_FIELD_NAME));
      checkpointPredicateIncludingSidecars =
          checkpointPredicate.map(p -> new Or(p, containsSidecarPredicate));
    } else {
      checkpointPredicateIncludingSidecars = checkpointPredicate;
    }
    final CloseableIterator<ColumnarBatch> topLevelIter;
    StructType finalModifiedReadSchema = modifiedReadSchema;
    if (fileName.endsWith(".parquet")) {
      topLevelIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getParquetHandler()
                      .readParquetFiles(
                          singletonCloseableIterator(file),
                          finalModifiedReadSchema,
                          checkpointPredicateIncludingSidecars),
              "Reading parquet log file `%s` with readSchema=%s and predicate=%s",
              file,
              modifiedReadSchema,
              checkpointPredicateIncludingSidecars);
    } else if (fileName.endsWith(".json")) {
      topLevelIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getJsonHandler()
                      .readJsonFiles(
                          singletonCloseableIterator(file),
                          finalModifiedReadSchema,
                          checkpointPredicateIncludingSidecars),
              "Reading JSON log file `%s` with readSchema=%s and predicate=%s",
              file,
              modifiedReadSchema,
              checkpointPredicateIncludingSidecars);
    } else {
      throw new IOException("Unrecognized top level v2 checkpoint file format: " + fileName);
    }
    return new CloseableIterator<ColumnarBatch>() {
      @Override
      public void close() throws IOException {
        topLevelIter.close();
      }

      @Override
      public boolean hasNext() {
        return topLevelIter.hasNext();
      }

      @Override
      public ColumnarBatch next() {
        ColumnarBatch batch = topLevelIter.next();
        if (schemaContainsAddOrRemoveFiles) {
          return extractSidecarsFromBatch(file, checkpointVersion, batch);
        }
        return batch;
      }
    };
  }

  /**
   * Reads SidecarFile actions from ColumnarBatch, removing sidecar actions from the ColumnarBatch.
   * Returns a list of SidecarFile actions found.
   */
  public ColumnarBatch extractSidecarsFromBatch(
      FileStatus checkpointFileStatus, long checkpointVersion, ColumnarBatch columnarBatch) {
    checkArgument(columnarBatch.getSchema().fieldNames().contains(LogReplay.SIDECAR_FIELD_NAME));

    Path deltaLogPath = new Path(checkpointFileStatus.getPath()).getParent();

    // Sidecars will exist in schema. Extract sidecar files, then remove sidecar files from
    // batch output.
    int sidecarIndex = columnarBatch.getSchema().fieldNames().indexOf(LogReplay.SIDECAR_FIELD_NAME);
    ColumnVector sidecarVector = columnarBatch.getColumnVector(sidecarIndex);
    for (int i = 0; i < columnarBatch.getSize(); i++) {
      SidecarFile sidecarFile = SidecarFile.fromColumnVector(sidecarVector, i);
      if (sidecarFile == null) {
        continue;
      }
      FileStatus sideCarFileStatus =
          FileStatus.of(
              FileNames.sidecarFile(deltaLogPath, sidecarFile.getPath()),
              sidecarFile.getSizeInBytes(),
              sidecarFile.getModificationTime());

      filesList.add(DeltaLogFile.ofSideCar(sideCarFileStatus, checkpointVersion));
    }

    // Delete SidecarFile actions from the schema.
    return columnarBatch.withDeletedColumnAt(sidecarIndex);
  }

  /**
   * Get the next file from `filesList` (.json or .checkpoint.parquet) read it + inject the
   * `isFromCheckpoint` information.
   *
   * <p>Requires that `filesList.isEmpty` is false.
   */
  private CloseableIterator<ActionWrapper> getNextActionsIter() {
    final DeltaLogFile nextLogFile = filesList.pop();
    final FileStatus nextFile = nextLogFile.getFile();
    final Path nextFilePath = new Path(nextFile.getPath());
    final String fileName = nextFilePath.getName();
    try {
      switch (nextLogFile.getLogType()) {
        case COMMIT:
          {
            final long fileVersion = FileNames.deltaVersion(nextFilePath);
            // We can not read multiple JSON files in parallel (like the checkpoint files),
            // because each one has a different version, and we need to associate the
            // version with actions read from the JSON file for further optimizations later
            // on (faster metadata & protocol loading in subsequent runs by remembering
            // the version of the last version where the metadata and protocol are found).
            final CloseableIterator<ColumnarBatch> dataIter =
                wrapEngineExceptionThrowsIO(
                    () ->
                        engine
                            .getJsonHandler()
                            .readJsonFiles(
                                singletonCloseableIterator(nextFile), readSchema, Optional.empty()),
                    "Reading JSON log file `%s` with readSchema=%s",
                    nextFile,
                    readSchema);

            return combine(
                dataIter,
                false /* isFromCheckpoint */,
                fileVersion,
                Optional.of(nextFile.getModificationTime()) /* timestamp */);
          }
        case CHECKPOINT_CLASSIC:
        case V2_CHECKPOINT_MANIFEST:
          {
            // If the checkpoint file is a UUID or classic checkpoint, read the top-level
            // checkpoint file and any potential sidecars. Otherwise, look for any other
            // parts of the current multipart checkpoint.
            CloseableIterator<ColumnarBatch> dataIter =
                getActionsIterFromSinglePartOrV2Checkpoint(nextFile, fileName);
            long version = checkpointVersion(nextFilePath);
            return combine(dataIter, true /* isFromCheckpoint */, version, Optional.empty());
          }
        case MULTIPART_CHECKPOINT:
        case SIDECAR:
          {
            // Try to retrieve the remaining checkpoint files (if there are any) and issue
            // read request for all in one go, so that the {@link ParquetHandler} can do
            // optimizations like reading multiple files in parallel.
            CloseableIterator<FileStatus> checkpointFiles =
                retrieveRemainingCheckpointFiles(nextLogFile);
            CloseableIterator<ColumnarBatch> dataIter =
                wrapEngineExceptionThrowsIO(
                    () ->
                        engine
                            .getParquetHandler()
                            .readParquetFiles(checkpointFiles, readSchema, checkpointPredicate),
                    "Reading checkpoint sidecars [%s] with readSchema=%s and predicate=%s",
                    checkpointFiles,
                    readSchema,
                    checkpointPredicate);

            long version = checkpointVersion(nextFilePath);
            return combine(dataIter, true /* isFromCheckpoint */, version, Optional.empty());
          }
        default:
          throw new IOException("Unrecognized log type: " + nextLogFile.getLogType());
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** Take input (iterator<T>, boolean) and produce an iterator<T, boolean>. */
  private CloseableIterator<ActionWrapper> combine(
      CloseableIterator<ColumnarBatch> fileReadDataIter,
      boolean isFromCheckpoint,
      long version,
      Optional<Long> timestamp) {
    return new CloseableIterator<ActionWrapper>() {
      @Override
      public boolean hasNext() {
        return fileReadDataIter.hasNext();
      }

      @Override
      public ActionWrapper next() {
        return new ActionWrapper(fileReadDataIter.next(), isFromCheckpoint, version, timestamp);
      }

      @Override
      public void close() throws IOException {
        fileReadDataIter.close();
      }
    };
  }

  /**
   * Given a checkpoint file, retrieve all the files that are part of the same checkpoint or sidecar
   * files.
   *
   * <p>This is done by looking at the log file type and finding all the files that have the same
   * version number.
   */
  private CloseableIterator<FileStatus> retrieveRemainingCheckpointFiles(
      DeltaLogFile deltaLogFile) {

    // Find the contiguous parquet files that are part of the same checkpoint
    final List<FileStatus> checkpointFiles = new ArrayList<>();

    // Add the already retrieved checkpoint file to the list.
    checkpointFiles.add(deltaLogFile.getFile());

    // Sidecar or multipart checkpoint types are the only files that can have multiple parts.
    if (deltaLogFile.getLogType() == SIDECAR || deltaLogFile.getLogType() == MULTIPART_CHECKPOINT) {

      DeltaLogFile peek = filesList.peek();
      while (peek != null
          && deltaLogFile.getLogType() == peek.getLogType()
          && deltaLogFile.getVersion() == peek.getVersion()) {
        checkpointFiles.add(filesList.pop().getFile());
        peek = filesList.peek();
      }
    }

    return toCloseableIterator(checkpointFiles.iterator());
  }
}
