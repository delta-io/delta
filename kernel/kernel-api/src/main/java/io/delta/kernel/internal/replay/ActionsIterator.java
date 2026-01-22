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
import static io.delta.kernel.internal.replay.DeltaLogFile.LogType.*;
import static io.delta.kernel.internal.util.FileNames.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.annotation.VisibleForTesting;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes as input a list of delta files (.json, .checkpoint.parquet) and produces an
 * iterator of (ColumnarBatch, isFromCheckpoint) tuples, where the schema of the ColumnarBatch
 * semantically represents actions (or, a subset of action fields) parsed from the Delta Log.
 *
 * <p>Users must pass in a `deltaReadSchema` to select which actions and sub-fields they want to
 * consume.
 *
 * <p>Users can also pass in an optional `checkpointReadSchema` if it is different from
 * `deltaReadSchema`.
 */
public class ActionsIterator implements CloseableIterator<ActionWrapper> {
  private static final Logger logger = LoggerFactory.getLogger(ActionsIterator.class);
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

  /** Schema used for reading delta files. */
  private final StructType deltaReadSchema;

  /**
   * Schema to be used for reading checkpoint files. Checkpoint files can be Parquet or JSON in the
   * case of v2 checkpoints.
   */
  private final StructType checkpointReadSchema;

  private final boolean schemaContainsAddOrRemoveFiles;

  /**
   * The current (ColumnarBatch, isFromCheckpoint) tuple. Whenever this iterator is exhausted, we
   * will try and fetch the next one from the `filesList`.
   *
   * <p>If it is ever empty, that means there are no more batches to produce.
   */
  private Optional<CloseableIterator<ActionWrapper>> actionsIter;

  private final Optional<PaginationContext> paginationContextOpt;
  private long numCheckpointFilesSkipped = 0;
  private long numSidecarFilesSkipped = 0;
  private boolean closed;

  public ActionsIterator(
      Engine engine,
      List<FileStatus> files,
      StructType deltaReadSchema,
      Optional<Predicate> checkpointPredicate) {
    this(
        engine,
        files,
        deltaReadSchema,
        deltaReadSchema,
        checkpointPredicate,
        Optional.empty() /* paginationContextOpt */);
  }

  public ActionsIterator(
      Engine engine,
      List<FileStatus> files,
      StructType deltaReadSchema,
      StructType checkpointReadSchema,
      Optional<Predicate> checkpointPredicate,
      Optional<PaginationContext> paginationContextOpt) {
    this.engine = engine;
    this.checkpointPredicate = checkpointPredicate;
    this.filesList = new LinkedList<>();
    this.paginationContextOpt = paginationContextOpt;
    this.filesList.addAll(
        files.stream()
            .map(DeltaLogFile::forFileStatus)
            .filter(this::paginatedFilter)
            .collect(Collectors.toList()));
    this.deltaReadSchema = deltaReadSchema;
    this.checkpointReadSchema = checkpointReadSchema;
    this.actionsIter = Optional.empty();
    this.schemaContainsAddOrRemoveFiles = LogReplay.containsAddOrRemoveFileActions(deltaReadSchema);
  }

  /**
   * Filters a log segment file based on the pagination context.
   *
   * <p>If this method returns {@code true}, the current file will be kept; otherwise, it will be
   * skipped.
   *
   * <ul>
   *   <li>If pagination is not enabled (i.e., {@code paginationContextOpt} is not present), return
   *       {@code true}.
   *   <li>If the pagination context is present but doesn't include a last read log file path,
   *       return {@code true} (indicates reading the first page).
   *   <li>If the file is a JSON log file, return {@code true} — we never skip JSON files as they're
   *       needed to build hash sets.
   *   <li>If the file is a V2 checkpoint manifest, return {@code true} — these should never be
   *       skipped.
   *   <li>If the file is a checkpoint file and comes after the last log file recorded in the page
   *       token, return {@code false} (skip it).
   * </ul>
   *
   * <p><b>Note:</b> The {@code nextLogFile} parameter cannot be a sidecar file because sidecar
   * files are not included in the log segment list. Sidecar files are handled separately later,
   * after the V2 manifest file has been read, specifically in the {@code extractSidecarFiles()}
   * method.
   *
   * @param nextLogFile the log file to evaluate
   * @return {@code true} to include the file; {@code false} to skip it
   */
  @VisibleForTesting
  // TODO: verify numCheckpointFilesSkipped is correct in E2E test
  // TODO: add unit test for this method
  public boolean paginatedFilter(DeltaLogFile nextLogFile) {
    Objects.requireNonNull(paginationContextOpt);
    // Pagination isn't enabled.
    if (!paginationContextOpt.isPresent()) return true;
    String nextFilePath = nextLogFile.getFile().getPath();
    Optional<String> lastReadLogFilePathOpt = paginationContextOpt.get().getLastReadLogFilePath();
    // Reading the first page
    if (!lastReadLogFilePathOpt.isPresent()) {
      logger.info("Pagination: no page token present, reading the first page");
      return true;
    }
    logger.info("Pagination: lastReadLogFilePath in token is {}", lastReadLogFilePathOpt.get());
    logger.info("Pagination: nextFilePath is {}", nextFilePath);
    switch (nextLogFile.getLogType()) {
      case COMMIT:
      case LOG_COMPACTION:
      case CHECKPOINT_CLASSIC:
      case V2_CHECKPOINT_MANIFEST:
        return true;
      case MULTIPART_CHECKPOINT:
        if (isFullyConsumedFile(nextFilePath, lastReadLogFilePathOpt.get())) {
          logger.info("Pagination: skip reading multi-part checkpoint file {}", nextFilePath);
          numCheckpointFilesSkipped++;
          return false;
        } else {
          return true;
        }
      case SIDECAR:
        throw new IllegalArgumentException(
            "Sidecar file shouldn't exist in log segment! Path: " + nextFilePath);
      default:
        throw new IllegalArgumentException("Unknown log file type!");
    }
  }

  /**
   * Returns true if the given file was already fully consumed in a previous page that ends at
   * lastReadLogFilePath.
   */
  private boolean isFullyConsumedFile(String filePath, String lastReadLogFilePath) {
    // Files are sorted in reverse lexicographic order.so if `filePath` is *greater* than
    // `lastReadLogFilePath`,
    // it actually comes before lastReadLogFilePath in the log stream, meaning we have already
    // paginated past it.
    return filePath.compareTo(lastReadLogFilePath) > 0;
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
   *     instance {@link #deltaReadSchema} or {@link #checkpointReadSchema} (the latter when when
   *     isFromCheckpoint=true).
   */
  @Override
  public ActionWrapper next() {
    if (closed) {
      throw new IllegalStateException("Can't call `next` on a closed iterator.");
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("Thread was interrupted");
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
    StructType modifiedReadSchema = checkpointReadSchema;
    if (schemaContainsAddOrRemoveFiles) {
      modifiedReadSchema = LogReplay.withSidecarFileSchema(checkpointReadSchema);
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
    StructType finalReadSchema = modifiedReadSchema;

    if (fileName.endsWith(".parquet")) {
      topLevelIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getParquetHandler()
                      .readParquetFiles(
                          singletonCloseableIterator(file),
                          finalReadSchema,
                          checkpointPredicateIncludingSidecars)
                      .map(FileReadResult::getData),
              "Reading parquet log file `%s` with readSchema=%s and predicate=%s",
              file,
              finalReadSchema,
              checkpointPredicateIncludingSidecars);
    } else if (fileName.endsWith(".json")) {
      topLevelIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getJsonHandler()
                      .readJsonFiles(
                          singletonCloseableIterator(file),
                          finalReadSchema,
                          checkpointPredicateIncludingSidecars),
              "Reading JSON log file `%s` with readSchema=%s and predicate=%s",
              file,
              finalReadSchema,
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
    List<DeltaLogFile> outputFiles = new ArrayList<>();
    int sidecarFieldIndexInSchema =
        columnarBatch.getSchema().fieldNames().indexOf(LogReplay.SIDECAR_FIELD_NAME);
    ColumnVector sidecarVector = columnarBatch.getColumnVector(sidecarFieldIndexInSchema);
    int sidecarIndexInV2Manifest = -1; // sidecar file index start from 0 in v2 manifest checkpoint
    for (int i = 0; i < columnarBatch.getSize(); i++) {
      SidecarFile sidecarFile = SidecarFile.fromColumnVector(sidecarVector, i);
      if (sidecarFile == null) {
        continue;
      }
      sidecarIndexInV2Manifest++;
      if (paginationContextOpt.isPresent()) {
        // Different from regular log files, name of sidecars are not in order, so we need to use
        // sidecar index in V2 manifest to compare
        if (paginationContextOpt.get().getLastReadSidecarFileIdx().isPresent()
            && sidecarIndexInV2Manifest
                < paginationContextOpt.get().getLastReadSidecarFileIdx().get()) {
          logger.info(
              "Pagination: skip reading sidecar file: index={}, path={}",
              sidecarIndexInV2Manifest,
              sidecarFile.getPath());
          numSidecarFilesSkipped++;
          continue;
        }
      }

      FileStatus sideCarFileStatus =
          FileStatus.of(
              FileNames.sidecarFile(deltaLogPath, sidecarFile.getPath()),
              sidecarFile.getSizeInBytes(),
              sidecarFile.getModificationTime());

      filesList.add(DeltaLogFile.ofSideCar(sideCarFileStatus, checkpointVersion));
    }

    if (paginationContextOpt.isPresent()) {
      logger.info("Pagination: number of sidecar files skipped is {}", numSidecarFilesSkipped);
    }

    // Delete SidecarFile actions from the schema.
    return columnarBatch.withDeletedColumnAt(sidecarFieldIndexInSchema);
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
            return readCommitOrCompactionFile(fileVersion, nextFile);
          }
        case LOG_COMPACTION:
          {
            // use end version as this is like a mini checkpoint, and that's what checkpoints do
            final long fileVersion = FileNames.logCompactionVersions(nextFilePath)._2;
            return readCommitOrCompactionFile(fileVersion, nextFile);
          }
        case CHECKPOINT_CLASSIC:
        case V2_CHECKPOINT_MANIFEST:
          {
            // If the checkpoint file is a UUID or classic checkpoint, read the top-level
            // checkpoint file and any potential sidecars. Otherwise, look for any other
            // parts of the current multipart checkpoint.
            CloseableIterator<FileReadResult> dataIter =
                getActionsIterFromSinglePartOrV2Checkpoint(nextFile, fileName)
                    .map(batch -> new FileReadResult(batch, nextFile.getPath()));
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
            CloseableIterator<FileReadResult> dataIter =
                wrapEngineExceptionThrowsIO(
                    () ->
                        engine
                            .getParquetHandler()
                            .readParquetFiles(
                                checkpointFiles, deltaReadSchema, checkpointPredicate),
                    "Reading checkpoint sidecars [%s] with readSchema=%s and predicate=%s",
                    checkpointFiles,
                    deltaReadSchema,
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

  private CloseableIterator<ActionWrapper> readCommitOrCompactionFile(
      long fileVersion, FileStatus nextFile) throws IOException {
    // We can not read multiple JSON files in parallel (like the checkpoint files),
    // because each one has a different version, and we need to associate the
    // version with actions read from the JSON file for further optimizations later
    // on (faster metadata & protocol loading in subsequent runs by remembering
    // the version of the last version where the metadata and protocol are found).
    CloseableIterator<FileReadResult> dataIter = null;
    try {
      dataIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getJsonHandler()
                      .readJsonFiles(
                          singletonCloseableIterator(nextFile), deltaReadSchema, Optional.empty())
                      .map(batch -> new FileReadResult(batch, nextFile.getPath())),
              "Reading JSON log file `%s` with readSchema=%s",
              nextFile,
              deltaReadSchema);
      return combine(
          dataIter,
          false /* isFromCheckpoint */,
          fileVersion,
          Optional.of(nextFile.getModificationTime()) /* timestamp */);
    } catch (Exception e) {
      if (dataIter != null) {
        Utils.closeCloseablesSilently(dataIter); // close it avoid leaking resources
      }
      throw e;
    }
  }

  /**
   * Takes an input iterator of actions read from the file and metadata about the file read, and
   * combines it to return an Iterator<ActionWrapper>. The timestamp in the ActionWrapper is only
   * set when the input file is not a Checkpoint. The timestamp will be set to be the
   * inCommitTimestamp of the delta file when available, otherwise it will be the modification time
   * of the file.
   */
  private CloseableIterator<ActionWrapper> combine(
      CloseableIterator<FileReadResult> fileReadDataIter,
      boolean isFromCheckpoint,
      long version,
      Optional<Long> timestamp) {
    // For delta files, we want to use the inCommitTimestamp from commitInfo
    // as the commit timestamp for the file.
    // Since CommitInfo should be the first action in the delta when inCommitTimestamp is
    // enabled, we will read the first batch and try to extract the timestamp from it.
    // We also ensure that rewoundFileReadDataIter is identical to the original
    // fileReadDataIter before any data was consumed.
    final CloseableIterator<FileReadResult> rewoundFileReadDataIter;
    Optional<Long> inCommitTimestampOpt = Optional.empty();
    if (!isFromCheckpoint && fileReadDataIter.hasNext()) {
      FileReadResult fileReadResult = fileReadDataIter.next();
      rewoundFileReadDataIter =
          singletonCloseableIterator(fileReadResult).combine(fileReadDataIter);
      inCommitTimestampOpt =
          InCommitTimestampUtils.tryExtractInCommitTimestamp(fileReadResult.getData());
    } else {
      rewoundFileReadDataIter = fileReadDataIter;
    }
    final Optional<Long> finalResolvedCommitTimestamp =
        inCommitTimestampOpt.isPresent() ? inCommitTimestampOpt : timestamp;

    return new CloseableIterator<ActionWrapper>() {
      @Override
      public boolean hasNext() {
        return rewoundFileReadDataIter.hasNext();
      }

      @Override
      public ActionWrapper next() {
        FileReadResult fileReadResult = rewoundFileReadDataIter.next();
        return new ActionWrapper(
            fileReadResult.getData(),
            isFromCheckpoint,
            version,
            finalResolvedCommitTimestamp,
            fileReadResult.getFilePath());
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
