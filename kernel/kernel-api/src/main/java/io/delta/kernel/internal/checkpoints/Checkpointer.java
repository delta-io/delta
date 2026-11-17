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
package io.delta.kernel.internal.checkpoints;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.TableConfig.EXPIRED_LOG_CLEANUP_ENABLED;
import static io.delta.kernel.internal.TableConfig.LOG_RETENTION;
import static io.delta.kernel.internal.snapshot.MetadataCleanup.cleanupExpiredLogs;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to load and write the {@link CheckpointMetaData} from `_last_checkpoint` file. */
public class Checkpointer {

  ////////////////////////////////
  // Static variables / methods //
  ////////////////////////////////

  private static final Logger logger = LoggerFactory.getLogger(Checkpointer.class);

  private static final int READ_LAST_CHECKPOINT_FILE_MAX_RETRIES = 3;

  /** The name of the last checkpoint file */
  public static final String LAST_CHECKPOINT_FILE_NAME = "_last_checkpoint";

  static final long DEFAULT_CHECKPOINT_SEARCH_WINDOW = 1000;

  // Thread-local so a caller can tune the search for one snapshot load without affecting
  // concurrent loads of other tables in the same JVM.
  private static final ThreadLocal<Long> checkpointSearchWindow =
      ThreadLocal.withInitial(() -> DEFAULT_CHECKPOINT_SEARCH_WINDOW);

  /**
   * Runs {@code body} with the backward checkpoint search listing {@code window} versions per step
   * instead of the default 1000.
   *
   * <p>The search lists every file in a window, so tables that checkpoint often with many parts per
   * checkpoint are cheaper to search with a small window, while tables with sparse checkpoints need
   * more listing calls.
   *
   * <p>Only searches that run on the calling thread before {@code body} returns use the window.
   * Snapshot builds that defer log segment loading (for example time travel with {@code
   * withProtocolAndMetadata}) must force the log segment inside {@code body}.
   */
  public static <T> T withCheckpointSearchWindow(long window, Supplier<T> body) {
    if (window < 1) {
      throw new IllegalArgumentException(
          "Checkpoint search window must be positive, got " + window);
    }
    long previous = checkpointSearchWindow.get();
    checkpointSearchWindow.set(window);
    try {
      return body.get();
    } finally {
      checkpointSearchWindow.set(previous);
    }
  }

  static long getCheckpointSearchWindow() {
    return checkpointSearchWindow.get();
  }

  public static void checkpoint(Engine engine, Clock clock, SnapshotImpl snapshot)
      throws TableNotFoundException, IOException {
    final Path tablePath = snapshot.getDataPath();
    final Path logPath = snapshot.getLogPath();
    final long version = snapshot.getVersion();

    logger.info("{}: Starting checkpoint for version: {}", tablePath, version);

    // Check if writing to the given table protocol version/features is supported in Kernel
    TableFeatures.validateKernelCanWriteToTable(
        snapshot.getProtocol(), snapshot.getMetadata(), snapshot.getDataPath().toString());

    final Path checkpointPath = FileNames.checkpointFileSingular(logPath, version);

    long numberOfAddFiles = 0;
    try (CreateCheckpointIterator checkpointDataIter =
        snapshot.getCreateCheckpointIterator(engine)) {
      // Write the iterator actions to the checkpoint using the Parquet handler
      wrapEngineExceptionThrowsIO(
          () -> {
            engine
                .getParquetHandler()
                .writeParquetFileAtomically(checkpointPath.toString(), checkpointDataIter);

            logger.info("{}: Finished writing checkpoint file for version: {}", tablePath, version);

            return null;
          },
          "Writing checkpoint file %s",
          checkpointPath.toString());

      // Get the metadata of the checkpoint file
      numberOfAddFiles = checkpointDataIter.getNumberOfAddActions();
    } catch (FileAlreadyExistsException faee) {
      throw new CheckpointAlreadyExistsException(version);
    } catch (IOException io) {
      if (io.getCause() instanceof FileAlreadyExistsException) {
        throw new CheckpointAlreadyExistsException(version);
      }
      throw io;
    }

    final CheckpointMetaData checkpointMetaData =
        new CheckpointMetaData(version, numberOfAddFiles, Optional.empty());

    new Checkpointer(logPath).writeLastCheckpointFile(engine, checkpointMetaData);

    logger.info(
        "{}: Finished writing last checkpoint metadata file for version: {}", tablePath, version);

    // Clean up delta log files if enabled.
    final Metadata metadata = snapshot.getMetadata();
    if (EXPIRED_LOG_CLEANUP_ENABLED.fromMetadata(metadata)) {
      cleanupExpiredLogs(engine, clock, tablePath, LOG_RETENTION.fromMetadata(metadata));
    } else {
      logger.info(
          "{}: Log cleanup is disabled. Skipping the deletion of expired log files", tablePath);
    }
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  public static Optional<CheckpointInstance> getLatestCompleteCheckpointFromList(
      List<CheckpointInstance> instances, CheckpointInstance notLaterThan) {
    final List<CheckpointInstance> completeCheckpoints =
        instances.stream()
            .filter(c -> c.isNotLaterThan(notLaterThan))
            .collect(Collectors.groupingBy(c -> c))
            .entrySet()
            .stream()
            .filter(
                entry -> {
                  final CheckpointInstance key = entry.getKey();
                  final List<CheckpointInstance> inst = entry.getValue();

                  if (key.numParts.isPresent()) {
                    return inst.size() == entry.getKey().numParts.get();
                  } else {
                    return inst.size() == 1;
                  }
                })
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    if (completeCheckpoints.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(Collections.max(completeCheckpoints));
    }
  }

  /** Find the last complete checkpoint before (strictly less than) a given version. */
  public static Optional<CheckpointInstance> findLastCompleteCheckpointBefore(
      Engine engine, Path tableLogPath, long version) {
    return findLastCompleteCheckpointBeforeHelper(engine, tableLogPath, version)._1;
  }

  /**
   * Helper method for `findLastCompleteCheckpointBefore` which also return the number of files
   * searched. This helps in testing
   */
  public static Tuple2<Optional<CheckpointInstance>, Long> findLastCompleteCheckpointBeforeHelper(
      Engine engine, Path tableLogPath, long version) {
    CheckpointInstance upperBoundCheckpoint = new CheckpointInstance(version);
    logger.info("Try to find the last complete checkpoint before version {}", version);

    // This is a just a tracker for testing purposes
    long numberOfFilesSearched = 0;
    long currentVersion = version;
    long window = getCheckpointSearchWindow();

    // Listing only goes forward from a prefix, so step backwards one window at a time and stop at
    // the first window that holds a complete checkpoint.
    while (currentVersion >= 0) {
      try {
        long searchLowerBound = Math.max(0, currentVersion - window);
        CloseableIterator<FileStatus> deltaLogFileIter =
            wrapEngineExceptionThrowsIO(
                () ->
                    engine
                        .getFileSystemClient()
                        .listFrom(FileNames.listingPrefix(tableLogPath, searchLowerBound)),
                "Listing from %s",
                FileNames.listingPrefix(tableLogPath, searchLowerBound));

        List<CheckpointInstance> checkpoints = new ArrayList<>();
        while (deltaLogFileIter.hasNext()) {
          FileStatus fileStatus = deltaLogFileIter.next();
          String fileName = new Path(fileStatus.getPath()).getName();

          long currentFileVersion;
          if (FileNames.isCommitFile(fileName)) {
            currentFileVersion = FileNames.deltaVersion(fileName);
          } else if (FileNames.isCheckpointFile(fileName)) {
            currentFileVersion = FileNames.checkpointVersion(fileName);
          } else {
            // Skip unrecognized file types (e.g. .crc files). If we assigned
            // them a version, they could prematurely trigger the break condition
            // below and cause us to miss recent checkpoints.
            numberOfFilesSearched++;
            continue;
          }

          boolean shouldContinue =
              // only consider files with version in the range and
              // before the target version
              (currentVersion == 0 || currentFileVersion <= currentVersion)
                  && currentFileVersion < version;

          if (!shouldContinue) {
            break;
          }
          if (validCheckpointFile(fileStatus)) {
            checkpoints.add(new CheckpointInstance(fileStatus.getPath()));
          }
          numberOfFilesSearched++;
        }

        Optional<CheckpointInstance> latestCheckpoint =
            getLatestCompleteCheckpointFromList(checkpoints, upperBoundCheckpoint);

        if (latestCheckpoint.isPresent()) {
          logger.info(
              "Found the last complete checkpoint before version {} at {}",
              version,
              latestCheckpoint.get());
          return new Tuple2<>(latestCheckpoint, numberOfFilesSearched);
        }
        currentVersion -= window;
      } catch (IOException e) {
        String msg =
            String.format(
                "Failed to list checkpoint files for version %s in %s.", version, tableLogPath);
        logger.warn(msg, e);
        return new Tuple2<>(Optional.empty(), numberOfFilesSearched);
      }
    }
    logger.info("No complete checkpoint found before version {} in {}", version, tableLogPath);
    return new Tuple2<>(Optional.empty(), numberOfFilesSearched);
  }

  private static boolean validCheckpointFile(FileStatus fileStatus) {
    return FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName())
        && fileStatus.getSize() > 0;
  }

  ////////////////////////////////
  // Member variables / methods //
  ////////////////////////////////

  /** The path to the file that holds metadata about the most recent checkpoint. */
  private final Path lastCheckpointFilePath;

  public Checkpointer(Path logPath) {
    this.lastCheckpointFilePath = new Path(logPath, LAST_CHECKPOINT_FILE_NAME);
  }

  /** Returns information about the most recent checkpoint. */
  public Optional<CheckpointMetaData> readLastCheckpointFile(Engine engine) {
    return loadMetadataFromFile(engine, 0 /* tries */);
  }

  /**
   * Write the given data to last checkpoint metadata file.
   *
   * @param engine {@link Engine} instance to use for writing
   * @param checkpointMetaData Checkpoint metadata to write
   * @throws IOException For any I/O issues.
   */
  public void writeLastCheckpointFile(Engine engine, CheckpointMetaData checkpointMetaData)
      throws IOException {
    wrapEngineExceptionThrowsIO(
        () -> {
          engine
              .getJsonHandler()
              .writeJsonFileAtomically(
                  lastCheckpointFilePath.toString(),
                  singletonCloseableIterator(checkpointMetaData.toRow()),
                  true /* overwrite */);
          return null;
        },
        "Writing last checkpoint file at `%s`",
        lastCheckpointFilePath);
  }

  /**
   * Loads the checkpoint metadata from the _last_checkpoint file.
   *
   * @param engine {@link Engine instance to use}
   * @param tries Number of times already tried to load the metadata before this call.
   */
  private Optional<CheckpointMetaData> loadMetadataFromFile(Engine engine, int tries) {
    if (tries >= READ_LAST_CHECKPOINT_FILE_MAX_RETRIES) {
      // We have tried 3 times and failed. Assume the checkpoint metadata file is corrupt.
      logger.warn(
          "Failed to load checkpoint metadata from file {} after {} attempts.",
          lastCheckpointFilePath,
          READ_LAST_CHECKPOINT_FILE_MAX_RETRIES);
      return Optional.empty();
    }

    logger.info(
        "Loading last checkpoint from the _last_checkpoint file. Attempt: {} / {}",
        tries + 1,
        READ_LAST_CHECKPOINT_FILE_MAX_RETRIES);

    try {
      // Use arbitrary values for size and mod time as they are not available.
      // We could list and find the values, but it is an unnecessary FS call.
      FileStatus lastCheckpointFile =
          FileStatus.of(lastCheckpointFilePath.toString(), 0 /* size */, 0 /* modTime */);

      try (CloseableIterator<ColumnarBatch> jsonIter =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getJsonHandler()
                      .readJsonFiles(
                          singletonCloseableIterator(lastCheckpointFile),
                          CheckpointMetaData.READ_SCHEMA,
                          Optional.empty()),
              "Reading the last checkpoint file as JSON")) {
        Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
        if (checkpointRow.isPresent()) {
          return Optional.of(CheckpointMetaData.fromRow(checkpointRow.get()));
        }

        // Checkpoint has no data. This is a valid case on some file systems where the
        // contents are not visible until the file stream is closed.
        // Sleep for one second and retry.
        logger.warn(
            "Last checkpoint file {} has no data. " + "Retrying after 1sec. (current attempt = {})",
            lastCheckpointFilePath,
            tries);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return Optional.empty();
        }
        return loadMetadataFromFile(engine, tries + 1);
      }
    } catch (Exception e) {
      if (e instanceof FileNotFoundException
          || (e instanceof KernelEngineException
              && e.getCause() instanceof FileNotFoundException)) {
        return Optional.empty(); // there's no point in retrying
      }
      String msg =
          String.format(
              "Failed to load checkpoint metadata from file %s. "
                  + "It must be in the process of being written. "
                  + "Retrying after 1sec. (current attempt of %s (max 3)",
              lastCheckpointFilePath, tries);
      logger.warn(msg, e);
      // we can retry until max tries are exhausted. It saves latency as the alternative
      // is to list files and find the last checkpoint file. And the `_last_checkpoint`
      // file is possibly being written to.
      return loadMetadataFromFile(engine, tries + 1);
    }
  }
}
