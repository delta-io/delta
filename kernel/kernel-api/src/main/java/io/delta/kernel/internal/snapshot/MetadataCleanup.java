/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.snapshot;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.checkpoints.Checkpointer.getLatestCompleteCheckpointFromList;
import static io.delta.kernel.internal.lang.ListUtils.getFirst;
import static io.delta.kernel.internal.lang.ListUtils.getLast;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCleanup {
  private static final Logger logger = LoggerFactory.getLogger(MetadataCleanup.class);

  private MetadataCleanup() {}

  /**
   * Delete the Delta log files (delta and checkpoint files) that are expired according to the table
   * metadata retention settings. While deleting the log files, it makes sure the time travel
   * continues to work for all unexpired table versions.
   *
   * <p>Here is algorithm:
   *
   * <ul>
   *   <li>Initial the potential delete file list: `potentialFilesToDelete` as an empty list
   *   <li>Initialize the last seen checkpoint file list: `lastSeenCheckpointFiles`. There could be
   *       one or more checkpoint files for a given version.
   *   <li>List the delta log files starting with prefix "00000000000000000000." (%020d). For each
   *       file:
   *       <ul>
   *         <li>Step 1: Check if the `lastSeenCheckpointFiles` contains a complete checkpoint, then
   *             <ul>
   *               <li>Step 1.1: delete all files in `potentialFilesToDelete`. Now we know there is
   *                   a checkpoint that contains the compacted Delta log up to the checkpoint
   *                   version and all commit/checkpoint files before this checkpoint version are
   *                   not needed.
   *               <li>Step 1.2: add `lastCheckpointFiles` to `potentialFileStoDelete` list. This
   *                   checkpoint is potential candidate to delete later if we find another
   *                   checkpoint
   *             </ul>
   *         <li>Step 2: If the timestamp falls within the retention period, stop
   *         <li>Step 3: If the file is a delta log file, add it to the `potentialFilesToDelete`
   *             list
   *         <li>Step 4: If the file is a checkpoint file, add it to the `lastSeenCheckpointFiles`
   *       </ul>
   * </ul>
   *
   * @param engine {@link Engine} instance to delete the expired log files
   * @param clock {@link Clock} instance to get the current time. Useful in testing to mock the
   *     current time.
   * @param tablePath Table location
   * @param retentionMillis Log file retention period in milliseconds
   * @return number of log files deleted
   * @throws IOException if an error occurs while deleting the log files
   */
  public static long cleanupExpiredLogs(
      Engine engine, Clock clock, Path tablePath, long retentionMillis) throws IOException {
    checkArgument(retentionMillis >= 0, "Retention period must be non-negative");

    List<String> potentialLogFilesToDelete = new ArrayList<>();
    long lastSeenCheckpointVersion = -1; // -1 indicates no checkpoint seen yet
    List<String> lastSeenCheckpointFiles = new ArrayList<>();

    long fileCutOffTime = clock.getTimeMillis() - retentionMillis;
    logger.info("{}: Starting the deletion of log files older than {}", tablePath, fileCutOffTime);
    long numDeleted = 0;
    try (CloseableIterator<FileStatus> files = listDeltaLogs(engine, tablePath)) {
      while (files.hasNext()) {
        // Step 1: Check if the `lastSeenCheckpointFiles` contains a complete checkpoint
        Optional<CheckpointInstance> lastCompleteCheckpoint =
            getLatestCompleteCheckpointFromList(
                lastSeenCheckpointFiles.stream().map(CheckpointInstance::new).collect(toList()),
                CheckpointInstance.MAX_VALUE);

        if (lastCompleteCheckpoint.isPresent()) {
          // Step 1.1: delete all files in `potentialFilesToDelete`. Now we know there is a
          //   checkpoint that contains the compacted Delta log up to the checkpoint version and all
          //   commit/checkpoint files before this checkpoint version are not needed. add
          //   `lastCheckpointFiles` to `potentialFileStoDelete` list. This checkpoint is potential
          //   candidate to delete later if we find another checkpoint
          if (!potentialLogFilesToDelete.isEmpty()) {
            logger.info(
                "{}: Deleting log files (start = {}, end = {}) because a checkpoint at "
                    + "version {} indicates that these log files are no longer needed.",
                tablePath,
                getFirst(potentialLogFilesToDelete),
                getLast(potentialLogFilesToDelete),
                lastSeenCheckpointVersion);

            numDeleted += deleteLogFiles(engine, potentialLogFilesToDelete);
            potentialLogFilesToDelete.clear();
          }

          // Step 1.2: add `lastCheckpointFiles` to `potentialFileStoDelete` list. This checkpoint
          // is potential candidate to delete later if we find another checkpoint
          potentialLogFilesToDelete.addAll(lastSeenCheckpointFiles);
          lastSeenCheckpointFiles.clear();
          lastSeenCheckpointVersion = -1;
        }

        FileStatus nextFile = files.next();

        // Step 2: If the timestamp is earlier than the retention period, stop
        if (nextFile.getModificationTime() > fileCutOffTime) {
          if (!potentialLogFilesToDelete.isEmpty()) {
            logger.info(
                "{}: Skipping deletion of expired log files {}, because there is no checkpoint "
                    + "file that indicates that the log files are no longer needed. ",
                tablePath,
                potentialLogFilesToDelete.size());
          }
          break;
        }

        if (FileNames.isCommitFile(nextFile.getPath())) {
          // Step 3: If the file is a delta log file, add it to the `potentialFilesToDelete` list
          // We can't delete these files until we encounter a checkpoint later that indicates
          // that the log files are no longer needed.
          potentialLogFilesToDelete.add(nextFile.getPath());
        } else if (FileNames.isCheckpointFile(nextFile.getPath())) {
          // Step 4: If the file is a checkpoint file, add it to the `lastSeenCheckpointFiles`
          long newLastSeenCheckpointVersion = FileNames.checkpointVersion(nextFile.getPath());
          checkArgument(
              lastSeenCheckpointVersion == -1
                  || newLastSeenCheckpointVersion >= lastSeenCheckpointVersion);

          if (lastSeenCheckpointVersion != -1
              && newLastSeenCheckpointVersion > lastSeenCheckpointVersion) {
            // We have found checkpoint file for a new version. This means the files gathered for
            // the last checkpoint version are not complete (most likely an incomplete multipart
            // checkpoint). We should delete the files gathered so far and start fresh
            // last seen checkpoint state
            logger.info(
                "{}: Incomplete checkpoint files found at version {}, ignoring the checkpoint"
                    + " files and adding them to potential log file delete list",
                tablePath,
                lastSeenCheckpointVersion);
            potentialLogFilesToDelete.addAll(lastSeenCheckpointFiles);
            lastSeenCheckpointFiles.clear();
          }

          lastSeenCheckpointFiles.add(nextFile.getPath());
          lastSeenCheckpointVersion = newLastSeenCheckpointVersion;
        }
        // Ignore non-delta and non-checkpoint files.
      }
    }
    logger.info("{}: Deleted {} log files older than {}", tablePath, numDeleted, fileCutOffTime);
    return numDeleted;
  }

  private static CloseableIterator<FileStatus> listDeltaLogs(Engine engine, Path tablePath)
      throws IOException {
    Path logPath = new Path(tablePath, "_delta_log");
    // TODO: Currently we don't update the timestamps of files to be monotonically increasing.
    // In future we can do something similar to Delta Spark to make the timestamps monotonically
    // increasing. See `BufferingLogDeletionIterator` in Delta Spark.
    return engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, 0));
  }

  private static int deleteLogFiles(Engine engine, List<String> logFiles) throws IOException {
    int numDeleted = 0;
    for (String logFile : logFiles) {
      if (wrapEngineExceptionThrowsIO(
          () -> engine.getFileSystemClient().delete(logFile),
          "Failed to delete the log file as part of the metadata cleanup %s",
          logFile)) {
        numDeleted++;
      }
    }
    return numDeleted;
  }
}
