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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.*;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * Class to load the {@link CheckpointMetaData} from `_last_checkpoint` file.
 */
public class Checkpointer {
    private static final Logger logger = LoggerFactory.getLogger(Checkpointer.class);

    /**
     * The name of the last checkpoint file
     */
    public static final String LAST_CHECKPOINT_FILE_NAME = "_last_checkpoint";

    /**
     * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
     * later than `notLaterThan`.
     */
    public static Optional<CheckpointInstance> getLatestCompleteCheckpointFromList(
        List<CheckpointInstance> instances,
        CheckpointInstance notLaterThan) {
        final List<CheckpointInstance> completeCheckpoints = instances
            .stream()
            .filter(c -> c.isNotLaterThan(notLaterThan))
            .collect(Collectors.groupingBy(c -> c))
            .entrySet()
            .stream()
            .filter(entry -> {
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

    /**
     * Find the last complete checkpoint before (strictly less than) a given version.
     */
    public static Optional<CheckpointInstance> findLastCompleteCheckpointBefore(
            TableClient tableClient,
            Path tableLogPath,
            long version) {
        return findLastCompleteCheckpointBeforeHelper(tableClient, tableLogPath, version)._1;
    }

    /**
     * Helper method for `findLastCompleteCheckpointBefore` which also return the number
     * of files searched. This helps in testing
     */
    protected static Tuple2<Optional<CheckpointInstance>, Long>
            findLastCompleteCheckpointBeforeHelper(
                    TableClient tableClient,
                    Path tableLogPath,
                    long version) {
        CheckpointInstance upperBoundCheckpoint = new CheckpointInstance(version);
        logger.info("Try to find the last complete checkpoint before version {}", version);

        // This is a just a tracker for testing purposes
        long numberOfFilesSearched = 0;
        long currentVersion = version;

        // Some cloud storage APIs make a calls to fetch 1000 at a time.
        // To make use of that observation and to avoid making more listing calls than
        // necessary, list 1000 at a time (backwards from the given version). Search
        // within that list if a checkpoint is found. If found stop, otherwise list the previous
        // 1000 entries. Repeat until a checkpoint is found or there are no more delta commits.
        while (currentVersion >= 0) {
            try {
                long searchLowerBound = Math.max(0, currentVersion - 1000);
                CloseableIterator<FileStatus> deltaLogFileIter = tableClient.getFileSystemClient()
                        .listFrom(FileNames.listingPrefix(tableLogPath, searchLowerBound));

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
                        // allow all other types of files.
                        currentFileVersion = currentVersion;
                    }

                    boolean shouldContinue =
                            // only consider files with version in the range and
                            // before the target version
                            (currentVersion == 0 || currentFileVersion <= currentVersion) &&
                            currentFileVersion < version;

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
                    logger.info("Found the last complete checkpoint before version {} at {}",
                            version, latestCheckpoint.get());
                    return new Tuple2<>(latestCheckpoint, numberOfFilesSearched);
                }
                currentVersion -= 1000; // search for checkpoint in previous 1000 entries
            } catch (IOException e) {
                String msg = String.format("Failed to list checkpoint files for version %s in %s.",
                        version, tableLogPath);
                logger.warn(msg, e);
                return new Tuple2<>(Optional.empty(), numberOfFilesSearched);
            }
        }
        logger.info("No complete checkpoint found before version {} in {}", version, tableLogPath);
        return new Tuple2<>(Optional.empty(), numberOfFilesSearched);
    }

    private static boolean validCheckpointFile(FileStatus fileStatus) {
        return FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName()) &&
                fileStatus.getSize() > 0;
    }

    /**
     * The path to the file that holds metadata about the most recent checkpoint.
     */
    private final Path lastCheckpointFilePath;

    public Checkpointer(Path tableLogPath) {
        this.lastCheckpointFilePath = new Path(tableLogPath, LAST_CHECKPOINT_FILE_NAME);
    }

    /**
     * Returns information about the most recent checkpoint.
     */
    public Optional<CheckpointMetaData> readLastCheckpointFile(TableClient tableClient) {
        return loadMetadataFromFile(tableClient, 0 /* tries */);
    }

    /**
     * Loads the checkpoint metadata from the _last_checkpoint file.
     * <p>
     * @param tableClient {@link TableClient instance to use}
     * @param tries Number of times already tried to load the metadata before this call.
     */
    private Optional<CheckpointMetaData> loadMetadataFromFile(TableClient tableClient, int tries) {
        if (tries >= 3) {
            // We have tried 3 times and failed. Assume the checkpoint metadata file is corrupt.
            logger.warn(
                    "Failed to load checkpoint metadata from file {} after 3 attempts.",
                    lastCheckpointFilePath);
            return Optional.empty();
        }
        try {
            // Use arbitrary values for size and mod time as they are not available.
            // We could list and find the values, but it is an unnecessary FS call.
            FileStatus lastCheckpointFile = FileStatus.of(
                    lastCheckpointFilePath.toString(), 0 /* size */, 0 /* modTime */);

            try (CloseableIterator<ColumnarBatch> jsonIter =
                         tableClient.getJsonHandler().readJsonFiles(
                                 singletonCloseableIterator(lastCheckpointFile),
                                 CheckpointMetaData.READ_SCHEMA,
                                 Optional.empty())) {
                Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
                if (checkpointRow.isPresent()) {
                    return Optional.of(CheckpointMetaData.fromRow(checkpointRow.get()));
                }

                // Checkpoint has no data. This is a valid case on some file systems where the
                // contents are not visible until the file stream is closed.
                // Sleep for one second and retry.
                logger.warn(
                        "Last checkpoint file {} has no data. " +
                                "Retrying after 1sec. (current attempt = {})",
                        lastCheckpointFilePath,
                        tries);
                Thread.sleep(1000);
                return loadMetadataFromFile(tableClient, tries + 1);
            }
        } catch (FileNotFoundException ex) {
            return Optional.empty(); // there is no point retrying
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception ex) {
            String msg = String.format(
                    "Failed to load checkpoint metadata from file %s. " +
                            "Retrying after 1sec. (current attempt = %s)",
                    lastCheckpointFilePath, tries);
            logger.warn(msg, ex);
            // we can retry until max tries are exhausted
            return loadMetadataFromFile(tableClient, tries + 1);
        }
    }
}
