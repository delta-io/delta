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

package io.delta.kernel.internal.snapshot;

import static io.delta.kernel.internal.fs.Path.getName;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.fs.FileStatus;

import io.delta.kernel.internal.fs.Path;

import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.CheckpointMetaData;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Logging;

public class SnapshotManager
    implements Logging
{
    public SnapshotManager() {}

    /**
     * Construct the latest snapshot for given table.
     *
     * @param tableClient Instance of {@link TableClient} to use.
     * @param logPath Where the Delta log files are located.
     * @param dataPath Where the Delta data files are located.
     * @return
     * @throws TableNotFoundException
     */
    public Snapshot buildLatestSnapshot(TableClient tableClient, Path logPath, Path dataPath)
        throws TableNotFoundException
    {
        return getSnapshotAtInit(tableClient, logPath, dataPath);
    }

    /**
     * Get an iterator of files in the _delta_log directory starting with the startVersion.
     */
    private CloseableIterator<FileStatus> listFrom(
        Path logPath,
        TableClient tableClient,
        long startVersion)
        throws FileNotFoundException
    {
        logDebug(String.format("startVersion: %s", startVersion));
        return tableClient
            .getFileSystemClient()
            .listFrom(FileNames.listingPrefix(logPath, startVersion));
    }

    /**
     * Returns true if the given file name is delta log files. Delta log files can be delta commit
     * file (e.g., 000000000.json), or checkpoint file.
     * (e.g., 000000001.checkpoint.00001.00003.parquet)
     *
     * @param fileName Name of the file (not the full path)
     * @return Boolean Whether the file is delta log files
     */
    private boolean isDeltaCommitOrCheckpointFile(String fileName)
    {
        return FileNames.isCheckpointFile(fileName) || FileNames.isCommitFile(fileName);
    }

    /**
     * Returns an iterator containing a list of files found from the provided path
     */
    private Optional<CloseableIterator<FileStatus>> listFromOrNone(
        Path logPath,
        TableClient tableClient,
        long startVersion)
    {
        // LIST the directory, starting from the provided lower bound (treat missing dir as empty).
        // NOTE: "empty/missing" is _NOT_ equivalent to "contains no useful commit files."
        try {
            CloseableIterator<FileStatus> results = listFrom(
                logPath,
                tableClient,
                startVersion);
            if (results.hasNext()) {
                return Optional.of(results);
            }
            else {
                return Optional.empty();
            }
        }
        catch (FileNotFoundException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns the delta files and checkpoint files starting from the given `startVersion`.
     * `versionToLoad` is an optional parameter to set the max bound. It's usually used to load a
     * table snapshot for a specific version.
     *
     * @param startVersion the version to start. Inclusive.
     * @param versionToLoad the optional parameter to set the max version we should return.
     * Inclusive.
     * @return Some array of files found (possibly empty, if no usable commit files are present), or
     * None if the listing returned no files at all.
     */
    protected final Optional<List<FileStatus>> listDeltaAndCheckpointFiles(
        Path logPath,
        TableClient tableClient,
        long startVersion,
        Optional<Long> versionToLoad)
    {
        logDebug(String.format("startVersion: %s, versionToLoad: %s", startVersion, versionToLoad));

        return listFromOrNone(
            logPath,
            tableClient,
            startVersion).map(fileStatusesIter -> {
            final List<FileStatus> output = new ArrayList<>();

            while (fileStatusesIter.hasNext()) {
                final FileStatus fileStatus = fileStatusesIter.next();

                // Pick up all checkpoint and delta files
                if (!isDeltaCommitOrCheckpointFile(getName(fileStatus.getPath()))) {
                    continue;
                }

                // Checkpoint files of 0 size are invalid but may be ignored silently when read,
                // hence we drop them so that we never pick up such checkpoints.
                if (FileNames.isCheckpointFile(getName(fileStatus.getPath())) &&
                    fileStatus.getSize() == 0) {
                    continue;
                }

                // Take files until the version we want to load
                final boolean versionWithinRange = versionToLoad
                    .map(v -> FileNames.getFileVersion(new Path(fileStatus.getPath())) <= v)
                    .orElse(true);

                if (!versionWithinRange) {
                    break;
                }

                output.add(fileStatus);
            }

            return output;
        });
    }

    /**
     * Load the Snapshot for this Delta table at initialization. This method uses the
     * `lastCheckpoint` file as a hint on where to start listing the transaction log directory.
     */
    private SnapshotImpl getSnapshotAtInit(TableClient tableClient, Path logPath, Path dataPath)
        throws TableNotFoundException
    {
        Checkpointer checkpointer = new Checkpointer(logPath);
        Optional<CheckpointMetaData> lastCheckpointOpt =
            checkpointer.readLastCheckpointFile(tableClient);
        Optional<LogSegment> logSegmentOpt =
            getLogSegmentFrom(logPath, tableClient, lastCheckpointOpt);

        return logSegmentOpt
            .map(logSegment -> createSnapshot(
                logSegment,
                logPath,
                dataPath,
                tableClient))
            .orElseThrow(TableNotFoundException::new);
    }

    private SnapshotImpl createSnapshot(
        LogSegment initSegment,
        Path logPath,
        Path dataPath,
        TableClient tableClient)
    {
        final String startingFromStr = initSegment
            .checkpointVersionOpt
            .map(v -> String.format(" starting from checkpoint version %s.", v))
            .orElse(".");
        logInfo(() -> String.format("Loading version %s%s", initSegment.version, startingFromStr));

        return new SnapshotImpl(
            logPath,
            dataPath,
            initSegment.version,
            initSegment,
            tableClient,
            initSegment.lastCommitTimestamp
        );
    }

    /**
     * Get the LogSegment that will help in computing the Snapshot of the table at DeltaLog
     * initialization, or None if the directory was empty/missing.
     *
     * @param startingCheckpoint A checkpoint that we can start our listing from
     */
    private Optional<LogSegment> getLogSegmentFrom(
        Path logPath,
        TableClient tableClient,
        Optional<CheckpointMetaData> startingCheckpoint)
    {
        return getLogSegmentForVersion(
            logPath,
            tableClient,
            startingCheckpoint.map(x -> x.version),
            Optional.empty());
    }

    /**
     * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
     * `versionToLoad` is not provided, will generate the list of files that are needed to load the
     * latest version of the Delta table. This method also performs checks to ensure that the delta
     * files are contiguous.
     *
     * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
     * typically that of a known checkpoint. If this version's not provided,
     * we will start listing from version 0.
     * @param versionToLoad A specific version to load. Typically used with time travel and the
     * Delta streaming source. If not provided, we will try to load the latest
     * version of the table.
     * @return Some LogSegment to build a Snapshot if files do exist after the given
     * startCheckpoint. None, if the directory was missing or empty.
     */
    private Optional<LogSegment> getLogSegmentForVersion(
        Path logPath,
        TableClient tableClient,
        Optional<Long> startCheckpoint,
        Optional<Long> versionToLoad)
    {
        // List from the starting checkpoint. If a checkpoint doesn't exist, this will still return
        // deltaVersion=0.
        final Optional<List<FileStatus>> newFiles =
            listDeltaAndCheckpointFiles(
                logPath,
                tableClient,
                startCheckpoint.orElse(0L),
                versionToLoad);
        return getLogSegmentForVersion(
            logPath,
            tableClient,
            startCheckpoint,
            versionToLoad,
            newFiles);
    }

    /**
     * Helper function for the getLogSegmentForVersion above. Called with a provided files list,
     * and will then try to construct a new LogSegment using that.
     */
    private Optional<LogSegment> getLogSegmentForVersion(
        Path logPath,
        TableClient tableClient,
        Optional<Long> startCheckpointOpt,
        Optional<Long> versionToLoadOpt,
        Optional<List<FileStatus>> filesOpt)
    {
        final List<FileStatus> newFiles;
        if (filesOpt.isPresent()) {
            newFiles = filesOpt.get();
        }
        else {
            // No files found even when listing from 0 => empty directory =>
            // table does not exist yet.
            if (!startCheckpointOpt.isPresent()) {
                return Optional.empty();
            }

            // FIXME: We always write the commit and checkpoint files before updating
            //  _last_checkpoint. If the listing came up empty, then we either encountered a
            // list-after-put inconsistency in the underlying log store, or somebody corrupted the
            // table by deleting files. Either way, we can't safely continue.
            //
            // For now, we preserve existing behavior by returning Array.empty, which will trigger a
            // recursive call to [[getLogSegmentForVersion]] below (same as before the refactor).
            newFiles = Collections.emptyList();
        }
        logDebug(() ->
            String.format(
                "newFiles: %s",
                Arrays.toString(newFiles.stream()
                    .map(x -> new Path(x.getPath()).getName()).toArray())
            )
        );

        if (newFiles.isEmpty() && !startCheckpointOpt.isPresent()) {
            // We can't construct a snapshot because the directory contained no usable commit
            // files... but we can't return Optional.empty either, because it was not truly empty.
            throw new RuntimeException(
                String.format("Empty directory: %s", logPath)
            );
        }
        else if (newFiles.isEmpty()) {
            // The directory may be deleted and recreated and we may have stale state in our
            // DeltaLog singleton, so try listing from the first version
            return getLogSegmentForVersion(
                logPath,
                tableClient,
                Optional.empty(),
                versionToLoadOpt);
        }

        Tuple2<List<FileStatus>, List<FileStatus>> checkpointsAndDeltas = ListUtils
            .partition(
                newFiles,
                fileStatus -> FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName())
            );
        final List<FileStatus> checkpoints = checkpointsAndDeltas._1;
        final List<FileStatus> deltas = checkpointsAndDeltas._2;

        logDebug(() ->
            String.format(
                "\ncheckpoints: %s\ndeltas: %s",
                Arrays.toString(checkpoints.stream().map(
                    x -> new Path(x.getPath()).getName()).toArray()),
                Arrays.toString(deltas.stream().map(
                    x -> new Path(x.getPath()).getName()).toArray())
            )
        );

        // Find the latest checkpoint in the listing that is not older than the versionToLoad
        final CheckpointInstance lastCheckpoint = versionToLoadOpt.map(CheckpointInstance::new)
            .orElse(CheckpointInstance.MAX_VALUE);
        logDebug(String.format("lastCheckpoint: %s", lastCheckpoint));

        final List<CheckpointInstance> checkpointFiles = checkpoints
            .stream()
            .map(f -> new CheckpointInstance(new Path(f.getPath())))
            .collect(Collectors.toList());
        logDebug(() ->
            String.format("checkpointFiles: %s", Arrays.toString(checkpointFiles.toArray())));

        final Optional<CheckpointInstance> newCheckpointOpt =
            Checkpointer.getLatestCompleteCheckpointFromList(checkpointFiles, lastCheckpoint);
        logDebug(String.format("newCheckpointOpt: %s", newCheckpointOpt));

        final long newCheckpointVersion = newCheckpointOpt
            .map(c -> c.version)
            .orElseGet(() -> {
                // If we do not have any checkpoint, pass new checkpoint version as -1 so that
                // first delta version can be 0.
                startCheckpointOpt.map(startCheckpoint -> {
                    // `startCheckpointOpt` was given but no checkpoint found on delta log.
                    // This means that the last checkpoint we thought should exist (the
                    // `_last_checkpoint` file) no longer exists.
                    // Try to look up another valid checkpoint and create `LogSegment` from it.
                    //
                    // FIXME: Something has gone very wrong if the checkpoint doesn't
                    // exist at all. This code should only handle rejected incomplete
                    // checkpoints.
                    final long snapshotVersion = versionToLoadOpt.orElseGet(() -> {
                        final FileStatus lastDelta = deltas.get(deltas.size() - 1);
                        return FileNames.deltaVersion(new Path(lastDelta.getPath()));
                    });

                    return getLogSegmentWithMaxExclusiveCheckpointVersion(
                        snapshotVersion, startCheckpoint)
                        .orElseThrow(() ->
                            // No alternative found, but the directory contains files
                            // so we cannot return None.
                            new RuntimeException(String.format(
                                "Checkpoint file to load version: %s is missing.",
                                startCheckpoint)
                            )
                        );
                });

                return -1L;
            });
        logDebug(String.format("newCheckpointVersion: %s", newCheckpointVersion));

        // TODO: we can calculate deltasAfterCheckpoint and deltaVersions more efficiently
        // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
        // it will list all existing delta files.
        final List<FileStatus> deltasAfterCheckpoint = deltas
            .stream()
            .filter(fileStatus ->
                FileNames.deltaVersion(
                    new Path(fileStatus.getPath())) > newCheckpointVersion)
            .collect(Collectors.toList());

        logDebug(() ->
            String.format(
                "deltasAfterCheckpoint: %s",
                Arrays.toString(deltasAfterCheckpoint.stream().map(
                    x -> new Path(x.getPath()).getName()).toArray())
            )
        );

        final LinkedList<Long> deltaVersions = deltasAfterCheckpoint
            .stream()
            .map(fileStatus -> FileNames.deltaVersion(new Path(fileStatus.getPath())))
            .collect(Collectors.toCollection(LinkedList::new));

        logDebug(() ->
            String.format("deltaVersions: %s", Arrays.toString(deltaVersions.toArray())));

        // We may just be getting a checkpoint file after the filtering
        if (!deltaVersions.isEmpty()) {
            if (deltaVersions.getFirst() != newCheckpointVersion + 1) {
                throw new RuntimeException(
                    String.format(
                        "Log file not found.\nExpected: %s\nFound:%s",
                        FileNames.deltaFile(logPath, newCheckpointVersion + 1),
                        FileNames.deltaFile(logPath, deltaVersions.get(0))
                    )
                );
            }
            verifyDeltaVersions(
                deltaVersions, Optional.of(newCheckpointVersion + 1), versionToLoadOpt);
        }

        // TODO: double check newCheckpointOpt.get() won't error out

        final long newVersion = deltaVersions.isEmpty() ?
            newCheckpointOpt.get().version : deltaVersions.getLast();

        // In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
        // they may just be before the checkpoint version unless we have a bug in log cleanup.
        if (deltas.isEmpty()) {
            throw new IllegalStateException(
                String.format("Could not find any delta files for version %s", newVersion)
            );
        }

        if (versionToLoadOpt.map(v -> v != newVersion).orElse(false)) {
            throw new IllegalStateException(
                String.format("Trying to load a non-existent version %s",
                    versionToLoadOpt.get())
            );
        }

        final long lastCommitTimestamp = deltas.get(deltas.size() - 1).getModificationTime();

        final List<FileStatus> newCheckpointFiles = newCheckpointOpt.map(newCheckpoint -> {
            final Set<Path> newCheckpointPaths =
                new HashSet<>(newCheckpoint.getCorrespondingFiles(logPath));
            final List<FileStatus> newCheckpointFileList = checkpoints
                .stream()
                .filter(f -> newCheckpointPaths.contains(new Path(f.getPath())))
                .collect(Collectors.toList());

            if (newCheckpointFileList.size() != newCheckpointPaths.size()) {
                String msg = String.format(
                    "Seems like the checkpoint is corrupted. Failed in getting the file " +
                        "information for:\n%s\namong\n%s",
                    newCheckpointPaths.stream()
                        .map(Path::toString).collect(Collectors.toList()),
                    checkpoints
                        .stream()
                        .map(FileStatus::getPath)
                        .collect(Collectors.joining("\n - "))
                );
                throw new IllegalStateException(msg);
            }
            return newCheckpointFileList;
        }).orElse(Collections.emptyList());

        return Optional.of(
            new LogSegment(
                logPath,
                newVersion,
                deltasAfterCheckpoint,
                newCheckpointFiles,
                newCheckpointOpt.map(x -> x.version),
                lastCommitTimestamp
            )
        );
    }

    /**
     * Returns a [[LogSegment]] for reading `snapshotVersion` such that the segment's checkpoint
     * version (if checkpoint present) is LESS THAN `maxExclusiveCheckpointVersion`.
     * This is useful when trying to skip a bad checkpoint. Returns `None` when we are not able to
     * construct such [[LogSegment]], for example, no checkpoint can be used but we don't have the
     * entire history from version 0 to version `snapshotVersion`.
     */
    private Optional<LogSegment> getLogSegmentWithMaxExclusiveCheckpointVersion(
        long snapshotVersion,
        long maxExclusiveCheckpointVersion)
    {
        // TODO
        return Optional.empty();
    }

    /**
     * - Verify the versions are contiguous.
     * - Verify the versions start with `expectedStartVersion` if it's specified.
     * - Verify the versions end with `expectedEndVersion` if it's specified.
     */
    public static void verifyDeltaVersions(
        List<Long> versions,
        Optional<Long> expectedStartVersion,
        Optional<Long> expectedEndVersion)
    {
        if (!versions.isEmpty()) {
            // TODO: check if contiguous
        }
        expectedStartVersion.ifPresent(v -> {
            assert (!versions.isEmpty() && Objects.equals(versions.get(0), v)) :
                String.format(
                    "Did not get the first delta file version %s to compute Snapshot", v);
        });
        expectedEndVersion.ifPresent(v -> {
            assert (!versions.isEmpty() && Objects.equals(versions.get(versions.size() - 1), v)) :
                String.format(
                    "Did not get the last delta file version %s to compute Snapshot", v);
        });
    }
}
