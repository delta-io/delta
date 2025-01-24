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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.TableConfig.EXPIRED_LOG_CLEANUP_ENABLED;
import static io.delta.kernel.internal.TableConfig.LOG_RETENTION;
import static io.delta.kernel.internal.TableFeatures.validateWriteSupportedTable;
import static io.delta.kernel.internal.replay.LogReplayUtils.assertLogFilesBelongToTable;
import static io.delta.kernel.internal.snapshot.MetadataCleanup.cleanupExpiredLogs;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.*;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checkpoints.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotManager {

  /**
   * The latest {@link SnapshotHint} for this table. The initial value inside the AtomicReference is
   * `null`.
   */
  private final AtomicReference<SnapshotHint> latestSnapshotHint;

  private final Path logPath;
  private final Path tablePath;

  public SnapshotManager(Path logPath, Path tablePath) {
    this.latestSnapshotHint = new AtomicReference<>();
    this.logPath = logPath;
    this.tablePath = tablePath;
  }

  private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Construct the latest snapshot for given table.
   *
   * @param engine Instance of {@link Engine} to use.
   * @return the latest {@link Snapshot} of the table
   * @throws TableNotFoundException if the table does not exist
   * @throws InvalidTableException if the table is in an invalid state
   */
  public Snapshot buildLatestSnapshot(Engine engine, SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    final LogSegment logSegment =
        getLogSegmentForVersion(engine, Optional.empty() /* versionToLoad */);

    snapshotContext.setVersion(logSegment.version);

    return createSnapshot(logSegment, engine, snapshotContext);
  }

  /**
   * Construct the snapshot for the given table at the version provided.
   *
   * @param engine Instance of {@link Engine} to use.
   * @param version The snapshot version to construct
   * @return a {@link Snapshot} of the table at version {@code version}
   * @throws TableNotFoundException if the table does not exist
   * @throws InvalidTableException if the table is in an invalid state
   */
  public Snapshot getSnapshotAt(Engine engine, long version, SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    final LogSegment logSegment =
        getLogSegmentForVersion(engine, Optional.of(version) /* versionToLoadOpt */);

    return createSnapshot(logSegment, engine, snapshotContext);
  }

  /**
   * Construct the snapshot for the given table at the provided timestamp.
   *
   * @param engine Instance of {@link Engine} to use.
   * @param millisSinceEpochUTC timestamp to fetch the snapshot for in milliseconds since the unix
   *     epoch
   * @return a {@link Snapshot} of the table at the provided timestamp
   * @throws TableNotFoundException
   */
  public Snapshot getSnapshotForTimestamp(
      Engine engine, long millisSinceEpochUTC, SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    long versionToRead =
        snapshotContext
            .getSnapshotMetrics()
            .timestampToVersionResolutionTimer
            .time(
                () ->
                    DeltaHistoryManager.getActiveCommitAtTimestamp(
                            engine,
                            logPath,
                            millisSinceEpochUTC,
                            true /* mustBeRecreatable */,
                            false /* canReturnLastCommit */,
                            false /* canReturnEarliestCommit */)
                        .getVersion());
    logger.info(
        "{}: Took {} ms to fetch version at timestamp {}",
        tablePath,
        snapshotContext.getSnapshotMetrics().timestampToVersionResolutionTimer.totalDurationMs(),
        millisSinceEpochUTC);
    // We update the query context version as soon as we resolve timestamp --> version
    snapshotContext.setVersion(versionToRead);

    return getSnapshotAt(engine, versionToRead, snapshotContext);
  }

  public void checkpoint(Engine engine, Clock clock, long version)
      throws TableNotFoundException, IOException {
    logger.info("{}: Starting checkpoint for version: {}", tablePath, version);
    // Get the snapshot corresponding the version
    SnapshotImpl snapshot =
        (SnapshotImpl)
            getSnapshotAt(
                engine,
                version,
                SnapshotQueryContext.forVersionSnapshot(tablePath.toString(), version));

    // Check if writing to the given table protocol version/features is supported in Kernel
    validateWriteSupportedTable(
        snapshot.getProtocol(),
        snapshot.getMetadata(),
        snapshot.getSchema(engine),
        tablePath.toString());

    Path checkpointPath = FileNames.checkpointFileSingular(logPath, version);

    long numberOfAddFiles = 0;
    try (CreateCheckpointIterator checkpointDataIter =
        snapshot.getCreateCheckpointIterator(engine)) {
      // Write the iterator actions to the checkpoint using the Parquet handler
      wrapEngineExceptionThrowsIO(
          () -> {
            engine
                .getParquetHandler()
                .writeParquetFileAtomically(checkpointPath.toString(), checkpointDataIter);
            return null;
          },
          "Writing checkpoint file %s",
          checkpointPath.toString());

      logger.info("{}: Checkpoint file is written for version: {}", tablePath, version);

      // Get the metadata of the checkpoint file
      numberOfAddFiles = checkpointDataIter.getNumberOfAddActions();
    } catch (FileAlreadyExistsException faee) {
      throw new CheckpointAlreadyExistsException(version);
    }

    CheckpointMetaData checkpointMetaData =
        new CheckpointMetaData(version, numberOfAddFiles, Optional.empty());

    Checkpointer checkpointer = new Checkpointer(logPath);
    checkpointer.writeLastCheckpointFile(engine, checkpointMetaData);

    logger.info("{}: Last checkpoint metadata file is written for version: {}", tablePath, version);

    logger.info("{}: Finished checkpoint for version: {}", tablePath, version);

    // Clean up delta log files if enabled.
    Metadata metadata = snapshot.getMetadata();
    if (EXPIRED_LOG_CLEANUP_ENABLED.fromMetadata(metadata)) {
      cleanupExpiredLogs(engine, clock, tablePath, LOG_RETENTION.fromMetadata(metadata));
    } else {
      logger.info(
          "{}: Log cleanup is disabled. Skipping the deletion of expired log files", tablePath);
    }
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /**
   * Given a list of delta versions, verifies that they are (1) contiguous, (2) start with
   * expectedStartVersion (if provided), and (3) end with expectedEndVersionOpt (if provided).
   * Throws an exception if any of these are not true.
   *
   * @param versions List of versions in sorted increasing order according
   */
  @VisibleForTesting
  public static void verifyDeltaVersions(
      List<Long> versions,
      Optional<Long> expectedStartVersion,
      Optional<Long> expectedEndVersion,
      Path tablePath) {
    for (int i = 1; i < versions.size(); i++) {
      if (versions.get(i) != versions.get(i - 1) + 1) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format("Missing delta files: versions are not contiguous: (%s)", versions));
      }
    }
    expectedStartVersion.ifPresent(
        v -> {
          checkArgument(
              !versions.isEmpty() && Objects.equals(versions.get(0), v),
              "Did not get the first delta file version %s to compute Snapshot",
              v);
        });
    expectedEndVersion.ifPresent(
        v -> {
          checkArgument(
              !versions.isEmpty() && Objects.equals(ListUtils.getLast(versions), v),
              "Did not get the last delta file version %s to compute Snapshot",
              v);
        });
  }

  /**
   * Updates the current `latestSnapshotHint` with the `newHint` if and only if the newHint is newer
   * (i.e. has a later table version).
   *
   * <p>Must be thread-safe.
   */
  private void registerHint(SnapshotHint newHint) {
    latestSnapshotHint.updateAndGet(
        currHint -> {
          if (currHint == null) return newHint; // the initial reference value is null
          if (newHint.getVersion() > currHint.getVersion()) return newHint;
          return currHint;
        });
  }

  private SnapshotImpl createSnapshot(
      LogSegment initSegment, Engine engine, SnapshotQueryContext snapshotContext) {
    final String startingFromStr =
        initSegment
            .checkpointVersionOpt
            .map(v -> format("starting from checkpoint version %s.", v))
            .orElse(".");
    logger.info("{}: Loading version {} {}", tablePath, initSegment.version, startingFromStr);

    long startTimeMillis = System.currentTimeMillis();

    LogReplay logReplay =
        new LogReplay(
            logPath,
            tablePath,
            initSegment.version,
            engine,
            initSegment,
            Optional.ofNullable(latestSnapshotHint.get()),
            snapshotContext.getSnapshotMetrics());

    assertLogFilesBelongToTable(logPath, initSegment.allLogFilesUnsorted());

    final SnapshotImpl snapshot =
        new SnapshotImpl(
            tablePath,
            initSegment,
            logReplay,
            logReplay.getProtocol(),
            logReplay.getMetadata(),
            snapshotContext);

    // Push snapshot report to engine
    engine.getMetricsReporters().forEach(reporter -> reporter.report(snapshot.getSnapshotReport()));

    logger.info(
        "{}: Took {}ms to construct the snapshot (loading protocol and metadata) for {} {}",
        tablePath,
        System.currentTimeMillis() - startTimeMillis,
        initSegment.version,
        startingFromStr);

    final SnapshotHint hint =
        new SnapshotHint(
            snapshot.getVersion(engine), snapshot.getProtocol(), snapshot.getMetadata());

    registerHint(hint);

    return snapshot;
  }

  /**
   * Generates a {@link LogSegment} for the given `versionToLoadOpt`. If no `versionToLoadOpt` is
   * provided, generates a {@code LogSegment} for the latest version of the table.
   *
   * <p>This primarily consists of three steps:
   *
   * <ol>
   *   <li>First, determine the starting checkpoint version that is at or before `versionToLoadOpt`.
   *       If no `versionToLoadOpt` is provided, will use the checkpoint pointed to by the
   *       _last_checkpoint file.
   *   <li>Second, LIST the _delta_log for all delta and checkpoint files newer than the starting
   *       checkpoint version.
   *   <li>Third, process and validate this list of _delta_log files to yield a {@code LogSegment}.
   * </ol>
   */
  private LogSegment getLogSegmentForVersion(Engine engine, Optional<Long> versionToLoadOpt) {
    final String versionToLoadStr = versionToLoadOpt.map(String::valueOf).orElse("latest");
    logger.info("Loading log segment for version {}", versionToLoadStr);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 1: Find the latest checkpoint version. If $versionToLoadOpt is empty, use the version //
    //         referenced by the _LAST_CHECKPOINT file. If $versionToLoad is present, search for  //
    //         the previous latest complete checkpoint at or before $versionToLoad.               //
    ////////////////////////////////////////////////////////////////////////////////////////////////

    final Optional<Long> getStartCheckpointVersionOpt =
        getStartCheckpointVersion(engine, versionToLoadOpt);

    // TODO: make this method *deep*. Conslidate all of the getLogSegment methods to one.

    return getLogSegmentForVersion(engine, getStartCheckpointVersionOpt, versionToLoadOpt);
  }

  /**
   * Helper function for the {@link #getLogSegmentForVersion(Engine, Optional)} above. Exposes the
   * startCheckpoint param for testing.
   */
  @VisibleForTesting
  public LogSegment getLogSegmentForVersion(
      Engine engine, Optional<Long> startCheckpointVersionOpt, Optional<Long> versionToLoadOpt) {
    /////////////////////////////////////////////////////////////////
    // Step 2: Determine the actual version to start listing from. //
    /////////////////////////////////////////////////////////////////

    final long listFromStartVersion =
        startCheckpointVersionOpt.orElseGet(
            () -> {
              logger.warn(
                  "{}: Starting checkpoint is missing. Listing from version as 0", tablePath);
              return 0L;
            });

    /////////////////////////////////////////////////////////////////
    // Step 3: List the files from $startVersion to $versionToLoad //
    /////////////////////////////////////////////////////////////////

    final long startTimeMillis = System.currentTimeMillis();
    final List<FileStatus> newFiles =
        DeltaLogActionUtils.listDeltaLogFiles(
            engine,
            new HashSet<>(Arrays.asList(DeltaLogFileType.COMMIT, DeltaLogFileType.CHECKPOINT)),
            tablePath,
            listFromStartVersion,
            versionToLoadOpt,
            true /* mustBeRecreatable */);
    logger.info(
        "{}: Took {}ms to list the files after starting checkpoint",
        tablePath,
        System.currentTimeMillis() - startTimeMillis);

    try {
      return getLogSegmentForVersionHelper(startCheckpointVersionOpt, versionToLoadOpt, newFiles);
    } finally {
      logger.info(
          "{}: Took {}ms to construct a log segment",
          tablePath,
          System.currentTimeMillis() - startTimeMillis);
    }
  }

  /**
   * Helper function for the getLogSegmentForVersion above. Called with a provided files list, and
   * will then try to construct a new LogSegment using that.
   */
  private LogSegment getLogSegmentForVersionHelper(
      Optional<Long> startCheckpointOpt,
      Optional<Long> versionToLoadOpt,
      List<FileStatus> newFiles) {
    if (newFiles.isEmpty()) {
      if (startCheckpointOpt.isPresent()) {
        // We either (a) determined this checkpoint version from the _LAST_CHECKPOINT file, or (b)
        // found the last complete checkpoint before our versionToLoad. In either case, we didn't
        // see the checkpoint file in the listing.
        // TODO: for case (a), re-load the delta log but ignore the _LAST_CHECKPOINT file.
        throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointOpt.get());
      } else {
        // Either no files found OR no *delta* files found even when listing from 0. This means that
        // the delta table does not exist yet.
        throw new TableNotFoundException(
            tablePath.toString(), format("No delta files found in the directory: %s", logPath));
      }
    }

    logDebug(
        () ->
            format(
                "newFiles: %s",
                Arrays.toString(
                    newFiles.stream().map(x -> new Path(x.getPath()).getName()).toArray())));

    Tuple2<List<FileStatus>, List<FileStatus>> checkpointsAndDeltas =
        ListUtils.partition(
            newFiles,
            fileStatus -> FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName()));
    final List<FileStatus> checkpoints = checkpointsAndDeltas._1;
    final List<FileStatus> deltas = checkpointsAndDeltas._2;

    logDebug(
        () ->
            format(
                "\ncheckpoints: %s\ndeltas: %s",
                Arrays.toString(
                    checkpoints.stream().map(x -> new Path(x.getPath()).getName()).toArray()),
                Arrays.toString(
                    deltas.stream().map(x -> new Path(x.getPath()).getName()).toArray())));

    // Find the latest checkpoint in the listing that is not older than the versionToLoad
    final CheckpointInstance maxCheckpoint =
        versionToLoadOpt.map(CheckpointInstance::new).orElse(CheckpointInstance.MAX_VALUE);
    logger.debug("lastCheckpoint: {}", maxCheckpoint);

    final List<CheckpointInstance> checkpointFiles =
        checkpoints.stream()
            .map(f -> new CheckpointInstance(f.getPath()))
            .collect(Collectors.toList());
    logDebug(() -> format("checkpointFiles: %s", Arrays.toString(checkpointFiles.toArray())));

    final Optional<CheckpointInstance> newCheckpointOpt =
        Checkpointer.getLatestCompleteCheckpointFromList(checkpointFiles, maxCheckpoint);
    logger.debug("newCheckpointOpt: {}", newCheckpointOpt);

    final long newCheckpointVersion =
        newCheckpointOpt
            .map(c -> c.version)
            .orElseGet(
                () -> {
                  // If we do not have any checkpoint, pass new checkpoint version as -1 so that
                  // first delta version can be 0.
                  startCheckpointOpt.map(
                      startCheckpoint -> {
                        // `startCheckpointOpt` was given but no checkpoint found on delta log.
                        // This means that the last checkpoint we thought should exist (the
                        // `_last_checkpoint` file) no longer exists.
                        // Try to look up another valid checkpoint and create `LogSegment` from it.
                        //
                        // FIXME: Something has gone very wrong if the checkpoint doesn't
                        // exist at all. This code should only handle rejected incomplete
                        // checkpoints.
                        final long snapshotVersion =
                            versionToLoadOpt.orElseGet(
                                () -> {
                                  final FileStatus lastDelta = deltas.get(deltas.size() - 1);
                                  return FileNames.deltaVersion(new Path(lastDelta.getPath()));
                                });

                        return getLogSegmentWithMaxExclusiveCheckpointVersion(
                                snapshotVersion, startCheckpoint)
                            .orElseThrow(
                                () ->
                                    // No alternative found, but the directory contains files
                                    // so we cannot return None.
                                    new RuntimeException(
                                        format(
                                            "Checkpoint file to load version: %s is missing.",
                                            startCheckpoint)));
                      });

                  return -1L;
                });
    logger.debug("newCheckpointVersion: {}", newCheckpointVersion);

    // TODO: we can calculate deltasAfterCheckpoint and deltaVersions more efficiently
    // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
    // it will list all existing delta files.
    final List<FileStatus> deltasAfterCheckpoint =
        deltas.stream()
            .filter(
                fileStatus ->
                    FileNames.deltaVersion(new Path(fileStatus.getPath())) > newCheckpointVersion)
            .collect(Collectors.toList());

    logDebug(
        () ->
            format(
                "deltasAfterCheckpoint: %s",
                Arrays.toString(
                    deltasAfterCheckpoint.stream()
                        .map(x -> new Path(x.getPath()).getName())
                        .toArray())));

    final List<Long> deltaVersionsAfterCheckpoint =
        deltasAfterCheckpoint.stream()
            .map(fileStatus -> FileNames.deltaVersion(new Path(fileStatus.getPath())))
            .collect(Collectors.toList());

    logDebug(
        () -> format("deltaVersions: %s", Arrays.toString(deltaVersionsAfterCheckpoint.toArray())));

    final long newVersion =
        deltaVersionsAfterCheckpoint.isEmpty()
            ? newCheckpointOpt.get().version
            : ListUtils.getLast(deltaVersionsAfterCheckpoint);

    // There should be a delta file present for the newVersion that we are loading
    // (Even if `deltasAfterCheckpoint` is empty, `deltas` should not be)
    if (deltas.isEmpty()
        || FileNames.deltaVersion(deltas.get(deltas.size() - 1).getPath()) < newVersion) {
      throw new InvalidTableException(
          tablePath.toString(), String.format("Missing delta file for version %s", newVersion));
    }

    versionToLoadOpt
        .filter(v -> v != newVersion)
        .ifPresent(
            v -> {
              throw DeltaErrors.versionAfterLatestCommit(tablePath.toString(), v, newVersion);
            });

    // We may just be getting a checkpoint file after the filtering
    if (!deltaVersionsAfterCheckpoint.isEmpty()) {
      // If we have deltas after the checkpoint, the first file should be 1 greater than our
      // last checkpoint version. If no checkpoint is present, this means the first delta file
      // should be version 0.
      if (deltaVersionsAfterCheckpoint.get(0) != newCheckpointVersion + 1) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format(
                "Unable to reconstruct table state: missing log file for version %s",
                newCheckpointVersion + 1));
      }

      verifyDeltaVersions(
          deltaVersionsAfterCheckpoint,
          Optional.of(newCheckpointVersion + 1),
          versionToLoadOpt,
          tablePath);

      logger.info(
          "Verified delta files are contiguous from version {} to {}",
          newCheckpointVersion + 1,
          newVersion);
    }

    final long lastCommitTimestamp = deltas.get(deltas.size() - 1).getModificationTime();

    final List<FileStatus> newCheckpointFiles =
        newCheckpointOpt
            .map(
                newCheckpoint -> {
                  final Set<Path> newCheckpointPaths =
                      new HashSet<>(newCheckpoint.getCorrespondingFiles(logPath));
                  final List<FileStatus> newCheckpointFileList =
                      checkpoints.stream()
                          .filter(f -> newCheckpointPaths.contains(new Path(f.getPath())))
                          .collect(Collectors.toList());

                  if (newCheckpointFileList.size() != newCheckpointPaths.size()) {
                    String msg =
                        format(
                            "Seems like the checkpoint is corrupted. Failed in getting the file "
                                + "information for:\n%s\namong\n%s",
                            newCheckpointPaths.stream()
                                .map(Path::toString)
                                .collect(Collectors.toList()),
                            checkpoints.stream()
                                .map(FileStatus::getPath)
                                .collect(Collectors.joining("\n - ")));
                    throw new IllegalStateException(msg);
                  }
                  return newCheckpointFileList;
                })
            .orElse(Collections.emptyList());

    return new LogSegment(
        logPath,
        newVersion,
        deltasAfterCheckpoint,
        newCheckpointFiles,
        newCheckpointOpt.map(x -> x.version),
        lastCommitTimestamp);
  }

  /////////////////////////
  // getLogSegment utils //
  /////////////////////////

  /**
   * Determine the starting checkpoint version that is at or before `versionToLoadOpt`. If no
   * `versionToLoadOpt` is provided, will use the checkpoint pointed to by the _last_checkpoint
   * file.
   */
  private Optional<Long> getStartCheckpointVersion(Engine engine, Optional<Long> versionToLoadOpt) {
    return versionToLoadOpt
        .map(
            versionToLoad -> {
              logger.info(
                  "Finding last complete checkpoint at or before version {}", versionToLoad);
              final long startTimeMillis = System.currentTimeMillis();
              return Checkpointer.findLastCompleteCheckpointBefore(
                      engine, logPath, versionToLoad + 1)
                  .map(checkpointInstance -> checkpointInstance.version)
                  .map(
                      checkpointVersion -> {
                        checkArgument(
                            checkpointVersion <= versionToLoad,
                            "Last complete checkpoint version %s was not <= targetVersion %s",
                            checkpointVersion,
                            versionToLoad);

                        logger.info(
                            "{}: Took {}ms to find last complete checkpoint <= targetVersion {}",
                            tablePath,
                            System.currentTimeMillis() - startTimeMillis,
                            versionToLoad);

                        return checkpointVersion;
                      });
            })
        .orElseGet(
            () -> {
              logger.info("Loading last checkpoint from the _last_checkpoint file");
              return new Checkpointer(logPath).readLastCheckpointFile(engine).map(x -> x.version);
            });
  }

  /**
   * Returns a [[LogSegment]] for reading `snapshotVersion` such that the segment's checkpoint
   * version (if checkpoint present) is LESS THAN `maxExclusiveCheckpointVersion`. This is useful
   * when trying to skip a bad checkpoint. Returns `None` when we are not able to construct such
   * [[LogSegment]], for example, no checkpoint can be used but we don't have the entire history
   * from version 0 to version `snapshotVersion`.
   */
  private Optional<LogSegment> getLogSegmentWithMaxExclusiveCheckpointVersion(
      long snapshotVersion, long maxExclusiveCheckpointVersion) {
    // TODO
    return Optional.empty();
  }

  // TODO logger interface to support this across kernel-api module
  private void logDebug(Supplier<String> message) {
    if (logger.isDebugEnabled()) {
      logger.debug(message.get());
    }
  }
}
