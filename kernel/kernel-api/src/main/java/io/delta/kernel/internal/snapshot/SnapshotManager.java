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
import static io.delta.kernel.internal.util.Preconditions.checkState;
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
import io.delta.kernel.internal.logging.KernelLogger;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotManager {
  private static final Logger LOGGER_IMPL = LoggerFactory.getLogger(SnapshotManager.class);

  /**
   * The latest {@link SnapshotHint} for this table. The initial value inside the AtomicReference is
   * `null`.
   */
  private AtomicReference<SnapshotHint> latestSnapshotHint;

  private final Path logPath;
  private final Path tablePath;
  private final LogFileLister logFileLister;
  private final KernelLogger logger;
  private final Checkpointer checkpointer;

  public SnapshotManager(Path logPath, Path tablePath) {
    this.latestSnapshotHint = new AtomicReference<>();
    this.logPath = logPath;
    this.tablePath = tablePath;
    this.logFileLister = new LogFileLister(tablePath, logPath);
    this.logger = new KernelLogger(LOGGER_IMPL, tablePath.toString());
    this.checkpointer = new Checkpointer(logPath);
  }

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
  public Snapshot buildLatestSnapshot(Engine engine, SnapshotQueryContext ctx)
      throws TableNotFoundException {
    checkArgument(
        !ctx.getVersion().isPresent() && !ctx.getProvidedTimestamp().isPresent(),
        "buildLatestSnapshot: snapshotContext version and timestamp should not be set");

    final LogSegment logSegment = getLogSegment(engine, Optional.empty() /* versionToLoad */);
    // We update the query context version with the resolved version from the log segment listing
    // if it exists
    ctx.setVersion(logSegment.version);
    return createSnapshot(engine, logSegment, ctx);
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
  public Snapshot getSnapshotAt(Engine engine, long version, SnapshotQueryContext ctx)
      throws TableNotFoundException {
    checkArgument(
        ctx.getVersion().isPresent() && !ctx.getProvidedTimestamp().isPresent(),
        "getSnapshotAt: only snapshotContext version should be set");

    final LogSegment logSegment = getLogSegment(engine, Optional.of(version));
    return createSnapshot(engine, logSegment, ctx);
  }

  /**
   * Construct the snapshot for the given table at the provided timestamp.
   *
   * @param engine Instance of {@link Engine} to use.
   * @param millisSinceEpochUTC timestamp to fetch the snapshot in milliseconds since the unix epoch
   * @return a {@link Snapshot} of the table at the provided timestamp
   * @throws TableNotFoundException if the table does not exist
   * @throws InvalidTableException if the table is in an invalid state
   */
  public Snapshot getSnapshotForTimestamp(
      Engine engine, long millisSinceEpochUTC, SnapshotQueryContext ctx)
      throws TableNotFoundException {
    checkArgument(
        ctx.getVersion().isPresent() && !ctx.getProvidedTimestamp().isPresent(),
        "getSnapshotForTimestamp: only snapshotContext timestamp should be set");

    final long versionToRead =
        ctx.getSnapshotMetrics()
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
        "Took {} ms to fetch version at timestamp {}",
        ctx.getSnapshotMetrics().timestampToVersionResolutionTimer.totalDurationMs(),
        millisSinceEpochUTC);
    // We update the query context version as soon as we resolve timestamp --> version
    ctx.setVersion(versionToRead);

    return getSnapshotAt(engine, versionToRead, ctx);
  }

  public void checkpoint(Engine engine, Clock clock, long version)
      throws TableNotFoundException, IOException {
    logger.info("Starting checkpoint for version: {}", version);
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

    long numberOfAddFiles;
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

      logger.info("Checkpoint file is written for version: {}", version);

      // Get the metadata of the checkpoint file
      numberOfAddFiles = checkpointDataIter.getNumberOfAddActions();
    } catch (FileAlreadyExistsException faee) {
      throw new CheckpointAlreadyExistsException(version);
    }

    CheckpointMetaData checkpointMetaData =
        new CheckpointMetaData(version, numberOfAddFiles, Optional.empty());

    checkpointer.writeLastCheckpointFile(engine, checkpointMetaData);

    logger.info("Last checkpoint metadata file is written for version: {}", version);

    // Clean up delta log files if enabled.
    Metadata metadata = snapshot.getMetadata();
    if (EXPIRED_LOG_CLEANUP_ENABLED.fromMetadata(metadata)) {
      cleanupExpiredLogs(engine, clock, tablePath, LOG_RETENTION.fromMetadata(metadata));
    } else {
      logger.info("Log cleanup is disabled. Skipping the deletion of expired log files");
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
          if (versions.isEmpty() || !Objects.equals(versions.get(0), v)) {
            throw new InvalidTableException(
                tablePath.toString(),
                String.format(
                    "Did not get the first delta file version %s to compute Snapshot", v));
          }
        });
    expectedEndVersion.ifPresent(
        v -> {
          if (versions.isEmpty() || !Objects.equals(ListUtils.getLast(versions), v)) {
            throw new InvalidTableException(
                tablePath.toString(),
                String.format("Did not get the last delta file version %s to compute Snapshot", v));
          }
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
      Engine engine, LogSegment initSegment, SnapshotQueryContext snapshotContext) {
    final String startingFromStr =
        initSegment
            .checkpointVersionOpt
            .map(v -> format("starting from checkpoint version %s.", v))
            .orElse(".");
    logger.info("Loading version {} {}", initSegment.version, startingFromStr);

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
        "Took {}ms to construct the snapshot (loading protocol and metadata) for {} {}",
        System.currentTimeMillis() - startTimeMillis,
        initSegment.version,
        startingFromStr);

    final SnapshotHint hint =
        new SnapshotHint(
            snapshot.getVersion(engine), snapshot.getProtocol(), snapshot.getMetadata());

    registerHint(hint);

    return snapshot;
  }

  private LogSegment getLogSegment(Engine engine, Optional<Long> versionToLoad) {
    final String versionToLoadStr = versionToLoad.map(String::valueOf).orElse("latest");
    logger.info("Loading log segment for version {}", versionToLoadStr);

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Step 1: Find the latest checkpoint version. If $versionToLoad is empty, use the version   //
    //         referenced by the _LAST_CHECKPOINT file. If $versionToLoad is present, search for //
    //         the previous latest complete checkpoint at or before $versionToLoad.              //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    final Optional<Long> startCheckpointVersion;

    if (versionToLoad.isPresent()) {
      final long targetVersion = versionToLoad.get();
      startCheckpointVersion =
          logger.timeOperation(
              String.format("Load last checkpoint at or before version %s", targetVersion),
              () ->
                  Checkpointer.findLastCompleteCheckpointBefore(engine, logPath, targetVersion + 1)
                      .map(checkpointInstance -> checkpointInstance.version)
                      .map(
                          checkpointVersion -> {
                            // Double-check this result.
                            checkState(
                                checkpointVersion <= targetVersion,
                                String.format(
                                    "Expected checkpointVersion %s to be less than or equal to "
                                        + "targetVersion %s",
                                    checkpointVersion, targetVersion));
                            return checkpointVersion;
                          }));
    } else {
      startCheckpointVersion = checkpointer.readLastCheckpointFile(engine).map(x -> x.version);
    }

    return getLogSegmentHelper(engine, startCheckpointVersion, versionToLoad);
  }

  @VisibleForTesting
  public LogSegment getLogSegmentHelper(
      Engine engine, Optional<Long> startCheckpointVersion, Optional<Long> versionToLoad) {
    startCheckpointVersion.ifPresent(
        start -> {
          versionToLoad.ifPresent(
              target -> {
                checkArgument(
                    start <= target,
                    "startCheckpointVersion %s is greater than versionToLoad %s",
                    start,
                    target);
              });
        });

    /////////////////////////////////////////////////////////////////
    // Step 2: Determine the actual version to start loading from. //
    /////////////////////////////////////////////////////////////////

    final long startVersion =
        startCheckpointVersion
            .map(
                version -> {
                  logger.info("Found a complete checkpoint at version {}.", version);
                  return version;
                })
            .orElseGet(
                () -> {
                  logger.warn("Cannot find a complete checkpoint. Listing from version 0.");
                  return 0L;
                });

    /////////////////////////////////////////////////////////////////
    // Step 3: List the files from $startVersion to $versionToLoad //
    /////////////////////////////////////////////////////////////////

    final Optional<List<FileStatus>> listedFileStatusesOpt =
        logger.timeOperation(
            String.format("List the files from versions %s to %s", startVersion, versionToLoad),
            () -> logFileLister.listDeltaAndCheckpointFiles(engine, startVersion, versionToLoad));

    /////////////////////////////////////////////
    // Step 4: Perform some basic validations. //
    /////////////////////////////////////////////

    if (!listedFileStatusesOpt.isPresent()) {
      if (!startCheckpointVersion.isPresent()) {
        // No files found even when listing from 0 => empty directory => table does not exist.
        throw new TableNotFoundException(tablePath.toString());
      } else {
        // There are no files present at all in the directory yet, previously, we had found a valid
        // $startCheckpointVersion. The directory may have been deleted.
        throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointVersion.get());
      }
    } else if (listedFileStatusesOpt.get().isEmpty()) {
      if (!startCheckpointVersion.isPresent()) {
        // We can't construct a Snapshot because the directory contained no usable commit files.
        throw new RuntimeException(
            String.format("No delta files found in the directory: %s", logPath));
      } else {
        // There are some files present in the directory but no useful Delta files. But previously
        // we had found a valid $startCheckpointVersion. The directory may have been tampered with.
        throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointVersion.get());
      }
    }

    final List<FileStatus> listedFileStatuses = listedFileStatusesOpt.get();

    logDebugFileStatuses("listedFileStatuses", listedFileStatuses);

    ////////////////////////////////////////////////////////////////////////////
    // Step 5: Partition $listedFileStatuses into the checkpoints and deltas. //
    ////////////////////////////////////////////////////////////////////////////

    final Tuple2<List<FileStatus>, List<FileStatus>> listedCheckpointAndDeltaFileStatuses =
        ListUtils.partition(
            listedFileStatuses,
            fileStatus -> FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName()));
    final List<FileStatus> listedCheckpointFileStatuses = listedCheckpointAndDeltaFileStatuses._1;
    final List<FileStatus> listedDeltaFileStatuses = listedCheckpointAndDeltaFileStatuses._2;

    logDebugFileStatuses("listedCheckpointFileStatuses", listedCheckpointFileStatuses);
    logDebugFileStatuses("listedDeltaFileStatuses", listedDeltaFileStatuses);

    ///////////////////////////////////////////////////////////////
    // Step 6: Determine the latest complete checkpoint version. //
    ///////////////////////////////////////////////////////////////

    final List<CheckpointInstance> checkpointInstances =
        listedCheckpointFileStatuses.stream()
            .map(f -> new CheckpointInstance(f.getPath()))
            .collect(Collectors.toList());

    final CheckpointInstance notLaterThanCheckpoint =
        versionToLoad.map(CheckpointInstance::new).orElse(CheckpointInstance.MAX_VALUE);

    final Optional<CheckpointInstance> latestCompleteCheckpointOpt =
        Checkpointer.getLatestCompleteCheckpointFromList(
            checkpointInstances, notLaterThanCheckpoint);

    if (!latestCompleteCheckpointOpt.isPresent() && startCheckpointVersion.isPresent()) {
      // In Step 1 we found the $startCheckpointVersion but now our LIST of the file system doesn't
      // see it. This means that the checkpoint we thought should exist no longer does.
      throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointVersion.get());
    }

    final long latestCompleteCheckpointVersion =
        latestCompleteCheckpointOpt.map(x -> x.version).orElse(-1L);

    logger.info("Latest complete checkpoint version: {}", latestCompleteCheckpointVersion);

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Step 7: Grab all the deltas in range [$latestCompleteCheckpointVersion, $versionToLoad]. //
    //////////////////////////////////////////////////////////////////////////////////////////////

    final List<FileStatus> deltasAfterCheckpoint =
        listedDeltaFileStatuses.stream()
            .filter(
                fs -> {
                  final long deltaVersion = FileNames.deltaVersion(new Path(fs.getPath()));
                  return latestCompleteCheckpointVersion + 1 <= deltaVersion
                      && deltaVersion <= versionToLoad.orElse(Long.MAX_VALUE);
                })
            .collect(Collectors.toList());

    logDebugFileStatuses("deltasAfterCheckpoint", deltasAfterCheckpoint);

    ////////////////////////////////////////////////////////////////////
    // Step 8: Determine the version of the snapshot we can now load. //
    ////////////////////////////////////////////////////////////////////

    final List<Long> deltaVersionsAfterCheckpoint =
        deltasAfterCheckpoint.stream()
            .map(fileStatus -> FileNames.deltaVersion(new Path(fileStatus.getPath())))
            .collect(Collectors.toList());

    final long newVersion =
        deltaVersionsAfterCheckpoint.isEmpty()
            ? latestCompleteCheckpointVersion
            : ListUtils.getLast(deltaVersionsAfterCheckpoint);

    logger.info("New version to load: {}", newVersion);

    /////////////////////////////////////////////
    // Step 9: Perform some basic validations. //
    /////////////////////////////////////////////

    if (!latestCompleteCheckpointOpt.isPresent() && deltasAfterCheckpoint.isEmpty()) {
      throw new InvalidTableException(
          tablePath.toString(), "No complete checkpoint found and no delta files found");
    }

    // If there's a checkpoint at version N then we also require that there's a delta file at that
    // version.
    if (latestCompleteCheckpointOpt.isPresent()
        && listedDeltaFileStatuses.stream()
            .map(x -> FileNames.deltaVersion(new Path(x.getPath())))
            .noneMatch(v -> v == latestCompleteCheckpointVersion)) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format("Missing delta file for version %s", latestCompleteCheckpointVersion));
    }

    // We know from steps 7 and 8 that $newVersion will be in [$latestCompleteCheckpointVersion,
    // $versionToLoad]. If $newVersion != $versionToLoad then $newVersion must be < $versionToLoad.
    versionToLoad.ifPresent(
        v -> {
          if (!v.equals(newVersion)) {
            throw DeltaErrors.versionToLoadAfterLatestCommit(tablePath.toString(), v, newVersion);
          }
        });

    if (!deltasAfterCheckpoint.isEmpty()) {
      verifyDeltaVersions(
          deltaVersionsAfterCheckpoint /* we can pass this in even if it is empty */,
          Optional.of(latestCompleteCheckpointVersion + 1) /* expected first version */,
          versionToLoad /* expected end version */,
          tablePath);

      logger.info(
          "Verified delta files are contiguous from version {} to {}",
          latestCompleteCheckpointVersion + 1,
          newVersion);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    // Step 10: Grab the actual checkpoint file statuses for latestCompleteCheckpointVersion. //
    ////////////////////////////////////////////////////////////////////////////////////////////

    final List<FileStatus> latestCompleteCheckpointFileStatuses =
        latestCompleteCheckpointOpt
            .map(
                latestCompleteCheckpoint -> {
                  final Set<Path> newCheckpointPaths =
                      new HashSet<>(latestCompleteCheckpoint.getCorrespondingFiles(logPath));

                  final List<FileStatus> newCheckpointFileStatuses =
                      listedCheckpointFileStatuses.stream()
                          .filter(f -> newCheckpointPaths.contains(new Path(f.getPath())))
                          .collect(Collectors.toList());

                  logDebugFileStatuses("newCheckpointFileStatuses", newCheckpointFileStatuses);

                  if (newCheckpointFileStatuses.size() != newCheckpointPaths.size()) {
                    final String msg =
                        format(
                            "Seems like the checkpoint is corrupted. Failed in getting the file "
                                + "information for:\n%s\namong\n%s",
                            newCheckpointPaths.stream()
                                .map(Path::toString)
                                .collect(Collectors.joining("\n - ")),
                            listedCheckpointFileStatuses.stream()
                                .map(FileStatus::getPath)
                                .collect(Collectors.joining("\n - ")));
                    throw new IllegalStateException(msg);
                  }

                  return newCheckpointFileStatuses;
                })
            .orElse(Collections.emptyList());

    ///////////////////////////////////////////////////
    // Step 11: Construct the LogSegment and return. //
    ///////////////////////////////////////////////////

    logger.info("Successfully constructed LogSegment at version {}", newVersion);

    final long lastCommitTimestamp =
        ListUtils.getLast(listedDeltaFileStatuses).getModificationTime();

    return new LogSegment(
        logPath,
        newVersion,
        deltasAfterCheckpoint,
        latestCompleteCheckpointFileStatuses,
        latestCompleteCheckpointOpt.map(x -> x.version),
        lastCommitTimestamp);
  }

  private void logDebugFileStatuses(String varName, List<FileStatus> fileStatuses) {
    logger.debug(
        () ->
            String.format(
                "%s: %s",
                varName,
                Arrays.toString(
                    fileStatuses.stream().map(x -> new Path(x.getPath()).getName()).toArray())));
  }
}
