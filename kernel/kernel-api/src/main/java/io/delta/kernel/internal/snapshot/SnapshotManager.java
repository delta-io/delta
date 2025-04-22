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

import static io.delta.kernel.internal.replay.LogReplayUtils.assertLogFilesBelongToTable;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.*;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checkpoints.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotManager {

  /**
   * The latest {@link SnapshotHint} for this table. The initial value inside the AtomicReference is
   * `null`.
   */
  private final AtomicReference<SnapshotHint> latestSnapshotHint;

  private final Path tablePath;
  private final Path logPath;

  public SnapshotManager(Path tablePath) {
    this.latestSnapshotHint = new AtomicReference<>();
    this.tablePath = tablePath;
    this.logPath = new Path(tablePath, "_delta_log");
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

    snapshotContext.setVersion(logSegment.getVersion());

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
  public SnapshotImpl getSnapshotAt(
      Engine engine, long version, SnapshotQueryContext snapshotContext)
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
   * @throws TableNotFoundException if the table does not exist
   * @throws InvalidTableException if the table is in an invalid state
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

  ////////////////////
  // Helper Methods //
  ////////////////////

  /**
   * Verify that a list of delta versions is contiguous.
   *
   * @throws InvalidTableException if the versions are not contiguous
   */
  @VisibleForTesting
  public static void verifyDeltaVersionsContiguous(List<Long> versions, Path tablePath) {
    for (int i = 1; i < versions.size(); i++) {
      if (versions.get(i) != versions.get(i - 1) + 1) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format("Missing delta files: versions are not contiguous: (%s)", versions));
      }
    }
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
            .getCheckpointVersionOpt()
            .map(v -> format("starting from checkpoint version %s.", v))
            .orElse(".");
    logger.info("{}: Loading version {} {}", tablePath, initSegment.getVersion(), startingFromStr);

    long startTimeMillis = System.currentTimeMillis();

    LogReplay logReplay =
        new LogReplay(
            logPath,
            tablePath,
            initSegment.getVersion(),
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
        initSegment.getVersion(),
        startingFromStr);

    final SnapshotHint hint =
        new SnapshotHint(snapshot.getVersion(), snapshot.getProtocol(), snapshot.getMetadata());

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
  @VisibleForTesting
  public LogSegment getLogSegmentForVersion(Engine engine, Optional<Long> versionToLoadOpt) {
    final String versionToLoadStr = versionToLoadOpt.map(String::valueOf).orElse("latest");
    logger.info("Loading log segment for version {}", versionToLoadStr);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 1: Find the latest checkpoint version. If $versionToLoadOpt is empty, use the version //
    //         referenced by the _LAST_CHECKPOINT file. If $versionToLoad is present, search for  //
    //         the previous latest complete checkpoint at or before $versionToLoad.               //
    ////////////////////////////////////////////////////////////////////////////////////////////////

    final Optional<Long> startCheckpointVersionOpt =
        getStartCheckpointVersion(engine, versionToLoadOpt);

    /////////////////////////////////////////////////////////////////
    // Step 2: Determine the actual version to start listing from. //
    /////////////////////////////////////////////////////////////////

    final long listFromStartVersion =
        startCheckpointVersionOpt
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

    final long startTimeMillis = System.currentTimeMillis();
    final List<FileStatus> listedFileStatuses =
        DeltaLogActionUtils.listDeltaLogFilesAsIter(
                engine,
                new HashSet<>(Arrays.asList(DeltaLogFileType.COMMIT, DeltaLogFileType.CHECKPOINT)),
                tablePath,
                listFromStartVersion,
                versionToLoadOpt,
                true /* mustBeRecreatable */)
            .toInMemoryList();

    logger.info(
        "{}: Took {}ms to list the files after starting checkpoint",
        tablePath,
        System.currentTimeMillis() - startTimeMillis);

    ////////////////////////////////////////////////////////////////////////
    // Step 4: Perform some basic validations on the listed file statuses //
    ////////////////////////////////////////////////////////////////////////

    if (listedFileStatuses.isEmpty()) {
      if (startCheckpointVersionOpt.isPresent()) {
        // We either (a) determined this checkpoint version from the _LAST_CHECKPOINT file, or (b)
        // found the last complete checkpoint before our versionToLoad. In either case, we didn't
        // see the checkpoint file in the listing.
        // TODO: throw a more specific error based on case (a) or (b)
        throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointVersionOpt.get());
      } else {
        // Either no files found OR no *delta* files found even when listing from 0. This means that
        // the delta table does not exist yet.
        throw new TableNotFoundException(
            tablePath.toString(), format("No delta files found in the directory: %s", logPath));
      }
    }

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

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Step 6: Determine the latest complete checkpoint version. The intuition here is that we //
    //         LISTed from the startingCheckpoint but may have found a newer complete          //
    //         checkpoint.                                                                     //
    /////////////////////////////////////////////////////////////////////////////////////////////

    final List<CheckpointInstance> listedCheckpointInstances =
        listedCheckpointFileStatuses.stream()
            .map(f -> new CheckpointInstance(f.getPath()))
            .collect(Collectors.toList());

    final CheckpointInstance notLaterThanCheckpoint =
        versionToLoadOpt.map(CheckpointInstance::new).orElse(CheckpointInstance.MAX_VALUE);

    final Optional<CheckpointInstance> latestCompleteCheckpointOpt =
        Checkpointer.getLatestCompleteCheckpointFromList(
            listedCheckpointInstances, notLaterThanCheckpoint);

    if (!latestCompleteCheckpointOpt.isPresent() && startCheckpointVersionOpt.isPresent()) {
      // In Step 1 we found a $startCheckpointVersion but now our LIST of the file system doesn't
      // see it. This means that the checkpoint we thought should exist no longer does.
      throw DeltaErrors.missingCheckpoint(tablePath.toString(), startCheckpointVersionOpt.get());
    }

    final long latestCompleteCheckpointVersion =
        latestCompleteCheckpointOpt.map(x -> x.version).orElse(-1L);

    logger.info("Latest complete checkpoint version: {}", latestCompleteCheckpointVersion);

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Step 7: Grab all deltas in range [$latestCompleteCheckpointVersion + 1, $versionToLoad] //
    /////////////////////////////////////////////////////////////////////////////////////////////

    final List<FileStatus> deltasAfterCheckpoint =
        listedDeltaFileStatuses.stream()
            .filter(
                fs -> {
                  final long deltaVersion = FileNames.deltaVersion(new Path(fs.getPath()));
                  return latestCompleteCheckpointVersion + 1 <= deltaVersion
                      && deltaVersion <= versionToLoadOpt.orElse(Long.MAX_VALUE);
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

    // Check that we have found at least one checkpoint or delta file
    if (!latestCompleteCheckpointOpt.isPresent() && deltasAfterCheckpoint.isEmpty()) {
      throw new InvalidTableException(
          tablePath.toString(), "No complete checkpoint found and no delta files found");
    }

    // Check that, for a checkpoint at version N, there's a delta file at N, too.
    if (latestCompleteCheckpointOpt.isPresent()
        && listedDeltaFileStatuses.stream()
            .map(x -> FileNames.deltaVersion(new Path(x.getPath())))
            .noneMatch(v -> v == latestCompleteCheckpointVersion)) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format("Missing delta file for version %s", latestCompleteCheckpointVersion));
    }

    // Check that the $newVersion we actually loaded is the desired $versionToLoad
    versionToLoadOpt.ifPresent(
        versionToLoad -> {
          if (newVersion < versionToLoad) {
            throw DeltaErrors.versionToLoadAfterLatestCommit(
                tablePath.toString(), versionToLoad, newVersion);
          } else if (newVersion > versionToLoad) {
            throw new IllegalStateException(
                String.format(
                    "%s: Expected to load version %s but actually loaded version %s",
                    tablePath, versionToLoad, newVersion));
          }
        });

    if (!deltasAfterCheckpoint.isEmpty()) {
      // Check that the delta versions are contiguous
      verifyDeltaVersionsContiguous(deltaVersionsAfterCheckpoint, tablePath);

      // Check that the delta versions start with $latestCompleteCheckpointVersion + 1. If they
      // don't, then we have a gap in between the checkpoint and the first delta file.
      if (!deltaVersionsAfterCheckpoint.get(0).equals(latestCompleteCheckpointVersion + 1)) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format(
                "Cannot compute snapshot. Missing delta file version %d.",
                latestCompleteCheckpointVersion + 1));
      }

      // Note: We have already asserted above that $versionToLoad equals $newVersion.
      // Note: We already know that the last element of deltasAfterCheckpoint is $newVersion IF
      //       $deltasAfterCheckpoint is not empty.

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
              final long startTimeMillis = System.currentTimeMillis();
              try {
                return new Checkpointer(logPath)
                    .readLastCheckpointFile(engine).map(x -> x.version);
              } finally {
                logger.info(
                    "{}: Took {}ms for the task to read or attempt to read the `_last_checkpoint` file",
                    tablePath,
                    System.currentTimeMillis() - startTimeMillis);
              }
            });
  }

  private void logDebugFileStatuses(String varName, List<FileStatus> fileStatuses) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {}",
          varName,
          Arrays.toString(
              fileStatuses.stream().map(x -> new Path(x.getPath()).getName()).toArray()));
    }
  }
}
