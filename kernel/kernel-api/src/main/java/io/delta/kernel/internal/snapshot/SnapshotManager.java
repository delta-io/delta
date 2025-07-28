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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.*;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.checkpoints.*;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogCategory;
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public SnapshotImpl buildLatestSnapshot(Engine engine, SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    final LogSegment logSegment =
        snapshotContext
            .getSnapshotMetrics()
            .loadLogSegmentTotalDurationTimer
            .time(() -> getLogSegmentForVersion(engine, Optional.empty() /* versionToLoad */));
    snapshotContext.setVersion(logSegment.getVersion());
    snapshotContext.setCheckpointVersion(logSegment.getCheckpointVersionOpt());

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
        snapshotContext
            .getSnapshotMetrics()
            .loadLogSegmentTotalDurationTimer
            .time(
                () -> getLogSegmentForVersion(engine, Optional.of(version) /* versionToLoadOpt */));

    snapshotContext.setCheckpointVersion(logSegment.getCheckpointVersionOpt());
    snapshotContext.setVersion(logSegment.getVersion());

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
  public SnapshotImpl getSnapshotForTimestamp(
      Engine engine,
      SnapshotImpl latestSnapshot,
      long millisSinceEpochUTC,
      SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    long versionToRead =
        snapshotContext
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .time(
                () ->
                    DeltaHistoryManager.getActiveCommitAtTimestamp(
                            engine,
                            latestSnapshot,
                            logPath,
                            millisSinceEpochUTC,
                            true /* mustBeRecreatable */,
                            false /* canReturnLastCommit */,
                            false /* canReturnEarliestCommit */)
                        .getVersion());
    logger.info(
        "{}: Took {} ms to fetch version at timestamp {}",
        tablePath,
        snapshotContext
            .getSnapshotMetrics()
            .computeTimestampToVersionTotalDurationTimer
            .totalDurationMs(),
        millisSinceEpochUTC);

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
    // Note: LogReplay now loads the protocol and metadata (P & M) only when invoked (as opposed to
    //       eagerly in its constructor). Nonetheless, we invoke it right away, so SnapshotImpl is
    //       still constructed with an "eagerly"-loaded P & M.

    final LogReplay logReplay =
        new LogReplay(
            tablePath,
            engine,
            new Lazy<>(() -> initSegment),
            Optional.ofNullable(latestSnapshotHint.get()),
            snapshotContext.getSnapshotMetrics());

    final SnapshotImpl snapshot =
        new SnapshotImpl(
            tablePath,
            initSegment,
            logReplay,
            logReplay.getProtocol(),
            logReplay.getMetadata(),
            snapshotContext);

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
  public LogSegment getLogSegmentForVersion(Engine engine, Optional<Long> versionToLoadOpt) {
    return getLogSegmentForVersion(engine, versionToLoadOpt, Collections.emptyList());
  }

  /**
   * [delta-io/delta#4765]: Right now, we are only supporting sorted and contiguous log datas of
   * type {@link ParsedLogType#RATIFIED_STAGED_COMMIT}s.
   */
  public LogSegment getLogSegmentForVersion(
      Engine engine, Optional<Long> versionToLoadOpt, List<ParsedLogData> parsedLogDatas) {
    final long versionToLoad = versionToLoadOpt.orElse(Long.MAX_VALUE);

    // Defaulting to listing the files for now. This has low cost. We can make this a configurable
    // option in the future if we need to.
    final boolean USE_COMPACTED_FILES = true;

    final String versionToLoadStr = versionToLoadOpt.map(String::valueOf).orElse("latest");
    logger.info("Loading log segment for version {}", versionToLoadStr);
    final long logSegmentBuildingStartTimeMillis = System.currentTimeMillis();

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

    Set<DeltaLogFileType> fileTypes =
        new HashSet<>(
            Arrays.asList(
                DeltaLogFileType.COMMIT, DeltaLogFileType.CHECKPOINT, DeltaLogFileType.CHECKSUM));
    if (USE_COMPACTED_FILES) {
      fileTypes.add(DeltaLogFileType.LOG_COMPACTION);
    }

    final long listingStartTimeMillis = System.currentTimeMillis();
    final List<FileStatus> listedFileStatuses =
        DeltaLogActionUtils.listDeltaLogFilesAsIter(
                engine,
                fileTypes,
                tablePath,
                listFromStartVersion,
                versionToLoadOpt,
                true /* mustBeRecreatable */)
            .toInMemoryList();

    logger.info(
        "{}: Took {}ms to list the files after starting checkpoint",
        tablePath,
        System.currentTimeMillis() - listingStartTimeMillis);

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

    //////////////////////////////////////////////////////////////////////////////////////////
    // Step 5: Partition $listedFileStatuses into the checkpoints, deltas, and compactions. //
    //////////////////////////////////////////////////////////////////////////////////////////

    final Map<ParsedLogData.ParsedLogCategory, List<ParsedLogData>> partitionedFiles =
        listedFileStatuses.stream()
            .map(ParsedLogData::forFileStatus)
            .collect(
                Collectors.groupingBy(
                    ParsedLogData::getCategory,
                    LinkedHashMap::new, // Ensure order is maintained
                    Collectors.toList()));

    final List<ParsedLogData> allPublishedDeltas =
        partitionedFiles.getOrDefault(ParsedLogCategory.DELTA, Collections.emptyList());

    final List<FileStatus> listedCheckpointFileStatuses =
        partitionedFiles.getOrDefault(ParsedLogCategory.CHECKPOINT, Collections.emptyList())
            .stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    final List<FileStatus> listedCompactionFileStatuses =
        partitionedFiles.getOrDefault(ParsedLogCategory.LOG_COMPACTION, Collections.emptyList())
            .stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    final List<FileStatus> listedChecksumFileStatuses =
        partitionedFiles.getOrDefault(ParsedLogCategory.CHECKSUM, Collections.emptyList()).stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList());

    logDebugParsedLogDatas("allPublishedDeltas", allPublishedDeltas);
    logDebugFileStatuses("listedCheckpointFileStatuses", listedCheckpointFileStatuses);
    logDebugFileStatuses("listedCompactionFileStatuses", listedCompactionFileStatuses);
    logDebugFileStatuses("listedCheckSumFileStatuses", listedChecksumFileStatuses);

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

    final List<ParsedLogData> allDeltasAfterCheckpoint =
        getAllDeltasAfterCheckpointWithCatalogPriority(
            allPublishedDeltas, parsedLogDatas, latestCompleteCheckpointVersion, versionToLoad);

    logDebugParsedLogDatas("allDeltasAfterCheckpoint", allDeltasAfterCheckpoint);

    //////////////////////////////////////////////////////////////////////////////////
    // Step 8: Grab all compactions in range [$latestCompleteCheckpointVersion + 1, //
    //         $versionToLoad]                                                      //
    //////////////////////////////////////////////////////////////////////////////////

    final List<FileStatus> compactionsAfterCheckpoint =
        listedCompactionFileStatuses.stream()
            .filter(
                fs -> {
                  final Tuple2<Long, Long> compactionVersions =
                      FileNames.logCompactionVersions(new Path(fs.getPath()));
                  return latestCompleteCheckpointVersion + 1 <= compactionVersions._1
                      && compactionVersions._2 <= versionToLoad;
                })
            .collect(Collectors.toList());

    logDebugFileStatuses("compactionsAfterCheckpoint", compactionsAfterCheckpoint);

    ////////////////////////////////////////////////////////////////////
    // Step 9: Determine the version of the snapshot we can now load. //
    ////////////////////////////////////////////////////////////////////

    final long newVersion =
        allDeltasAfterCheckpoint.isEmpty()
            ? latestCompleteCheckpointVersion
            : ListUtils.getLast(allDeltasAfterCheckpoint).version;

    logger.info("New version to load: {}", newVersion);

    /////////////////////////////////////////////
    // Step 10: Perform some basic validations. //
    /////////////////////////////////////////////

    // Check that we have found at least one checkpoint or delta file
    if (!latestCompleteCheckpointOpt.isPresent() && allDeltasAfterCheckpoint.isEmpty()) {
      throw new InvalidTableException(
          tablePath.toString(), "No complete checkpoint found and no delta files found");
    }

    final Lazy<Optional<ParsedLogData>> lazyDeltaAtCheckpointVersionOpt =
        new Lazy<>(
            () ->
                allPublishedDeltas.stream()
                    .filter(x -> x.version == latestCompleteCheckpointVersion)
                    .findFirst());

    // Check that, for a checkpoint at version N, there's a delta file at N, too.
    if (latestCompleteCheckpointOpt.isPresent()
        && !lazyDeltaAtCheckpointVersionOpt.get().isPresent()) {
      throw new InvalidTableException(
          tablePath.toString(),
          String.format("Missing delta file for version %s", latestCompleteCheckpointVersion));
    }

    // Check that the $newVersion we actually loaded is the desired $versionToLoad
    if (versionToLoadOpt.isPresent()) {
      if (newVersion < versionToLoad) {
        throw DeltaErrors.versionToLoadAfterLatestCommit(
            tablePath.toString(), versionToLoad, newVersion);
      } else if (newVersion > versionToLoad) {
        throw new IllegalStateException(
            String.format(
                "%s: Expected to load version %s but actually loaded version %s",
                tablePath, versionToLoad, newVersion));
      }
    }

    if (!allDeltasAfterCheckpoint.isEmpty()) {
      // Check that the delta versions are contiguous
      verifyDeltaVersionsContiguous(
          // TODO: refactor `verifyDeltaVersionsContiguous` to operate on ParsedLogData so we can
          //      avoid making an entirely new list here
          allDeltasAfterCheckpoint.stream().map(x -> x.version).collect(Collectors.toList()),
          tablePath);

      // Check that the delta versions start with $latestCompleteCheckpointVersion + 1. If they
      // don't, then we have a gap in between the checkpoint and the first delta file.
      if (allDeltasAfterCheckpoint.get(0).version != latestCompleteCheckpointVersion + 1) {
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
    // Step 11: Grab the actual checkpoint file statuses for latestCompleteCheckpointVersion. //
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

    //////////////////////////////////////////
    // Step 12: Grab the last seen checksum //
    //////////////////////////////////////////

    Optional<FileStatus> lastSeenChecksumFile = Optional.empty();
    if (!listedChecksumFileStatuses.isEmpty()) {
      FileStatus latestChecksum = ListUtils.getLast(listedChecksumFileStatuses);
      long checksumVersion = FileNames.checksumVersion(new Path(latestChecksum.getPath()));
      if (checksumVersion >= latestCompleteCheckpointVersion) {
        lastSeenChecksumFile = Optional.of(latestChecksum);
      }
    }

    ///////////////////////////////////////////////////
    // Step 13: Construct the LogSegment and return. //
    ///////////////////////////////////////////////////

    logger.info(
        "Successfully constructed LogSegment at version {}, took {}ms",
        newVersion,
        System.currentTimeMillis() - logSegmentBuildingStartTimeMillis);

    // If our LogSegment has deltas (allDeltasAfterCheckpoint), we use that timestamp.
    // Else, our LogSegment only has a checkpoint, and we have checked above that if there's a
    // checkpoint then the `lazyDeltaAtCheckpointVersionOpt` exists.
    final long lastCommitTimestamp =
        allDeltasAfterCheckpoint.isEmpty()
            ? lazyDeltaAtCheckpointVersionOpt.get().get().getFileStatus().getModificationTime()
            : ListUtils.getLast(allDeltasAfterCheckpoint).getFileStatus().getModificationTime();

    return new LogSegment(
        logPath,
        newVersion,
        allDeltasAfterCheckpoint.stream()
            .map(ParsedLogData::getFileStatus)
            .collect(Collectors.toList()),
        compactionsAfterCheckpoint,
        latestCompleteCheckpointFileStatuses,
        lastSeenChecksumFile,
        lastCommitTimestamp);
  }

  /////////////////////////
  // getLogSegment utils //
  /////////////////////////

  /**
   * Filters and concats (a) a list of published Deltas (from cloud LIST call), and (b) a list of
   * {@link ParsedLogData} injected by the {@link TableManager}, to return a new list of all Deltas
   * since the latest complete checkpoint, up to and including the target version to load.
   *
   * <ul>
   *   <li>Assumes that {@code allPublishedDeltas} is sorted and contiguous.
   *   <li>Assumes that {@code parsedLogDatas} is sorted and contiguous.
   *   <li>[delta-io/delta#4765] For now, only accepts parsedLogData of type {@link
   *       ParsedLogType#RATIFIED_STAGED_COMMIT}
   *   <li>If there is both a published Delta and a ratified staged commit for the same version,
   *       prioritizes the ratified staged commit
   * </ul>
   */
  private List<ParsedLogData> getAllDeltasAfterCheckpointWithCatalogPriority(
      List<ParsedLogData> allPublishedDeltas,
      List<ParsedLogData> parsedLogDatas,
      long latestCompleteCheckpointVersion,
      long versionToLoad) {
    final List<ParsedLogData> allPublishedDeltasAfterCheckpoint =
        allPublishedDeltas.stream()
            .filter(x -> x.type == ParsedLogType.PUBLISHED_DELTA)
            .filter(x -> latestCompleteCheckpointVersion < x.version && x.version <= versionToLoad)
            .collect(Collectors.toList());

    if (parsedLogDatas.isEmpty()) {
      return allPublishedDeltasAfterCheckpoint;
    }

    final List<ParsedLogData> allRatifiedCommitsAfterCheckpoint =
        parsedLogDatas.stream()
            .filter(x -> x.type == ParsedLogType.RATIFIED_STAGED_COMMIT)
            .filter(x -> latestCompleteCheckpointVersion < x.version && x.version <= versionToLoad)
            .collect(Collectors.toList());

    if (allRatifiedCommitsAfterCheckpoint.isEmpty()) {
      return allPublishedDeltasAfterCheckpoint;
    }

    if (allPublishedDeltasAfterCheckpoint.isEmpty()) {
      return allRatifiedCommitsAfterCheckpoint;
    }

    final long firstRatified = allRatifiedCommitsAfterCheckpoint.get(0).version;
    final long lastRatified = ListUtils.getLast(allRatifiedCommitsAfterCheckpoint).version;

    return Stream.of(
            allPublishedDeltasAfterCheckpoint.stream().filter(x -> x.version < firstRatified),
            allRatifiedCommitsAfterCheckpoint.stream(),
            allPublishedDeltasAfterCheckpoint.stream().filter(x -> x.version > lastRatified))
        .flatMap(Function.identity())
        .collect(Collectors.toList());
  }

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
            () -> new Checkpointer(logPath).readLastCheckpointFile(engine).map(x -> x.version));
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

  private void logDebugParsedLogDatas(String varName, List<ParsedLogData> logDatas) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}:\n  {}",
          varName,
          logDatas.stream().map(Object::toString).collect(Collectors.joining("\n  ")));
    }
  }
}
