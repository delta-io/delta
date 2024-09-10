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
import static io.delta.kernel.internal.TableFeatures.validateWriteSupportedTable;
import static io.delta.kernel.internal.checkpoints.Checkpointer.findLastCompleteCheckpointBefore;
import static io.delta.kernel.internal.fs.Path.getName;
import static io.delta.kernel.internal.replay.LogReplayUtils.assertLogFilesBelongToTable;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.coordinatedcommits.Commit;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.*;
import io.delta.kernel.internal.checkpoints.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotManager {

  /**
   * The latest {@link SnapshotHint} for this table. The initial value inside the AtomicReference is
   * `null`.
   */
  private AtomicReference<SnapshotHint> latestSnapshotHint;

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
   * - Verify the versions are contiguous. - Verify the versions start with `expectedStartVersion`
   * if it's specified. - Verify the versions end with `expectedEndVersion` if it's specified.
   */
  public static void verifyDeltaVersions(
      List<Long> versions,
      Optional<Long> expectedStartVersion,
      Optional<Long> expectedEndVersion,
      Path tablePath) {
    if (!versions.isEmpty()) {
      List<Long> contVersions =
          LongStream.rangeClosed(versions.get(0), versions.get(versions.size() - 1))
              .boxed()
              .collect(Collectors.toList());
      if (!contVersions.equals(versions)) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format("Missing delta files: versions are not continuous: (%s)", versions));
      }
    }
    expectedStartVersion.ifPresent(
        v -> {
          checkArgument(
              !versions.isEmpty() && Objects.equals(versions.get(0), v),
              format("Did not get the first delta file version %s to compute Snapshot", v));
        });
    expectedEndVersion.ifPresent(
        v -> {
          checkArgument(
              !versions.isEmpty() && Objects.equals(versions.get(versions.size() - 1), v),
              format("Did not get the last delta file version %s to compute Snapshot", v));
        });
  }

  /**
   * Construct the latest snapshot for given table.
   *
   * @param engine Instance of {@link Engine} to use.
   * @return
   * @throws TableNotFoundException
   */
  public Snapshot buildLatestSnapshot(Engine engine) throws TableNotFoundException {
    return getSnapshotAtInit(engine);
  }

  /**
   * Construct the snapshot for the given table at the version provided.
   *
   * @param engine Instance of {@link Engine} to use.
   * @param version The snapshot version to construct
   * @return a {@link Snapshot} of the table at version {@code version}
   * @throws TableNotFoundException
   */
  public Snapshot getSnapshotAt(Engine engine, long version) throws TableNotFoundException {

    Optional<LogSegment> logSegmentOpt =
        getLogSegmentAtOrBeforeVersion(
            engine,
            Optional.empty(), /* startCheckpointOpt */
            Optional.of(version) /* versionToLoadOpt */,
            Optional.empty() /* tableCommitHandlerOpt */);

    // For non-coordinated commit table, the {@code getCoodinatedCommitsAwareSnapshot} will
    // create the snapshot with the {@code logSegmentOpt} built here and will not trigger other
    // operations. For coordinated commit table, the {@code getCoodinatedCommitsAwareSnapshot}
    // will create the snapshot with the {@code logSegmentOpt} built here and will build the
    // logSegment again by also fetching the unbackfilled commits from the commit coordinator.
    // With the unbackfilled commits plus the backfilled commits in Delta log, a new snapshot
    // will be created.
    SnapshotImpl snapshot =
        logSegmentOpt
            .map(
                logSegment ->
                    getCoordinatedCommitsAwareSnapshot(engine, logSegment, Optional.of(version)))
            .orElseThrow(() -> new TableNotFoundException(tablePath.toString()));
    long snapshotVer = snapshot.getVersion(engine);
    if (snapshotVer != version) {
      throw DeltaErrors.versionAfterLatestCommit(tablePath.toString(), version, snapshotVer);
    }
    return snapshot;
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
  public Snapshot getSnapshotForTimestamp(Engine engine, long millisSinceEpochUTC)
      throws TableNotFoundException {
    long startTimeMillis = System.currentTimeMillis();
    long versionToRead =
        DeltaHistoryManager.getActiveCommitAtTimestamp(
                engine,
                logPath,
                millisSinceEpochUTC,
                true /* mustBeRecreatable */,
                false /* canReturnLastCommit */,
                false /* canReturnEarliestCommit */)
            .getVersion();
    logger.info(
        "{}: Took {}ms to fetch version at timestamp {}",
        tablePath,
        System.currentTimeMillis() - startTimeMillis,
        millisSinceEpochUTC);

    return getSnapshotAt(engine, versionToRead);
  }

  public void checkpoint(Engine engine, long version) throws TableNotFoundException, IOException {
    logger.info("{}: Starting checkpoint for version: {}", tablePath, version);
    // Get the snapshot corresponding the version
    SnapshotImpl snapshot = (SnapshotImpl) getSnapshotAt(engine, version);

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
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

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

  /** Get an iterator of files in the _delta_log directory starting with the startVersion. */
  private CloseableIterator<FileStatus> listFrom(Engine engine, long startVersion)
      throws IOException {
    logger.debug("{}: startVersion: {}", tablePath, startVersion);
    return wrapEngineExceptionThrowsIO(
        () -> engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, startVersion)),
        "Listing from %s",
        FileNames.listingPrefix(logPath, startVersion));
  }

  /**
   * Returns true if the given file name is delta log files. Delta log files can be delta commit
   * file (e.g., 000000000.json), or checkpoint file. (e.g.,
   * 000000001.checkpoint.00001.00003.parquet)
   *
   * @param fileName Name of the file (not the full path)
   * @return Boolean Whether the file is delta log files
   */
  private boolean isDeltaCommitOrCheckpointFile(String fileName) {
    return FileNames.isCheckpointFile(fileName) || FileNames.isCommitFile(fileName);
  }

  /**
   * Returns an iterator containing a list of files found in the _delta_log directory starting with
   * the startVersion. Returns None if no files are found or the directory is missing.
   */
  private Optional<CloseableIterator<FileStatus>> listFromOrNone(Engine engine, long startVersion) {
    // LIST the directory, starting from the provided lower bound (treat missing dir as empty).
    // NOTE: "empty/missing" is _NOT_ equivalent to "contains no useful commit files."
    try {
      CloseableIterator<FileStatus> results = listFrom(engine, startVersion);
      if (results.hasNext()) {
        return Optional.of(results);
      } else {
        return Optional.empty();
      }
    } catch (FileNotFoundException e) {
      return Optional.empty();
    } catch (IOException io) {
      throw new UncheckedIOException("Failed to list the files in delta log", io);
    }
  }

  /**
   * Returns the delta files and checkpoint files starting from the given `startVersion`.
   * `versionToLoad` is an optional parameter to set the max bound. It's usually used to load a
   * table snapshot for a specific version. If no delta or checkpoint files exist below the
   * versionToLoad and at least one delta file exists, throws an exception that the state is not
   * reconstructable.
   *
   * @param startVersion the version to start. Inclusive.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   *     Must be >= startVersion if provided.
   *     <p>This method lists all delta files and checkpoint files with the following steps:
   *     <ul>
   *       <li>When the table is set up with a commit coordinator, retrieve any files that haven't
   *           been backfilled by initiating a request to the commit coordinator service. If the
   *           table is not configured to use a commit coordinator, this list will be empty.
   *       <li>Collect commit files (aka backfilled commits) and checkpoint files by listing the
   *           contents of the Delta log on storage.
   *       <li>Filter un-backfilled files to exclude overlapping delta files collected from both
   *           commit-coordinator and file-system to avoid duplicates.
   *       <li>Merge and return the backfilled files and filtered un-backfilled files.
   *     </ul>
   *     <p>*Note*: If table is a coordinated-commits table, the commit-coordinator client MUST be
   *     passed to correctly list the commits.
   * @param startVersion the version to start. Inclusive.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   *     Must be >= startVersion if provided.
   * @param tableCommitHandlerOpt the optional commit-coordinator client handler to use for fetching
   *     un-backfilled commits.
   * @return Some array of files found (possibly empty, if no usable commit files are present), or
   *     None if the listing returned no files at all.
   */
  protected final Optional<List<FileStatus>> listDeltaAndCheckpointFiles(
      Engine engine,
      long startVersion,
      Optional<Long> versionToLoad,
      Optional<TableCommitCoordinatorClientHandler> tableCommitHandlerOpt) {
    versionToLoad.ifPresent(
        v ->
            checkArgument(
                v >= startVersion,
                format("versionToLoad=%s provided is less than startVersion=%s", v, startVersion)));
    logger.debug(
        "startVersion: {}, versionToLoad: {}, coordinated commits enabled: {}",
        startVersion,
        versionToLoad,
        tableCommitHandlerOpt.isPresent());

    // Fetching the unbackfilled commits before doing the log directory listing to avoid a gap
    // in delta versions if some delta files are backfilled after the log directory listing but
    // before the unbackfilled commits listing
    List<Commit> unbackfilledCommits =
        getUnbackfilledCommits(tableCommitHandlerOpt, startVersion, versionToLoad);

    final AtomicLong maxDeltaVersionSeen = new AtomicLong(startVersion - 1);
    Optional<CloseableIterator<FileStatus>> listing = listFromOrNone(engine, startVersion);
    Optional<List<FileStatus>> resultFromFsListingOpt =
        listing.map(
            fileStatusesIter -> {
              final List<FileStatus> output = new ArrayList<>();

              while (fileStatusesIter.hasNext()) {
                final FileStatus fileStatus = fileStatusesIter.next();
                final String fileName = getName(fileStatus.getPath());

                // Pick up all checkpoint and delta files
                if (!isDeltaCommitOrCheckpointFile(fileName)) {
                  continue;
                }

                // Checkpoint files of 0 size are invalid but may be ignored silently when read,
                // hence we drop them so that we never pick up such checkpoints.
                if (FileNames.isCheckpointFile(fileName) && fileStatus.getSize() == 0) {
                  continue;
                }
                // Take files until the version we want to load
                final boolean versionWithinRange =
                    versionToLoad
                        .map(v -> FileNames.getFileVersion(new Path(fileStatus.getPath())) <= v)
                        .orElse(true);

                if (!versionWithinRange) {
                  // If we haven't taken any files yet and the first file we see is greater
                  // than the versionToLoad then the versionToLoad is not reconstructable
                  // from the existing logs
                  if (output.isEmpty()) {
                    long earliestVersion =
                        DeltaHistoryManager.getEarliestRecreatableCommit(engine, logPath);
                    throw DeltaErrors.versionBeforeFirstAvailableCommit(
                        tablePath.toString(), versionToLoad.get(), earliestVersion);
                  }
                  break;
                }

                // Ideally listFromOrNone should return lexiographically sorted
                // files and so maxDeltaVersionSeen should be equal to fileVersion.
                // But we are being defensive here and taking max of all the
                // fileVersions seen.
                if (FileNames.isCommitFile(fileName)) {
                  maxDeltaVersionSeen.set(
                      Math.max(
                          maxDeltaVersionSeen.get(), FileNames.deltaVersion(fileStatus.getPath())));
                }
                output.add(fileStatus);
              }

              return output;
            });

    if (!tableCommitHandlerOpt.isPresent()) {
      return resultFromFsListingOpt;
    }
    List<FileStatus> relevantUnbackfilledCommits =
        unbackfilledCommits.stream()
            .filter((commit) -> commit.getVersion() > maxDeltaVersionSeen.get())
            .filter(
                (commit) ->
                    !versionToLoad.isPresent() || commit.getVersion() <= versionToLoad.get())
            .map(Commit::getFileStatus)
            .collect(Collectors.toList());

    return resultFromFsListingOpt.map(
        fsListing -> {
          fsListing.addAll(relevantUnbackfilledCommits);
          return fsListing;
        });
  }

  private List<Commit> getUnbackfilledCommits(
      Optional<TableCommitCoordinatorClientHandler> tableCommitHandlerOpt,
      long startVersion,
      Optional<Long> versionToLoad) {
    try {
      return tableCommitHandlerOpt
          .map(
              commitCoordinatorClientHandler -> {
                logger.info(
                    "Getting un-backfilled commits from commit coordinator for " + "table: {}",
                    tablePath);
                return commitCoordinatorClientHandler
                    .getCommits(startVersion, versionToLoad.orElse(null))
                    .getCommits();
              })
          .orElse(Collections.emptyList());
    } catch (Exception e) {
      logger.error(
          "Failed to get unbackfilled commits of table {} with commit coordinator: {}",
          tablePath,
          e);
      throw e;
    }
  }

  /**
   * Load the Snapshot for this Delta table at initialization. This method uses the `lastCheckpoint`
   * file as a hint on where to start listing the transaction log directory.
   */
  private SnapshotImpl getSnapshotAtInit(Engine engine) throws TableNotFoundException {
    Checkpointer checkpointer = new Checkpointer(logPath);
    Optional<CheckpointMetaData> lastCheckpointOpt = checkpointer.readLastCheckpointFile(engine);
    if (!lastCheckpointOpt.isPresent()) {
      logger.warn(
          "{}: Last checkpoint file is missing or corrupted. "
              + "Will search for the checkpoint files directly.",
          tablePath);
    }
    Optional<LogSegment> logSegmentOpt = getLogSegmentFrom(engine, lastCheckpointOpt);

    return logSegmentOpt
        .map(logSegment -> getCoordinatedCommitsAwareSnapshot(engine, logSegment, Optional.empty()))
        .orElseThrow(() -> new TableNotFoundException(tablePath.toString()));
  }

  /**
   * This can be optimized by making snapshot hint optimization to work with coordinated commits.
   *
   * @see <a href="https://github.com/delta-io/delta/issues/3437">issue #3437</a>.
   */
  private SnapshotImpl getCoordinatedCommitsAwareSnapshot(
      Engine engine, LogSegment initialSegmentForNewSnapshot, Optional<Long> versionToLoadOpt) {
    SnapshotImpl newSnapshot = createSnapshot(initialSegmentForNewSnapshot, engine);

    if (versionToLoadOpt.isPresent() && newSnapshot.getVersion(engine) == versionToLoadOpt.get()) {
      return newSnapshot;
    }

    Optional<TableCommitCoordinatorClientHandler> newTableCommitCoordinatorClientHandlerOpt =
        newSnapshot.getTableCommitCoordinatorClientHandlerOpt(engine);

    if (newTableCommitCoordinatorClientHandlerOpt.isPresent()) {
      Optional<LogSegment> segmentOpt =
          getLogSegmentAtOrBeforeVersion(
              engine,
              newSnapshot.getLogSegment().checkpointVersionOpt, /* startCheckpointOpt */
              versionToLoadOpt /* versionToLoadOpt */,
              newTableCommitCoordinatorClientHandlerOpt /* tableCommitHandlerOpt */);
      newSnapshot =
          segmentOpt
              .map(segment -> createSnapshot(segment, engine))
              .orElseThrow(() -> new TableNotFoundException(tablePath.toString()));
    }
    return newSnapshot;
  }

  private SnapshotImpl createSnapshot(LogSegment initSegment, Engine engine) {
    final String startingFromStr =
        initSegment
            .checkpointVersionOpt
            .map(v -> format("starting from checkpoint version %s.", v))
            .orElse(".");
    logger.info("{}: Loading version {} {}", tablePath, initSegment.version, startingFromStr);

    LogReplay logReplay =
        new LogReplay(
            logPath,
            tablePath,
            initSegment.version,
            engine,
            initSegment,
            Optional.ofNullable(latestSnapshotHint.get()));

    long startTimeMillis = System.currentTimeMillis();

    assertLogFilesBelongToTable(logPath, initSegment.allLogFilesUnsorted());

    final SnapshotImpl snapshot =
        new SnapshotImpl(
            tablePath, initSegment, logReplay, logReplay.getProtocol(), logReplay.getMetadata());

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
   * Get the LogSegment that will help in computing the Snapshot of the table at DeltaLog
   * initialization, or None if the directory was empty/missing.
   *
   * @param startingCheckpoint A checkpoint that we can start our listing from
   */
  private Optional<LogSegment> getLogSegmentFrom(
      Engine engine, Optional<CheckpointMetaData> startingCheckpoint) {
    return getLogSegmentAtOrBeforeVersion(
        engine, startingCheckpoint.map(x -> x.version), Optional.empty(), Optional.empty());
  }

  /**
   * Get a list of files that can be used to compute a Snapshot at or before version
   * `versionToLoad`, If `versionToLoad` is not provided, will generate the list of files that are
   * needed to load the latest version of the Delta table. This method also performs checks to
   * ensure that the delta files are contiguous.
   *
   * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
   *     typically that of a known checkpoint. If this version's not provided, we will start listing
   *     from version 0.
   * @param versionToLoad A specific version we try to load, but we may only load a version before
   *     this version if this version of commit is un-backfilled. Typically used with time travel
   *     and the Delta streaming source. If not provided, we will try to load the latest version of
   *     the table.
   * @return Some LogSegment to build a Snapshot if files do exist after the given startCheckpoint.
   *     None, if the delta log directory was missing or empty.
   */
  protected Optional<LogSegment> getLogSegmentAtOrBeforeVersion(
      Engine engine,
      Optional<Long> startCheckpoint,
      Optional<Long> versionToLoad,
      Optional<TableCommitCoordinatorClientHandler> tableCommitHandlerOpt) {
    // Only use startCheckpoint if it is <= versionToLoad
    Optional<Long> startCheckpointToUse =
        startCheckpoint.filter(v -> !versionToLoad.isPresent() || v <= versionToLoad.get());

    // if we are loading a specific version and there is no usable starting checkpoint
    // try to load a checkpoint that is <= version to load
    if (!startCheckpointToUse.isPresent() && versionToLoad.isPresent()) {
      long beforeVersion = versionToLoad.get() + 1;
      long startTimeMillis = System.currentTimeMillis();
      startCheckpointToUse =
          findLastCompleteCheckpointBefore(engine, logPath, beforeVersion).map(x -> x.version);

      logger.info(
          "{}: Took {}ms to load last checkpoint before version {}",
          tablePath,
          System.currentTimeMillis() - startTimeMillis,
          beforeVersion);
    }

    long startVersion =
        startCheckpointToUse.orElseGet(
            () -> {
              logger.warn(
                  "{}: Starting checkpoint is missing. Listing from version as 0", tablePath);
              return 0L;
            });

    long startTimeMillis = System.currentTimeMillis();
    final Optional<List<FileStatus>> newFiles =
        listDeltaAndCheckpointFiles(engine, startVersion, versionToLoad, tableCommitHandlerOpt);
    logger.info(
        "{}: Took {}ms to list the files after starting checkpoint",
        tablePath,
        System.currentTimeMillis() - startTimeMillis);

    startTimeMillis = System.currentTimeMillis();
    try {
      return getLogSegmentAtOrBeforeVersion(
          engine, startCheckpointToUse, versionToLoad, newFiles, tableCommitHandlerOpt);
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
  protected Optional<LogSegment> getLogSegmentAtOrBeforeVersion(
      Engine engine,
      Optional<Long> startCheckpointOpt,
      Optional<Long> versionToLoadOpt,
      Optional<List<FileStatus>> filesOpt,
      Optional<TableCommitCoordinatorClientHandler> tableCommitHandlerOpt) {
    final List<FileStatus> newFiles;
    if (filesOpt.isPresent()) {
      newFiles = filesOpt.get();
    } else {
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
    logDebug(
        () ->
            format(
                "newFiles: %s",
                Arrays.toString(
                    newFiles.stream().map(x -> new Path(x.getPath()).getName()).toArray())));

    if (newFiles.isEmpty() && !startCheckpointOpt.isPresent()) {
      // We can't construct a snapshot because the directory contained no usable commit
      // files... but we can't return Optional.empty either, because it was not truly empty.
      throw new RuntimeException(format("No delta files found in the directory: %s", logPath));
    } else if (newFiles.isEmpty()) {
      // The directory may be deleted and recreated and we may have stale state in our
      // DeltaLog singleton, so try listing from the first version
      return getLogSegmentAtOrBeforeVersion(
          engine, Optional.empty(), versionToLoadOpt, tableCommitHandlerOpt);
    }

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

    final LinkedList<Long> deltaVersionsAfterCheckpoint =
        deltasAfterCheckpoint.stream()
            .map(fileStatus -> FileNames.deltaVersion(new Path(fileStatus.getPath())))
            .collect(Collectors.toCollection(LinkedList::new));

    logDebug(
        () -> format("deltaVersions: %s", Arrays.toString(deltaVersionsAfterCheckpoint.toArray())));

    final long newVersion =
        deltaVersionsAfterCheckpoint.isEmpty()
            ? newCheckpointOpt.get().version
            : deltaVersionsAfterCheckpoint.getLast();

    // There should be a delta file present for the newVersion that we are loading
    // (Even if `deltasAfterCheckpoint` is empty, `deltas` should not be)
    if (deltas.isEmpty()
        || FileNames.deltaVersion(deltas.get(deltas.size() - 1).getPath()) < newVersion) {
      throw new InvalidTableException(
          tablePath.toString(), String.format("Missing delta file for version %s", newVersion));
    }

    versionToLoadOpt
        .filter(v -> v < newVersion)
        .ifPresent(
            v -> {
              throw DeltaErrors.versionAfterLatestCommit(tablePath.toString(), v, newVersion);
            });

    // We may just be getting a checkpoint file after the filtering
    if (!deltaVersionsAfterCheckpoint.isEmpty()) {
      // If we have deltas after the checkpoint, the first file should be 1 greater than our
      // last checkpoint version. If no checkpoint is present, this means the first delta file
      // should be version 0.
      if (deltaVersionsAfterCheckpoint.getFirst() != newCheckpointVersion + 1) {
        throw new InvalidTableException(
            tablePath.toString(),
            String.format(
                "Unable to reconstruct table state: missing log file for version %s",
                newCheckpointVersion + 1));
      }
      logger.info(
          "Verified delta files are contiguous from version {} to {}",
          newCheckpointVersion + 1,
          newVersion);
      verifyDeltaVersions(
          deltaVersionsAfterCheckpoint,
          Optional.of(newCheckpointVersion + 1),
          Optional.of(newVersion),
          tablePath);
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

    return Optional.of(
        new LogSegment(
            logPath,
            newVersion,
            deltasAfterCheckpoint,
            newCheckpointFiles,
            newCheckpointOpt.map(x -> x.version),
            lastCommitTimestamp));
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
