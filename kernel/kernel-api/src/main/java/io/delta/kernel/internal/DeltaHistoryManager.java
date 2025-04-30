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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.fs.Path.getName;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeltaHistoryManager {

  private DeltaHistoryManager() {}

  private static final Logger logger = LoggerFactory.getLogger(DeltaHistoryManager.class);

  /**
   * Returns the latest commit that happened at or before {@code timestamp}.
   *
   * <p>If the timestamp is outside the range of [earliestCommit, latestCommit] then use parameters
   * {@code canReturnLastCommit} and {@code canReturnEarliestCommit} to control whether an exception
   * is thrown or the corresponding earliest/latest commit is returned.
   *
   * @param engine instance of {@link Engine} to use
   * @param logPath the _delta_log path of the table
   * @param timestamp the timestamp find the version for in milliseconds since the unix epoch
   * @param mustBeRecreatable whether the state at the returned commit should be recreatable
   * @param canReturnLastCommit whether we can return the latest version of the table if the
   *     provided timestamp is after the latest commit
   * @param canReturnEarliestCommit whether we can return the earliest version of the table if the
   *     provided timestamp is before the earliest commit
   * @throws KernelException if the provided timestamp is before the earliest commit and
   *     canReturnEarliestCommit is false
   * @throws KernelException if the provided timestamp is after the latest commit and
   *     canReturnLastCommit is false
   * @throws TableNotFoundException when there is no Delta table at the given path
   */
  public static Commit getActiveCommitAtTimestamp(
      Engine engine,
      SnapshotImpl latestSnapshot,
      Path logPath,
      long timestamp,
      boolean mustBeRecreatable,
      boolean canReturnLastCommit,
      boolean canReturnEarliestCommit)
      throws TableNotFoundException {

    long earliestVersion =
        (mustBeRecreatable)
            ? getEarliestRecreatableCommit(engine, logPath)
            : getEarliestDeltaFile(engine, logPath);

    Commit placeholderEarliestCommit = new Commit(earliestVersion, -1L /* timestamp */);
    Commit ictEnablementCommit = getICTEnablementCommit(latestSnapshot, placeholderEarliestCommit);
    Optional<Commit> result;
    if (ictEnablementCommit.getTimestamp() <= timestamp) {
      long latestSnapshotTimestamp = latestSnapshot.getTimestamp(engine);
      if (latestSnapshotTimestamp <= timestamp) {
        // We just proved we should use the latest snapshot
        result = Optional.of(new Commit(latestSnapshot.getVersion(), latestSnapshotTimestamp));
      } else {
        // start ICT search over [earliest available ICT version, latestVersion)
        boolean ictEnabledForEntireWindow = (ictEnablementCommit.version <= earliestVersion);
        Commit searchWindowLowerBoundCommit =
            ictEnabledForEntireWindow ? placeholderEarliestCommit : ictEnablementCommit;
        try {
          result =
              getActiveCommitAtTimeFromICTRange(
                  timestamp,
                  searchWindowLowerBoundCommit,
                  latestSnapshot.getVersion() + 1,
                  engine,
                  latestSnapshot.getLogPath(),
                  4 /* numChunks */);
        } catch (IOException e) {
          // TODO: proper error message.
          throw new RuntimeException(
              "There was an error while reading a historical commit while performing a timestamp"
                  + "based lookup. This can happen when the commit log is corrupted or when "
                  + "there is a parallel operation like metadata cleanup that is deleting "
                  + "commits. Please retry the query.",
              e);
        }
      }
    } else {
      // ICT was NOT enabled as-of the requested time
      if (ictEnablementCommit.version <= earliestVersion) {
        // We're searching for a non-ICT time but the non-ICT commits are all missing.
        // If `canReturnEarliestCommit` is `false`, we need the details of the
        // earliest commit to populate the TimestampEarlierThanCommitRetentionException
        // error correctly.
        // Else, when `canReturnEarliestCommit` is `true`, the earliest commit
        // is the desired result.
        // TODO: set the correct timestamp for the placeholderEarliestCommit.
        Optional<CommitInfo> commitInfoOpt =
            CommitInfo.getCommitInfoOpt(
                engine, latestSnapshot.getLogPath(), placeholderEarliestCommit.getVersion());
        long ict =
            CommitInfo.getRequiredInCommitTimestamp(
                commitInfoOpt, Long.toString(placeholderEarliestCommit.getVersion()), logPath);
        result = Optional.of(new Commit(placeholderEarliestCommit.getVersion(), ict));
      } else {
        // start non-ICT search over [earliestVersion, ictEnablementVersion)
        // Search for the commit
        List<Commit> commits = getCommits(engine, logPath, earliestVersion);
        Commit commit =
            lastCommitBeforeOrAtTimestamp(commits, timestamp)
                .orElse(
                    commits.get(0)); // This is only returned if canReturnEarliestCommit (see below)
        result = Optional.of(commit);
      }
    }

    if (!result.isPresent()) {
      // TODO: proper error message.
      throw new RuntimeException(String.format("No commit found at %s", logPath));
    }
    Commit commit = result.get();

    // If timestamp is before the earliest commit
    if (commit.timestamp > timestamp && !canReturnEarliestCommit) {
      throw DeltaErrors.timestampBeforeFirstAvailableCommit(
          logPath.getParent().toString(), /* use dataPath */
          timestamp,
          commit.timestamp,
          commit.version);
    }
    // If timestamp is after the last commit of the table
    if (commit.timestamp < timestamp && !canReturnLastCommit) {
      throw DeltaErrors.timestampAfterLatestCommit(
          logPath.getParent().toString(), /* use dataPath */
          timestamp,
          commit.timestamp,
          commit.version);
    }

    return commit;
  }

  private static Optional<Commit> getActiveCommitAtTimeFromICTRange(
      long timestamp,
      Commit startCommit,
      long endVersion,
      Engine engine,
      Path logPath,
      int numChunks)
      throws IOException {
    final StructType COMMITINFO_READ_SCHEMA =
            new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA);
    Commit curStartCommit = startCommit;
    long curEnd = endVersion;
    while (curStartCommit.version < curEnd) {
      long numVersionsInRange = curEnd - curStartCommit.version;
      long chunkSize = Math.max(numVersionsInRange / numChunks, 1);
      final long curStartVersion = curStartCommit.version;
      final long curEndForIterator = curEnd;
      CloseableIterator<FileStatus> filesToRead =
          new CloseableIterator<FileStatus>() {
            long curVersion = curStartVersion;

            @Override
            public boolean hasNext() {
              return curVersion + chunkSize < curEndForIterator;
            }

            @Override
            public FileStatus next() {
              curVersion += chunkSize;
              return FileStatus.of(FileNames.deltaFile(logPath, curVersion));
            }

            @Override
            public void close() throws IOException {
              // No-op
            }
          };

      CloseableIterator<ColumnarBatch> columnarBatchIter =
          engine
              .getJsonHandler()
              .readJsonFiles(filesToRead, COMMITINFO_READ_SCHEMA, Optional.empty() /* predicate */);
      Optional<Commit> knownTightestLowerBoundCommit = Optional.empty();
      long curVersion = curStartCommit.getVersion();
      while (columnarBatchIter.hasNext()) {
        final ColumnarBatch columnarBatch = columnarBatchIter.next();
        assert (columnarBatch.getSchema().equals(COMMITINFO_READ_SCHEMA));
        final ColumnVector commitInfoVector = columnarBatch.getColumnVector(0);
        Optional<CommitInfo> commitInfo = Optional.empty();
        for (int i = 0; i < commitInfoVector.getSize(); i++) {
          if (!commitInfoVector.isNullAt(i)) {
            commitInfo = Optional.ofNullable(CommitInfo.fromColumnVector(commitInfoVector, i));
            break;
          }
        }
        long ict =
            CommitInfo.getRequiredInCommitTimestamp(commitInfo, Long.toString(curVersion), logPath);
        if (ict > timestamp) {
          break;
        }
        knownTightestLowerBoundCommit = Optional.of(new Commit(curVersion, ict));
      }
      if (!knownTightestLowerBoundCommit.isPresent()) {
        // No commit found in this range, so we can stop searching
        break;
      }
      Commit nextStartCommit = knownTightestLowerBoundCommit.get();
      long nextEnd = Math.min(nextStartCommit.version + chunkSize, curEnd);
      if (nextStartCommit.version + 2 > nextEnd
          || knownTightestLowerBoundCommit.get().timestamp == timestamp) {
        return knownTightestLowerBoundCommit;
      }
      curStartCommit = nextStartCommit;
      curEnd = nextEnd;
    }
    return Optional.empty();
  }

  private static Commit getICTEnablementCommit(
      SnapshotImpl snapshot, Commit placeholderEarliestCommit) {
    Metadata metadata = snapshot.getMetadata();
    if (!IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      // Pretend ICT will be enabled after the latest version and requested timestamp.
      // This will force us to use the non-ICT search path.
      return new Commit(snapshot.getVersion() + 1, Long.MAX_VALUE);
    }
    Optional<Long> enablementTimestampOpt =
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(metadata);
    Optional<Long> enablementVersionOpt =
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(metadata);
    if (enablementTimestampOpt.isPresent() && enablementVersionOpt.isPresent()) {
      return new Commit(enablementVersionOpt.get(), enablementTimestampOpt.get());
    } else if (!enablementTimestampOpt.isPresent() && !enablementVersionOpt.isPresent()) {
      // This means that ICT has been enabled for the entire history.
      return placeholderEarliestCommit;
    } else {
      throw new IllegalStateException(
          "Both enablement version and timestamp should be present or absent together.");
    }
  }

  /**
   * Gets the earliest commit that we can recreate. Note that this version isn't guaranteed to exist
   * when performing an action as a concurrent operation can delete the file during cleanup. This
   * value must be used as a lower bound.
   *
   * <p>We search for the earliest checkpoint we have, or whether we have the 0th delta file. This
   * method assumes that the commits are contiguous.
   */
  public static long getEarliestRecreatableCommit(Engine engine, Path logPath)
      throws TableNotFoundException {
    try (CloseableIterator<FileStatus> files =
        listFrom(engine, logPath, 0)
            .filter(
                fs ->
                    FileNames.isCommitFile(getName(fs.getPath()))
                        || FileNames.isCheckpointFile(getName(fs.getPath())))) {

      if (!files.hasNext()) {
        // listFrom already throws an error if the directory is truly empty, thus this must
        // be because no files are checkpoint or delta files
        throw new RuntimeException(
            String.format("No delta files found in the directory: %s", logPath));
      }

      // A map of checkpoint version and number of parts to number of parts observed
      Map<Tuple2<Long, Integer>, Integer> checkpointMap = new HashMap<>();
      long smallestDeltaVersion = Long.MAX_VALUE;
      Optional<Long> lastCompleteCheckpoint = Optional.empty();

      // Iterate through the log files - this will be in order starting from the lowest
      // version. Checkpoint files come before deltas, so when we see a checkpoint, we
      // remember it and return it once we detect that we've seen a smaller or equal delta
      // version.
      while (files.hasNext()) {
        String nextFilePath = files.next().getPath();
        if (FileNames.isCommitFile(getName(nextFilePath))) {
          long version = FileNames.deltaVersion(nextFilePath);
          if (version == 0L) {
            return version;
          }
          smallestDeltaVersion = Math.min(version, smallestDeltaVersion);

          // Note that we also check this condition at the end of the function - we check
          // it here too to try and avoid more file listing when it's unnecessary.
          if (lastCompleteCheckpoint.isPresent()
              && lastCompleteCheckpoint.get() >= smallestDeltaVersion) {
            return lastCompleteCheckpoint.get();
          }
        } else if (FileNames.isCheckpointFile(nextFilePath)) {
          long checkpointVersion = FileNames.checkpointVersion(nextFilePath);
          CheckpointInstance checkpointInstance = new CheckpointInstance(nextFilePath);
          if (!checkpointInstance.numParts.isPresent()) {
            lastCompleteCheckpoint = Optional.of(checkpointVersion);
          } else {
            // if we have a multi-part checkpoint, we need to check that all parts exist
            int numParts = checkpointInstance.numParts.orElse(1);
            int preCount = checkpointMap.getOrDefault(new Tuple2<>(checkpointVersion, numParts), 0);
            if (numParts == preCount + 1) {
              lastCompleteCheckpoint = Optional.of(checkpointVersion);
            }
            checkpointMap.put(new Tuple2<>(checkpointVersion, numParts), preCount + 1);
          }
        }
      }

      if (lastCompleteCheckpoint.isPresent()
          && lastCompleteCheckpoint.get() >= smallestDeltaVersion) {
        return lastCompleteCheckpoint.get();
      } else if (smallestDeltaVersion < Long.MAX_VALUE) {
        // This is a corrupt table where 000.json does not exist and there are no complete
        // checkpoints OR the earliest complete checkpoint does not have a corresponding
        // commit file (but there are other later commit files present)
        throw new RuntimeException(String.format("No recreatable commits found at %s", logPath));
      } else {
        throw new RuntimeException(String.format("No commits found at %s", logPath));
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not close iterator", e);
    }
  }

  /**
   * Get the earliest commit available for this table. Note that this version isn't guaranteed to
   * exist when performing an action as a concurrent operation can delete the file during cleanup.
   * This value must be used as a lower bound.
   */
  public static long getEarliestDeltaFile(Engine engine, Path logPath)
      throws TableNotFoundException {

    try (CloseableIterator<FileStatus> files =
        listFrom(engine, logPath, 0).filter(fs -> FileNames.isCommitFile(getName(fs.getPath())))) {

      if (files.hasNext()) {
        return FileNames.deltaVersion(files.next().getPath());
      } else {
        // listFrom already throws an error if the directory is truly empty, thus this must
        // be because no files are delta files
        throw new RuntimeException(
            String.format("No delta files found in the directory: %s", logPath));
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not close iterator", e);
    }
  }

  /**
   * Returns an iterator containing a list of files found in the _delta_log directory starting with
   * {@code startVersion}. Throws a {@link TableNotFoundException} if the directory doesn't exist or
   * is empty.
   */
  private static CloseableIterator<FileStatus> listFrom(
      Engine engine, Path logPath, long startVersion) throws TableNotFoundException {
    Path tablePath = logPath.getParent();
    try {
      CloseableIterator<FileStatus> files =
          wrapEngineExceptionThrowsIO(
              () ->
                  engine
                      .getFileSystemClient()
                      .listFrom(FileNames.listingPrefix(logPath, startVersion)),
              "Listing files in the delta log starting from %s",
              FileNames.listingPrefix(logPath, startVersion));

      if (!files.hasNext()) {
        // We treat an empty directory as table not found
        throw new TableNotFoundException(tablePath.toString());
      }
      return files;
    } catch (FileNotFoundException e) {
      throw new TableNotFoundException(tablePath.toString());
    } catch (IOException io) {
      throw new UncheckedIOException("Failed to list the files in delta log", io);
    }
  }

  /**
   * Returns the commit version and timestamps of all commits starting from version {@code start}.
   * Guarantees that the commits returned have both monotonically increasing versions and
   * timestamps.
   */
  private static List<Commit> getCommits(Engine engine, Path logPath, long start)
      throws TableNotFoundException {
    CloseableIterator<Commit> commits =
        listFrom(engine, logPath, start)
            .filter(fs -> FileNames.isCommitFile(getName(fs.getPath())))
            .map(fs -> new Commit(FileNames.deltaVersion(fs.getPath()), fs.getModificationTime()));
    return monotonizeCommitTimestamps(commits);
  }

  /**
   * Makes sure that the commit timestamps are monotonically increasing with respect to commit
   * versions. Requires the input commits to be sorted by the commit version.
   */
  private static List<Commit> monotonizeCommitTimestamps(CloseableIterator<Commit> commits) {
    List<Commit> monotonizedCommits = new ArrayList<>();
    long prevTimestamp = Long.MIN_VALUE;
    long prevVersion = Long.MIN_VALUE;
    while (commits.hasNext()) {
      Commit newElem = commits.next();
      assert (prevVersion < newElem.version); // Verify commits are ordered
      if (prevTimestamp >= newElem.timestamp) {
        logger.warn(
            "Found Delta commit {} with a timestamp {} which is greater than the next "
                + "commit timestamp {}.",
            prevVersion,
            prevTimestamp,
            newElem.timestamp);
        newElem = new Commit(newElem.version, prevTimestamp + 1);
      }
      monotonizedCommits.add(newElem);
      prevTimestamp = newElem.timestamp;
      prevVersion = newElem.version;
    }
    return monotonizedCommits;
  }

  /** Returns the latest commit that happened at or before {@code timestamp} */
  private static Optional<Commit> lastCommitBeforeOrAtTimestamp(
      List<Commit> commits, long timestamp) {
    int i = -1;
    while (i + 1 < commits.size() && commits.get(i + 1).timestamp <= timestamp) {
      i++;
    }
    return Optional.ofNullable((i < 0) ? null : commits.get(i));
  }

  public static class Commit {

    private final long version;
    private final long timestamp;

    Commit(long version, long timestamp) {
      this.version = version;
      this.timestamp = timestamp;
    }

    public long getVersion() {
      return version;
    }

    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Commit other = (Commit) o;
      return Objects.equals(version, other.version) && Objects.equals(timestamp, other.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hash(version, timestamp);
    }
  }
}
