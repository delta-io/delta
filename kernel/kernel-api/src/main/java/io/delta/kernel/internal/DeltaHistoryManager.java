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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.InCommitTimestampUtils;
import io.delta.kernel.internal.util.Tuple2;
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
    Commit searchResult;
    if (ictEnablementCommit.getTimestamp() <= timestamp) {
      // The target commit is in the ICT range.
      long latestSnapshotTimestamp = latestSnapshot.getTimestamp(engine);
      if (latestSnapshotTimestamp <= timestamp) {
        // We just proved we should use the latest snapshot
        // Note that if `latestSnapshotTimestamp` is less than `timestamp`, we only
        // return this search result if `canReturnLastCommit` is true.
        // If `canReturnLastCommit` is false, we still need this commit to
        // throw the timestampAfterLatestCommit error.
        searchResult = new Commit(latestSnapshot.getVersion(), latestSnapshotTimestamp);
      } else {
        // start ICT search over [earliest available ICT version, latestVersion)
        boolean ictEnabledForEntireWindow = (ictEnablementCommit.version <= earliestVersion);
        long searchWindowLowerBound =
            ictEnabledForEntireWindow
                ? placeholderEarliestCommit.getVersion()
                : ictEnablementCommit.getVersion();
        try {
          searchResult =
              getActiveCommitAtTimeFromICTRange(
                  timestamp,
                  searchWindowLowerBound,
                  latestSnapshot.getVersion(),
                  engine,
                  latestSnapshot.getLogPath());
        } catch (IOException e) {
          throw new RuntimeException(
              "There was an error while reading a historical commit while performing a timestamp-"
                  + "based lookup. This usually happens when there is a parallel operation like "
                  + "metadata cleanup that is deleting commits. Please retry the query.",
              e);
        }
      }
    } else {
      // ICT was NOT enabled as-of the requested time
      if (ictEnablementCommit.version <= earliestVersion) {
        // We're searching for a non-ICT time but the non-ICT commits are all missing.
        // If `canReturnEarliestCommit` is `false`, we need the details of the
        // earliest commit to populate the timestampBeforeFirstAvailableCommit
        // error correctly.
        // Else, when `canReturnEarliestCommit` is `true`, the earliest commit
        // is the desired result.
        long ict =
            CommitInfo.getRequiredInCommitTimestampFromFile(
                engine, logPath, placeholderEarliestCommit.getVersion());
        searchResult = new Commit(placeholderEarliestCommit.getVersion(), ict);
      } else {
        // start non-ICT linear search over [earliestVersion, )
        List<Commit> commits = getCommits(engine, logPath, earliestVersion);
        searchResult =
            lastCommitBeforeOrAtTimestamp(commits, timestamp)
                .orElse(
                    commits.get(0)); // This is only returned if canReturnEarliestCommit (see below)
      }
    }

    // If timestamp is before the earliest commit
    if (searchResult.timestamp > timestamp && !canReturnEarliestCommit) {
      throw DeltaErrors.timestampBeforeFirstAvailableCommit(
          logPath.getParent().toString(), /* use dataPath */
          timestamp,
          searchResult.timestamp,
          searchResult.version);
    }
    // If timestamp is after the last commit of the table
    if (searchResult.version == latestSnapshot.getVersion()
        && searchResult.timestamp < timestamp
        && !canReturnLastCommit) {
      throw DeltaErrors.timestampAfterLatestCommit(
          logPath.getParent().toString(), /* use dataPath */
          timestamp,
          searchResult.timestamp,
          searchResult.version);
    }

    return searchResult;
  }

  /**
   * Finds the commit with the latest in-commit timestamp that is less than or equal to the
   * searchTimestamp. All commits from `startCommitVersionInclusive` till
   * `endCommitVersionInclusive` must have ICT enabled. Also, this method assumes that we have
   * already proven that `searchTimestamp` is in the given range.
   */
  private static Commit getActiveCommitAtTimeFromICTRange(
      long searchTimestamp,
      long startCommitVersionInclusive,
      long endCommitVersionInclusive,
      Engine engine,
      Path logPath)
      throws IOException {
    // Now we have a range of commits to search through. We can use binary search to find the
    // commit that is closest to the search timestamp.
    Optional<Tuple2<Long, Long>> greatestLowerBoundOpt =
        InCommitTimestampUtils.greatestLowerBound(
            searchTimestamp,
            startCommitVersionInclusive,
            endCommitVersionInclusive,
            version -> CommitInfo.getRequiredInCommitTimestampFromFile(engine, logPath, version));
    // This indicates that the search timestamp is less than the earliest commit.
    if (!greatestLowerBoundOpt.isPresent()) {
      long startIct =
          CommitInfo.getRequiredInCommitTimestampFromFile(
              engine, logPath, startCommitVersionInclusive);
      return new Commit(startCommitVersionInclusive, startIct);
    }
    Tuple2<Long, Long> greatestLowerBound = greatestLowerBoundOpt.get();
    return new Commit(greatestLowerBound._1, greatestLowerBound._2);
  }

  /**
   * Gets the commit that enabled in-commit timestamps.
   *
   * @param snapshot The latest snapshot of the table. This is used to determine when in-commit
   *     timestamps were enabled.
   * @param earliestCommit The earliest commit under consideration. If in-commit timestamps were
   *     enabled for the entire history, this function will return this commit.
   * @return The commit that enabled in-commit timestamps. If the table does not have in-commit
   *     timestamps enabled, this will be the commit after the latest version. If in-commit
   *     timestamps were enabled for the entire history, this will be `earliestCommit`.
   */
  private static Commit getICTEnablementCommit(SnapshotImpl snapshot, Commit earliestCommit) {
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
      return earliestCommit;
    } else {
      throw new IllegalStateException(
          String.format(
              "Both %s and %s should be present or absent together"
                  + "when inCommitTimestamp is enabled.",
              IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey(),
              IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey()));
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
