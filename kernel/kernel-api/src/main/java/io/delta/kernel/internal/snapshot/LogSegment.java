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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSegment {

  //////////////////////////////////
  // Static variables and methods //
  //////////////////////////////////

  public static LogSegment empty(Path logPath) {
    return new LogSegment(
        logPath,
        -1,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        -1);
  }

  private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

  //////////////////////////////////
  // Member variables and methods //
  //////////////////////////////////

  private final Path logPath;
  private final long version;
  private final List<FileStatus> deltas;
  private final List<FileStatus> compactions;
  private final List<FileStatus> checkpoints;
  private final Optional<Long> checkpointVersionOpt;
  private final Optional<FileStatus> lastSeenChecksum;
  private final long lastCommitTimestamp;
  private final List<FileStatus> deltasAndCheckpoints;
  private final Lazy<List<FileStatus>> deltasAndCheckpointsReversed;
  private final Lazy<List<FileStatus>> compactionsReversed;
  private final Lazy<List<FileStatus>> deltasCheckpointsCompactionsReversed;

  /**
   * Provides information around which files in the transaction log need to be read to create the
   * given version of the log.
   *
   * <p>This constructor validates and guarantees that:
   *
   * <ul>
   *   <li>All deltas are valid deltas files
   *   <li>All checkpoints are valid checkpoint files
   *   <li>All checkpoint files have the same version
   *   <li>All deltas are contiguous and range from {@link #checkpointVersionOpt} to version
   *   <li>If no deltas are present then {@link #checkpointVersionOpt} is equal to version
   * </ul>
   *
   * <p>Notably, this constructor does not guarantee that this LogSegment is complete and fully
   * describes a Snapshot version. You may use the {@link #isComplete()} method to check this.
   *
   * @param logPath The path to the _delta_log directory
   * @param version The Snapshot version to generate
   * @param deltas The delta commit files (.json) to read
   * @param compactions Any found log compactions files that can be used in place of some or all of
   *     the deltas
   * @param checkpoints The checkpoint file(s) to read
   * @param lastCommitTimestamp The "unadjusted" timestamp of the last commit within this segment.
   *     By unadjusted, we mean that the commit timestamps may not necessarily be monotonically
   *     increasing for the commits within this segment.
   */
  public LogSegment(
      Path logPath,
      long version,
      List<FileStatus> deltas,
      List<FileStatus> compactions,
      List<FileStatus> checkpoints,
      Optional<FileStatus> lastSeenChecksum,
      long lastCommitTimestamp) {

    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(logPath, "logPath is null");
    requireNonNull(deltas, "deltas is null");
    requireNonNull(compactions, "compactions is null");
    requireNonNull(checkpoints, "checkpoints is null");
    requireNonNull(lastSeenChecksum, "lastSeenChecksum null");

    validateDeltasAreDeltas(deltas);
    validateCompactionsAreCompactions(compactions);
    validateCheckpointsAreCheckpoints(checkpoints);
    validateIndividualCompactionVersions(compactions);

    this.checkpointVersionOpt =
        checkpoints.isEmpty()
            ? Optional.empty()
            : Optional.of(FileNames.checkpointVersion(new Path(checkpoints.get(0).getPath())));

    validateCheckpointVersionsAreSame(checkpoints, checkpointVersionOpt);
    validateLastSeenChecksumWithinLogSegmentStartEndVersionRange(
        lastSeenChecksum, version, checkpointVersionOpt);

    if (version != -1) {
      checkArgument(!deltas.isEmpty() || !checkpoints.isEmpty(), "No files to read");

      if (!deltas.isEmpty()) {
        final List<Long> deltaVersions =
            deltas.stream()
                .map(fs -> FileNames.deltaVersion(new Path(fs.getPath())))
                .collect(Collectors.toList());

        validateFirstDeltaVersionIsCheckpointVersionPlusOne(deltaVersions, checkpointVersionOpt);
        validateLastDeltaVersionIsLogSegmentVersion(deltaVersions, version);
        validateDeltaVersionsAreContiguous(deltaVersions);
        validateCompactionVersionsAreInRange(compactions, version, checkpointVersionOpt);
      } else {
        validateCheckpointVersionEqualsLogSegmentVersion(checkpointVersionOpt, version);
      }
    } else {
      checkArgument(deltas.isEmpty() && checkpoints.isEmpty(), "Version -1 should have no files");
    }

    // Make sure input delta commits (JSON file), checkpoints and log compactions are valid.
    assertLogFilesBelongToTable(
        logPath,
        Stream.concat(checkpoints.stream(), Stream.concat(deltas.stream(), compactions.stream()))
            .collect(Collectors.toList()));

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.logPath = logPath;
    this.version = version;
    this.deltas = deltas;
    this.compactions = compactions;
    this.checkpoints = checkpoints;
    this.lastSeenChecksum = lastSeenChecksum;
    this.lastCommitTimestamp = lastCommitTimestamp;
    this.deltasAndCheckpoints =
        Stream.concat(checkpoints.stream(), deltas.stream()).collect(Collectors.toList());

    this.deltasAndCheckpointsReversed = lazyLoadDeltasAndCheckpointsReversed(deltasAndCheckpoints);

    // We sort by the end version. since we work backward through the list, so this is the same as
    // lexicographic, except when a compaction has a bigger range, which makes it "better", so we
    // prefer it
    this.compactionsReversed = lazyLoadCompactionsReversed(compactions);

    this.deltasCheckpointsCompactionsReversed =
        lazyLoadDeltasCheckpointsCompactionsReversed(
            deltasAndCheckpointsReversed, compactionsReversed, compactions);

    logger.debug("Created LogSegment: {}", this);
  }

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * @return true if this LogSegment is complete and fully describes a Snapshot version. A partial
   *     LogSegment is missing some information. We consider an empty LogSegment to be incomplete.
   */
  public boolean isComplete() {
    // A LogSegment is complete if and only if either
    // (a) It has a checkpoint and has delta versions from checkpointVersion + 1 to version, or
    // (b) It has no checkpoint and has deltas from 0 to version
    //
    // Because we have already done extensive validation in the constructor, all that that remains
    // to check is whether (1) We have a checkpoint, or (2) We have N + 1 deltas. All other
    // requirements are taken care of.
    return version >= 0 && (!checkpoints.isEmpty() || (deltas.size() == version + 1));
  }

  public Path getLogPath() {
    return logPath;
  }

  public long getVersion() {
    return version;
  }

  public List<FileStatus> getDeltas() {
    return deltas;
  }

  public List<FileStatus> getCompactions() {
    return compactions;
  }

  public List<FileStatus> getCheckpoints() {
    return checkpoints;
  }

  public Optional<Long> getCheckpointVersionOpt() {
    return checkpointVersionOpt;
  }

  /**
   * Returns the most recent checksum file encountered during log directory listing, if available.
   *
   * <p>Note: This checksum file's version is guaranteed to:
   *
   * <ul>
   *   <li>Be less than or equal to the LogSegment version (enforced by constructor)
   *   <li>Be greater than or equal to the checkpoint version if a checkpoint exists (filtered
   *       during initialization)
   * </ul>
   *
   * @return Optional containing the most recent valid checksum file encountered, or empty if none
   *     found
   */
  public Optional<FileStatus> getLastSeenChecksum() {
    return lastSeenChecksum;
  }

  public long getLastCommitTimestamp() {
    return lastCommitTimestamp;
  }

  /**
   * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment, with
   *     no ordering guarantees.
   */
  public List<FileStatus> allLogFilesUnsorted() {
    return deltasAndCheckpoints;
  }

  /**
   * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment,
   *     sorted in reverse (00012.json, 00011.json, 00010.checkpoint.parquet) order.
   */
  public List<FileStatus> allLogFilesReversed() {
    return deltasAndCheckpointsReversed.get();
  }

  /**
   * @return all files sorted in reverse order in this log segment, but omitting the deltas (.json)
   *     files that are covered by log compaction files. This will include deltas (xxx.json) that
   *     are not covered by a log compaction, compaction files (xxx.xxx.json), and checkpoints
   *     (.checkpoint.parquet).
   */
  public List<FileStatus> allFilesWithCompactionsReversed() {
    return deltasCheckpointsCompactionsReversed.get();
  }

  @Override
  public String toString() {
    return String.format(
        "LogSegment {\n"
            + "  logPath='%s',\n"
            + "  version=%d,\n"
            + "  deltas=[%s\n  ],\n"
            + "  checkpoints=[%s\n  ],\n"
            + "  lastSeenChecksum=%s,\n"
            + "  checkpointVersion=%s,\n"
            + "  lastCommitTimestamp=%d\n"
            + "}",
        logPath,
        version,
        formatList(deltas),
        formatList(checkpoints),
        lastSeenChecksum.map(FileStatus::toString).orElse("None"),
        checkpointVersionOpt.map(String::valueOf).orElse("None"),
        lastCommitTimestamp);
  }

  @Override
  public int hashCode() {
    // TODO: support staged commits #4927
    return Objects.hash(deltas, checkpoints, compactions);
  }

  //////////////////////////////
  // Input validation methods //
  //////////////////////////////

  private void validateDeltasAreDeltas(List<FileStatus> deltas) {
    checkArgument(
        deltas.stream().allMatch(fs -> FileNames.isCommitFile(fs.getPath())),
        "deltas must all be actual delta (commit) files");
  }

  private void validateCompactionsAreCompactions(List<FileStatus> compactions) {
    checkArgument(
        compactions.stream().allMatch(fs -> FileNames.isLogCompactionFile(fs.getPath())),
        "compactions must all be actual log compaction files");
  }

  private void validateCheckpointsAreCheckpoints(List<FileStatus> checkpoints) {
    checkArgument(
        checkpoints.stream().allMatch(fs -> FileNames.isCheckpointFile(fs.getPath())),
        "checkpoints must all be actual checkpoint files");
  }

  private void validateIndividualCompactionVersions(List<FileStatus> compactions) {
    checkArgument(
        compactions.stream()
            .allMatch(
                fs -> {
                  Tuple2<Long, Long> versions = FileNames.logCompactionVersions(fs.getPath());
                  return versions._1 < versions._2;
                }),
        "compactions must have start version less than end version");
  }

  private void validateCheckpointVersionsAreSame(
      List<FileStatus> checkpoints, Optional<Long> checkpointVersionOpt) {
    if (!checkpoints.isEmpty()) {
      checkArgument(
          checkpoints.stream()
              .map(fs -> FileNames.checkpointVersion(new Path(fs.getPath())))
              .allMatch(v -> checkpointVersionOpt.get().equals(v)),
          "All checkpoint files must have the same version");
    }
  }

  private void validateLastSeenChecksumWithinLogSegmentStartEndVersionRange(
      Optional<FileStatus> lastSeenChecksum, long version, Optional<Long> checkpointVersionOpt) {
    lastSeenChecksum.ifPresent(
        checksumFile -> {
          long checksumVersion = FileNames.checksumVersion(new Path(checksumFile.getPath()));
          checkArgument(
              checksumVersion <= version,
              "checksum file's version should be less than or equal to logSegment's version");
          checkpointVersionOpt.ifPresent(
              checkpointVersion ->
                  checkArgument(
                      checksumVersion >= checkpointVersion,
                      "checksum file's version %s should be greater than or equal to "
                          + "checkpoint version %s",
                      checksumVersion,
                      checkpointVersion));
        });
  }

  private void validateFirstDeltaVersionIsCheckpointVersionPlusOne(
      List<Long> deltaVersions, Optional<Long> checkpointVersionOpt) {
    checkpointVersionOpt.ifPresent(
        checkpointVersion -> {
          checkArgument(
              deltaVersions.get(0) == checkpointVersion + 1,
              "First delta file version must equal checkpointVersion + 1");
        });
  }

  private void validateLastDeltaVersionIsLogSegmentVersion(List<Long> deltaVersions, long version) {
    checkArgument(
        ListUtils.getLast(deltaVersions) == version,
        "Last delta file version must equal the version of this LogSegment");
  }

  private void validateDeltaVersionsAreContiguous(List<Long> deltaVersions) {
    for (int i = 1; i < deltaVersions.size(); i++) {
      if (deltaVersions.get(i) != deltaVersions.get(i - 1) + 1) {
        throw new IllegalArgumentException(
            String.format("Delta versions must be contiguous: %s", deltaVersions));
      }
    }
  }

  private void validateCompactionVersionsAreInRange(
      List<FileStatus> compactions, long version, Optional<Long> checkpointVersionOpt) {
    checkArgument(
        compactions.stream()
            .allMatch(
                fs -> {
                  Tuple2<Long, Long> versions = FileNames.logCompactionVersions(fs.getPath());
                  boolean checkpointVersionOkay =
                      checkpointVersionOpt
                          .map(checkpointVersion -> versions._1 > checkpointVersion)
                          .orElse(true);
                  return checkpointVersionOkay && versions._2 <= version;
                }),
        "compactions must have start version > checkpointVersion AND end version <= version");
  }

  private void validateCheckpointVersionEqualsLogSegmentVersion(
      Optional<Long> checkpointVersionOpt, long version) {
    checkpointVersionOpt.ifPresent(
        checkpointVersion -> {
          checkArgument(
              checkpointVersion == version,
              "If there are no deltas, then checkpointVersion must equal the version "
                  + "of this LogSegment");
        });
  }

  //////////////////////////
  // Other helper methods //
  //////////////////////////

  private Lazy<List<FileStatus>> lazyLoadDeltasAndCheckpointsReversed(
      List<FileStatus> deltasAndCheckpoints) {
    return new Lazy<>(
        () ->
            deltasAndCheckpoints.stream()
                .sorted(
                    Comparator.comparing((FileStatus a) -> new Path(a.getPath()).getName())
                        .reversed())
                .collect(Collectors.toList()));
  }

  private Lazy<List<FileStatus>> lazyLoadCompactionsReversed(List<FileStatus> compactions) {
    return new Lazy<>(
        () ->
            compactions.stream()
                .sorted(
                    Comparator.comparing(
                            (FileStatus a) -> FileNames.logCompactionVersions(a.getPath())._2)
                        .reversed())
                .collect(Collectors.toList()));
  }

  private Lazy<List<FileStatus>> lazyLoadDeltasCheckpointsCompactionsReversed(
      Lazy<List<FileStatus>> deltasAndCheckpointsReversed,
      Lazy<List<FileStatus>> compactionsReversed,
      List<FileStatus> compactions) {
    return new Lazy<>(
        () -> {
          if (compactions.isEmpty()) {
            return deltasAndCheckpointsReversed.get();
          } else {
            LogCompactionResolver resolver =
                new LogCompactionResolver(
                    deltasAndCheckpointsReversed.get(), compactionsReversed.get());
            return resolver.resolveFiles();
          }
        });
  }

  private String formatList(List<FileStatus> list) {
    if (list.isEmpty()) {
      return "";
    }
    return "\n    "
        + list.stream().map(FileStatus::toString).collect(Collectors.joining(",\n    "));
  }

  /**
   * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
   * Visible only for testing.
   */
  @VisibleForTesting
  static void assertLogFilesBelongToTable(Path logPath, List<FileStatus> allFiles) {
    String logPathStr = logPath.toString(); // fully qualified path
    for (FileStatus fileStatus : allFiles) {
      String filePath = fileStatus.getPath();
      if (!filePath.startsWith(logPathStr)) {
        throw new RuntimeException(
            String.format(
                "File (%s) doesn't belong in the transaction log at %s.", filePath, logPathStr));
      }
    }
  }

  ////////////////////
  // Helper classes //
  ////////////////////

  // Class to resolve the final list of deltas + log compactions to return
  private class LogCompactionResolver {
    // note that currentCompactionPos _always_ points to a valid compaction we'll be including, _or_
    // past the end of the list of compactions (meaning we've consumed them all). The compaction
    // pointed to will be added to the output when we hit a delta with a version equal to the low
    // version of the compaction.
    int currentCompactionPos = 0;
    long currentCompactionHi = -1;
    long currentCompactionLo = -1;
    final List<FileStatus> compactionsReversed;

    final Iterator<FileStatus> deltaIt;

    LogCompactionResolver(List<FileStatus> allFilesReversed, List<FileStatus> compactionsReversed) {
      this.deltaIt = allFilesReversed.iterator();
      this.compactionsReversed = compactionsReversed;
    }

    // We have two lists, one of deltas and one of compactions. Each is sorted in DESCENDING
    // order. Given this, resolves as follows:
    // - set a "hi/lo" goalpost around the next compactions
    // - for each delta, if its version is:
    //   - greater than the current compaction high point, include it, move to next delta
    //   - less than (but not equal to) the current compaction low point, skip it, move to next
    //     delta
    //   - equal to the current compaction low point, we're about to transition out of the
    //     compaction, so, include the compaction, find the next compaction that has a high
    //     point lower than our current low point and set that to the current compaction to
    //     consider. This deals with overlapping compactions in a greedy way, ensuring we
    //     ignore any overlapping compactions.
    List<FileStatus> resolveFiles() {
      ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
      setHiLo();
      while (deltaIt.hasNext()) {
        FileStatus currentDelta = deltaIt.next();
        long deltaVersion = FileNames.deltaVersion(currentDelta.getPath());
        if (deltaVersion == currentCompactionLo) {
          // we're about to cross out of the compaction. insert the compaction and advance to the
          // next compaction. We don't want to include this delta here.
          ret.add(compactionsReversed.get(currentCompactionPos));
          advanceCompactionPos();
          setHiLo();
        } else if (deltaVersion > currentCompactionHi) {
          // this delta is not covered by the next compaction, include it.
          ret.add(currentDelta);
        }
        // just skip the file if none of the above are true, it's covered by the current compaction
      }
      return ret;
    }

    // Advance the compaction pos until we're pointing a compaction that has a end lower than our
    // current low mark (recall we move backwards through versions). This takes compactions in a
    // greedy manner, and ensures we don't use any overlapping compactions.
    private void advanceCompactionPos() {
      currentCompactionPos += 1;
      while (currentCompactionPos < compactionsReversed.size()) {
        Tuple2<Long, Long> versions =
            FileNames.logCompactionVersions(
                compactionsReversed.get(currentCompactionPos).getPath());
        if (versions._2 < currentCompactionLo) {
          break;
        }
        currentCompactionPos += 1;
      }
    }

    // Set the high/low position based on the current currentCompactionPos
    private void setHiLo() {
      if (currentCompactionPos < compactionsReversed.size()) {
        Tuple2<Long, Long> versions =
            FileNames.logCompactionVersions(
                compactionsReversed.get(currentCompactionPos).getPath());
        currentCompactionLo = versions._1;
        currentCompactionHi = versions._2;
      } else {
        currentCompactionLo = currentCompactionHi = -1;
      }
    }
  }
}
