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

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogSegment {

  //////////////////////////////////
  // Static variables and methods //
  //////////////////////////////////

  public static LogSegment empty(Path logPath) {
    return new LogSegment(
        logPath, -1, Collections.emptyList(), Collections.emptyList(), Optional.empty(), -1);
  }

  //////////////////////////////////
  // Member variables and methods //
  //////////////////////////////////

  private final Path logPath;
  private final long version;
  private final List<FileStatus> deltas;
  private final List<FileStatus> checkpoints;
  private final Optional<Long> checkpointVersionOpt;
  private final Optional<FileStatus> lastSeenCheckSum;
  private final long lastCommitTimestamp;
  private final Lazy<List<FileStatus>> allFiles;
  private final Lazy<List<FileStatus>> allFilesReversed;

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
   * @param checkpoints The checkpoint file(s) to read
   * @param lastCommitTimestamp The "unadjusted" timestamp of the last commit within this segment.
   *     By unadjusted, we mean that the commit timestamps may not necessarily be monotonically
   *     increasing for the commits within this segment.
   */
  public LogSegment(
      Path logPath,
      long version,
      List<FileStatus> deltas,
      List<FileStatus> checkpoints,
      Optional<FileStatus> lastSeenCheckSum,
      long lastCommitTimestamp) {

    ///////////////////////
    // Input validations //
    ///////////////////////

    requireNonNull(logPath, "logPath is null");
    requireNonNull(deltas, "deltas is null");
    requireNonNull(checkpoints, "checkpoints is null");
    checkArgument(
        deltas.stream().allMatch(fs -> FileNames.isCommitFile(fs.getPath())),
        "deltas must all be actual delta (commit) files");
    checkArgument(
        checkpoints.stream().allMatch(fs -> FileNames.isCheckpointFile(fs.getPath())),
        "checkpoints must all be actual checkpoint files");

    this.checkpointVersionOpt =
        checkpoints.isEmpty()
            ? Optional.empty()
            : Optional.of(FileNames.checkpointVersion(new Path(checkpoints.get(0).getPath())));

    checkArgument(
        checkpoints.stream()
            .map(fs -> FileNames.checkpointVersion(new Path(fs.getPath())))
            .allMatch(v -> checkpointVersionOpt.get().equals(v)),
        "All checkpoint files must have the same version");

    if (version != -1) {
      checkArgument(!deltas.isEmpty() || !checkpoints.isEmpty(), "No files to read");

      if (!deltas.isEmpty()) {
        final List<Long> deltaVersions =
            deltas.stream()
                .map(fs -> FileNames.deltaVersion(new Path(fs.getPath())))
                .collect(Collectors.toList());

        // Check the first delta version
        this.checkpointVersionOpt.ifPresent(
            checkpointVersion -> {
              checkArgument(
                  deltaVersions.get(0) == checkpointVersion + 1,
                  "First delta file version must equal checkpointVersion + 1");
            });

        // Check the last delta version
        checkArgument(
            ListUtils.getLast(deltaVersions) == version,
            "Last delta file version must equal the version of this LogSegment");

        // Ensure the delta versions are contiguous
        for (int i = 1; i < deltaVersions.size(); i++) {
          if (deltaVersions.get(i) != deltaVersions.get(i - 1) + 1) {
            throw new IllegalArgumentException(
                String.format("Delta versions must be contiguous: %s", deltaVersions));
          }
        }
      } else {
        this.checkpointVersionOpt.ifPresent(
            checkpointVersion -> {
              checkArgument(
                  checkpointVersion == version,
                  "If there are no deltas, then checkpointVersion must equal the version "
                      + "of this LogSegment");
            });
      }
    } else {
      checkArgument(deltas.isEmpty() && checkpoints.isEmpty(), "Version -1 should have no files");
    }

    ////////////////////////////////
    // Member variable assignment //
    ////////////////////////////////

    this.logPath = logPath;
    this.version = version;
    this.deltas = deltas;
    this.checkpoints = checkpoints;
    this.lastSeenCheckSum = lastSeenCheckSum;
    this.lastCommitTimestamp = lastCommitTimestamp;

    this.allFiles =
        new Lazy<>(
            () ->
                Stream.concat(checkpoints.stream(), deltas.stream()).collect(Collectors.toList()));

    this.allFilesReversed =
        new Lazy<>(
            () ->
                allFiles.get().stream()
                    .sorted(
                        Comparator.comparing((FileStatus a) -> new Path(a.getPath()).getName())
                            .reversed())
                    .collect(Collectors.toList()));
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

  public List<FileStatus> getCheckpoints() {
    return checkpoints;
  }

  public Optional<Long> getCheckpointVersionOpt() {
    return checkpointVersionOpt;
  }

  public long getLastCommitTimestamp() {
    return lastCommitTimestamp;
  }

  /**
   * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment, with
   *     no ordering guarantees.
   */
  public List<FileStatus> allLogFilesUnsorted() {
    return allFiles.get();
  }

  /**
   * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment,
   *     sorted in reverse (00012.json, 00011.json, 00010.checkpoint.parquet) order.
   */
  public List<FileStatus> allLogFilesReversed() {
    return allFilesReversed.get();
  }

  @Override
  public String toString() {
    return String.format(
        "LogSegment {\n"
            + "  logPath='%s',\n"
            + "  version=%d,\n"
            + "  deltas=[%s\n  ],\n"
            + "  checkpoints=[%s\n  ],\n"
            + "  lastSeenCheckSum=%s,\n"
            + "  checkpointVersion=%s,\n"
            + "  lastCommitTimestamp=%d\n"
            + "}",
        logPath,
        version,
        formatList(deltas),
        formatList(checkpoints),
        lastSeenCheckSum.map(FileStatus::toString).orElse("None"),
        checkpointVersionOpt.map(String::valueOf).orElse("None"),
        lastCommitTimestamp);
  }

  private String formatList(List<FileStatus> list) {
    if (list.isEmpty()) {
      return "";
    }
    return "\n    "
        + list.stream().map(FileStatus::toString).collect(Collectors.joining(",\n    "));
  }

  public Optional<FileStatus> getLastSeenCheckSum() {
    return lastSeenCheckSum;
  }
}
