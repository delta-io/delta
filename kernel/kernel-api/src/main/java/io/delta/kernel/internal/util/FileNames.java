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

package io.delta.kernel.internal.util;

import io.delta.kernel.internal.fs.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class FileNames {

  private FileNames() {}

  private static final Pattern DELTA_FILE_PATTERN = Pattern.compile("\\d+\\.json");

  // Example: 00000000000000000001.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.json
  private static final Pattern UUID_DELTA_FILE_REGEX = Pattern.compile("(\\d+)\\.([^\\.]+)\\.json");

  private static final Pattern CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint((\\.\\d+\\.\\d+)?\\.parquet|\\.[^.]+\\.(json|parquet))");

  private static final Pattern CLASSIC_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("\\d+\\.checkpoint\\.parquet");

  private static final Pattern V2_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint\\.[^.]+\\.(json|parquet)");

  private static final Pattern MULTI_PART_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint\\.\\d+\\.\\d+\\.parquet");

  public static final String SIDECAR_DIRECTORY = "_sidecars";

  /**
   * The subdirectory in the delta log directory where un-backfilled commits are stored as part of
   * the coordinated commits table feature.
   */
  public static final String COMMIT_SUBDIR = "_commits";

  /** Returns the delta (json format) path for a given delta file. */
  public static String deltaFile(Path path, long version) {
    return String.format("%s/%020d.json", path, version);
  }

  /**
   * Returns the un-backfilled uuid formatted delta (json format) path for a given version.
   *
   * @param logPath The root path of the delta log.
   * @param version The version of the delta file.
   * @param uuidString An optional UUID string.
   * @return The path to the un-backfilled delta file: <logPath>/_commits/<version>.<uuid>.json
   */
  public static String unbackfilledDeltaFile(Path logPath, long version, String uuidString) {
    Path commitsPath = commitDirPath(logPath);
    return new Path(commitsPath, String.format("%020d.%s.json", version, uuidString)).toString();
  }

  /** Returns the version for the given delta path. */
  public static long deltaVersion(Path path) {
    return Long.parseLong(path.getName().split("\\.")[0]);
  }

  public static long deltaVersion(String path) {
    final int slashIdx = path.lastIndexOf(Path.SEPARATOR);
    final String name = path.substring(slashIdx + 1);
    return Long.parseLong(name.split("\\.")[0]);
  }

  /** Returns the version for the given checkpoint path. */
  public static long checkpointVersion(Path path) {
    return Long.parseLong(path.getName().split("\\.")[0]);
  }

  public static long checkpointVersion(String path) {
    final int slashIdx = path.lastIndexOf(Path.SEPARATOR);
    final String name = path.substring(slashIdx + 1);
    return Long.parseLong(name.split("\\.")[0]);
  }

  public static String sidecarFile(Path path, String sidecar) {
    return String.format("%s/%s/%s", path.toString(), SIDECAR_DIRECTORY, sidecar);
  }

  /**
   * Returns the prefix of all delta log files for the given version.
   *
   * <p>Intended for use with listFrom to get all files from this version onwards. The returned Path
   * will not exist as a file.
   */
  public static String listingPrefix(Path path, long version) {
    return String.format("%s/%020d.", path, version);
  }

  /**
   * Returns the path for a singular checkpoint up to the given version.
   *
   * <p>In a future protocol version this path will stop being written.
   */
  public static Path checkpointFileSingular(Path path, long version) {
    return new Path(path, String.format("%020d.checkpoint.parquet", version));
  }

  /**
   * Returns the path for a top-level V2 checkpoint file up to the given version with a given UUID
   * and filetype (JSON or Parquet).
   */
  public static Path topLevelV2CheckpointFile(
      Path path, long version, String uuid, String fileType) {
    assert (fileType.equals("json") || fileType.equals("parquet"));
    return new Path(path, String.format("%020d.checkpoint.%s.%s", version, uuid, fileType));
  }

  /** Returns the path for a V2 sidecar file with a given UUID. */
  public static Path v2CheckpointSidecarFile(Path path, String uuid) {
    return new Path(String.format("%s/_sidecars/%s.parquet", path.toString(), uuid));
  }

  /**
   * Returns the paths for all parts of the checkpoint up to the given version.
   *
   * <p>In a future protocol version we will write this path instead of checkpointFileSingular.
   *
   * <p>Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
   * checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
   * lexicographic sorting.
   */
  public static List<Path> checkpointFileWithParts(Path path, long version, int numParts) {
    final List<Path> output = new ArrayList<>();
    for (int i = 1; i < numParts + 1; i++) {
      output.add(
          new Path(
              path, String.format("%020d.checkpoint.%010d.%010d.parquet", version, i, numParts)));
    }
    return output;
  }

  public static boolean isCheckpointFile(String fileName) {
    return CHECKPOINT_FILE_PATTERN.matcher(new Path(fileName).getName()).matches();
  }

  public static boolean isClassicCheckpointFile(String fileName) {
    return CLASSIC_CHECKPOINT_FILE_PATTERN.matcher(fileName).matches();
  }

  public static boolean isMultiPartCheckpointFile(String fileName) {
    return MULTI_PART_CHECKPOINT_FILE_PATTERN.matcher(fileName).matches();
  }

  public static boolean isV2CheckpointFile(String fileName) {
    return V2_CHECKPOINT_FILE_PATTERN.matcher(fileName).matches();
  }

  public static boolean isCommitFile(String filePathStr) {
    Path filePath = new Path(filePathStr);
    String fileName = filePath.getName();
    if (DELTA_FILE_PATTERN.matcher(fileName).matches()) {
      return true;
    } else {
      String fileParentName = filePath.getParent().getName();
      // If parent is _commits dir, then match against un-backfilled commit file name pattern.
      return COMMIT_SUBDIR.equals(fileParentName)
          && UUID_DELTA_FILE_REGEX.matcher(fileName).matches();
    }
  }

  /**
   * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
   * file type is seen. These unexpected files should be filtered out to ensure forward
   * compatibility in cases where new file types are added, but without an explicit protocol
   * upgrade.
   */
  public static long getFileVersion(Path path) {
    if (isCheckpointFile(path.getName())) {
      return checkpointVersion(path);
    } else if (isCommitFile(path.getName())) {
      return deltaVersion(path);
      // } else if (isChecksumFile(path)) {
      //    checksumVersion(path);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected file type found in transaction log: %s", path));
    }
  }

  /** Returns path to the sidecar directory */
  public static Path commitDirPath(Path logPath) {
    return new Path(logPath, COMMIT_SUBDIR);
  }
}
