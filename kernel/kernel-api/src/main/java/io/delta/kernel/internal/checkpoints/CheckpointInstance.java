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
package io.delta.kernel.internal.checkpoints;

import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Objects;
import java.util.Optional;

/** Metadata about Delta checkpoint. */
public class CheckpointInstance implements Comparable<CheckpointInstance> {

  public enum CheckpointFormat {
    // Note that the order of these enum values is important for comparison of checkpoint
    // instances (we prefer V2 > MULTI_PART > CLASSIC).
    CLASSIC,
    MULTI_PART,
    V2;

    // Indicates that the checkpoint (may) contain SidecarFile actions. For compatibility,
    // V2 checkpoints can be named with classic-style names, so any checkpoint other than a
    // multipart checkpoint may contain SidecarFile actions.
    public boolean usesSidecars() {
      return this == CLASSIC || this == V2;
    }
  }

  /** Placeholder to identify the version that is always the latest on timeline */
  public static final CheckpointInstance MAX_VALUE = new CheckpointInstance(Long.MAX_VALUE);

  public final long version;
  public final CheckpointFormat format;
  public final Optional<FileStatus> fileStatus; // Guaranteed to be present for V2 checkpoints.
  public final Optional<Integer> partNum;
  public final Optional<Integer> numParts;

  public CheckpointInstance(FileStatus fileStatus) {
    Preconditions.checkArgument(
        FileNames.isCheckpointFile(fileStatus.getPath()), "not a valid checkpoint file name");

    this.fileStatus = Optional.of(fileStatus);
    this.version = FileNames.checkpointVersion(fileStatus.getPath());

    if (FileNames.isClassicCheckpointFile(fileStatus.getPath())) {
      // Classic checkpoint 00000000000000000010.checkpoint.parquet
      this.partNum = Optional.empty();
      this.numParts = Optional.empty();
      this.format = CheckpointFormat.CLASSIC;
    } else if (FileNames.isMultiPartCheckpointFile(fileStatus.getPath())) {
      // Multi-part checkpoint 00000000000000000010.checkpoint.0000000001.0000000003.parquet
      final Tuple2<Integer, Integer> partAndNumParts =
          FileNames.multiPartCheckpointPartAndNumParts(fileStatus.getPath());
      this.partNum = Optional.of(partAndNumParts._1);
      this.numParts = Optional.of(partAndNumParts._2);
      this.format = CheckpointFormat.MULTI_PART;
    } else if (FileNames.isV2CheckpointFile(fileStatus.getPath())) {
      // V2 checkpoint 00000000000000000010.checkpoint.UUID.(parquet|json)
      this.partNum = Optional.empty();
      this.numParts = Optional.empty();
      this.format = CheckpointFormat.V2;
    } else {
      throw new RuntimeException("Unrecognized checkpoint path format: " + fileStatus.getPath());
    }
  }

  public CheckpointInstance(long version) {
    this(version, Optional.empty());
  }

  public CheckpointInstance(long version, Optional<Integer> numParts) {
    this.version = version;
    this.numParts = numParts;
    this.partNum = Optional.empty();
    this.fileStatus = Optional.empty();
    if (numParts.orElse(0) == 0) {
      this.format = CheckpointFormat.CLASSIC;
    } else {
      this.format = CheckpointFormat.MULTI_PART;
    }
  }

  boolean isNotLaterThan(CheckpointInstance other) {
    if (other == CheckpointInstance.MAX_VALUE) {
      return true;
    }
    return version <= other.version;
  }

  boolean isEarlierThan(CheckpointInstance other) {
    if (other == CheckpointInstance.MAX_VALUE) {
      return true;
    }
    return version < other.version;
  }

  /**
   * Comparison rules:
   *
   * <ol>
   *   <li>1. A CheckpointInstance with higher version is greater than the one with lower version.
   *   <li>2. CheckpointInstance with a V2 checkpoint is greater than a classic checkpoint (to
   *       filter avoid selecting the compatibility file) or a Multi-part checkpoint.
   *   <li>3. For CheckpointInstances with same version, a Multi-part checkpoint is greater than a
   *       Single part checkpoint.
   *   <li>4. For Multi-part CheckpointInstance corresponding to same version, the one with more
   *       parts is greater than the one with fewer parts.
   *   <li>5. For V2 checkpoints, use the file path to break ties
   * </ol>
   */
  @Override
  public int compareTo(CheckpointInstance that) {
    // Compare versions.
    if (version != that.version) {
      return Long.compare(version, that.version);
    }

    // Compare formats.
    if (format != that.format) {
      return Integer.compare(format.ordinal(), that.format.ordinal());
    }

    // Use format-specific tiebreakers if versions and formats are the same.
    switch (format) {
      case CLASSIC:
        return 0; // No way to break ties if both are classic checkpoints.
      case MULTI_PART:
        return Long.compare(numParts.orElse(1), that.numParts.orElse(1));
      case V2:
        return fileStatus.get().getPath().compareTo(that.fileStatus.get().getPath());
      default:
        throw new IllegalStateException("Unexpected format: " + format);
    }
  }

  @Override
  public String toString() {
    return "CheckpointInstance{version="
        + version
        + ", numParts="
        + numParts
        + ", format="
        + format
        + ", fileStatus="
        + fileStatus
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CheckpointInstance checkpointInstance = (CheckpointInstance) o;
    return this.compareTo(checkpointInstance) == 0;
  }

  @Override
  public int hashCode() {
    // For V2 checkpoints, the filepath is included in the hash of the instance (as we consider
    // different UUID checkpoints to be different checkpoint instances. Otherwise, ignore
    // the filepath (which is empty) when hashing.
    return Objects.hash(version, numParts, format, fileStatus);
  }

  private String getPathName(String path) {
    int slash = path.lastIndexOf("/");
    return path.substring(slash + 1);
  }
}
