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

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Preconditions;
import java.util.Collections;
import java.util.List;
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
  public final Optional<Integer> numParts;

  public final CheckpointFormat format;

  public final Optional<Path> filePath; // Guaranteed to be present for V2 checkpoints.

  public CheckpointInstance(Path path) {
    Preconditions.checkArgument(
        FileNames.isCheckpointFile(path), "not a valid checkpoint file name");

    String[] pathParts = path.getName().split("\\.");

    if (pathParts.length == 3 && pathParts[2].equals("parquet")) {
      // Classic checkpoint 00000000000000000010.checkpoint.parquet
      this.version = Long.parseLong(pathParts[0]);
      this.numParts = Optional.empty();
      this.format = CheckpointFormat.CLASSIC;
      this.filePath = Optional.empty();
    } else if (pathParts.length == 5 && pathParts[4].equals("parquet")) {
      // Multi-part checkpoint 00000000000000000010.checkpoint.0000000001.0000000003.parquet
      this.version = Long.parseLong(pathParts[0]);
      this.numParts = Optional.of(Integer.parseInt(pathParts[3]));
      this.format = CheckpointFormat.MULTI_PART;
      this.filePath = Optional.empty();
    } else if (pathParts.length == 4
        && (pathParts[3].equals("parquet") || pathParts[3].equals("json"))) {
      // V2 checkpoint 00000000000000000010.checkpoint.UUID.(parquet|json)
      this.version = Long.parseLong(pathParts[0]);
      this.numParts = Optional.empty();
      this.format = CheckpointFormat.V2;
      this.filePath = Optional.of(path);
    } else {
      throw new RuntimeException("Unrecognized checkpoint path format: " + path.getName());
    }
  }

  public CheckpointInstance(long version) {
    this(version, Optional.empty());
  }

  public CheckpointInstance(long version, Optional<Integer> numParts) {
    this.version = version;
    this.numParts = numParts;
    this.filePath = Optional.empty();
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

  public List<Path> getCorrespondingFiles(Path path) {
    if (this == CheckpointInstance.MAX_VALUE) {
      throw new IllegalStateException("Can't get files for CheckpointVersion.MaxValue.");
    }

    // This is safe because the only way to construct a V2 CheckpointInstance is with the path.
    if (format == CheckpointFormat.V2) {
      return Collections.singletonList(filePath.get());
    }

    return numParts
        .map(parts -> FileNames.checkpointFileWithParts(path, version, parts))
        .orElseGet(
            () -> Collections.singletonList(FileNames.checkpointFileSingular(path, version)));
  }

  /**
   * Comparison rules: 1. A CheckpointInstance with higher version is greater than the one with
   * lower version. 2. A CheckpointInstance for a V2 checkpoint is greater than a classic checkpoint
   * (to filter avoid selecting the compatibility file) or a multipart checkpoint. 3. For
   * CheckpointInstances with same version, a Multi-part checkpoint is greater than a Single part
   * checkpoint. 4. For Multi-part CheckpointInstance corresponding to same version, the one with
   * more parts is greater than the one with fewer parts. 5. For V2 checkpoints, use the file path
   * to break ties.
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
        return filePath.get().getName().compareTo(that.filePath.get().getName());
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
        + ", filePath="
        + filePath
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
    return Objects.hash(version, numParts, format, filePath);
  }
}
