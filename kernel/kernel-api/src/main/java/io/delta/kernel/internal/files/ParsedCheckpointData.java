/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.files;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.Optional;

/**
 * Abstract checkpoint file that contains a complete snapshot of table state at a specific version.
 */
public abstract class ParsedCheckpointData extends ParsedLogData
    implements Comparable<ParsedCheckpointData> {

  public static ParsedCheckpointData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isClassicCheckpointFile(path)) {
      return ParsedClassicCheckpointData.forFileStatus(fileStatus);
    } else if (FileNames.isV2CheckpointFile(path)) {
      return ParsedV2CheckpointData.forFileStatus(fileStatus);
    } else if (FileNames.isMultiPartCheckpointFile(path)) {
      return ParsedMultiPartCheckpointData.forFileStatus(fileStatus);
    } else {
      throw new IllegalArgumentException("Unknown checkpoint file type: " + path);
    }
  }

  /**
   * Enum representing checkpoint type priorities for comparison when multiple checkpoint types
   * exist at the same version. Higher ordinal values indicate higher priority.
   */
  protected enum CheckpointTypePriority {
    CLASSIC, // priority 0 - least preferred
    MULTIPART, // priority 1 - better than classic
    V2 // priority 2 - most preferred
  }

  protected ParsedCheckpointData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  /**
   * Returns the checkpoint type priority used as a tiebreaker when multiple checkpoint types exist
   * at the same version.
   */
  protected abstract CheckpointTypePriority getCheckpointTypePriority();

  /**
   * Compares two checkpoints of the same version and same type. Subclasses should implement
   * type-specific comparison logic for final tiebreaking.
   */
  protected abstract int compareToSameType(ParsedCheckpointData that);

  @Override
  public Class<? extends ParsedLogData> getGroupByCategoryClass() {
    return ParsedCheckpointData.class;
  }

  /**
   * Compares checkpoints for ordering preference. Returns positive if *this* checkpoint is
   * preferred over *that* checkpoint, negative if *that* is preferred, or zero if equal.
   *
   * <p>Comparison hierarchy:
   *
   * <ol>
   *   <li><strong>Version (most important):</strong> Higher version numbers are always preferred
   *       over lower ones, as newer checkpoints contain more recent data
   *   <li><strong>Checkpoint type:</strong> When versions are equal, prefer by type priority based
   *       on safety and performance characteristics (V2 &gt; MultiPart &gt; Classic)
   *   <li><strong>Type-specific logic:</strong> When version and type are equal, use type-specific
   *       comparison (e.g., MultiPart prefers more parts for better parallelization)
   * </ol>
   */
  @Override
  public int compareTo(ParsedCheckpointData that) {
    // 1. Compare versions - newer checkpoints are always preferred
    if (version != that.version) {
      return Long.compare(version, that.version);
    }

    // 2. Compare types by priority (V2 > MultiPart > Classic)
    CheckpointTypePriority thisTypePriority = this.getCheckpointTypePriority();
    CheckpointTypePriority thatTypePriority = that.getCheckpointTypePriority();
    if (thisTypePriority != thatTypePriority) {
      return thisTypePriority.compareTo(thatTypePriority);
    }

    // 3. Use type-specific comparison when version and type are the same
    return compareToSameType(that);
  }

  /**
   * Compares checkpoints by data source preference and deterministic tiebreaking.
   *
   * <p>Prefers inline data to file data because inline data is already loaded in memory, avoiding
   * the need for additional file I/O operations.
   *
   * <p>When both are files or both are inline, uses lexicographic path comparison as an arbitrary
   * but deterministic tiebreaker to ensure consistent ordering.
   */
  protected final int compareByDataSource(ParsedCheckpointData that) {
    if (this.isInline() && that.isFile()) {
      return 1; // Prefer this (inline data)
    } else if (this.isFile() && that.isInline()) {
      return -1; // Prefer that (inline data)
    } else if (this.isFile() && that.isFile()) {
      // Both are files - use path as arbitrary but deterministic tiebreaker
      return this.getFileStatus().getPath().compareTo(that.getFileStatus().getPath());
    } else {
      // Both are inline - no preference
      return 0;
    }
  }
}
