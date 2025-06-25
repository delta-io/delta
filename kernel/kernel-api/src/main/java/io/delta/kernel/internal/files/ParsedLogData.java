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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Delta Log "file" - the actual content may be materialized to disk (with a file
 * status) or stored inline (as a columnar batch).
 */
// TODO: [delta-io/delta#4816] Move this to be a public API
public class ParsedLogData {

  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public enum ParsedLogCategory {
    DELTA,
    LOG_COMPACTION,
    CHECKPOINT,
    CHECKSUM
  }

  public enum ParsedLogType {
    PUBLISHED_DELTA(ParsedLogCategory.DELTA),
    RATIFIED_STAGED_COMMIT(ParsedLogCategory.DELTA),
    RATIFIED_INLINE_COMMIT(ParsedLogCategory.DELTA),
    LOG_COMPACTION(ParsedLogCategory.LOG_COMPACTION),
    CHECKSUM(ParsedLogCategory.CHECKSUM),

    // Note that the order of these checkpoint enum values is important for comparison of checkpoint
    // files as we prefer V2 > MULTI_PART > CLASSIC.
    CLASSIC_CHECKPOINT(ParsedLogCategory.CHECKPOINT),
    MULTIPART_CHECKPOINT(ParsedLogCategory.CHECKPOINT),
    V2_CHECKPOINT(ParsedLogCategory.CHECKPOINT);

    public final ParsedLogCategory category;

    ParsedLogType(ParsedLogCategory category) {
      this.category = category;
    }
  }

  public static ParsedLogData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isLogCompactionFile(path)) {
      return ParsedLogCompactionData.forFileStatus(fileStatus);
    } else if (FileNames.isCheckpointFile(path)) {
      return ParsedCheckpointData.forFileStatus(fileStatus);
    }

    final long version;
    final ParsedLogType type;

    if (FileNames.isPublishedDeltaFile(path)) {
      version = FileNames.deltaVersion(path);
      type = ParsedLogType.PUBLISHED_DELTA;
    } else if (FileNames.isStagedDeltaFile(path)) {
      version = FileNames.deltaVersion(path);
      type = ParsedLogType.RATIFIED_STAGED_COMMIT;
    } else if (FileNames.isChecksumFile(path)) {
      version = FileNames.checksumVersion(path);
      type = ParsedLogType.CHECKSUM;
    } else {
      throw new IllegalArgumentException("Unknown log file type: " + path);
    }

    return new ParsedLogData(version, type, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedLogData forInlineData(
      long version, ParsedLogType type, ColumnarBatch inlineData) {
    if (type == ParsedLogType.PUBLISHED_DELTA || type == ParsedLogType.RATIFIED_STAGED_COMMIT) {
      throw new IllegalArgumentException(
          "For PUBLISHED_DELTA|RATIFIED_STAGED_COMMIT, use ParsedLogData.forFileStatus() instead");
    } else if (type == ParsedLogType.LOG_COMPACTION) {
      throw new IllegalArgumentException(
          "For LOG_COMPACTION, use ParsedLogCompactionData.forInlineData() instead");
    } else if (type.category == ParsedLogCategory.CHECKPOINT) {
      return ParsedCheckpointData.forInlineData(version, type, inlineData);
    }
    return new ParsedLogData(version, type, Optional.empty(), Optional.of(inlineData));
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  public final long version;
  public final ParsedLogType type;
  public final Optional<FileStatus> fileStatusOpt;
  public final Optional<ColumnarBatch> inlineDataOpt;

  protected ParsedLogData(
      long version,
      ParsedLogType type,
      Optional<FileStatus> fileStatusOpt,
      Optional<ColumnarBatch> inlineDataOpt) {
    checkArgument(
        fileStatusOpt.isPresent() ^ inlineDataOpt.isPresent(),
        "Exactly one of fileStatusOpt or inlineDataOpt must be present");
    checkArgument(version >= 0, "version must be non-negative");
    this.version = version;
    this.type = type;
    this.fileStatusOpt = fileStatusOpt;
    this.inlineDataOpt = inlineDataOpt;
  }

  public boolean isMaterialized() {
    return fileStatusOpt.isPresent();
  }

  public boolean isInline() {
    return inlineDataOpt.isPresent();
  }

  /**
   * Callers must check {@link #isMaterialized()} before calling this method.
   *
   * @throws NoSuchElementException if {@link #isMaterialized()} is false
   */
  public FileStatus getFileStatus() {
    return fileStatusOpt.get();
  }

  /**
   * Callers must check {@link #isInline()} before calling this method.
   *
   * @throws NoSuchElementException if {@link #isInline()} is false
   */
  public ColumnarBatch getInlineData() {
    return inlineDataOpt.get();
  }

  public ParsedLogCategory getCategory() {
    return type.category;
  }

  /** Protected method for subclasses to override to add output to {@link #toString}. */
  protected void appendAdditionalToStringFields(StringBuilder sb) {
    // Default implementation does nothing
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParsedLogData that = (ParsedLogData) o;
    return version == that.version
        && type == that.type
        && Objects.equals(fileStatusOpt, that.fileStatusOpt)
        && Objects.equals(inlineDataOpt, that.inlineDataOpt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, type, fileStatusOpt, inlineDataOpt);
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(getClass().getSimpleName())
            .append("{version=")
            .append(version)
            .append(", type=")
            .append(type)
            .append(", source=");
    if (isMaterialized()) {
      sb.append(fileStatusOpt.get());
    } else {
      sb.append("inline");
    }

    appendAdditionalToStringFields(sb);

    sb.append('}');
    return sb.toString();
  }
}
