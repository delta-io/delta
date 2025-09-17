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
 * Represents a Delta Log "file" - the actual content may be written to disk as a file or stored
 * inline as a columnar batch.
 */
// TODO: Move this to be a public API
public abstract class ParsedLogData {

  public static ParsedLogData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isCommitFile(path)) {
      return ParsedDeltaData.forFileStatus(fileStatus);
    } else if (FileNames.isLogCompactionFile(path)) {
      return ParsedLogCompactionData.forFileStatus(fileStatus);
    } else if (FileNames.isChecksumFile(path)) {
      return ParsedChecksumData.forFileStatus(fileStatus);
    } else if (FileNames.isClassicCheckpointFile(path)) {
      return ParsedClassicCheckpointData.forFileStatus(fileStatus);
    } else if (FileNames.isV2CheckpointFile(path)) {
      return ParsedV2CheckpointData.forFileStatus(fileStatus);
    } else if (FileNames.isMultiPartCheckpointFile(path)) {
      return ParsedMultiPartCheckpointData.forFileStatus(fileStatus);
    } else {
      throw new IllegalArgumentException("Unknown log file type: " + path);
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  public final long version;
  public final Optional<FileStatus> fileStatusOpt;
  public final Optional<ColumnarBatch> inlineDataOpt;

  protected ParsedLogData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    checkArgument(
        fileStatusOpt.isPresent() ^ inlineDataOpt.isPresent(),
        "Exactly one of fileStatusOpt or inlineDataOpt must be present");
    checkArgument(version >= 0, "version must be non-negative");
    this.version = version;
    this.fileStatusOpt = fileStatusOpt;
    this.inlineDataOpt = inlineDataOpt;
  }

  public boolean isFile() {
    return fileStatusOpt.isPresent();
  }

  public boolean isInline() {
    return inlineDataOpt.isPresent();
  }

  /**
   * Callers must check {@link #isFile()} before calling this method.
   *
   * @throws NoSuchElementException if {@link #isFile()} is false
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

  public abstract String getParentCategoryName();

  public abstract Class<? extends ParsedLogData> getParentCategoryClass();

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
        && Objects.equals(fileStatusOpt, that.fileStatusOpt)
        && Objects.equals(inlineDataOpt, that.inlineDataOpt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(getClass().getSimpleName())
            .append("{version=")
            .append(version)
            .append(", source=");
    if (isFile()) {
      sb.append(fileStatusOpt.get());
    } else {
      sb.append("inline");
    }

    appendAdditionalToStringFields(sb);

    sb.append('}');
    return sb.toString();
  }
}
