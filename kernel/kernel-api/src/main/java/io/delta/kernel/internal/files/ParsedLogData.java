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
 * Abstract representation of any valid log type in the Delta log.
 *
 * <p>Different child classes are used to represent the different log types.
 *
 * <p>Any given log type can be written as a file or represented inline and given to Kernel as a
 * {@link ColumnarBatch}. That is: Kernel just needs to know how to parse and interpret a given log
 * type (Kernel will of course treat Deltas differently than Checksums) as well as how to get that
 * log type's bytes. This is why a given log type can be represented as either a file or inline.
 *
 * <p>For now, our APIs only allow creating {@link ParsedCatalogCommitData} inline, but we may
 * change and expand this capability in the future.
 *
 * <p>The supported log types are:
 *
 * <ul>
 *   <li>Published Deltas: {@code 00000000000000000001.json}
 *   <li>Catalog Commits: {@code _staged_commits/00000000000000000001.uuid-1234.json}
 *   <li>Log compaction files: {@code 00000000000000000001.00000000000000000009.compacted.json}
 *   <li>Checksum files: {@code 00000000000000000001.crc}
 *   <li>Classic Checkpoint files: {@code 00000000000000000001.checkpoint.parquet}
 *   <li>V2 checkpoint files: {@code 00000000000000000001.checkpoint.uuid-1234.json}
 *   <li>Multi-part checkpoint files: {@code
 *       00000000000000000001.checkpoint.0000000001.0000000010.parquet}
 * </ul>
 */
// TODO: Move this to be a public API
public abstract class ParsedLogData {

  public static ParsedLogData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isCommitFile(path)) {
      return ParsedDeltaData.forFileStatus(fileStatus);
    } else if (FileNames.isCheckpointFile(path)) {
      return ParsedCheckpointData.forFileStatus(fileStatus);
    } else if (FileNames.isLogCompactionFile(path)) {
      return ParsedLogCompactionData.forFileStatus(fileStatus);
    } else if (FileNames.isChecksumFile(path)) {
      return ParsedChecksumData.forFileStatus(fileStatus);
    } else {
      throw new IllegalArgumentException("Unknown log file type: " + path);
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  protected final long version;
  protected final Optional<FileStatus> fileStatusOpt;
  protected final Optional<ColumnarBatch> inlineDataOpt;

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

  /**
   * Returns true if this log data is stored as a file on disk. When false, the data is stored
   * inline.
   */
  public boolean isFile() {
    return fileStatusOpt.isPresent();
  }

  /**
   * Returns true if this log data is stored inline as a ColumnarBatch. When false, the data is
   * stored as a file on disk.
   */
  public boolean isInline() {
    return inlineDataOpt.isPresent();
  }

  /** Return the version of this log data. */
  public long getVersion() {
    return version;
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

  /** Returns the category class used for grouping collections of LISTed ParsedLogData instances. */
  public abstract Class<? extends ParsedLogData> getGroupByCategoryClass();

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
