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
package io.delta.spark.internal.v2.read;

import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.internal.actions.AddFile;
import org.apache.spark.sql.delta.sources.AdmittableFile;

/**
 * Java version of IndexedFile.scala that uses Kernel's action classes.
 *
 * <p>File: represents a data file in Delta.
 *
 * <p>Indexed: refers to the index in DeltaSourceOffset, assigned by the streaming engine.
 *
 * <p>For CDC reads, this class also holds:
 *
 * <ul>
 *   <li>changeType (e.g., "insert") — assigned by the streaming engine for initial snapshot files
 *   <li>commitTimestamp (milliseconds since epoch)
 * </ul>
 */
public class IndexedFile implements AdmittableFile {
  private final long version;
  private final long index;
  private final AddFile addFile;
  private final String changeType;
  private final long commitTimestamp;

  /** Constructor for non-CDC AddFile actions. */
  public IndexedFile(long version, long index, AddFile addFile) {
    this.version = version;
    this.index = index;
    this.addFile = addFile;
    this.changeType = null;
    this.commitTimestamp = -1;
  }

  /** Constructor for CDC - inferred from AddFile (e.g., initial snapshot "insert"). */
  public IndexedFile(
      long version, long index, AddFile addFile, String changeType, long commitTimestamp) {
    this.version = version;
    this.index = index;
    this.addFile = addFile;
    this.changeType = changeType;
    this.commitTimestamp = commitTimestamp;
  }

  public long getVersion() {
    return version;
  }

  public long getIndex() {
    return index;
  }

  public AddFile getAddFile() {
    return addFile;
  }

  public String getChangeType() {
    return changeType;
  }

  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  /** Returns true if this IndexedFile is for CDC (has changeType set). */
  public boolean isCDC() {
    return changeType != null;
  }

  @Override
  public boolean hasFileAction() {
    return addFile != null;
  }

  @Override
  public long getFileSize() {
    checkState(addFile != null, "check hasFileAction() before calling getFileSize()");
    return addFile.getSize();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("IndexedFile{");
    sb.append("version=").append(version);
    sb.append(", index=").append(index);
    if (addFile != null) {
      sb.append(", addFile=").append(addFile);
    }
    if (changeType != null) {
      sb.append(", changeType='").append(changeType).append('\'');
    }
    if (commitTimestamp >= 0) {
      sb.append(", commitTimestamp=").append(commitTimestamp);
    }
    sb.append('}');
    return sb.toString();
  }
}
