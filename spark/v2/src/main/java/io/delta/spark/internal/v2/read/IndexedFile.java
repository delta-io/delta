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
import io.delta.kernel.internal.actions.RemoveFile;
import org.apache.spark.sql.delta.sources.AdmittableFile;

/**
 * Java version of IndexedFile.scala that uses Kernel's action classes.
 *
 * <p>File: represents a data file in Delta.
 *
 * <p>Indexed: refers to the index in DeltaSourceOffset, assigned by the streaming engine.
 */
public class IndexedFile implements AdmittableFile {
  private final long version;
  private final long index;
  private final AddFile addFile;
  private final RemoveFile removeFile;
  private final CDCFileInfo cdcFile;
  private final String changeType;
  private final long commitTimestamp;
  private final boolean shouldSkip;

  private IndexedFile(
      long version,
      long index,
      AddFile addFile,
      RemoveFile removeFile,
      CDCFileInfo cdcFile,
      String changeType,
      long commitTimestamp,
      boolean shouldSkip) {
    this.version = version;
    this.index = index;
    this.addFile = addFile;
    this.removeFile = removeFile;
    this.cdcFile = cdcFile;
    this.changeType = changeType;
    this.commitTimestamp = commitTimestamp;
    this.shouldSkip = shouldSkip;
  }

  /** Creates a sentinel IndexedFile (BEGIN or END marker) with no file action. */
  public static IndexedFile sentinel(long version, long index) {
    return new IndexedFile(version, index, null, null, null, null, -1, false);
  }

  /** Creates an IndexedFile for a non-CDC AddFile action. */
  public static IndexedFile forAddFile(long version, long index, AddFile addFile) {
    return new IndexedFile(version, index, addFile, null, null, null, -1, false);
  }

  /** Creates an IndexedFile for CDC inferred from an AddFile (e.g., "insert"). */
  public static IndexedFile forCDCAddFile(
      long version, long index, AddFile addFile, String changeType, long commitTimestamp) {
    return new IndexedFile(version, index, addFile, null, null, changeType, commitTimestamp, false);
  }

  /**
   * Creates an IndexedFile for CDC inferred from an AddFile, with shouldSkip flag.
   *
   * <p>MERGE can sometimes rewrite files in a way which *could* have changed data (so dataChange =
   * true) but did not actually do so (so no CDC will be produced). In this case, shouldSkip=true
   * indicates that CDC shouldn't be produced from them.
   */
  public static IndexedFile forCDCAddFile(
      long version,
      long index,
      AddFile addFile,
      String changeType,
      long commitTimestamp,
      boolean shouldSkip) {
    return new IndexedFile(
        version, index, addFile, null, null, changeType, commitTimestamp, shouldSkip);
  }

  /** Creates an IndexedFile for CDC inferred from a RemoveFile (delete), with shouldSkip flag. */
  public static IndexedFile forCDCRemoveFile(
      long version,
      long index,
      RemoveFile removeFile,
      String changeType,
      long commitTimestamp,
      boolean shouldSkip) {
    return new IndexedFile(
        version, index, null, removeFile, null, changeType, commitTimestamp, shouldSkip);
  }

  /** Creates an IndexedFile for an explicit CDC file (AddCDCFile action). */
  public static IndexedFile forCDCFile(
      long version, long index, CDCFileInfo cdcFile, long commitTimestamp) {
    return new IndexedFile(version, index, null, null, cdcFile, null, commitTimestamp, false);
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

  public RemoveFile getRemoveFile() {
    return removeFile;
  }

  public CDCFileInfo getCdcFile() {
    return cdcFile;
  }

  public String getChangeType() {
    return changeType;
  }

  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  public boolean shouldSkip() {
    return shouldSkip;
  }

  /** Returns true if this IndexedFile contains an explicit CDC file (AddCDCFile action). */
  public boolean isCDCFile() {
    return cdcFile != null;
  }

  /** Returns true if this IndexedFile contains a RemoveFile action (for inferred CDC deletes). */
  public boolean isRemoveFile() {
    return removeFile != null;
  }

  /** Returns true if this IndexedFile is for CDC (has changeType or cdcFile set). */
  public boolean isCDC() {
    return changeType != null || cdcFile != null;
  }

  @Override
  public boolean hasFileAction() {
    return addFile != null || removeFile != null || cdcFile != null;
  }

  @Override
  public long getFileSize() {
    checkState(hasFileAction(), "check hasFileAction() before calling getFileSize()");
    if (addFile != null) {
      return addFile.getSize();
    } else if (removeFile != null) {
      // RemoveFile.getSize() returns Optional<Long>
      return removeFile.getSize().orElse(0L);
    } else {
      return cdcFile.getSize();
    }
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
    if (removeFile != null) {
      sb.append(", removeFile=").append(removeFile);
    }
    if (cdcFile != null) {
      sb.append(", cdcFile=").append(cdcFile);
    }
    if (changeType != null) {
      sb.append(", changeType='").append(changeType).append('\'');
    }
    if (commitTimestamp >= 0) {
      sb.append(", commitTimestamp=").append(commitTimestamp);
    }
    if (shouldSkip) {
      sb.append(", shouldSkip=true");
    }
    sb.append('}');
    return sb.toString();
  }
}
