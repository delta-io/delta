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

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddCDCFile;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import javax.annotation.Nullable;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;

/**
 * Wrapper for all CDC file variants: inferred from AddFile (insert/update), inferred from
 * RemoveFile (delete), or explicit AddCDCFile actions.
 */
public class CDCDataFile {
  @Nullable private final AddFile addFile;
  @Nullable private final RemoveFile removeFile;
  private final boolean isAddCDCFile;
  @Nullable private final String cdcPath;
  @Nullable private final MapValue cdcPartitionValues;
  @Nullable private final String changeType;
  private final long commitTimestamp;
  private final long fileSize;

  private CDCDataFile(
      @Nullable AddFile addFile,
      @Nullable RemoveFile removeFile,
      boolean isAddCDCFile,
      @Nullable String cdcPath,
      @Nullable MapValue cdcPartitionValues,
      @Nullable String changeType,
      long commitTimestamp,
      long fileSize) {
    this.addFile = addFile;
    this.removeFile = removeFile;
    this.isAddCDCFile = isAddCDCFile;
    this.cdcPath = cdcPath;
    this.cdcPartitionValues = cdcPartitionValues;
    this.changeType = changeType;
    this.commitTimestamp = commitTimestamp;
    this.fileSize = fileSize;
  }

  /** Create a CDCDataFile inferred from an AddFile action (always "insert"). */
  public static CDCDataFile fromAddFile(AddFile addFile, long commitTimestamp) {
    return new CDCDataFile(
        addFile,
        /* removeFile= */ null,
        /* isAddCDCFile= */ false,
        /* cdcPath= */ null,
        /* cdcPartitionValues= */ null,
        CDCReader.CDC_TYPE_INSERT(),
        commitTimestamp,
        addFile.getSize());
  }

  /** Create a CDCDataFile inferred from a RemoveFile action (always "delete"). */
  public static CDCDataFile fromRemoveFile(RemoveFile removeFile, long commitTimestamp) {
    return new CDCDataFile(
        /* addFile= */ null,
        removeFile,
        /* isAddCDCFile= */ false,
        /* cdcPath= */ null,
        /* cdcPartitionValues= */ null,
        CDCReader.CDC_TYPE_DELETE_STRING(),
        commitTimestamp,
        removeFile.getSize().orElse(0L));
  }

  /** Create a CDCDataFile for an explicit AddCDCFile action. */
  public static CDCDataFile fromAddCDCFile(Row cdcRow, long commitTimestamp) {
    String path = cdcRow.getString(AddCDCFile.FULL_SCHEMA.indexOf("path"));
    MapValue partitionValues = cdcRow.getMap(AddCDCFile.FULL_SCHEMA.indexOf("partitionValues"));
    long size = cdcRow.getLong(AddCDCFile.FULL_SCHEMA.indexOf("size"));
    return new CDCDataFile(
        /* addFile= */ null,
        /* removeFile= */ null,
        /* isAddCDCFile= */ true,
        /* cdcPath= */ path,
        /* cdcPartitionValues= */ partitionValues,
        /* changeType= */ null,
        commitTimestamp,
        size);
  }

  @Nullable
  public AddFile getAddFile() {
    return addFile;
  }

  @Nullable
  public RemoveFile getRemoveFile() {
    return removeFile;
  }

  /** Returns the file path for any CDC file variant. */
  public String getPath() {
    if (addFile != null) {
      return addFile.getPath();
    } else if (removeFile != null) {
      return removeFile.getPath();
    } else {
      return cdcPath;
    }
  }

  /**
   * Returns the partition values for any CDC file variant. May be null for RemoveFile when
   * extendedFileMetadata is not present.
   */
  @Nullable
  public MapValue getPartitionValues() {
    if (addFile != null) return addFile.getPartitionValues();
    if (removeFile != null) return removeFile.getPartitionValues().orElse(null);
    return cdcPartitionValues;
  }

  @Nullable
  public String getChangeType() {
    return changeType;
  }

  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  public long getFileSize() {
    return fileSize;
  }

  /** Returns true if this is an explicit CDC file (from AddCDCFile action). */
  public boolean isAddCDCFile() {
    return isAddCDCFile;
  }

  /** Returns true if the underlying file action has a deletion vector. */
  public boolean hasDeletionVector() {
    if (addFile != null) {
      return addFile.getDeletionVector().isPresent();
    }
    if (removeFile != null) {
      return removeFile.getDeletionVector().isPresent();
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CDCDataFile{");
    if (addFile != null) {
      sb.append("addFile=").append(addFile);
    } else if (removeFile != null) {
      sb.append("removeFile=").append(removeFile);
    } else {
      sb.append("explicit=true");
    }
    if (changeType != null) {
      sb.append(", changeType='").append(changeType).append("'");
    }
    sb.append(", commitTimestamp=").append(commitTimestamp);
    sb.append("}");
    return sb.toString();
  }
}
