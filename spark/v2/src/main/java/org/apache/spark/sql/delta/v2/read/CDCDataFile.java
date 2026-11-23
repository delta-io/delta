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
package org.apache.spark.sql.delta.v2.read;

import io.delta.kernel.internal.util.Preconditions;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.actions.AddCDCFile;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.RemoveFile;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import scala.collection.immutable.Map;

/**
 * Wrapper for all CDC file variants: inferred from AddFile (insert/update), inferred from
 * RemoveFile (delete), or explicit AddCDCFile actions.
 */
public class CDCDataFile {
  @Nullable private final AddFile addFile;
  @Nullable private final RemoveFile removeFile;
  @Nullable private final AddCDCFile addCDCFile;
  @Nullable private final String changeType;
  private final long commitTimestamp;
  // base64-encoded inline DV of the changed rows (the bitmap diff between the Add and Remove DVs).
  @Nullable private final String dvBase64Override;

  private CDCDataFile(
      @Nullable AddFile addFile,
      @Nullable RemoveFile removeFile,
      @Nullable AddCDCFile addCDCFile,
      @Nullable String changeType,
      long commitTimestamp,
      @Nullable String dvBase64Override) {
    Preconditions.checkArgument(
        addFile != null || removeFile != null || addCDCFile != null,
        "CDCDataFile must have at least one of addFile, removeFile, or addCDCFile");
    this.addFile = addFile;
    this.removeFile = removeFile;
    this.addCDCFile = addCDCFile;
    this.changeType = changeType;
    this.commitTimestamp = commitTimestamp;
    this.dvBase64Override = dvBase64Override;
  }

  /** Create a CDCDataFile inferred from an AddFile action (always "insert"). */
  public static CDCDataFile fromAddFile(AddFile addFile, long commitTimestamp) {
    return new CDCDataFile(
        addFile,
        /* removeFile= */ null,
        /* addCDCFile= */ null,
        CDCReader.CDC_TYPE_INSERT(),
        commitTimestamp,
        /* dvBase64Override= */ null);
  }

  /** Create a CDCDataFile inferred from a RemoveFile action (always "delete"). */
  public static CDCDataFile fromRemoveFile(RemoveFile removeFile, long commitTimestamp) {
    return new CDCDataFile(
        /* addFile= */ null,
        removeFile,
        /* addCDCFile= */ null,
        CDCReader.CDC_TYPE_DELETE_STRING(),
        commitTimestamp,
        /* dvBase64Override= */ null);
  }

  /** Create a CDCDataFile for an explicit AddCDCFile action. */
  public static CDCDataFile fromAddCDCFile(AddCDCFile addCDCFile, long commitTimestamp) {
    return new CDCDataFile(
        /* addFile= */ null,
        /* removeFile= */ null,
        addCDCFile,
        /* changeType= */ null,
        commitTimestamp,
        /* dvBase64Override= */ null);
  }

  /**
   * Create a CDCDataFile for a DV-diff same-path pair: an Add+Remove on the same parquet file where
   * the DV bitmap diff identifies the changed rows.
   */
  public static CDCDataFile fromDVDiff(
      AddFile addFile, String changeType, long commitTimestamp, String dvBase64) {
    return new CDCDataFile(
        addFile,
        /* removeFile= */ null,
        /* addCDCFile= */ null,
        changeType,
        commitTimestamp,
        dvBase64);
  }

  @Nullable
  public AddFile getAddFile() {
    return addFile;
  }

  @Nullable
  public RemoveFile getRemoveFile() {
    return removeFile;
  }

  @Nullable
  AddCDCFile getAddCDCFile() {
    return addCDCFile;
  }

  /** Returns the file path for any CDC file variant. */
  public String getPath() {
    if (addFile != null) {
      return addFile.path();
    } else if (removeFile != null) {
      return removeFile.path();
    } else {
      return addCDCFile.path();
    }
  }

  /**
   * Returns the partition values for any CDC file variant. May be null for RemoveFile when
   * extendedFileMetadata is not present.
   */
  @Nullable
  public Map<String, String> getPartitionValues() {
    if (addFile != null) return addFile.partitionValues();
    if (removeFile != null) return removeFile.partitionValues();
    return addCDCFile.partitionValues();
  }

  /**
   * Returns the DV bytes to use for filtering when reading this CDC file. For DV-diff entries, this
   * is the override bitmap (the diff between the Add and Remove DVs). For regular CDC inferred from
   * an AddFile/RemoveFile, this is the underlying file's DV if any. Empty if the file has no DV at
   * all.
   */
  public Optional<String> getEffectiveDv() {
    if (dvBase64Override != null) {
      return Optional.of(dvBase64Override);
    }
    if (addFile != null && addFile.deletionVector() != null) {
      return Optional.of(addFile.deletionVector().serializeToBase64());
    }
    if (removeFile != null && removeFile.deletionVector() != null) {
      return Optional.of(removeFile.deletionVector().serializeToBase64());
    }
    return Optional.empty();
  }

  /**
   * Returns the filter polarity to apply with {@link #getEffectiveDv()}. DV-diff files use
   * IF_NOT_CONTAINED (keep rows marked in the bitmap); all other files use IF_CONTAINED (skip rows
   * marked in the bitmap).
   */
  public RowIndexFilterType getDvFilterType() {
    return dvBase64Override != null
        ? RowIndexFilterType.IF_NOT_CONTAINED
        : RowIndexFilterType.IF_CONTAINED;
  }

  @Nullable
  public String getChangeType() {
    return changeType;
  }

  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  public long getFileSize() {
    if (addFile != null) return addFile.getFileSize();
    if (removeFile != null) return removeFile.getFileSize();
    return addCDCFile.getFileSize();
  }

  /** Returns true if this is an explicit AddCDCFile action. */
  public boolean isAddCDCFile() {
    return addCDCFile != null;
  }

  public boolean hasDeletionVector() {
    return dvBase64Override != null
        || (addFile != null && addFile.deletionVector() != null)
        || (removeFile != null && removeFile.deletionVector() != null);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CDCDataFile{");
    if (addFile != null) {
      sb.append("addFile=").append(addFile);
    } else if (removeFile != null) {
      sb.append("removeFile=").append(removeFile);
    } else {
      sb.append("explicit=true, path=").append(addCDCFile.path());
    }
    if (changeType != null) {
      sb.append(", changeType='").append(changeType).append("'");
    }
    sb.append(", commitTimestamp=").append(commitTimestamp);
    if (dvBase64Override != null) {
      sb.append(", dvDiff=true");
    }
    sb.append("}");
    return sb.toString();
  }
}
