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
import javax.annotation.Nullable;
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
  @Nullable private final AddFile addFile;
  @Nullable private final CDCDataFile cdcFile;

  /** Creates a sentinel IndexedFile (no file action) for offset tracking boundaries. */
  public static IndexedFile sentinel(long version, long index) {
    return new IndexedFile(version, index, /* addFile= */ null, /* cdcFile= */ null);
  }

  /** Creates a CDC IndexedFile wrapping a CDCDataFile. */
  public static IndexedFile cdc(long version, long index, CDCDataFile cdcFile) {
    return new IndexedFile(version, index, /* addFile= */ null, cdcFile);
  }

  /** Creates an IndexedFile for a non-CDC AddFile action. */
  public static IndexedFile addFile(long version, long index, AddFile addFile) {
    return new IndexedFile(version, index, addFile, /* cdcFile= */ null);
  }

  private IndexedFile(long version, long index, AddFile addFile, CDCDataFile cdcFile) {
    checkState(addFile == null || cdcFile == null, "At most one of addFile, cdcFile can be set");
    this.version = version;
    this.index = index;
    this.addFile = addFile;
    this.cdcFile = cdcFile;
  }

  public long getVersion() {
    return version;
  }

  public long getIndex() {
    return index;
  }

  @Nullable
  public AddFile getAddFile() {
    return addFile;
  }

  @Nullable
  public CDCDataFile getCDCFile() {
    return cdcFile;
  }

  @Override
  public boolean hasFileAction() {
    return addFile != null || cdcFile != null;
  }

  @Override
  public long getFileSize() {
    if (addFile != null) {
      return addFile.getSize();
    } else if (cdcFile != null) {
      return cdcFile.getFileSize();
    }
    throw new IllegalStateException("check hasFileAction() before calling getFileSize()");
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
    if (cdcFile != null) {
      sb.append(", cdcFile=").append(cdcFile);
    }
    sb.append('}');
    return sb.toString();
  }
}
