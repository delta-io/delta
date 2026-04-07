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

import io.delta.kernel.internal.actions.AddFile;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;

/** Wrapper for inferred CDF files. */
public class CDCDataFile {
  private final AddFile addFile;
  private final String changeType;
  private final long commitTimestamp;

  /** Creates a CDC file representing an insert (initial snapshot or new AddFile). */
  public static CDCDataFile fromAddFile(AddFile addFile, long commitTimestamp) {
    return new CDCDataFile(addFile, CDCReader.CDC_TYPE_INSERT(), commitTimestamp);
  }

  private CDCDataFile(AddFile addFile, String changeType, long commitTimestamp) {
    this.addFile = addFile;
    this.changeType = changeType;
    this.commitTimestamp = commitTimestamp;
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

  public long getFileSize() {
    return addFile.getSize();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CDCDataFile{");
    sb.append("addFile=").append(addFile);
    sb.append(", changeType='").append(changeType).append("'");
    sb.append(", commitTimestamp=").append(commitTimestamp);
    sb.append("}");
    return sb.toString();
  }
}
