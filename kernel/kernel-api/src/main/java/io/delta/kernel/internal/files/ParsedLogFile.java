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

import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;

public class ParsedLogFile {

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
    // STAGED_DELTA(ParsedLogCategory.DELTA),
    LOG_COMPACTION(ParsedLogCategory.LOG_COMPACTION),
    CHECKPOINT_CLASSIC(ParsedLogCategory.CHECKPOINT),
    MULTIPART_CHECKPOINT(ParsedLogCategory.CHECKPOINT),
    V2_CHECKPOINT_MANIFEST(ParsedLogCategory.CHECKPOINT),
    CHECKSUM(ParsedLogCategory.CHECKSUM);

    private final ParsedLogCategory category;

    ParsedLogType(ParsedLogCategory category) {
      this.category = category;
    }

    public ParsedLogCategory getCategory() {
      return category;
    }
  }

  public static ParsedLogFile forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isCommitFile(path)) {
      return new ParsedLogFile(
          fileStatus, FileNames.deltaVersion(path), ParsedLogType.PUBLISHED_DELTA);
    } else if (FileNames.isLogCompactionFile(path)) {
      return ParsedCompactionFile.parsedCompactionFile(fileStatus);
    } else if (FileNames.isCheckpointFile(path)) {
      return ParsedCheckpointFile.parsedCheckpointFile(fileStatus);
    } else if (FileNames.isChecksumFile(path)) {
      return new ParsedCheckpointFile(
          fileStatus, FileNames.checksumVersion(path), ParsedLogType.CHECKSUM);
    } else {
      throw new IllegalArgumentException("File is not a recognized delta log type: " + path);
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final FileStatus fileStatus;
  private final long version;
  private final ParsedLogType type;

  protected ParsedLogFile(FileStatus fileStatus, long version, ParsedLogType type) {
    this.fileStatus = fileStatus;
    this.version = version;
    this.type = type;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getVersion() {
    return version;
  }

  public ParsedLogType getType() {
    return type;
  }

  public ParsedLogCategory getCategory() {
    return type.getCategory();
  }
}
