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

public class ParsedCheckpointFile extends ParsedLogFile {

  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public static ParsedCheckpointFile parsedCheckpointFile(FileStatus fileStatus) {
    if (!FileNames.isCheckpointFile(fileStatus.getPath())) {
      throw new IllegalArgumentException(
          "File is not a recognized checkpoint file: " + fileStatus.getPath());
    }

    if (FileNames.isMultiPartCheckpointFile(fileStatus.getPath())) {
      return ParsedMultiPartCheckpointFile.parsedMultiPartCheckpointFile(fileStatus);
    }

    final long version = FileNames.checkpointVersion(fileStatus.getPath());
    final ParsedLogType type;
    if (FileNames.isV2CheckpointFile(fileStatus.getPath())) {
      type = ParsedLogType.V2_CHECKPOINT_MANIFEST;
    } else if (FileNames.isClassicCheckpointFile(fileStatus.getPath())) {
      type = ParsedLogType.CHECKPOINT_CLASSIC;
    } else {
      throw new IllegalArgumentException(
          "File is not a recognized checkpoint file: " + fileStatus.getPath());
    }

    return new ParsedCheckpointFile(fileStatus, version, type);
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  protected ParsedCheckpointFile(FileStatus fileStatus, long version, ParsedLogType type) {
    super(fileStatus, version, type);
  }

  public boolean mayContainSidecarFiles() {
    return getType() == ParsedLogType.CHECKPOINT_CLASSIC
        || getType() == ParsedLogType.V2_CHECKPOINT_MANIFEST;
  }
}
