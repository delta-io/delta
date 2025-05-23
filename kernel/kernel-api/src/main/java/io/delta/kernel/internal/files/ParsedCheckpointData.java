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
import java.util.Optional;

public class ParsedCheckpointData extends ParsedLogData {
  public static ParsedCheckpointData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isMultiPartCheckpointFile(path)) {
      return ParsedMultiPartCheckpointData.forFileStatus(fileStatus);
    }

    final long version;
    final ParsedLogType type;

    if (FileNames.isClassicCheckpointFile(path)) {
      version = FileNames.checkpointVersion(path);
      type = ParsedLogType.CLASSIC_CHECKPOINT;
    } else if (FileNames.isV2CheckpointFile(path)) {
      version = FileNames.checkpointVersion(path);
      type = ParsedLogType.V2_CHECKPOINT;
    } else {
      throw new IllegalArgumentException("File is not a recognized checkpoint type: " + path);
    }

    return new ParsedCheckpointData(version, type, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedCheckpointData forInlineData(
      long version, ParsedLogType type, ColumnarBatch inlineData) {
    if (type == ParsedLogType.MULTIPART_CHECKPOINT) {
      throw new IllegalArgumentException(
          "For MULTIPART_CHECKPOINT, use ParsedMultiPartCheckpointData.forInlineData() instead");
    }

    return new ParsedCheckpointData(version, type, Optional.empty(), Optional.of(inlineData));
  }

  protected ParsedCheckpointData(
      long version,
      ParsedLogType type,
      Optional<FileStatus> fileStatusOpt,
      Optional<ColumnarBatch> inlineDataOpt) {
    super(version, type, fileStatusOpt, inlineDataOpt);
    checkArgument(type.category == ParsedLogCategory.CHECKPOINT, "Must be a checkpoint");
  }
}
