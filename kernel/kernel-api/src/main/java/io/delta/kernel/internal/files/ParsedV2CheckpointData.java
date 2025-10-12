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

/**
 * V2 checkpoint with UUID-based naming.
 *
 * <p>Example: {@code 00000000000000000001.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json}
 */
public final class ParsedV2CheckpointData extends ParsedCheckpointData {

  public static ParsedV2CheckpointData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isV2CheckpointFile(fileStatus.getPath()),
        "Expected a V2 checkpoint file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.checkpointVersion(path);
    return new ParsedV2CheckpointData(version, Optional.of(fileStatus), Optional.empty());
  }

  private ParsedV2CheckpointData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  protected CheckpointTypePriority getCheckpointTypePriority() {
    return CheckpointTypePriority.V2;
  }

  @Override
  protected int compareToSameType(ParsedCheckpointData that) {
    return compareByDataSource(that);
  }
}
