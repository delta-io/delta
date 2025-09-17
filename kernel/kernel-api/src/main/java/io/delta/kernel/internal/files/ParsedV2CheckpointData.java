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

public class ParsedV2CheckpointData extends ParsedCheckpointData {

  public static ParsedV2CheckpointData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isV2CheckpointFile(fileStatus.getPath()),
        "Expected a V2 checkpoint file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.checkpointVersion(path);
    return new ParsedV2CheckpointData(version, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedV2CheckpointData forInlineData(long version, ColumnarBatch inlineData) {
    return new ParsedV2CheckpointData(version, Optional.empty(), Optional.of(inlineData));
  }

  private ParsedV2CheckpointData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  protected int getCheckpointTypePriority() {
    return 2; // Classic (0) < MultiPart (1) < V2 (2). V2 has the highest priority.
  }
}
